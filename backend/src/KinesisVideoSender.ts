import {
	GetDataEndpointCommand,
	KinesisVideoClient,
} from '@aws-sdk/client-kinesis-video'
import { fromNodeProviderChain } from '@aws-sdk/credential-providers'
import aws4 from 'aws4'
import https from 'node:https'
import type { Readable } from 'node:stream'
import { Logger } from './Logger.ts'

export type KinesisVideoSenderConfig = {
	streamName: string
	region: string
}

export type PutMediaAck = {
	AckEventType?: 'Buffering' | 'Received' | 'Persisted' | 'Error' | 'Idle'
	FragmentTimecode?: number
	FragmentNumber?: string
	ErrorId?: number
	ErrorCode?: string
}

/**
 * Sends MKV media to a Kinesis Video Stream via the PutMedia API.
 * Uses GetDataEndpoint for the data-plane endpoint and signs the request with SigV4.
 */
/** Throttle repeated Error ACKs (same fragment) to avoid log spam. */
const ERROR_ACK_THROTTLE_MS = 60_000

export class KinesisVideoSender {
	private readonly config: KinesisVideoSenderConfig
	private readonly logger: Logger
	private readonly client: KinesisVideoClient
	private dataEndpointUrl: string | null = null
	private lastErrorAckKey: string | null = null
	private lastErrorAckTime = 0
	private putMediaConnectionLogged = false
	private firstFragmentAckLogged = false

	constructor(config: KinesisVideoSenderConfig) {
		this.config = config
		this.logger = new Logger('KinesisVideoSender')
		this.client = new KinesisVideoClient({ region: config.region })
	}

	/**
	 * Resolves the PutMedia data endpoint for the stream (cached until refreshed).
	 */
	async getPutMediaEndpoint(): Promise<string> {
		if (
			this.dataEndpointUrl !== undefined &&
			this.dataEndpointUrl !== null &&
			this.dataEndpointUrl !== ''
		) {
			return this.dataEndpointUrl
		}
		const response = await this.client.send(
			new GetDataEndpointCommand({
				StreamName: this.config.streamName,
				APIName: 'PUT_MEDIA',
			}),
		)
		const endpoint = response.DataEndpoint
		if (endpoint === undefined || endpoint === null || endpoint === '') {
			throw new Error('GetDataEndpoint did not return DataEndpoint')
		}
		// Endpoint does not include path; we append /putMedia
		this.dataEndpointUrl = endpoint.endsWith('/')
			? `${endpoint}putMedia`
			: `${endpoint}/putMedia`
		return this.dataEndpointUrl
	}

	/**
	 * Invalidates cached endpoint (e.g. after InvalidEndpointException).
	 */
	invalidateEndpoint(): void {
		this.dataEndpointUrl = null
	}

	/**
	 * Sends MKV stream to PutMedia and parses ACK lines from the response.
	 * Resolves when the request finishes or rejects on fatal errors.
	 */
	async putMedia(mkvStream: Readable): Promise<void> {
		const url = await this.getPutMediaEndpoint()
		const parsed = new URL(url)
		const producerStartTimestamp = Date.now()

		// Resolve credentials from default chain (env, IMDS on EC2, etc.); aws4 only reads env
		const credentialProvider = fromNodeProviderChain()
		const credentials = await credentialProvider()

		const headers: Record<string, string> = {
			'x-amzn-stream-name': this.config.streamName,
			'x-amzn-producer-start-timestamp': String(producerStartTimestamp),
			'x-amzn-fragment-timecode-type': 'RELATIVE',
			'Transfer-Encoding': 'chunked',
			'X-Amz-Content-Sha256': 'UNSIGNED-PAYLOAD',
		}

		const opts: aws4.Request = {
			host: parsed.hostname,
			path: parsed.pathname + parsed.search,
			method: 'POST',
			headers,
			body: '', // Sign with empty body; we stream chunked
			service: 'kinesisvideo',
			region: this.config.region,
			extraHeadersToIgnore: { 'content-length': true },
		}

		const signCreds: {
			accessKeyId: string
			secretAccessKey: string
			sessionToken?: string
		} = {
			accessKeyId: credentials.accessKeyId,
			secretAccessKey: credentials.secretAccessKey,
		}
		if (credentials.sessionToken != null && credentials.sessionToken !== '') {
			signCreds.sessionToken = credentials.sessionToken
		}
		aws4.sign(opts, signCreds)

		return new Promise((resolve, reject) => {
			const req = https.request(
				{
					hostname: opts.host,
					port: 443,
					path: opts.path,
					method: 'POST',
					headers: opts.headers as Record<string, string>,
				},
				(res) => {
					if (res.statusCode !== 200) {
						const chunks: Buffer[] = []
						res.on('data', (chunk: Buffer) => chunks.push(chunk))
						res.on('end', () => {
							const body = Buffer.concat(chunks).toString('utf8')
							const errorType = res.headers['x-amz-errortype'] as
								| string
								| undefined
							this.logger.error(
								'PutMedia HTTP error',
								new Error(
									`PutMedia failed: ${res.statusCode} ${res.statusMessage}`,
								),
								{
									streamName: this.config.streamName,
									statusCode: res.statusCode,
									errorType: errorType ?? '(none)',
									body: body.slice(0, 500),
								},
							)
							reject(
								new Error(
									`PutMedia failed: ${res.statusCode} ${res.statusMessage}${errorType !== undefined ? ` (${errorType})` : ''}${body ? ` - ${body.slice(0, 200)}` : ''}`,
								),
							)
						})
						return
					}
					if (!this.putMediaConnectionLogged) {
						this.putMediaConnectionLogged = true
						this.logger.info('PutMedia connection established', {
							streamName: this.config.streamName,
						})
					}
					let buffer = ''
					res.on('data', (chunk: Buffer) => {
						buffer += chunk.toString('utf8')
						const lines = buffer.split('\n')
						buffer = lines.pop() ?? ''
						for (const line of lines) {
							const trimmed = line.trim()
							if (!trimmed) continue
							try {
								const ack = JSON.parse(trimmed) as PutMediaAck
								this.handleAck(ack)
							} catch {
								// Ignore non-JSON lines
							}
						}
					})
					res.on('end', () => resolve())
					res.on('error', reject)
				},
			)
			req.on('error', (err) => {
				this.logger.error('PutMedia request error', err, {
					streamName: this.config.streamName,
				})
				reject(err)
			})
			mkvStream.pipe(req, { end: true })
			mkvStream.on('error', (err) => {
				req.destroy(err)
				reject(err)
			})
		})
	}

	private handleAck(ack: PutMediaAck): void {
		if (ack.AckEventType === 'Error') {
			const key = `${ack.ErrorCode ?? ''}:${ack.FragmentTimecode ?? ''}:${ack.FragmentNumber ?? ''}`
			const now = Date.now()
			const isRepeat =
				this.lastErrorAckKey === key &&
				now - this.lastErrorAckTime < ERROR_ACK_THROTTLE_MS
			if (!isRepeat) {
				this.lastErrorAckKey = key
				this.lastErrorAckTime = now
				this.logger.warn('PutMedia Error ACK', {
					streamName: this.config.streamName,
					ErrorId: ack.ErrorId,
					ErrorCode: ack.ErrorCode,
					FragmentTimecode: ack.FragmentTimecode,
				})
			}
			if (ack.ErrorCode === 'InvalidEndpointException') {
				this.invalidateEndpoint()
			}
		} else if (
			ack.AckEventType === 'Persisted' ||
			ack.AckEventType === 'Idle'
		) {
			this.logger.info('PutMedia ACK', {
				streamName: this.config.streamName,
				AckEventType: ack.AckEventType,
				FragmentTimecode: ack.FragmentTimecode,
				FragmentNumber: ack.FragmentNumber,
			})
		}
		// Log first Buffering/Received so we know fragments are flowing
		if (
			!this.firstFragmentAckLogged &&
			(ack.AckEventType === 'Buffering' || ack.AckEventType === 'Received')
		) {
			this.firstFragmentAckLogged = true
			this.logger.info('PutMedia first fragment ACK', {
				streamName: this.config.streamName,
				AckEventType: ack.AckEventType,
			})
		}
	}
}
