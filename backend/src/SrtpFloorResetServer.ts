import { createHmac, timingSafeEqual } from 'node:crypto'
import http from 'node:http'
import { Logger } from './Logger.ts'
import type { SrtpPortKey } from './SrtpKeyStore.ts'

/** Where the reset API listens; the load balancer forwards the same port. */
export const SRTP_FLOOR_RESET_PORT = 8080

/** How far a request's timestamp may be from this clock, either way. */
export const MAX_CLOCK_SKEW_MS = 60_000

/** The packet index is 48 bits: a 32-bit rollover counter and a 16-bit sequence number. */
const MAX_FLOOR = 2 ** 48 - 1

const MAX_BODY_BYTES = 1024

/** Separates this use of a key from every other, so a signature is good for nothing else. */
const SIGNATURE_CONTEXT = 'video.thingy.rocks/srtp-floor-reset/v1'

/**
 * Signs a floor reset for the client to send.
 *
 * The signature is an HMAC under a key derived from the port's SRTP key, so holding
 * the key is the credential - there is no other secret to provision, and the key
 * itself is never sent or used raw. It covers the port, the timestamp and the floor,
 * so none of the three can be altered; the timestamp makes the request single-use
 * (see StreamMetadataService.resetSrtpIndexFloor).
 */
export const signFloorReset = (
	keyHex: string,
	port: number,
	timestampMs: number,
	floor: number,
): string => {
	const derived = createHmac('sha256', Buffer.from(keyHex, 'hex'))
		.update(SIGNATURE_CONTEXT)
		.digest()
	return createHmac('sha256', derived)
		.update(`${port}\n${timestampMs}\n${floor}`)
		.digest('hex')
}

/** What the server needs from the transport; structural so tests can fake it. */
export type FloorResetTarget = {
	keyForPort(port: number): SrtpPortKey | undefined
	/** 'stale' when the request was replayed or is older than a reset already made. */
	resetFloor(
		port: number,
		reset: { requestedAtMs: number; floor: number },
	): Promise<'reset' | 'stale'>
}

const PATH = /^\/srtp\/(600[0-9])\/floor-reset$/

const respond = (
	res: http.ServerResponse,
	status: number,
	body: Record<string, unknown>,
): void => {
	res.writeHead(status, { 'content-type': 'application/json' })
	res.end(JSON.stringify(body))
}

const readBody = async (req: http.IncomingMessage): Promise<string | null> => {
	const chunks: Buffer[] = []
	let size = 0
	for await (const chunk of req) {
		size += (chunk as Buffer).length
		if (size > MAX_BODY_BYTES) return null
		chunks.push(chunk as Buffer)
	}
	return Buffer.concat(chunks).toString('utf8')
}

/**
 * `POST /srtp/{port}/floor-reset` - lets the holder of a port's SRTP key reset that
 * port's replay floor, so a sender that restarts its packet numbering is not dropped
 * as a replay of its own earlier session.
 *
 *     x-timestamp: <unix ms>
 *     x-signature: <signFloorReset(key, port, timestamp, floor)>
 *     {"floor": <packet index>}           (required)
 *
 * Not a handshake and not TLS: the request carries nothing secret, and what it can
 * do is bounded by the signature - a request is valid once, for one port, for about
 * a minute. Every failure to authenticate looks the same, an unkeyed port included.
 */
export class SrtpFloorResetServer {
	private server?: http.Server
	private readonly logger: Logger

	constructor(
		private readonly target: FloorResetTarget,
		logger?: Logger,
		private readonly now: () => number = Date.now,
	) {
		this.logger = logger ?? new Logger('SrtpFloorResetServer')
	}

	async start(listenPort: number = SRTP_FLOOR_RESET_PORT): Promise<number> {
		const server = http.createServer((req, res) => {
			this.handle(req, res).catch((err: unknown) => {
				this.logger.error(
					'Floor reset failed',
					err instanceof Error ? err : new Error(String(err)),
				)
				if (!res.headersSent) respond(res, 503, { error: 'unavailable' })
				else res.end()
			})
		})
		server.headersTimeout = 5_000
		server.requestTimeout = 10_000
		server.keepAliveTimeout = 1_000
		this.server = server
		return new Promise((resolve, reject) => {
			server.once('error', reject)
			server.listen(listenPort, '::', () => {
				const address = server.address()
				resolve(
					typeof address === 'object' && address ? address.port : listenPort,
				)
			})
		})
	}

	async stop(): Promise<void> {
		const server = this.server
		if (server === undefined) return
		this.server = undefined
		return new Promise((resolve) => {
			server.close(() => resolve())
			server.closeAllConnections()
		})
	}

	private async handle(
		req: http.IncomingMessage,
		res: http.ServerResponse,
	): Promise<void> {
		const match = PATH.exec(req.url ?? '')
		if (match === null) return respond(res, 404, { error: 'not found' })
		if (req.method !== 'POST') return respond(res, 405, { error: 'POST only' })
		const port = Number(match[1])

		const raw = await readBody(req)
		if (raw === null) return respond(res, 413, { error: 'body too large' })
		let parsed: unknown
		try {
			parsed = JSON.parse(raw)
		} catch {
			return respond(res, 400, { error: 'body must be JSON' })
		}
		const floor =
			typeof parsed === 'object' && parsed !== null
				? (parsed as Record<string, unknown>).floor
				: undefined
		if (
			typeof floor !== 'number' ||
			!Number.isSafeInteger(floor) ||
			floor < 0 ||
			floor > MAX_FLOOR
		) {
			return respond(res, 400, {
				error: `floor is required: an integer from 0 to ${MAX_FLOOR}`,
			})
		}

		const timestampHeader = req.headers['x-timestamp']
		const signature = req.headers['x-signature']
		const timestamp =
			typeof timestampHeader === 'string' &&
			/^[0-9]{1,16}$/.test(timestampHeader)
				? Number(timestampHeader)
				: undefined
		const key = this.target.keyForPort(port)
		if (
			key === undefined ||
			timestamp === undefined ||
			typeof signature !== 'string' ||
			Math.abs(this.now() - timestamp) > MAX_CLOCK_SKEW_MS ||
			!signatureMatches(
				signFloorReset(key.keyHex, port, timestamp, floor),
				signature,
			)
		) {
			return respond(res, 401, { error: 'unauthorized' })
		}

		const outcome = await this.target.resetFloor(port, {
			requestedAtMs: timestamp,
			floor,
		})
		if (outcome === 'stale') {
			return respond(res, 409, {
				error: 'stale',
				message: 'a reset with a newer timestamp has already been made',
			})
		}
		this.logger.info('SRTP floor reset', { port, floor })
		respond(res, 200, { port, floor })
	}
}

const signatureMatches = (expected: string, given: string): boolean => {
	const a = Buffer.from(expected)
	const b = Buffer.from(given)
	return a.length === b.length && timingSafeEqual(a, b)
}
