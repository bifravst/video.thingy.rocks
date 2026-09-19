import { spawn, spawnSync } from 'node:child_process'
import dgram from 'node:dgram'
import { EventEmitter } from 'node:events'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import type { Writable } from 'node:stream'

import { fromNodeProviderChain } from '@aws-sdk/credential-providers'
import { Logger } from './Logger.ts'
import type { SrtpKeyStore, SrtpPortKey } from './SrtpKeyStore.ts'

export type KinesisIngestionPipelineConfig = {
	streamNamePrefix: string
	region: string
	portRange: { start: number; end: number }
	/**
	 * Max packets to buffer before writing to GStreamer stdin (0 = write immediately).
	 * Packets are emitted in receive order; does not fix network reordering.
	 * Default 128. Helps smooth bursts and applies backpressure.
	 * Only applies to the FIFO/MPEG-TS transport (ports in `portRange`).
	 */
	reorderBufferSize?: number
	/**
	 * When true, log GStreamer/KVS stdout and stderr. Disabled by default to avoid noisy logs.
	 */
	logGstreamerOutput?: boolean
	/**
	 * Optional SRTP ingestion: a separate port range whose packets are RTP/H.264 wrapped in
	 * SRTP (RFC 6184 + static pre-shared key), relayed to a loopback udpsrc, decrypted by
	 * srtpdec, depayloaded, and sent to Kinesis the same way as the FIFO/MPEG-TS path.
	 */
	srtp?: {
		portRange: { start: number; end: number }
		streamNamePrefix: string
		/** Local relay port = publicPort + relayPortOffset. Default 10000. */
		relayPortOffset?: number
		/** rtpjitterbuffer latency in ms. Default 200. */
		jitterBufferLatencyMs?: number
		keyStore: SrtpKeyStore
	}
}

const DEFAULT_REORDER_BUFFER_SIZE = 128
const DEFAULT_SRTP_RELAY_PORT_OFFSET = 10_000
const DEFAULT_SRTP_JITTER_BUFFER_LATENCY_MS = 200
/** Grace period after spawning gst-launch-1.0 before replaying pre-buffered SRTP datagrams;
 * unlike the FIFO path (whose fs.open() blocks until filesrc opens for read), there is no
 * OS-level handshake for udpsrc binding a port, so we just wait a bit. A few early datagrams
 * lost here self-heal at the next H.264 keyframe, the same tolerance the system already has
 * for ordinary network loss. */
const SRTP_STARTUP_GRACE_MS = 300

type ReorderState = {
	nextSeq: number
	nextToEmit: number
	buffer: Map<number, Buffer>
}

/** Per-port throttle for GStreamer stderr warning categories. */
type GstStderrThrottle = { lastLog: Record<string, number> }

/** Per-port throttle for noisy GStreamer/KVS stdout lines (CONTINUITY, 0x30000005, etc.). */
type GstStdoutThrottle = { lastLog: Record<string, number> }

type FifoPortPipeline = {
	transport: 'fifo'
	gst: ReturnType<typeof spawn>
	/** Stream we write TS to (stdin pipe or FIFO). */
	inputStream: Writable
	reorder: ReorderState
	gstStderrThrottle: GstStderrThrottle
	gstStdoutThrottle: GstStdoutThrottle
	/** FIFO path when using filesrc; unlink on stop. */
	fifoPath: string
}

type SrtpRelayPortPipeline = {
	transport: 'srtp-relay'
	gst: ReturnType<typeof spawn>
	/** Node-side socket used to relay each UDP datagram, unmodified, to GStreamer's udpsrc. */
	relaySocket: dgram.Socket
	relayPort: number
	gstStderrThrottle: GstStderrThrottle
	gstStdoutThrottle: GstStdoutThrottle
}

type PortPipeline = FifoPortPipeline | SrtpRelayPortPipeline

/** Throttle repeated GStreamer stderr warnings (same category) per port. */
const GST_STDERR_THROTTLE_MS = 60_000
/** Throttle noisy stdout (CONTINUITY, KVS 0x30000005, "Could not write to resource") per port. */
const GST_STDOUT_THROTTLE_MS = 60_000

type ResolvedCredentials = {
	accessKeyId: string
	secretAccessKey: string
	sessionToken?: string
}

/**
 * Per-port pipeline: UDP packets -> GStreamer (TS -> H.264, or SRTP -> RTP -> H.264) -> kvssink
 * -> Kinesis Video. One GStreamer process per active port; kvssink sends directly to Kinesis
 * (no Node PutMedia).
 */
export class KinesisIngestionPipeline extends EventEmitter {
	private readonly config: KinesisIngestionPipelineConfig
	private readonly logger: Logger
	private readonly activePipelines: Map<number, PortPipeline> = new Map()
	/** Dedupe concurrent start(port) so only one credential fetch + spawn runs per port. */
	private readonly pendingStarts: Map<number, Promise<void>> = new Map()

	constructor(config: KinesisIngestionPipelineConfig) {
		super()
		this.config = config
		this.logger = new Logger('KinesisIngestionPipeline')
	}

	isSrtpPort(port: number): boolean {
		const srtp = this.config.srtp
		if (!srtp) return false
		return port >= srtp.portRange.start && port <= srtp.portRange.end
	}

	streamNameForPort(port: number): string {
		if (this.isSrtpPort(port)) {
			return `${this.config.srtp!.streamNamePrefix}-${port}`
		}
		return `${this.config.streamNamePrefix}-${port}`
	}

	isPortInRange(port: number): boolean {
		const inMainRange =
			port >= this.config.portRange.start && port <= this.config.portRange.end
		return inMainRange || this.isSrtpPort(port)
	}

	/**
	 * Logs GStreamer stderr; throttles repeated warnings per category.
	 */
	private logGstStderr(
		port: number,
		streamName: string,
		text: string,
		throttle: GstStderrThrottle,
	): void {
		if (this.config.logGstreamerOutput !== true) return
		const now = Date.now()
		const lines = text.split(/\r?\n/)
		for (const raw of lines) {
			const line = raw.trim()
			if (line.length === 0) continue

			let category: string | null = null
			if (/authentication failure|auth.*fail/i.test(line))
				category = 'srtp_auth'
			else if (/unknown ssrc|no matching key/i.test(line))
				category = 'srtp_ssrc_mismatch'
			else if (/error|ERROR/i.test(line)) category = 'error'
			else if (/warning|WARN/i.test(line)) category = 'warning'

			if (category !== null) {
				const last = throttle.lastLog[category] ?? 0
				if (now - last < GST_STDERR_THROTTLE_MS) continue
				throttle.lastLog[category] = now
			}

			this.logger.warn('GStreamer stderr', {
				port,
				streamName,
				message: line.slice(0, 500),
			})
		}
	}

	/**
	 * Logs GStreamer stdout; throttles noisy lines (CONTINUITY, KVS 0x30000005, "Could not write to resource").
	 */
	private logGstStdout(
		port: number,
		streamName: string,
		text: string,
		throttle: GstStdoutThrottle,
	): void {
		if (this.config.logGstreamerOutput !== true) return
		const now = Date.now()
		const lines = text.split(/\r?\n/)
		for (const raw of lines) {
			const line = raw.trim()
			if (line.length === 0) continue
			if (/streamLatencyPressure/i.test(line)) continue
			if (/droppedFrame callback/i.test(line)) continue
			if (/viewItemRemoved.*Reporting a dropped frame\/fragment/i.test(line))
				continue
			if (/Failed to submit ACK|0x52000047|status code: 0x52/i.test(line))
				continue

			let category: string | null = null
			if (/CONTINUITY:\s*Mismatch/i.test(line)) category = 'continuity'
			else if (
				/0x30000005|putKinesisVideoFrame.*Failed|Put frame.*failed/i.test(line)
			)
				category = 'kvs_putframe'
			else if (/Could not write to resource/i.test(line))
				category = 'write_resource'

			if (category !== null) {
				const last = throttle.lastLog[category] ?? 0
				if (now - last < GST_STDOUT_THROTTLE_MS) continue
				throttle.lastLog[category] = now
			}

			this.logger.info('GStreamer stdout', {
				port,
				streamName,
				message: line.slice(0, 1000),
			})
		}
	}

	/**
	 * Resolves AWS credentials for kvssink (which does not share Node's credential chain)
	 * and returns an env object with them injected, or undefined on failure (logged).
	 */
	private async resolveGstEnv(
		port: number,
		streamName: string,
	): Promise<NodeJS.ProcessEnv | undefined> {
		const credentialProvider = fromNodeProviderChain({
			timeout: 10_000,
			maxRetries: 5,
		})
		const maxResolutionAttempts = 3
		const resolutionDelayMs = 2000
		let credentials: ResolvedCredentials | undefined
		try {
			for (let attempt = 1; attempt <= maxResolutionAttempts; attempt++) {
				try {
					credentials = await credentialProvider()
					break
				} catch (e) {
					if (attempt === maxResolutionAttempts) throw e
					this.logger.warn('Credentials not yet available, retrying', {
						port,
						streamName,
						attempt,
						nextAttemptInMs: resolutionDelayMs,
					})
					await new Promise((r) => setTimeout(r, resolutionDelayMs))
				}
			}
		} catch (err) {
			this.logger.error(
				'Failed to resolve AWS credentials for kvssink',
				err instanceof Error ? err : new Error(String(err)),
				{ port, streamName },
			)
			return undefined
		}

		const env = { ...process.env }
		env.AWS_ACCESS_KEY_ID = credentials!.accessKeyId
		env.AWS_SECRET_ACCESS_KEY = credentials!.secretAccessKey
		if (
			credentials!.sessionToken !== undefined &&
			credentials!.sessionToken !== ''
		) {
			env.AWS_SESSION_TOKEN = credentials!.sessionToken
		}
		env.AWS_REGION = this.config.region
		if (env.KINESIS_GST_PLUGIN_PATH !== undefined) {
			env.GST_PLUGIN_PATH = env.KINESIS_GST_PLUGIN_PATH
		}
		if (env.KINESIS_LD_LIBRARY_PATH !== undefined) {
			env.LD_LIBRARY_PATH = env.KINESIS_LD_LIBRARY_PATH
		}
		return env
	}

	private logConfigPath(): string {
		return (
			process.env.KVS_LOG_CONFIG_PATH ??
			'/opt/video-streaming/kvs_log_configuration'
		)
	}

	/**
	 * Starts the pipeline for a port: spawns GStreamer with kvssink.
	 * Idempotent: no-op if already running for this port.
	 * Concurrent calls for the same port are deduped (single credential fetch + spawn).
	 *
	 * For FIFO/MPEG-TS ports, `initialData` (if provided) is a single Buffer written to stdin
	 * immediately after spawn. For SRTP ports, pass an array of individually-received
	 * datagrams (never Buffer.concat them - that would destroy the per-packet framing SRTP
	 * decryption depends on); each is relayed to GStreamer as its own UDP datagram.
	 */
	async start(port: number, initialData?: Buffer | Buffer[]): Promise<void> {
		if (!this.isPortInRange(port)) return
		if (this.activePipelines.has(port)) return

		const existing = this.pendingStarts.get(port)
		if (existing !== undefined) {
			await existing
			return
		}

		const promise = this.runStartForPort(port, initialData).finally(() => {
			this.pendingStarts.delete(port)
		})
		this.pendingStarts.set(port, promise)
		await promise
	}

	private async runStartForPort(
		port: number,
		initialData?: Buffer | Buffer[],
	): Promise<void> {
		if (this.activePipelines.has(port)) return

		if (this.isSrtpPort(port)) {
			const initialDatagrams = Array.isArray(initialData)
				? initialData
				: initialData !== undefined
					? [initialData]
					: []
			return this.runStartForSrtpPort(port, initialDatagrams)
		}

		const initialBuffer = Array.isArray(initialData)
			? Buffer.concat(initialData)
			: initialData
		return this.runStartForFifoPort(port, initialBuffer)
	}

	/**
	 * Single-run start logic for a FIFO/MPEG-TS port (credentials + spawn). Call only via
	 * start() so dedupe applies. If initialData is provided, it is written to stdin
	 * immediately after spawn so fdsrc has data when it first reads.
	 */
	private async runStartForFifoPort(
		port: number,
		initialData?: Buffer,
	): Promise<void> {
		if (this.activePipelines.has(port)) return

		const streamName = this.streamNameForPort(port)
		const region = this.config.region

		const env = await this.resolveGstEnv(port, streamName)
		if (env === undefined) return

		const logConfigPath = this.logConfigPath()

		// Use a FIFO so GStreamer reads via filesrc (real path). filesrc blocks until we open for write, so data is ready when it reads; avoids fdsrc "not-linked" / stream error with pipes.
		const fifoPath = path.join(
			os.tmpdir(),
			`kinesis-${port}-${process.pid}-${Date.now()}.fifo`,
		)
		const mkfifo = spawnSync('mkfifo', [fifoPath], {
			encoding: 'utf8',
			timeout: 5000,
		})
		const failed =
			mkfifo.error != null || (mkfifo.status != null && mkfifo.status !== 0)
		if (failed) {
			const stderrStr = mkfifo.stderr != null ? String(mkfifo.stderr) : ''
			const msg =
				mkfifo.error?.message ??
				(stderrStr.trim() || `mkfifo exit code ${mkfifo.status ?? 'unknown'}`)
			this.logger.error(
				'Failed to create FIFO for GStreamer',
				mkfifo.error ?? new Error(msg),
				{
					port,
					streamName,
					fifoPath,
					exitCode: mkfifo.status ?? undefined,
					stderr: stderrStr.slice(0, 500) || undefined,
				},
			)
			return
		}

		// tsdemux creates pads like video_0_0c00 (template video_%01x_%05x), not "video_0". Use "d." to link to any pad so delayed linking succeeds regardless of PID.
		const pipelineStr = `filesrc location="${fifoPath}" ! capsfilter caps="video/mpegts,systemstream=(boolean)true" ! queue ! tsparse set-timestamps=true ! tsdemux name=d d. ! queue ! h264parse ! capsfilter caps="video/x-h264,stream-format=avc,alignment=au" ! kvssink stream-name="${streamName}" aws-region="${region}" storage-size=128 log-config="${logConfigPath}"`
		const shellCmd = `gst-launch-1.0 ${pipelineStr}`
		this.logger.info('GStreamer command', {
			port,
			streamName,
			pipelineStr,
			shellCmd,
		})
		const gst = spawn('sh', ['-c', shellCmd], {
			stdio: ['ignore', 'pipe', 'pipe'],
			env,
		})

		// Open FIFO for writing (blocks until GStreamer filesrc opens for read); then write initial data so pipeline has data when it starts.
		const inputStream = await new Promise<Writable>((resolve, reject) => {
			fs.open(fifoPath, 'w', (err, fd) => {
				if (err) {
					reject(err)
					return
				}
				const w = fs.createWriteStream('', { fd, autoClose: true })
				const data = initialData ?? Buffer.alloc(0)
				if (data.length > 0) {
					w.write(data, (e) => (e ? reject(e) : resolve(w)))
				} else {
					resolve(w)
				}
			})
		})
		inputStream.on('error', (err: NodeJS.ErrnoException) => {
			if (err.code !== 'EPIPE') {
				this.logger.warn('GStreamer FIFO write error', {
					port,
					streamName,
					code: err.code,
					message: err.message,
				})
			}
		})

		const gstStderrThrottle: GstStderrThrottle = { lastLog: {} }
		const gstStdoutThrottle: GstStdoutThrottle = { lastLog: {} }
		gst.stdout?.on('data', (data: Buffer) => {
			const text = data.toString()
			if (text.trim().length > 0) {
				this.logGstStdout(port, streamName, text, gstStdoutThrottle)
			}
		})
		gst.stderr?.on('data', (data: Buffer) => {
			this.logGstStderr(port, streamName, data.toString(), gstStderrThrottle)
		})
		gst.on('error', (err) => {
			this.logger.error('GStreamer error', err, { port, streamName })
			this.activePipelines.delete(port)
		})
		gst.on('exit', (code, signal) => {
			// Emit before delete: if port was in activePipelines, this was an unexpected exit
			// (intentional stop() removes from map before killing the process)
			const wasUnexpected = this.activePipelines.has(port)
			this.logger.info('GStreamer exited', {
				port,
				streamName,
				code: code ?? undefined,
				signal: signal ?? undefined,
				unexpected: wasUnexpected,
			})
			this.activePipelines.delete(port)
			if (wasUnexpected) {
				this.emit('pipelineExited', { port, code, signal })
			}
		})

		const reorderBufferSize =
			this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		const reorder: ReorderState = {
			nextSeq: 0,
			nextToEmit: 1,
			buffer: new Map(),
		}
		this.activePipelines.set(port, {
			transport: 'fifo',
			gst,
			inputStream,
			reorder,
			gstStderrThrottle,
			gstStdoutThrottle,
			fifoPath,
		})
		this.logger.info('Kinesis ingestion started', {
			port,
			streamName,
			reorderBufferSize,
		})
	}

	/**
	 * Builds the argv (not a shell string) for the SRTP -> RTP -> H264 -> kvssink pipeline.
	 * Spawned directly (no `sh -c`) because, unlike the FIFO path's interpolated values
	 * (stream name, region, paths), this pipeline interpolates key material - avoiding a
	 * shell entirely avoids any shell-metacharacter injection risk.
	 */
	private buildSrtpPipelineArgs(params: {
		relayPort: number
		streamName: string
		region: string
		logConfigPath: string
		key: SrtpPortKey
		jitterBufferLatencyMs: number
	}): string[] {
		const {
			relayPort,
			streamName,
			region,
			logConfigPath,
			key,
			jitterBufferLatencyMs,
		} = params

		// Verify caps field names/values (especially srtp-cipher/srtp-auth enum literals) against
		// the actual installed GStreamer version via `gst-inspect-1.0 srtpdec` - see docs/TESTING-SRTP-INGESTION.md.
		const caps = [
			'application/x-srtp',
			'media=(string)video',
			'payload=(int)96',
			'clock-rate=(int)90000',
			'encoding-name=(string)H264',
			`ssrc=(uint)${key.ssrc}`,
			`srtp-key=(buffer)${key.keyHex}`,
			`srtp-cipher=(string)${key.cipher}`,
			`srtp-auth=(string)${key.auth}`,
			`srtcp-cipher=(string)${key.cipher}`,
			`srtcp-auth=(string)${key.auth}`,
		].join(',')

		return [
			'udpsrc',
			`port=${relayPort}`,
			'address=127.0.0.1',
			`caps=${caps}`,
			'!',
			'srtpdec',
			'!',
			'rtpjitterbuffer',
			`latency=${jitterBufferLatencyMs}`,
			'!',
			'rtph264depay',
			'!',
			'h264parse',
			'config-interval=-1',
			'!',
			'capsfilter',
			'caps=video/x-h264,stream-format=avc,alignment=au',
			'!',
			'kvssink',
			`stream-name=${streamName}`,
			`aws-region=${region}`,
			'storage-size=128',
			`log-config=${logConfigPath}`,
		]
	}

	/** Redacts key material from an argv array before logging it. */
	private redactSrtpArgv(argv: string[]): string[] {
		return argv.map((token) =>
			/^caps=.*srtp-key=\(buffer\)/.test(token)
				? token.replace(
						/srtp-key=\(buffer\)[0-9a-fA-F]+/,
						'srtp-key=(buffer)***REDACTED***',
					)
				: token,
		)
	}

	/**
	 * Single-run start logic for an SRTP port (credentials + spawn). Call only via start() so
	 * dedupe applies. `initialDatagrams` are individually-received UDP datagrams buffered
	 * before this port's Kinesis lock was acquired; they are replayed (each as its own
	 * datagram, never concatenated) once GStreamer's udpsrc has had a moment to bind.
	 */
	private async runStartForSrtpPort(
		port: number,
		initialDatagrams: Buffer[],
	): Promise<void> {
		if (this.activePipelines.has(port)) return
		const srtpConfig = this.config.srtp
		if (!srtpConfig) return

		const streamName = this.streamNameForPort(port)
		const region = this.config.region

		const key = srtpConfig.keyStore.getKeyForPort(port)
		if (!key) {
			this.logger.error(
				'No SRTP key configured for port; refusing to start ingestion',
				new Error('Missing SRTP key'),
				{ port, streamName },
			)
			return
		}

		const env = await this.resolveGstEnv(port, streamName)
		if (env === undefined) return

		const relayPortOffset =
			srtpConfig.relayPortOffset ?? DEFAULT_SRTP_RELAY_PORT_OFFSET
		const relayPort = port + relayPortOffset
		const jitterBufferLatencyMs =
			srtpConfig.jitterBufferLatencyMs ?? DEFAULT_SRTP_JITTER_BUFFER_LATENCY_MS

		const argv = this.buildSrtpPipelineArgs({
			relayPort,
			streamName,
			region,
			logConfigPath: this.logConfigPath(),
			key,
			jitterBufferLatencyMs,
		})

		this.logger.info('GStreamer command (SRTP)', {
			port,
			streamName,
			relayPort,
			argv: this.redactSrtpArgv(argv),
		})

		const gst = spawn('gst-launch-1.0', argv, {
			stdio: ['ignore', 'pipe', 'pipe'],
			env,
		})

		const relaySocket = dgram.createSocket('udp4')
		relaySocket.on('error', (err) => {
			this.logger.warn('SRTP relay socket error', {
				port,
				relayPort,
				message: err.message,
			})
		})

		const gstStderrThrottle: GstStderrThrottle = { lastLog: {} }
		const gstStdoutThrottle: GstStdoutThrottle = { lastLog: {} }
		gst.stdout?.on('data', (data: Buffer) => {
			const text = data.toString()
			if (text.trim().length > 0) {
				this.logGstStdout(port, streamName, text, gstStdoutThrottle)
			}
		})
		gst.stderr?.on('data', (data: Buffer) => {
			this.logGstStderr(port, streamName, data.toString(), gstStderrThrottle)
		})
		gst.on('error', (err) => {
			this.logger.error('GStreamer error', err, { port, streamName })
			this.activePipelines.delete(port)
			relaySocket.close()
		})
		gst.on('exit', (code, signal) => {
			const wasUnexpected = this.activePipelines.has(port)
			this.logger.info('GStreamer exited', {
				port,
				streamName,
				code: code ?? undefined,
				signal: signal ?? undefined,
				unexpected: wasUnexpected,
			})
			this.activePipelines.delete(port)
			relaySocket.close()
			if (wasUnexpected) {
				this.emit('pipelineExited', { port, code, signal })
			}
		})

		this.activePipelines.set(port, {
			transport: 'srtp-relay',
			gst,
			relaySocket,
			relayPort,
			gstStderrThrottle,
			gstStdoutThrottle,
		})

		if (initialDatagrams.length > 0) {
			await new Promise((resolve) => setTimeout(resolve, SRTP_STARTUP_GRACE_MS))
			for (const datagram of initialDatagrams) {
				relaySocket.send(datagram, relayPort, '127.0.0.1', (err) => {
					if (err) {
						this.logger.warn('Failed to relay initial SRTP datagram', {
							port,
							relayPort,
							message: err.message,
						})
					}
				})
			}
		}

		this.logger.info('SRTP Kinesis ingestion started', {
			port,
			streamName,
			relayPort,
		})
	}

	private drainFifo(port: number, stdin: Writable): void {
		const pipeline = this.activePipelines.get(port)
		if (pipeline?.transport !== 'fifo') return
		const { reorder } = pipeline
		while (reorder.buffer.has(reorder.nextToEmit)) {
			const data = reorder.buffer.get(reorder.nextToEmit)
			reorder.buffer.delete(reorder.nextToEmit)
			reorder.nextToEmit += 1
			const ok = stdin.write(data)
			if (!ok) {
				stdin.once('drain', () => this.drainFifo(port, stdin))
				return
			}
		}
		// If buffer overfull, skip missing packets so we don't stall on loss
		const maxBuf = this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		if (maxBuf > 0 && reorder.buffer.size >= maxBuf) {
			while (
				!reorder.buffer.has(reorder.nextToEmit) &&
				reorder.buffer.size > 0
			) {
				reorder.nextToEmit += 1
			}
			this.drainFifo(port, stdin)
		}
	}

	/**
	 * Writes a UDP packet into the pipeline for that port. FIFO/MPEG-TS ports buffer and
	 * reorder by receive-sequence (see `reorderBufferSize`) before writing to GStreamer's
	 * stdin/FIFO. SRTP ports relay each datagram immediately and unbuffered - GStreamer's
	 * rtpjitterbuffer does real RTP-sequence-aware reordering downstream, so Node must not
	 * reorder or coalesce these datagrams itself.
	 */
	writePacket(port: number, data: Buffer): void {
		const pipeline = this.activePipelines.get(port)
		if (!pipeline) return

		if (pipeline.transport === 'srtp-relay') {
			pipeline.relaySocket.send(
				data,
				pipeline.relayPort,
				'127.0.0.1',
				(err) => {
					if (err) {
						this.logger.warn('Failed to relay SRTP datagram', {
							port,
							message: err.message,
						})
					}
				},
			)
			return
		}

		const inputStream = pipeline.inputStream
		if (inputStream.writable !== true) return

		const maxBuf = this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		if (maxBuf <= 0) {
			const ok = inputStream.write(data)
			if (!ok) inputStream.once('drain', () => {})
			return
		}

		const { reorder } = pipeline
		reorder.nextSeq += 1
		reorder.buffer.set(reorder.nextSeq, data)
		this.drainFifo(port, inputStream)
	}

	/**
	 * Stops the pipeline for a port: flushes reorder buffer (FIFO) or closes the relay
	 * socket (SRTP), then waits for process exit.
	 */
	async stop(port: number): Promise<void> {
		const pipeline = this.activePipelines.get(port)
		if (!pipeline) return

		this.activePipelines.delete(port)
		const { gst } = pipeline

		if (pipeline.transport === 'fifo') {
			const { inputStream, reorder, fifoPath } = pipeline
			if (inputStream.writable) {
				while (reorder.buffer.has(reorder.nextToEmit)) {
					const buf = reorder.buffer.get(reorder.nextToEmit)
					reorder.buffer.delete(reorder.nextToEmit)
					reorder.nextToEmit += 1
					if (buf !== undefined) inputStream.write(buf)
				}
				inputStream.end()
			}
			try {
				fs.unlinkSync(fifoPath)
			} catch (e) {
				this.logger.warn('Failed to unlink FIFO', {
					port,
					fifoPath,
					message: e instanceof Error ? e.message : String(e),
				})
			}
		} else {
			pipeline.relaySocket.close()
		}

		try {
			await new Promise<void>((resolve) => {
				const t = setTimeout(resolve, 15_000)
				gst.once('exit', () => {
					clearTimeout(t)
					resolve()
				})
			})
		} catch (err) {
			this.logger.warn('Error waiting for pipeline stop', {
				port,
				error: err instanceof Error ? err.message : String(err),
			})
		}
		gst.kill('SIGTERM')
		this.logger.info('Kinesis ingestion stopped', { port })
	}

	/**
	 * Stops all active pipelines (e.g. on shutdown).
	 */
	async stopAll(): Promise<void> {
		const ports = Array.from(this.activePipelines.keys())
		await Promise.all(ports.map(async (port) => this.stop(port)))
	}
}
