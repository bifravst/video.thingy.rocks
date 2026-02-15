import { spawn, spawnSync } from 'node:child_process'
import { EventEmitter } from 'node:events'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import type { Writable } from 'node:stream'

import { fromNodeProviderChain } from '@aws-sdk/credential-providers'
import { Logger } from './Logger.ts'

export type KinesisIngestionPipelineConfig = {
	streamNamePrefix: string
	region: string
	portRange: { start: number; end: number }
	/**
	 * Max packets to buffer before writing to GStreamer stdin (0 = write immediately).
	 * Packets are emitted in receive order; does not fix network reordering.
	 * Default 128. Helps smooth bursts and applies backpressure.
	 */
	reorderBufferSize?: number
	/**
	 * When true, log GStreamer/KVS stdout and stderr. Disabled by default to avoid noisy logs.
	 */
	logGstreamerOutput?: boolean
}

const DEFAULT_REORDER_BUFFER_SIZE = 128

type ReorderState = {
	nextSeq: number
	nextToEmit: number
	buffer: Map<number, Buffer>
}

/** Per-port throttle for GStreamer stderr warning categories. */
type GstStderrThrottle = { lastLog: Record<string, number> }

/** Per-port throttle for noisy GStreamer/KVS stdout lines (CONTINUITY, 0x30000005, etc.). */
type GstStdoutThrottle = { lastLog: Record<string, number> }

type PortPipeline = {
	gst: ReturnType<typeof spawn>
	/** Stream we write TS to (stdin pipe or FIFO). */
	inputStream: Writable
	reorder: ReorderState
	gstStderrThrottle: GstStderrThrottle
	gstStdoutThrottle: GstStdoutThrottle
	/** FIFO path when using filesrc; unlink on stop. */
	fifoPath?: string
}

/** Throttle repeated GStreamer stderr warnings (same category) per port. */
const GST_STDERR_THROTTLE_MS = 60_000
/** Throttle noisy stdout (CONTINUITY, KVS 0x30000005, "Could not write to resource") per port. */
const GST_STDOUT_THROTTLE_MS = 60_000

/**
 * Per-port pipeline: UDP packets -> GStreamer (TS -> H.264) -> kvssink -> Kinesis Video.
 * One GStreamer process per active port; kvssink sends directly to Kinesis (no Node PutMedia).
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

	streamNameForPort(port: number): string {
		return `${this.config.streamNamePrefix}-${port}`
	}

	isPortInRange(port: number): boolean {
		return (
			port >= this.config.portRange.start && port <= this.config.portRange.end
		)
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
			if (/error|ERROR/i.test(line)) category = 'error'
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
	 * Starts the pipeline for a port: spawns GStreamer with kvssink.
	 * Idempotent: no-op if already running for this port.
	 * Concurrent calls for the same port are deduped (single credential fetch + spawn).
	 * Optional initialData is written to stdin immediately after spawn so fdsrc has data when it first reads.
	 */
	async start(port: number, initialData?: Buffer): Promise<void> {
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

	/**
	 * Single-run start logic for a port (credentials + spawn). Call only via start() so dedupe applies.
	 * If initialData is provided, it is written to stdin immediately after spawn so fdsrc has data when it first reads.
	 */
	private async runStartForPort(
		port: number,
		initialData?: Buffer,
	): Promise<void> {
		if (this.activePipelines.has(port)) return

		const streamName = this.streamNameForPort(port)
		const region = this.config.region

		// Resolve credentials before any GStreamer setup or spawn. kvssink (C++ SDK) does not use
		// the same credential chain as Node; we pass them via env so the child finds them.
		const credentialProvider = fromNodeProviderChain({
			timeout: 10_000,
			maxRetries: 5,
		})
		const maxResolutionAttempts = 3
		const resolutionDelayMs = 2000
		let credentials: Awaited<
			ReturnType<ReturnType<typeof fromNodeProviderChain>>
		>
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
			return
		}

		// Build env with credentials and plugin paths; only then build pipeline and spawn.
		const env = { ...process.env }
		env.AWS_ACCESS_KEY_ID = credentials!.accessKeyId
		env.AWS_SECRET_ACCESS_KEY = credentials!.secretAccessKey
		if (
			credentials!.sessionToken !== undefined &&
			credentials!.sessionToken !== ''
		) {
			env.AWS_SESSION_TOKEN = credentials!.sessionToken
		}
		env.AWS_REGION = region
		if (env.KINESIS_GST_PLUGIN_PATH !== undefined) {
			env.GST_PLUGIN_PATH = env.KINESIS_GST_PLUGIN_PATH
		}
		if (env.KINESIS_LD_LIBRARY_PATH !== undefined) {
			env.LD_LIBRARY_PATH = env.KINESIS_LD_LIBRARY_PATH
		}

		const logConfigPath =
			process.env.KVS_LOG_CONFIG_PATH ??
			'/opt/video-streaming/kvs_log_configuration'

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

	private drain(port: number, stdin: Writable): void {
		const pipeline = this.activePipelines.get(port)
		if (!pipeline) return
		const { reorder } = pipeline
		while (reorder.buffer.has(reorder.nextToEmit)) {
			const data = reorder.buffer.get(reorder.nextToEmit)
			reorder.buffer.delete(reorder.nextToEmit)
			reorder.nextToEmit += 1
			const ok = stdin.write(data)
			if (!ok) {
				stdin.once('drain', () => this.drain(port, stdin))
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
			this.drain(port, stdin)
		}
	}

	/**
	 * Writes a UDP packet into the GStreamer stdin for that port.
	 * When reorderBufferSize > 0, packets are buffered and emitted in receive order.
	 */
	writePacket(port: number, data: Buffer): void {
		const pipeline = this.activePipelines.get(port)
		const inputStream = pipeline?.inputStream
		if (inputStream?.writable !== true) return

		const maxBuf = this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		if (maxBuf <= 0) {
			const ok = inputStream.write(data)
			if (!ok) inputStream.once('drain', () => {})
			return
		}

		if (!pipeline) return
		const { reorder } = pipeline
		reorder.nextSeq += 1
		reorder.buffer.set(reorder.nextSeq, data)
		this.drain(port, inputStream)
	}

	/**
	 * Stops the pipeline for a port: flushes reorder buffer, closes stdin, waits for process exit.
	 */
	async stop(port: number): Promise<void> {
		const pipeline = this.activePipelines.get(port)
		if (!pipeline) return

		this.activePipelines.delete(port)
		const { gst, inputStream, reorder, fifoPath } = pipeline

		if (inputStream.writable) {
			while (reorder.buffer.has(reorder.nextToEmit)) {
				const buf = reorder.buffer.get(reorder.nextToEmit)
				reorder.buffer.delete(reorder.nextToEmit)
				reorder.nextToEmit += 1
				if (buf !== undefined) inputStream.write(buf)
			}
			inputStream.end()
		}
		if (fifoPath !== undefined) {
			try {
				fs.unlinkSync(fifoPath)
			} catch (e) {
				this.logger.warn('Failed to unlink FIFO', {
					port,
					fifoPath,
					message: e instanceof Error ? e.message : String(e),
				})
			}
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
