import { spawn } from 'node:child_process'

import type { Writable } from 'node:stream'
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
}

const DEFAULT_REORDER_BUFFER_SIZE = 128

type ReorderState = {
	nextSeq: number
	nextToEmit: number
	buffer: Map<number, Buffer>
}

/** Per-port throttle for GStreamer stderr warning categories. */
type GstStderrThrottle = { lastLog: Record<string, number> }

type PortPipeline = {
	gst: ReturnType<typeof spawn>
	reorder: ReorderState
	gstStderrThrottle: GstStderrThrottle
}

/** Throttle repeated GStreamer stderr warnings (same category) per port. */
const GST_STDERR_THROTTLE_MS = 60_000

/**
 * Per-port pipeline: UDP packets -> GStreamer (TS -> H.264) -> kvssink -> Kinesis Video.
 * One GStreamer process per active port; kvssink sends directly to Kinesis (no Node PutMedia).
 */
export class KinesisIngestionPipeline {
	private readonly config: KinesisIngestionPipelineConfig
	private readonly logger: Logger
	private readonly activePipelines: Map<number, PortPipeline> = new Map()

	constructor(config: KinesisIngestionPipelineConfig) {
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
	 * Starts the pipeline for a port: spawns GStreamer with kvssink.
	 * Idempotent: no-op if already running for this port.
	 */
	async start(port: number): Promise<void> {
		if (!this.isPortInRange(port)) return
		if (this.activePipelines.has(port)) return

		const streamName = this.streamNameForPort(port)
		const region = this.config.region

		// TS from stdin -> tsdemux -> H.264 -> kvssink (sends to Kinesis)
		// Use filesrc location=/dev/stdin instead of fdsrc fd=0 (same effect on Linux; some gst-launch parsers fail on fdsrc fd=0)
		// Quote kvssink values so hyphens in stream name are not parsed as minus by the grammar.
		// log-config: KVS C++ SDK requires a log4cplus config file; default "../kvs_log_configuration" fails when CWD is app dir. Use absolute path (EC2 user-data creates it) or env override for local.
		const logConfigPath =
			process.env.KVS_LOG_CONFIG_PATH ??
			'/opt/video-streaming/kvs_log_configuration'
		const pipelineStr = `filesrc location=/dev/stdin ! tsparse set-timestamps=true ! tsdemux name=d d.video_0 ! queue ! h264parse ! capsfilter caps="video/x-h264,stream-format=avc,alignment=au" ! kvssink stream-name="${streamName.replace(/"/g, '\\"')}" aws-region="${region.replace(/"/g, '\\"')}" storage-size=128 log-config="${logConfigPath.replace(/"/g, '\\"')}"`

		// Run via sh so we can pass pipeline from file: "$(cat "$PIPELINE_FILE")" gives gst-launch one exact argument with no Node->argv encoding
		const shellCmd = `gst-launch-1.0 ${pipelineStr}`
		this.logger.info('GStreamer command', {
			port,
			streamName,
			pipelineStr,
			shellCmd,
		})
		const gst = spawn('sh', ['-c', shellCmd], {
			stdio: ['pipe', 'ignore', 'pipe'],
			env: process.env,
		})

		// Prevent EPIPE from crashing the process when GStreamer exits early (e.g. pipeline syntax error)
		gst.stdin?.on('error', (err: NodeJS.ErrnoException) => {
			if (err.code !== 'EPIPE') {
				this.logger.warn('GStreamer stdin error', {
					port,
					streamName,
					code: err.code,
					message: err.message,
				})
			}
		})

		const gstStderrThrottle: GstStderrThrottle = { lastLog: {} }
		gst.stderr?.on('data', (data: Buffer) => {
			this.logGstStderr(port, streamName, data.toString(), gstStderrThrottle)
		})
		gst.on('error', (err) => {
			this.logger.error('GStreamer error', err, { port, streamName })
			this.activePipelines.delete(port)
		})
		gst.on('exit', (code, signal) => {
			this.logger.info('GStreamer exited', {
				port,
				streamName,
				code: code ?? undefined,
				signal: signal ?? undefined,
			})
			this.activePipelines.delete(port)
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
			reorder,
			gstStderrThrottle,
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
		const stdin = pipeline?.gst.stdin
		if (stdin === undefined || stdin === null || stdin.writable !== true) return

		const maxBuf = this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		if (maxBuf <= 0) {
			const ok = stdin.write(data)
			if (!ok) stdin.once('drain', () => {})
			return
		}

		if (!pipeline) return
		const { reorder } = pipeline
		reorder.nextSeq += 1
		reorder.buffer.set(reorder.nextSeq, data)
		this.drain(port, stdin)
	}

	/**
	 * Stops the pipeline for a port: flushes reorder buffer, closes stdin, waits for process exit.
	 */
	async stop(port: number): Promise<void> {
		const pipeline = this.activePipelines.get(port)
		if (!pipeline) return

		this.activePipelines.delete(port)
		const { gst, reorder } = pipeline

		const stdin = gst.stdin

		if (stdin !== undefined && stdin !== null && stdin.writable === true) {
			while (reorder.buffer.has(reorder.nextToEmit)) {
				const buf = reorder.buffer.get(reorder.nextToEmit)
				reorder.buffer.delete(reorder.nextToEmit)
				reorder.nextToEmit += 1
				if (buf !== undefined) stdin.write(buf)
			}
			stdin.end()
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
