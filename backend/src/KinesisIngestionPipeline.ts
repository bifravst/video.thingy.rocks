import { spawn } from 'node:child_process'
import type { Writable } from 'node:stream'
import { KinesisVideoSender } from './KinesisVideoSender.ts'
import { Logger } from './Logger.ts'

export type KinesisIngestionPipelineConfig = {
	streamNamePrefix: string
	region: string
	portRange: { start: number; end: number }
	ffmpegPath?: string
	/**
	 * Max packets to buffer before writing to FFmpeg (0 = write immediately).
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

/** Per-port throttle for FFmpeg stderr warning categories. */
type FfmpegStderrThrottle = { lastLog: Record<string, number> }

type PortPipeline = {
	ffmpeg: ReturnType<typeof spawn>
	sender: KinesisVideoSender
	putMediaPromise: Promise<void>
	reorder: ReorderState
	ffmpegStderrThrottle: FfmpegStderrThrottle
}

/** Throttle repeated FFmpeg stderr warnings (same category) per port. */
const FFMPEG_STDERR_THROTTLE_MS = 60_000

/** Progress line: size= 46683KiB time=00:03:24.86 bitrate=... speed=25.6x */
const FFMPEG_PROGRESS_REGEX = /size=\s*\d+KiB\s+time=|\sspeed=\d+\.\d+x/

/**
 * Per-port pipeline: UDP packets -> FFmpeg (TS -> MKV) -> Kinesis Video PutMedia.
 * One FFmpeg process and one PutMedia connection per active port.
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
	 * Logs FFmpeg stderr: skips progress lines, throttles repeated corrupt/DTS/non-monotonic warnings.
	 */
	private logFfmpegStderr(
		port: number,
		streamName: string,
		text: string,
		throttle: FfmpegStderrThrottle,
	): void {
		const now = Date.now()
		const lines = text.split(/\r?\n/)
		for (const raw of lines) {
			const line = raw.trim()
			if (line.length === 0) continue
			if (FFMPEG_PROGRESS_REGEX.test(line)) continue
			if (/^Last message repeated \d+ times\s*$/.test(line)) continue

			let category: string | null = null
			if (/corrupt|corrupt input packet/i.test(line)) category = 'corrupt'
			else if (/DTS .* out of order/i.test(line)) category = 'dts_order'
			else if (/Non-monotonic DTS/i.test(line)) category = 'non_monotonic'

			if (category !== null) {
				const last = throttle.lastLog[category] ?? 0
				if (now - last < FFMPEG_STDERR_THROTTLE_MS) continue
				throttle.lastLog[category] = now
			}

			this.logger.warn('FFmpeg stderr', {
				port,
				streamName,
				message: line.slice(0, 500),
			})
		}
	}

	/**
	 * Starts the pipeline for a port: spawns FFmpeg and begins PutMedia.
	 * Idempotent: no-op if already running for this port.
	 */
	async start(port: number): Promise<void> {
		if (!this.isPortInRange(port)) return
		if (this.activePipelines.has(port)) return

		const streamName = this.streamNameForPort(port)
		const sender = new KinesisVideoSender({
			streamName,
			region: this.config.region,
		})

		const ffmpegPath = this.config.ffmpegPath ?? 'ffmpeg'
		// MPEG-TS from stdin -> MKV to stdout (stream copy). discardcorrupt skips corrupt input packets.
		// flush_packets 1 flushes after each packet so MKV clusters reach PutMedia promptly (avoids 256KB buffer).
		const ffmpeg = spawn(
			ffmpegPath,
			[
				'-fflags',
				'+discardcorrupt',
				'-f',
				'mpegts',
				'-i',
				'pipe:0',
				'-c',
				'copy',
				'-f',
				'matroska',
				'-flush_packets',
				'1',
				'pipe:1',
			],
			{
				stdio: ['pipe', 'pipe', 'pipe'],
			},
		)

		const ffmpegStderrThrottle: FfmpegStderrThrottle = {
			lastLog: {},
		}
		ffmpeg.stderr?.on('data', (data: Buffer) => {
			this.logFfmpegStderr(
				port,
				streamName,
				data.toString(),
				ffmpegStderrThrottle,
			)
		})
		ffmpeg.on('error', (err) => {
			this.logger.error('FFmpeg error', err, { port, streamName })
			this.activePipelines.delete(port)
		})
		ffmpeg.on('exit', (code, signal) => {
			this.logger.info('FFmpeg exited', {
				port,
				streamName,
				code: code ?? undefined,
				signal: signal ?? undefined,
			})
			this.activePipelines.delete(port)
		})

		const stdout = ffmpeg.stdout
		if (stdout === null || stdout === undefined) {
			ffmpeg.kill('SIGTERM')
			throw new Error('FFmpeg stdout is not available')
		}
		const putMediaPromise = sender.putMedia(stdout).catch((err) => {
			this.logger.error(
				'PutMedia error',
				err instanceof Error ? err : new Error(String(err)),
				{
					port,
					streamName,
				},
			)
			// Ensure FFmpeg is killed if PutMedia fails
			ffmpeg.kill('SIGTERM')
		})

		const reorderBufferSize =
			this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		const reorder: ReorderState = {
			nextSeq: 0,
			nextToEmit: 1,
			buffer: new Map(),
		}
		this.activePipelines.set(port, {
			ffmpeg,
			sender,
			putMediaPromise,
			reorder,
			ffmpegStderrThrottle,
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
	 * Writes a UDP packet into the FFmpeg stdin for that port.
	 * When reorderBufferSize > 0, packets are buffered and emitted in receive order (smooths bursts; does not fix network reordering).
	 */
	writePacket(port: number, data: Buffer): void {
		const pipeline = this.activePipelines.get(port)
		const stdin = pipeline?.ffmpeg.stdin
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
	 * Stops the pipeline for a port: flushes reorder buffer, closes FFmpeg stdin, waits for exit and PutMedia to finish.
	 */
	async stop(port: number): Promise<void> {
		const pipeline = this.activePipelines.get(port)
		if (!pipeline) return

		this.activePipelines.delete(port)
		const { ffmpeg, putMediaPromise, reorder } = pipeline

		const stdin = ffmpeg.stdin
		if (stdin !== undefined && stdin !== null && stdin.writable === true) {
			// Flush reorder buffer in sequence order before closing
			while (reorder.buffer.has(reorder.nextToEmit)) {
				const buf = reorder.buffer.get(reorder.nextToEmit)
				reorder.buffer.delete(reorder.nextToEmit)
				reorder.nextToEmit += 1
				if (buf !== undefined) stdin.write(buf)
			}
			stdin.end()
		}
		try {
			await Promise.race([
				putMediaPromise,
				new Promise<void>((resolve) => {
					const t = setTimeout(resolve, 15_000)
					ffmpeg.once('exit', () => {
						clearTimeout(t)
						resolve()
					})
				}),
			])
		} catch (err) {
			this.logger.warn('Error waiting for pipeline stop', {
				port,
				error: err instanceof Error ? err.message : String(err),
			})
		}
		ffmpeg.kill('SIGTERM')
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
