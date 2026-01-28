import { EventEmitter } from 'node:events'
import { FFmpegTranscoder, type TranscodingConfig } from './FFmpegTranscoder.ts'

export type HLSPipelineConfig = {
	port: number
	s3Bucket: string
	segmentDuration?: number // seconds, default 6
	localOutputDir?: string // Local directory for FFmpeg output, default /tmp/video-streams
}

export class HLSTranscodingPipeline extends EventEmitter {
	private readonly config: HLSPipelineConfig
	private transcoder?: FFmpegTranscoder
	private readonly dataBuffer: Buffer[] = []
	private readonly maxBufferSize = 1024 * 1024 * 10 // 10MB buffer
	private readonly minBufferBeforeStart = 1024 * 100 // 100KB minimum before starting FFmpeg
	private transcoderStarted = false

	constructor(config: HLSPipelineConfig) {
		super()
		this.config = config
	}

	async start(): Promise<void> {
		// Don't start FFmpeg immediately - wait for data to arrive
		// FFmpeg will be started lazily when we have enough buffered data
		console.log(
			`[HLSTranscodingPipeline] Pipeline ready for port ${this.config.port}, waiting for data...`,
		)
	}

	async stop(): Promise<void> {
		if (this.transcoder) {
			await this.transcoder.stop()
		}
		this.dataBuffer.length = 0
	}

	writeData(data: Buffer): void {
		// If transcoder hasn't started yet, buffer data
		if (!this.transcoderStarted) {
			this.bufferData(data)

			// Check if we have enough data to start FFmpeg
			const currentBufferSize = this.dataBuffer.reduce(
				(sum, buf) => sum + buf.length,
				0,
			)

			if (currentBufferSize >= this.minBufferBeforeStart) {
				console.log(
					`[HLSTranscodingPipeline] Buffered ${currentBufferSize} bytes for port ${this.config.port}, starting FFmpeg...`,
				)
				void this.startTranscoder()
			}
			return
		}

		if (!this.transcoder) {
			// Buffer data if transcoder not ready
			this.bufferData(data)
			return
		}

		const status = this.transcoder.getStatus()
		if (status.isRunning !== true) {
			// Buffer data if transcoder not ready
			this.bufferData(data)
			return
		}

		const written = this.transcoder.write(data)
		if (written === false) {
			// If write failed, buffer the data
			this.bufferData(data)
		}
	}

	getStatus(): {
		isRunning: boolean
		currentSegment: number
		retryCount: number
		lastError?: string
	} {
		return (
			this.transcoder?.getStatus() ?? {
				isRunning: false,
				currentSegment: 0,
				retryCount: 0,
			}
		)
	}

	private bufferData(data: Buffer): void {
		const currentBufferSize = this.dataBuffer.reduce(
			(sum, buf) => sum + buf.length,
			0,
		)

		if (currentBufferSize + data.length > this.maxBufferSize) {
			console.warn(
				`[HLSTranscodingPipeline] Buffer full for port ${this.config.port}, dropping oldest data`,
			)
			// Drop oldest buffer to make room
			this.dataBuffer.shift()
		}

		this.dataBuffer.push(data)
	}

	private flushBuffer(): void {
		if (!this.transcoder || this.dataBuffer.length === 0) {
			return
		}

		console.log(
			`[HLSTranscodingPipeline] Flushing ${this.dataBuffer.length} buffered chunks for port ${this.config.port}`,
		)

		while (this.dataBuffer.length > 0) {
			const data = this.dataBuffer.shift()
			if (data) {
				this.transcoder.write(data)
			}
		}
	}

	private async startTranscoder(): Promise<void> {
		if (this.transcoderStarted) {
			return
		}

		this.transcoderStarted = true

		const transcodingConfig: TranscodingConfig = {
			port: this.config.port,
			outputPaths: {
				raw: `raw/${this.config.port}`,
				hls: `hls/${this.config.port}`,
				snapshot: `snapshots/${this.config.port}`,
			},
			hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
			segmentDuration: this.config.segmentDuration ?? 6,
			s3Bucket: this.config.s3Bucket,
			localOutputDir:
				this.config.localOutputDir ?? '/tmp/video-streams/transcoding',
		}

		this.transcoder = new FFmpegTranscoder(transcodingConfig)

		// Forward events
		this.transcoder.on('started', (port) => this.emit('started', port))
		this.transcoder.on('stopped', (port) => this.emit('stopped', port))
		this.transcoder.on('error', (port, error) =>
			this.emit('error', port, error),
		)
		this.transcoder.on('failed', (port, error) =>
			this.emit('failed', port, error),
		)

		try {
			await this.transcoder.start()

			// Flush buffered data to FFmpeg
			this.flushBuffer()
		} catch (error) {
			console.error(
				`[HLSTranscodingPipeline] Failed to start transcoder for port ${this.config.port}:`,
				error,
			)
			this.transcoderStarted = false
			throw error
		}
	}
}
