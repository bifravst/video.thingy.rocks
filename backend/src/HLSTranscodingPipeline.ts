import { EventEmitter } from 'node:events'
import { FFmpegTranscoder, type TranscodingConfig } from './FFmpegTranscoder.ts'

export type HLSPipelineConfig = {
	port: number
	s3Bucket: string
	segmentDuration?: number // seconds, default 6
}

export class HLSTranscodingPipeline extends EventEmitter {
	private readonly config: HLSPipelineConfig
	private transcoder?: FFmpegTranscoder
	private readonly dataBuffer: Buffer[] = []
	private readonly maxBufferSize = 1024 * 1024 * 10 // 10MB buffer

	constructor(config: HLSPipelineConfig) {
		super()
		this.config = config
	}

	async start(): Promise<void> {
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

		await this.transcoder.start()

		// Flush any buffered data
		this.flushBuffer()
	}

	async stop(): Promise<void> {
		if (this.transcoder) {
			await this.transcoder.stop()
		}
		this.dataBuffer.length = 0
	}

	writeData(data: Buffer): void {
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
}
