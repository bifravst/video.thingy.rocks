import { spawn, type ChildProcess } from 'node:child_process'
import { EventEmitter } from 'node:events'
import type { S3UploadService } from './S3UploadService.ts'
import type { StreamMetadataService } from './StreamMetadataService.ts'

export type SnapshotCaptureConfig = {
	port: number
	s3UploadService: S3UploadService
	streamMetadataService: StreamMetadataService
	captureInterval?: number // milliseconds, default 10000 (10 seconds)
}

export class SnapshotCapture extends EventEmitter {
	private readonly config: SnapshotCaptureConfig
	private captureTimer?: NodeJS.Timeout
	private lastCaptureTime?: Date
	private readonly dataBuffer: Buffer[] = []
	private readonly maxBufferSize = 1024 * 1024 * 5 // 5MB
	private captureProcess?: ChildProcess

	constructor(config: SnapshotCaptureConfig) {
		super()
		this.config = config
	}

	start(): void {
		const interval = this.config.captureInterval ?? 10000

		this.captureTimer = setInterval(() => {
			void this.captureSnapshot()
		}, interval)

		console.log(
			`[SnapshotCapture] Started snapshot capture for port ${this.config.port} (interval: ${interval}ms)`,
		)
	}

	stop(): void {
		if (this.captureTimer !== undefined) {
			clearInterval(this.captureTimer)
			this.captureTimer = undefined
		}
		if (this.captureProcess !== undefined) {
			this.captureProcess.kill()
			this.captureProcess = undefined
		}
		this.dataBuffer.length = 0
		console.log(
			`[SnapshotCapture] Stopped snapshot capture for port ${this.config.port}`,
		)
	}

	writeData(data: Buffer): void {
		const currentBufferSize = this.dataBuffer.reduce(
			(sum, buf) => sum + buf.length,
			0,
		)

		if (currentBufferSize + data.length > this.maxBufferSize) {
			// Drop oldest data to make room
			this.dataBuffer.shift()
		}

		this.dataBuffer.push(data)
	}

	async captureSnapshot(): Promise<void> {
		if (this.dataBuffer.length === 0) {
			console.log(
				`[SnapshotCapture] No data available for snapshot capture on port ${this.config.port}`,
			)
			return
		}

		try {
			const receptionTimestamp = new Date()
			const imageData = await this.extractIFrame()

			if (imageData.length === 0) {
				console.warn(
					`[SnapshotCapture] No I-frame extracted for port ${this.config.port}`,
				)
				return
			}

			// Upload to S3
			await this.config.s3UploadService.uploadSnapshot(
				this.config.port,
				imageData,
				receptionTimestamp,
			)

			// Update DynamoDB with snapshot path
			const s3Path = `snapshots/${this.config.port}/last_frame.jpg`
			await this.config.streamMetadataService.updateLastFramePath(
				this.config.port,
				s3Path,
			)

			this.lastCaptureTime = receptionTimestamp
			this.emit('captured', this.config.port, s3Path)

			console.log(
				`[SnapshotCapture] Captured and uploaded snapshot for port ${this.config.port}`,
			)
		} catch (error) {
			console.error(
				`[SnapshotCapture] Error capturing snapshot for port ${this.config.port}:`,
				error,
			)
			this.emit('error', this.config.port, error)
		}
	}

	getLastCaptureTime(): Date | undefined {
		return this.lastCaptureTime
	}

	private async extractIFrame(): Promise<Buffer> {
		return new Promise((resolve, reject) => {
			const args = [
				'-f',
				'mpegts', // Specify input format
				'-i',
				'pipe:0', // Read from stdin
				'-vf',
				"select='eq(pict_type,I)'", // Select I-frames only
				'-fps_mode',
				'vfr', // Variable frame rate
				'-frames:v',
				'1', // Extract only 1 frame
				'-f',
				'image2', // Output as image
				'-c:v',
				'mjpeg', // JPEG codec
				'pipe:1', // Write to stdout
			]

			const ffmpeg = spawn('ffmpeg', args, {
				stdio: ['pipe', 'pipe', 'pipe'],
			})

			const outputChunks: Buffer[] = []
			const errorChunks: Buffer[] = []

			ffmpeg.stdout.on('data', (chunk: Buffer) => {
				outputChunks.push(chunk)
			})

			ffmpeg.stderr.on('data', (chunk: Buffer) => {
				errorChunks.push(chunk)
			})

			ffmpeg.on('exit', (code) => {
				if (code === 0) {
					resolve(Buffer.concat(outputChunks))
				} else {
					const errorMsg = Buffer.concat(errorChunks).toString()
					reject(
						new Error(
							`FFmpeg exited with code ${code}: ${errorMsg.slice(0, 200)}`,
						),
					)
				}
			})

			ffmpeg.on('error', (error) => {
				reject(error)
			})

			// Write buffered data to FFmpeg stdin
			for (const chunk of this.dataBuffer) {
				ffmpeg.stdin.write(chunk)
			}
			ffmpeg.stdin.end()
		})
	}
}
