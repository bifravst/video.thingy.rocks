import { EventEmitter } from 'node:events'
import { HLSTranscodingPipeline } from './HLSTranscodingPipeline.ts'
import { RawStreamManager } from './RawStreamManager.ts'
import { S3UploadService } from './S3UploadService.ts'
import { SnapshotCapture } from './SnapshotCapture.ts'
import type { StreamMetadataService } from './StreamMetadataService.ts'

export type TranscodingManagerConfig = {
	s3Bucket: string
	s3Region?: string
	streamMetadataService: StreamMetadataService
	segmentDuration?: number
}

type StreamTranscoder = {
	pipeline: HLSTranscodingPipeline
	snapshot: SnapshotCapture
	rawStream: RawStreamManager
	mode: 'full' | 'raw-only' // full = HLS + raw, raw-only = fallback mode
	errorCount: number
	lastError?: string
}

export class TranscodingManager extends EventEmitter {
	private readonly config: TranscodingManagerConfig
	private readonly s3UploadService: S3UploadService
	private readonly transcoders: Map<number, StreamTranscoder> = new Map()
	private readonly maxErrorsBeforeFallback = 3

	constructor(config: TranscodingManagerConfig) {
		super()
		this.config = config

		this.s3UploadService = new S3UploadService({
			bucket: config.s3Bucket,
			region: config.s3Region,
		})

		// Handle S3 upload events
		this.s3UploadService.on('uploaded', (key) => {
			this.emit('segmentUploaded', key)
		})

		this.s3UploadService.on('failed', (key, error) => {
			console.error(`[TranscodingManager] S3 upload failed for ${key}:`, error)
			this.emit('uploadFailed', key, error)
		})

		this.s3UploadService.on('dropped', (key) => {
			console.warn(`[TranscodingManager] S3 upload dropped for ${key}`)
			this.emit('uploadDropped', key)
		})
	}

	async startTranscoding(port: number): Promise<void> {
		if (this.transcoders.has(port)) {
			console.warn(
				`[TranscodingManager] Transcoding already started for port ${port}`,
			)
			return
		}

		try {
			const pipeline = new HLSTranscodingPipeline({
				port,
				s3Bucket: this.config.s3Bucket,
				segmentDuration: this.config.segmentDuration,
			})

			const snapshot = new SnapshotCapture({
				port,
				s3UploadService: this.s3UploadService,
				streamMetadataService: this.config.streamMetadataService,
			})

			const rawStream = new RawStreamManager({
				port,
				s3Bucket: this.config.s3Bucket,
				s3Region: this.config.s3Region,
				streamMetadataService: this.config.streamMetadataService,
				segmentDuration: this.config.segmentDuration,
			})

			// Set up error handlers
			this.setupPipelineErrorHandlers(port, pipeline)

			// Start transcoding
			await pipeline.start()
			snapshot.start()
			rawStream.start()

			this.transcoders.set(port, {
				pipeline,
				snapshot,
				rawStream,
				mode: 'full',
				errorCount: 0,
			})

			console.log(
				`[TranscodingManager] Started transcoding for port ${port} in full mode`,
			)
			this.emit('transcodingStarted', port)
		} catch (error) {
			console.error(
				`[TranscodingManager] Failed to start transcoding for port ${port}:`,
				error,
			)
			this.emit('transcodingFailed', port, error)
			throw error
		}
	}

	async stopTranscoding(port: number): Promise<void> {
		const transcoder = this.transcoders.get(port)
		if (!transcoder) {
			console.log(`[TranscodingManager] No transcoder to stop for port ${port}`)
			return
		}

		try {
			await transcoder.pipeline.stop()
			transcoder.snapshot.stop()
			transcoder.rawStream.stop()

			console.log(`[TranscodingManager] Stopped transcoding for port ${port}`)
			this.emit('transcodingStopped', port)
		} catch (error) {
			console.error(
				`[TranscodingManager] Error stopping transcoding for port ${port}:`,
				error,
			)
			// Don't throw - we still want to clean up
		} finally {
			// Always remove from map, even if stop failed
			this.transcoders.delete(port)
			console.log(
				`[TranscodingManager] Removed transcoder from map for port ${port}`,
			)
		}
	}

	writeData(port: number, data: Buffer): void {
		const transcoder = this.transcoders.get(port)
		if (!transcoder) {
			console.warn(
				`[TranscodingManager] No transcoder found for port ${port}, data discarded`,
			)
			return
		}

		try {
			// Write to transcoding pipeline
			transcoder.pipeline.writeData(data)

			// Write to snapshot capture
			transcoder.snapshot.writeData(data)

			// Write to raw stream manager
			transcoder.rawStream.writeData(data)
		} catch (error) {
			console.error(
				`[TranscodingManager] Error writing data for port ${port}:`,
				error,
			)
			this.handleTranscoderError(port, error)
		}
	}

	getTranscoderStatus(port: number): {
		mode: 'full' | 'raw-only'
		errorCount: number
		lastError?: string
		pipelineStatus: {
			isRunning: boolean
			currentSegment: number
			retryCount: number
			lastError?: string
		}
		lastSnapshotTime?: Date
	} | null {
		const transcoder = this.transcoders.get(port)
		if (!transcoder) {
			return null
		}

		return {
			mode: transcoder.mode,
			errorCount: transcoder.errorCount,
			lastError: transcoder.lastError,
			pipelineStatus: transcoder.pipeline.getStatus(),
			lastSnapshotTime: transcoder.snapshot.getLastCaptureTime(),
		}
	}

	getActiveTranscoders(): number[] {
		return Array.from(this.transcoders.keys())
	}

	async stop(): Promise<void> {
		console.log('[TranscodingManager] Stopping all transcoders...')

		const stopPromises: Promise<void>[] = []
		for (const port of this.transcoders.keys()) {
			stopPromises.push(this.stopTranscoding(port))
		}

		await Promise.all(stopPromises)
		await this.s3UploadService.stop()

		console.log('[TranscodingManager] All transcoders stopped')
	}

	private setupPipelineErrorHandlers(
		port: number,
		pipeline: HLSTranscodingPipeline,
	): void {
		pipeline.on('error', (errorPort, error) => {
			console.error(
				`[TranscodingManager] Pipeline error for port ${errorPort}:`,
				error,
			)
			this.handleTranscoderError(port, error)
		})

		pipeline.on('failed', (errorPort, error) => {
			console.error(
				`[TranscodingManager] Pipeline failed for port ${errorPort}:`,
				error,
			)
			this.handleTranscoderFailure(port, error)
		})

		pipeline.on('started', (startedPort) => {
			console.log(
				`[TranscodingManager] Pipeline started for port ${startedPort}`,
			)
		})

		pipeline.on('stopped', (stoppedPort) => {
			console.log(
				`[TranscodingManager] Pipeline stopped for port ${stoppedPort}`,
			)
		})
	}

	private handleTranscoderError(port: number, error: unknown): void {
		const transcoder = this.transcoders.get(port)
		if (!transcoder) {
			return
		}

		transcoder.errorCount++
		transcoder.lastError =
			error instanceof Error ? error.message : String(error)

		console.warn(
			`[TranscodingManager] Error count for port ${port}: ${transcoder.errorCount}/${this.maxErrorsBeforeFallback}`,
		)

		// Check if we should fall back to raw-only mode
		if (
			transcoder.errorCount >= this.maxErrorsBeforeFallback &&
			transcoder.mode === 'full'
		) {
			console.warn(
				`[TranscodingManager] Falling back to raw-only mode for port ${port}`,
			)
			void this.fallbackToRawOnly(port)
		}

		this.emit('transcoderError', port, error)
	}

	private handleTranscoderFailure(port: number, error: unknown): void {
		const transcoder = this.transcoders.get(port)
		if (!transcoder) {
			return
		}

		console.error(
			`[TranscodingManager] Transcoder failed for port ${port}, attempting fallback`,
		)

		// Try to fall back to raw-only mode
		void this.fallbackToRawOnly(port)

		this.emit('transcoderFailed', port, error)
	}

	private async fallbackToRawOnly(port: number): Promise<void> {
		const transcoder = this.transcoders.get(port)
		if (!transcoder) {
			return
		}

		try {
			// Stop current pipeline
			await transcoder.pipeline.stop()

			// Update mode to raw-only
			transcoder.mode = 'raw-only'
			transcoder.errorCount = 0

			console.log(
				`[TranscodingManager] Port ${port} now in raw-only mode (HLS transcoding disabled)`,
			)
			this.emit('fallbackToRawOnly', port)

			// Note: In raw-only mode, we would need a simpler pipeline that just
			// copies the raw stream to S3 without transcoding. For now, we just
			// mark the mode and continue with snapshot capture.
		} catch (error) {
			console.error(
				`[TranscodingManager] Failed to fall back to raw-only mode for port ${port}:`,
				error,
			)
		}
	}
}
