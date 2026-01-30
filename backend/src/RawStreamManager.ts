import { EventEmitter } from 'node:events'
import { S3UploadService } from './S3UploadService.ts'
import type { StreamMetadataService } from './StreamMetadataService.ts'

export type RawStreamManagerConfig = {
	port: number
	s3Bucket: string
	s3Region?: string
	streamMetadataService: StreamMetadataService
	localOutputDir?: string
	segmentDuration?: number // seconds
}

type RawSegment = {
	filename: string
	duration: number
	timestamp: Date
}

export class RawStreamManager extends EventEmitter {
	private readonly config: RawStreamManagerConfig
	private readonly s3UploadService: S3UploadService
	private readonly segments: RawSegment[] = []
	private readonly maxSegments = 100 // Keep last 100 segments in manifest
	private segmentCounter = 0
	private currentSegmentData: Buffer[] = []
	private currentSegmentStartTime?: Date
	private flushTimer?: NodeJS.Timeout

	constructor(config: RawStreamManagerConfig) {
		super()
		this.config = config

		this.s3UploadService = new S3UploadService({
			bucket: config.s3Bucket,
			region: config.s3Region,
		})
	}

	start(): void {
		console.log(`[RawStreamManager] Started for port ${this.config.port}`)
	}

	stop(): void {
		if (this.flushTimer) {
			clearTimeout(this.flushTimer)
			this.flushTimer = undefined
		}

		// Flush any remaining data
		void this.flushSegment()

		console.log(`[RawStreamManager] Stopped for port ${this.config.port}`)
	}

	writeData(data: Buffer): void {
		// Start new segment if this is the first data
		if (!this.currentSegmentStartTime) {
			this.currentSegmentStartTime = new Date()
			this.scheduleFlush()
		}

		this.currentSegmentData.push(data)
	}

	private scheduleFlush(): void {
		const segmentDuration = (this.config.segmentDuration ?? 6) * 1000

		this.flushTimer = setTimeout(() => {
			void this.flushSegment()
		}, segmentDuration)
	}

	private async flushSegment(): Promise<void> {
		if (this.currentSegmentData.length === 0) {
			return
		}

		const segmentData = Buffer.concat(this.currentSegmentData)
		const startTime = this.currentSegmentStartTime ?? new Date()
		const endTime = new Date()
		const duration = (endTime.getTime() - startTime.getTime()) / 1000

		// Generate segment filename and increment counter BEFORE upload
		// This ensures sequential numbering even if uploads fail
		const filename = `segment_${String(this.segmentCounter).padStart(5, '0')}.ts`
		const s3Key = `raw/${this.config.port}/${filename}`
		const segmentNumber = this.segmentCounter
		this.segmentCounter++

		try {
			// Upload segment to S3
			await this.s3UploadService.uploadData(segmentData, s3Key, {
				contentType: 'video/mp2t',
				receptionTimestamp: startTime,
			})

			// Add to segment list
			this.segments.push({
				filename,
				duration,
				timestamp: startTime,
			})

			// Keep only last N segments
			if (this.segments.length > this.maxSegments) {
				this.segments.shift()
			}

			// Update manifest
			await this.updateManifest()

			console.log(
				`[RawStreamManager] Uploaded raw segment ${filename} for port ${this.config.port}`,
			)
		} catch (error) {
			console.error(
				`[RawStreamManager] Error uploading segment ${segmentNumber} for port ${this.config.port}:`,
				error,
			)
		}

		// Reset for next segment
		this.currentSegmentData = []
		this.currentSegmentStartTime = undefined

		// Schedule next flush
		this.scheduleFlush()
	}

	private async updateManifest(): Promise<void> {
		// Generate HLS manifest for raw stream
		let manifest = '#EXTM3U\n'
		manifest += '#EXT-X-VERSION:3\n'
		manifest += `#EXT-X-TARGETDURATION:${Math.ceil(Math.max(...this.segments.map((s) => s.duration), 6))}\n`
		manifest += `#EXT-X-MEDIA-SEQUENCE:${Math.max(0, this.segmentCounter - this.segments.length)}\n`
		manifest += '\n'

		for (const segment of this.segments) {
			manifest += `#EXTINF:${segment.duration.toFixed(6)},\n`
			manifest += `${segment.filename}\n`
		}

		const s3Key = `raw/${this.config.port}/playlist.m3u8`

		try {
			await this.s3UploadService.uploadData(Buffer.from(manifest), s3Key, {
				contentType: 'application/vnd.apple.mpegurl',
			})

			// Update metadata service with raw stream path
			await this.config.streamMetadataService.updateRawStreamPath(
				this.config.port,
				s3Key,
			)

			console.log(
				`[RawStreamManager] Updated raw stream manifest for port ${this.config.port}`,
			)
		} catch (error) {
			console.error(
				`[RawStreamManager] Error updating manifest for port ${this.config.port}:`,
				error,
			)
		}
	}
}
