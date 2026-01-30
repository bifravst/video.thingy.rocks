import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { EventEmitter } from 'node:events'
import { readFile } from 'node:fs/promises'

export type S3UploadConfig = {
	bucket: string
	region?: string
	/** Retry interval in ms; set to 0 to disable (e.g. in tests). Default 5000. */
	uploadIntervalMs?: number
}

export type UploadOptions = {
	receptionTimestamp?: Date
	contentType?: string
	metadata?: Record<string, string>
	cacheControl?: string
}

/**
 * Cache configuration for different file types
 * - Playlist files (.m3u8): no-cache to ensure players always get latest segment list
 * - Segment files (.ts): 60 seconds cache since they can be overwritten when new streams start
 * - Snapshot files (.jpg): no-cache to ensure latest frame is displayed
 */
export const CACHE_CONFIGS = {
	PLAYLIST: {
		cacheControl: 'no-cache, no-store, must-revalidate',
		expires: '0',
	},
	SEGMENT: {
		cacheControl: 'max-age=60, public', // 60 seconds
	},
	SNAPSHOT: {
		cacheControl: 'no-cache',
	},
} as const

type BufferedUpload = {
	key: string
	data: Buffer
	options: UploadOptions
	timestamp: Date
	retryCount: number
}

export class S3UploadService extends EventEmitter {
	private readonly client: S3Client
	private readonly bucket: string
	private readonly bufferedUploads: BufferedUpload[] = []
	private readonly maxBufferDuration = 60000 // 60 seconds
	private readonly maxRetries = 3
	private uploadInterval?: NodeJS.Timeout

	constructor(config: S3UploadConfig) {
		super()
		this.client = new S3Client({
			region: config.region ?? 'eu-central-1',
		})
		this.bucket = config.bucket

		const intervalMs = config.uploadIntervalMs ?? 5000
		if (intervalMs > 0) {
			this.uploadInterval = setInterval(() => {
				void this.processBufferedUploads()
			}, intervalMs)
		}
	}

	async uploadFile(
		filePath: string,
		s3Key: string,
		options: UploadOptions = {},
	): Promise<void> {
		try {
			const fileData = await readFile(filePath)
			await this.uploadData(fileData, s3Key, options)
		} catch (error) {
			console.error(
				`[S3UploadService] Error uploading file ${filePath}:`,
				error,
			)
			throw error
		}
	}

	async uploadData(
		data: Buffer,
		s3Key: string,
		options: UploadOptions = {},
	): Promise<void> {
		const metadata: Record<string, string> = {
			...options.metadata,
		}

		// Add reception timestamp to metadata
		if (options.receptionTimestamp) {
			metadata.receptionTimestamp = options.receptionTimestamp.toISOString()
		}

		// Determine cache headers based on file extension if not explicitly provided
		let cacheControl = options.cacheControl
		let expires: string | undefined

		if (
			cacheControl === null ||
			cacheControl === undefined ||
			cacheControl === ''
		) {
			const extension = s3Key.slice(s3Key.lastIndexOf('.'))
			if (extension === '.m3u8') {
				cacheControl = CACHE_CONFIGS.PLAYLIST.cacheControl
				expires = CACHE_CONFIGS.PLAYLIST.expires
			} else if (extension === '.ts') {
				cacheControl = CACHE_CONFIGS.SEGMENT.cacheControl
			} else if (extension === '.jpg' || extension === '.jpeg') {
				cacheControl = CACHE_CONFIGS.SNAPSHOT.cacheControl
			}
		}

		try {
			const command = new PutObjectCommand({
				Bucket: this.bucket,
				Key: s3Key,
				Body: data,
				ContentType: options.contentType ?? 'application/octet-stream',
				Metadata: metadata,
				CacheControl:
					cacheControl !== null &&
					cacheControl !== undefined &&
					cacheControl !== ''
						? cacheControl
						: undefined,
				Expires:
					expires !== null && expires !== undefined && expires !== ''
						? new Date(expires)
						: undefined,
			})

			await this.client.send(command)

			console.log(`[S3UploadService] Uploaded ${s3Key} to S3`)
			this.emit('uploaded', s3Key)
		} catch (error) {
			console.error(`[S3UploadService] Error uploading ${s3Key}:`, error)

			// Buffer the upload for retry
			this.bufferUpload({
				key: s3Key,
				data,
				options,
				timestamp: new Date(),
				retryCount: 0,
			})

			throw error
		}
	}

	async uploadRawSegment(
		port: number,
		segmentData: Buffer,
		segmentNumber: number,
		receptionTimestamp: Date,
	): Promise<void> {
		const s3Key = `raw/${port}/segment_${segmentNumber.toString().padStart(5, '0')}.ts`
		await this.uploadData(segmentData, s3Key, {
			receptionTimestamp,
			contentType: 'video/mp2t',
		})
	}

	async uploadHLSSegment(
		port: number,
		profile: string,
		segmentData: Buffer,
		segmentNumber: number,
		receptionTimestamp: Date,
	): Promise<void> {
		const s3Key = `hls/${port}/${profile}/segment_${segmentNumber.toString().padStart(5, '0')}.ts`
		await this.uploadData(segmentData, s3Key, {
			receptionTimestamp,
			contentType: 'video/mp2t',
			cacheControl: CACHE_CONFIGS.SEGMENT.cacheControl,
		})
	}

	async uploadHLSPlaylist(
		port: number,
		profile: string,
		playlistContent: string,
	): Promise<void> {
		const s3Key = `hls/${port}/${profile}/playlist.m3u8`
		await this.uploadData(Buffer.from(playlistContent), s3Key, {
			contentType: 'application/vnd.apple.mpegurl',
			cacheControl: CACHE_CONFIGS.PLAYLIST.cacheControl,
		})
	}

	async uploadMasterPlaylist(
		port: number,
		playlistContent: string,
	): Promise<void> {
		const s3Key = `hls/${port}/master.m3u8`
		await this.uploadData(Buffer.from(playlistContent), s3Key, {
			contentType: 'application/vnd.apple.mpegurl',
			cacheControl: CACHE_CONFIGS.PLAYLIST.cacheControl,
		})
	}

	async uploadSnapshot(
		port: number,
		imageData: Buffer,
		receptionTimestamp: Date,
	): Promise<void> {
		const s3Key = `snapshots/${port}/last_frame.jpg`
		await this.uploadData(imageData, s3Key, {
			receptionTimestamp,
			contentType: 'image/jpeg',
			cacheControl: CACHE_CONFIGS.SNAPSHOT.cacheControl,
		})
	}

	async stop(): Promise<void> {
		if (this.uploadInterval) {
			clearInterval(this.uploadInterval)
			this.uploadInterval = undefined
		}

		// Try to upload any remaining buffered uploads
		await this.processBufferedUploads()
	}

	getBufferedUploadCount(): number {
		return this.bufferedUploads.length
	}

	private bufferUpload(upload: BufferedUpload): void {
		// Check if buffer is getting too old
		const now = new Date()
		const oldestAllowedTime = now.getTime() - this.maxBufferDuration

		// Remove uploads older than maxBufferDuration
		while (
			this.bufferedUploads.length > 0 &&
			this.bufferedUploads[0] &&
			this.bufferedUploads[0].timestamp.getTime() < oldestAllowedTime
		) {
			const dropped = this.bufferedUploads.shift()
			console.warn(
				`[S3UploadService] Dropping buffered upload ${dropped?.key} (exceeded max buffer duration)`,
			)
			this.emit('dropped', dropped?.key)
		}

		this.bufferedUploads.push(upload)
		console.log(
			`[S3UploadService] Buffered upload ${upload.key} (buffer size: ${this.bufferedUploads.length})`,
		)
	}

	private async processBufferedUploads(): Promise<void> {
		if (this.bufferedUploads.length === 0) {
			return
		}

		console.log(
			`[S3UploadService] Processing ${this.bufferedUploads.length} buffered uploads`,
		)

		const uploadsToRetry = [...this.bufferedUploads]
		this.bufferedUploads.length = 0

		for (const upload of uploadsToRetry) {
			try {
				await this.uploadData(upload.data, upload.key, upload.options)
			} catch (error) {
				upload.retryCount++

				if (upload.retryCount >= this.maxRetries) {
					console.error(
						`[S3UploadService] Max retries reached for ${upload.key}, dropping`,
					)
					this.emit('failed', upload.key, error)
				} else {
					// Re-buffer for another retry
					this.bufferedUploads.push(upload)
					console.log(
						`[S3UploadService] Retry ${upload.retryCount}/${this.maxRetries} for ${upload.key}`,
					)
				}
			}
		}
	}
}
