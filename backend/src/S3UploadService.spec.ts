import type { PutObjectCommandInput, S3Client } from '@aws-sdk/client-s3'
import assert from 'node:assert'
import { describe, it, mock } from 'node:test'
import { CACHE_CONFIGS, S3UploadService } from './S3UploadService.ts'

const testConfig = { bucket: 'test-bucket', uploadIntervalMs: 0 } as const

void describe('S3UploadService', () => {
	void describe('constructor', () => {
		void it('should create service with bucket name', async () => {
			const service = new S3UploadService(testConfig)

			assert.ok(service !== undefined)
			await service.stop()
		})

		void it('should use default region if not specified', async () => {
			const service = new S3UploadService({
				...testConfig,
			})

			assert.ok(service !== undefined)
			await service.stop()
		})

		void it('should use custom region if specified', async () => {
			const service = new S3UploadService({
				...testConfig,
				region: 'us-west-2',
			})

			assert.ok(service !== undefined)
			await service.stop()
		})
	})

	void describe('getBufferedUploadCount', () => {
		void it('should return 0 initially', async () => {
			const service = new S3UploadService({
				...testConfig,
			})

			assert.strictEqual(service.getBufferedUploadCount(), 0)
			await service.stop()
		})
	})

	void describe('stop', () => {
		void it('should stop the service', async () => {
			const service = new S3UploadService({
				...testConfig,
			})

			await service.stop()
			// Give time for cleanup
			await new Promise((resolve) => setTimeout(resolve, 100))
			assert.ok(true)
		})
	})

	void describe('CACHE_CONFIGS', () => {
		void it('should define playlist cache configuration with no-cache headers', () => {
			assert.strictEqual(
				CACHE_CONFIGS.PLAYLIST.cacheControl,
				'no-cache, no-store, must-revalidate',
			)
			assert.strictEqual(CACHE_CONFIGS.PLAYLIST.expires, '0')
		})

		void it('should define segment cache configuration with short cache headers', () => {
			assert.strictEqual(
				CACHE_CONFIGS.SEGMENT.cacheControl,
				'max-age=60, public',
			)
		})

		void it('should define snapshot cache configuration with no-cache headers', () => {
			assert.strictEqual(CACHE_CONFIGS.SNAPSHOT.cacheControl, 'no-cache')
		})
	})

	void describe('uploadData cache headers', () => {
		void it('should set no-cache headers for .m3u8 playlist files', async () => {
			const service = new S3UploadService({
				...testConfig,
			})

			// Mock the S3 client send method to capture the command
			const sendMock = mock.fn<S3Client['send']>(async () => ({}))
			service['client'].send = sendMock

			const testData = Buffer.from('test playlist content')
			const s3Key = 'hls/5000/1080p/playlist.m3u8'

			await service.uploadData(testData, s3Key, {
				contentType: 'application/vnd.apple.mpegurl',
			})

			assert.ok(sendMock.mock.callCount() > 0, 'Command should be captured')
			assert.strictEqual(
				(sendMock.mock.calls[0]?.arguments[0]?.input as PutObjectCommandInput)
					?.CacheControl,
				'no-cache, no-store, must-revalidate',
			)
			assert.ok(
				(sendMock.mock.calls[0]?.arguments[0]?.input as PutObjectCommandInput)
					?.Expires instanceof Date,
				'Expires should be a Date object',
			)
			assert.strictEqual(
				(
					sendMock.mock.calls[0]?.arguments[0]?.input as PutObjectCommandInput
				)?.Expires?.getTime(),
				new Date('0').getTime(),
			)

			await service.stop()
		})

		void it('should set short cache headers for .ts segment files', async () => {
			const service = new S3UploadService({
				...testConfig,
			})

			// Mock the S3 client send method to capture the command
			const sendMock = mock.fn<S3Client['send']>(async () => ({}))
			service['client'].send = sendMock

			const testData = Buffer.from('test segment content')
			const s3Key = 'hls/5000/1080p/segment_00001.ts'

			await service.uploadData(testData, s3Key, {
				contentType: 'video/mp2t',
			})

			assert.ok(sendMock.mock.callCount() > 0, 'Command should be captured')
			assert.strictEqual(
				(sendMock.mock.calls[0]?.arguments[0]?.input as PutObjectCommandInput)
					?.CacheControl,
				'max-age=60, public',
			)
			assert.strictEqual(
				(sendMock.mock.calls[0]?.arguments[0]?.input as PutObjectCommandInput)
					?.Expires,
				undefined,
			)

			await service.stop()
		})

		void it('should set no-cache headers for .jpg snapshot files', async () => {
			const service = new S3UploadService({
				...testConfig,
			})

			// Mock the S3 client send method to capture the command
			const sendMock = mock.fn<S3Client['send']>(async () => ({}))
			service['client'].send = sendMock

			const testData = Buffer.from('test image content')
			const s3Key = 'snapshots/5000/last_frame.jpg'

			await service.uploadData(testData, s3Key, {
				contentType: 'image/jpeg',
			})

			assert.ok(sendMock.mock.callCount() > 0, 'Command should be captured')
			assert.strictEqual(
				(sendMock.mock.calls[0]?.arguments[0]?.input as PutObjectCommandInput)
					?.CacheControl,
				'no-cache',
			)
			assert.strictEqual(
				(sendMock.mock.calls[0]?.arguments[0]?.input as PutObjectCommandInput)
					?.Expires,
				undefined,
			)

			await service.stop()
		})

		void it('should use explicit cacheControl option when provided', async () => {
			const service = new S3UploadService({
				...testConfig,
			})

			// Mock the S3 client send method to capture the command
			const sendMock = mock.fn<S3Client['send']>(async () => ({}))
			service['client'].send = sendMock

			const testData = Buffer.from('test content')
			const s3Key = 'test/file.m3u8'
			const customCacheControl = 'public, max-age=3600'

			await service.uploadData(testData, s3Key, {
				contentType: 'application/vnd.apple.mpegurl',
				cacheControl: customCacheControl,
			})

			assert.ok(sendMock.mock.callCount() > 0, 'Command should be captured')
			assert.strictEqual(
				(sendMock.mock.calls[0]?.arguments[0]?.input as PutObjectCommandInput)
					?.CacheControl,
				customCacheControl,
			)
			assert.strictEqual(
				(sendMock.mock.calls[0]?.arguments[0]?.input as PutObjectCommandInput)
					?.Expires,
				undefined,
			)

			await service.stop()
		})

		void it('should not set cache headers for unknown file types', async () => {
			const service = new S3UploadService({
				...testConfig,
			})

			// Mock the S3 client send method to capture the command
			const sendMock = mock.fn<S3Client['send']>(async () => ({}))
			service['client'].send = sendMock

			const testData = Buffer.from('test content')
			const s3Key = 'test/file.txt'

			await service.uploadData(testData, s3Key, {
				contentType: 'text/plain',
			})

			assert.ok(sendMock.mock.callCount() > 0, 'Command should be captured')
			assert.strictEqual(
				(sendMock.mock.calls[0]?.arguments[0]?.input as PutObjectCommandInput)
					?.CacheControl,
				undefined,
			)
			assert.strictEqual(
				(sendMock.mock.calls[0]?.arguments[0]?.input as PutObjectCommandInput)
					?.Expires,
				undefined,
			)

			await service.stop()
		})
	})
})
