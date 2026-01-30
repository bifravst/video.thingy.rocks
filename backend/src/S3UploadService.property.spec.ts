import type { PutObjectCommandInput, S3Client } from '@aws-sdk/client-s3'
import * as fc from 'fast-check'
import assert from 'node:assert'
import { describe, it, mock } from 'node:test'
import { CACHE_CONFIGS, S3UploadService } from './S3UploadService.ts'

const testConfig = { bucket: 'test-bucket', uploadIntervalMs: 0 } as const

/**
 * Property-Based Tests for S3 Upload Service Cache Headers
 *
 * These tests verify universal properties that should hold across all valid
 * file uploads to S3.
 */
void describe('S3UploadService - Property Tests', () => {
	/**
	 * Property 3: Playlist Cache Headers
	 * **Validates: Requirements 2.3, 2.4, 2.5**
	 *
	 * For any playlist file (.m3u8) uploaded to S3, the Cache-Control header
	 * should be set to `no-cache, no-store, must-revalidate` and the content
	 * type should be `application/vnd.apple.mpegurl`.
	 */
	void describe('Property 3: Playlist Cache Headers', () => {
		// Arbitrary generator for port numbers
		const portArb = fc.integer({ min: 1024, max: 65535 })

		// Arbitrary generator for profile names
		const profileArb = fc.constantFrom(
			'1080p',
			'720p',
			'480p',
			'360p',
			'240p',
			'144p',
		)

		// Arbitrary generator for playlist content
		const playlistContentArb = fc
			.array(
				fc.record({
					duration: fc.double({ min: 1.0, max: 10.0, noNaN: true }),
					segmentNumber: fc.integer({ min: 0, max: 99999 }),
				}),
				{ minLength: 1, maxLength: 20 },
			)
			.map((segments) => {
				const lines = [
					'#EXTM3U',
					'#EXT-X-VERSION:3',
					'#EXT-X-TARGETDURATION:6',
					'#EXT-X-MEDIA-SEQUENCE:0',
					'#EXT-X-PLAYLIST-TYPE:EVENT',
				]

				for (const segment of segments) {
					lines.push(`#EXTINF:${segment.duration.toFixed(3)},`)
					lines.push(
						`segment_${segment.segmentNumber.toString().padStart(5, '0')}.ts`,
					)
				}

				return lines.join('\n')
			})

		void it('should set no-cache headers for all playlist files', async () => {
			await fc.assert(
				fc.asyncProperty(
					portArb,
					profileArb,
					playlistContentArb,
					async (port, profile, content) => {
						const service = new S3UploadService(testConfig)

						// Mock the S3 client send method to capture the command
						let capturedCommand: any
						service['client'].send = mock.fn(async (command: any) => {
							capturedCommand = command
							return {}
						})

						const s3Key = `hls/${port}/${profile}/playlist.m3u8`
						await service.uploadData(Buffer.from(content), s3Key, {
							contentType: 'application/vnd.apple.mpegurl',
						})

						// Verify Cache-Control header
						assert.strictEqual(
							capturedCommand.input.CacheControl,
							CACHE_CONFIGS.PLAYLIST.cacheControl,
							`Playlist ${s3Key} should have no-cache headers`,
						)

						// Verify Expires header
						assert.ok(
							capturedCommand.input.Expires instanceof Date,
							'Expires should be a Date object',
						)
						assert.strictEqual(
							capturedCommand.input.Expires.getTime(),
							new Date('0').getTime(),
							'Expires should be set to epoch 0',
						)

						await service.stop()
					},
				),
				{ numRuns: 100 },
			)
		})

		void it('should set correct content type for all playlist files', async () => {
			await fc.assert(
				fc.asyncProperty(
					portArb,
					profileArb,
					playlistContentArb,
					async (port, profile, content) => {
						const service = new S3UploadService(testConfig)

						// Mock the S3 client send method to capture the command
						let capturedCommand: any
						service['client'].send = mock.fn(async (command: any) => {
							capturedCommand = command
							return {}
						})

						const s3Key = `hls/${port}/${profile}/playlist.m3u8`
						await service.uploadData(Buffer.from(content), s3Key, {
							contentType: 'application/vnd.apple.mpegurl',
						})

						// Verify Content-Type header
						assert.strictEqual(
							capturedCommand.input.ContentType,
							'application/vnd.apple.mpegurl',
							`Playlist ${s3Key} should have correct content type`,
						)

						await service.stop()
					},
				),
				{ numRuns: 100 },
			)
		})

		void it('should set no-cache headers for master playlists', async () => {
			await fc.assert(
				fc.asyncProperty(portArb, playlistContentArb, async (port, content) => {
					const service = new S3UploadService(testConfig)

					// Mock the S3 client send method to capture the command
					let capturedCommand: any
					service['client'].send = mock.fn(async (command: any) => {
						capturedCommand = command
						return {}
					})

					const s3Key = `hls/${port}/master.m3u8`
					await service.uploadData(Buffer.from(content), s3Key, {
						contentType: 'application/vnd.apple.mpegurl',
					})

					// Verify Cache-Control header
					assert.strictEqual(
						capturedCommand.input.CacheControl,
						CACHE_CONFIGS.PLAYLIST.cacheControl,
						`Master playlist ${s3Key} should have no-cache headers`,
					)

					// Verify Expires header
					assert.ok(
						capturedCommand.input.Expires instanceof Date,
						'Expires should be a Date object',
					)

					await service.stop()
				}),
				{ numRuns: 100 },
			)
		})

		void it('should apply no-cache headers regardless of playlist size', async () => {
			await fc.assert(
				fc.asyncProperty(
					portArb,
					profileArb,
					fc.array(fc.uint8Array({ minLength: 0, maxLength: 1000 }), {
						minLength: 1,
						maxLength: 100,
					}),
					async (port, profile, chunks) => {
						const service = new S3UploadService(testConfig)

						// Mock the S3 client send method to capture the command
						let capturedCommand: any
						service['client'].send = mock.fn(async (command: any) => {
							capturedCommand = command
							return {}
						})

						// Create a large playlist by concatenating chunks
						const largeContent = Buffer.concat(chunks)
						const s3Key = `hls/${port}/${profile}/playlist.m3u8`

						await service.uploadData(largeContent, s3Key, {
							contentType: 'application/vnd.apple.mpegurl',
						})

						// Verify Cache-Control header is set regardless of size
						assert.strictEqual(
							capturedCommand.input.CacheControl,
							CACHE_CONFIGS.PLAYLIST.cacheControl,
							`Playlist ${s3Key} of size ${largeContent.length} should have no-cache headers`,
						)

						await service.stop()
					},
				),
				{ numRuns: 50 },
			)
		})
	})

	/**
	 * Property 4: Segment Cache Headers
	 * **Validates: Requirements 7.4, 7.5**
	 *
	 * For any segment file (.ts) uploaded to S3, the Cache-Control header
	 * should be set to `max-age=60, public` and the content type
	 * should be `video/mp2t`.
	 */
	void describe('Property 4: Segment Cache Headers', () => {
		// Arbitrary generator for port numbers
		const portArb = fc.integer({ min: 1024, max: 65535 })

		// Arbitrary generator for profile names
		const profileArb = fc.constantFrom(
			'1080p',
			'720p',
			'480p',
			'360p',
			'240p',
			'144p',
		)

		// Arbitrary generator for segment numbers
		const segmentNumberArb = fc.integer({ min: 0, max: 99999 })

		// Arbitrary generator for segment data (simulating video data)
		const segmentDataArb = fc.uint8Array({ minLength: 100, maxLength: 10000 })

		void it('should set public cache headers for all segment files', async () => {
			await fc.assert(
				fc.asyncProperty(
					portArb,
					profileArb,
					segmentNumberArb,
					segmentDataArb,
					async (port, profile, segmentNumber, data) => {
						const service = new S3UploadService(testConfig)

						// Mock the S3 client send method to capture the command
						let capturedCommand: any
						service['client'].send = mock.fn(async (command: any) => {
							capturedCommand = command
							return {}
						})

						const s3Key = `hls/${port}/${profile}/segment_${segmentNumber.toString().padStart(5, '0')}.ts`
						await service.uploadData(Buffer.from(data), s3Key, {
							contentType: 'video/mp2t',
						})

						// Verify Cache-Control header
						assert.strictEqual(
							capturedCommand.input.CacheControl,
							CACHE_CONFIGS.SEGMENT.cacheControl,
							`Segment ${s3Key} should have public cache headers`,
						)

						// Verify Expires is not set for segments
						assert.strictEqual(
							capturedCommand.input.Expires,
							undefined,
							'Segments should not have Expires header',
						)

						await service.stop()
					},
				),
				{ numRuns: 100 },
			)
		})

		void it('should set correct content type for all segment files', async () => {
			await fc.assert(
				fc.asyncProperty(
					portArb,
					profileArb,
					segmentNumberArb,
					segmentDataArb,
					async (port, profile, segmentNumber, data) => {
						const service = new S3UploadService(testConfig)

						// Mock the S3 client send method to capture the command
						let capturedCommand: any
						service['client'].send = mock.fn(async (command: any) => {
							capturedCommand = command
							return {}
						})

						const s3Key = `hls/${port}/${profile}/segment_${segmentNumber.toString().padStart(5, '0')}.ts`
						await service.uploadData(Buffer.from(data), s3Key, {
							contentType: 'video/mp2t',
						})

						// Verify Content-Type header
						assert.strictEqual(
							capturedCommand.input.ContentType,
							'video/mp2t',
							`Segment ${s3Key} should have correct content type`,
						)

						await service.stop()
					},
				),
				{ numRuns: 100 },
			)
		})

		void it('should set public headers for raw segments', async () => {
			await fc.assert(
				fc.asyncProperty(
					portArb,
					segmentNumberArb,
					segmentDataArb,
					async (port, segmentNumber, data) => {
						const service = new S3UploadService(testConfig)

						// Mock the S3 client send method to capture the command
						let capturedCommand: any
						service['client'].send = mock.fn(async (command: any) => {
							capturedCommand = command
							return {}
						})

						const s3Key = `raw/${port}/segment_${segmentNumber.toString().padStart(5, '0')}.ts`
						await service.uploadData(Buffer.from(data), s3Key, {
							contentType: 'video/mp2t',
						})

						// Verify Cache-Control header
						assert.strictEqual(
							capturedCommand.input.CacheControl,
							CACHE_CONFIGS.SEGMENT.cacheControl,
							`Raw segment ${s3Key} should have public cache headers`,
						)

						await service.stop()
					},
				),
				{ numRuns: 100 },
			)
		})

		void it('should apply public headers regardless of segment size', async () => {
			await fc.assert(
				fc.asyncProperty(
					portArb,
					profileArb,
					segmentNumberArb,
					fc.uint8Array({ minLength: 1, maxLength: 100000 }),
					async (port, profile, segmentNumber, data) => {
						const service = new S3UploadService(testConfig)

						// Mock the S3 client send method to capture the command
						let capturedCommand: any
						service['client'].send = mock.fn(async (command: any) => {
							capturedCommand = command
							return {}
						})

						const s3Key = `hls/${port}/${profile}/segment_${segmentNumber.toString().padStart(5, '0')}.ts`
						await service.uploadData(Buffer.from(data), s3Key, {
							contentType: 'video/mp2t',
						})

						// Verify Cache-Control header is set regardless of size
						assert.strictEqual(
							capturedCommand.input.CacheControl,
							CACHE_CONFIGS.SEGMENT.cacheControl,
							`Segment ${s3Key} of size ${data.length} should have public cache headers`,
						)

						await service.stop()
					},
				),
				{ numRuns: 50 },
			)
		})

		void it('should verify cache-control value is exactly as specified', async () => {
			await fc.assert(
				fc.asyncProperty(
					portArb,
					profileArb,
					segmentNumberArb,
					segmentDataArb,
					async (port, profile, segmentNumber, data) => {
						const service = new S3UploadService(testConfig)

						// Mock the S3 client send method to capture the command
						const sendMock = mock.fn<S3Client['send']>(async () => ({}))
						service['client'].send = sendMock

						const s3Key = `hls/${port}/${profile}/segment_${segmentNumber.toString().padStart(5, '0')}.ts`
						await service.uploadData(Buffer.from(data), s3Key, {
							contentType: 'video/mp2t',
						})

						// Verify exact cache-control value
						assert.strictEqual(
							(
								sendMock.mock.calls[0]?.arguments[0]
									?.input as PutObjectCommandInput
							)?.CacheControl,
							'max-age=60, public',
							'Cache-Control should be exactly "max-age=60, public"',
						)

						// Verify it contains both max-age and public
						assert.ok(
							(
								sendMock.mock.calls[0]?.arguments[0]
									?.input as PutObjectCommandInput
							)?.CacheControl?.includes('max-age=60') ?? false,
							'Cache-Control should include max-age=60',
						)
						assert.ok(
							(
								sendMock.mock.calls[0]?.arguments[0]
									?.input as PutObjectCommandInput
							)?.CacheControl?.includes('public') ?? false,
							'Cache-Control should include public',
						)

						await service.stop()
					},
				),
				{ numRuns: 100 },
			)
		})

		void it('should handle segments with various file path patterns', async () => {
			await fc.assert(
				fc.asyncProperty(
					portArb,
					fc.constantFrom('hls', 'raw', 'archive', 'backup'),
					profileArb,
					segmentNumberArb,
					segmentDataArb,
					async (port, prefix, profile, segmentNumber, data) => {
						const service = new S3UploadService(testConfig)

						// Mock the S3 client send method to capture the command
						let capturedCommand: any
						service['client'].send = mock.fn(async (command: any) => {
							capturedCommand = command
							return {}
						})

						const s3Key = `${prefix}/${port}/${profile}/segment_${segmentNumber.toString().padStart(5, '0')}.ts`
						await service.uploadData(Buffer.from(data), s3Key, {
							contentType: 'video/mp2t',
						})

						// Verify Cache-Control header is set for any .ts file
						assert.strictEqual(
							capturedCommand.input.CacheControl,
							CACHE_CONFIGS.SEGMENT.cacheControl,
							`Segment ${s3Key} should have public cache headers`,
						)

						await service.stop()
					},
				),
				{ numRuns: 100 },
			)
		})
	})
})
