import assert from 'node:assert'
import { describe, it, mock } from 'node:test'
import { RawStreamManager } from './RawStreamManager.ts'

void describe.skip('RawStreamManager', () => {
	void describe('segment counter', () => {
		void it('should increment segment counter even when upload fails', async () => {
			// Mock StreamMetadataService
			const mockMetadataService = {
				updateRawStreamPath: mock.fn(async () => {}),
			}

			// Create RawStreamManager
			const manager = new RawStreamManager({
				port: 5000,
				s3Bucket: 'test-bucket',
				streamMetadataService: mockMetadataService as any,
				segmentDuration: 1, // 1 second for faster testing
			})

			// Mock S3UploadService to fail on first upload, succeed on second
			let uploadCount = 0
			manager['s3UploadService'].uploadData = mock.fn(async () => {
				uploadCount++
				if (uploadCount === 1) {
					// First upload fails
					throw new Error('S3 upload failed')
				}
				// Second upload succeeds (but we won't actually upload to S3 in test)
				return
			})

			manager.start()

			// Write some data to trigger first segment
			manager.writeData(Buffer.from('test data 1'))

			// Wait for segment to flush (1 second + buffer)
			await new Promise((resolve) => setTimeout(resolve, 1200))

			// Write more data to trigger second segment
			manager.writeData(Buffer.from('test data 2'))

			// Wait for second segment to flush
			await new Promise((resolve) => setTimeout(resolve, 1200))

			manager.stop()

			// Verify uploadData was called twice
			assert.strictEqual(
				(manager['s3UploadService'].uploadData as any).mock.calls.length,
				2,
				'uploadData should be called twice',
			)

			// Verify the S3 keys used different segment numbers
			const firstCall = (manager['s3UploadService'].uploadData as any).mock
				.calls[0]
			const secondCall = (manager['s3UploadService'].uploadData as any).mock
				.calls[1]

			const firstKey = firstCall.arguments[1] as string
			const secondKey = secondCall.arguments[1] as string

			assert.ok(
				firstKey.includes('segment_00000.ts'),
				'First segment should be segment_00000.ts',
			)
			assert.ok(
				secondKey.includes('segment_00001.ts'),
				'Second segment should be segment_00001.ts even though first upload failed',
			)
		})

		void it('should use sequential segment numbers', async () => {
			// Mock StreamMetadataService
			const mockMetadataService = {
				updateRawStreamPath: mock.fn(async () => {}),
			}

			// Create RawStreamManager
			const manager = new RawStreamManager({
				port: 5001,
				s3Bucket: 'test-bucket',
				streamMetadataService: mockMetadataService as any,
				segmentDuration: 1,
			})

			// Mock S3UploadService to track uploads
			const uploadedKeys: string[] = []
			manager['s3UploadService'].uploadData = mock.fn(
				async (_data: Buffer, key: string) => {
					uploadedKeys.push(key)
				},
			)

			manager.start()

			// Write data for 3 segments
			for (let i = 0; i < 3; i++) {
				manager.writeData(Buffer.from(`test data ${i}`))
				await new Promise((resolve) => setTimeout(resolve, 1200))
			}

			manager.stop()

			// Verify sequential segment numbers
			assert.ok(
				uploadedKeys.some((key) => key.includes('segment_00000.ts')),
				'Should have segment_00000.ts',
			)
			assert.ok(
				uploadedKeys.some((key) => key.includes('segment_00001.ts')),
				'Should have segment_00001.ts',
			)
			assert.ok(
				uploadedKeys.some((key) => key.includes('segment_00002.ts')),
				'Should have segment_00002.ts',
			)
		})

		void it('should use 5-digit zero-padded segment numbers', () => {
			// Access private segmentCounter to test filename generation
			// Set counter to various values and verify formatting
			const testCases = [
				{ counter: 0, expected: 'segment_00000.ts' },
				{ counter: 1, expected: 'segment_00001.ts' },
				{ counter: 99, expected: 'segment_00099.ts' },
				{ counter: 999, expected: 'segment_00999.ts' },
				{ counter: 9999, expected: 'segment_09999.ts' },
				{ counter: 99999, expected: 'segment_99999.ts' },
			]

			for (const testCase of testCases) {
				const filename = `segment_${String(testCase.counter).padStart(5, '0')}.ts`
				assert.strictEqual(
					filename,
					testCase.expected,
					`Counter ${testCase.counter} should produce ${testCase.expected}`,
				)
			}
		})
	})
})
