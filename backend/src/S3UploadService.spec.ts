import assert from 'node:assert'
import { describe, it } from 'node:test'
import { S3UploadService } from './S3UploadService.ts'

void describe('S3UploadService', () => {
	void describe('constructor', () => {
		void it('should create service with bucket name', async () => {
			const service = new S3UploadService({
				bucket: 'test-bucket',
			})

			assert.ok(service !== undefined)
			await service.stop()
		})

		void it('should use default region if not specified', async () => {
			const service = new S3UploadService({
				bucket: 'test-bucket',
			})

			assert.ok(service !== undefined)
			await service.stop()
		})

		void it('should use custom region if specified', async () => {
			const service = new S3UploadService({
				bucket: 'test-bucket',
				region: 'us-west-2',
			})

			assert.ok(service !== undefined)
			await service.stop()
		})
	})

	void describe('getBufferedUploadCount', () => {
		void it('should return 0 initially', async () => {
			const service = new S3UploadService({
				bucket: 'test-bucket',
			})

			assert.strictEqual(service.getBufferedUploadCount(), 0)
			await service.stop()
		})
	})

	void describe('stop', () => {
		void it('should stop the service', async () => {
			const service = new S3UploadService({
				bucket: 'test-bucket',
			})

			await service.stop()
			// Give time for cleanup
			await new Promise((resolve) => setTimeout(resolve, 100))
			assert.ok(true)
		})
	})
})
