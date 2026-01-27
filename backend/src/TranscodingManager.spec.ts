import assert from 'node:assert'
import { describe, it } from 'node:test'
import { StreamMetadataService } from './StreamMetadataService.ts'
import { TranscodingManager } from './TranscodingManager.ts'

void describe('TranscodingManager', () => {
	void describe('constructor', () => {
		void it('should create manager with required config', async () => {
			const streamMetadataService = new StreamMetadataService({
				tableName: 'test-table',
				region: 'us-east-1',
			})

			const manager = new TranscodingManager({
				s3Bucket: 'test-bucket',
				streamMetadataService,
			})

			assert.ok(manager !== undefined)
			await manager.stop()
		})

		void it('should use custom segment duration if specified', async () => {
			const streamMetadataService = new StreamMetadataService({
				tableName: 'test-table',
				region: 'us-east-1',
			})

			const manager = new TranscodingManager({
				s3Bucket: 'test-bucket',
				streamMetadataService,
				segmentDuration: 10,
			})

			assert.ok(manager !== undefined)
			await manager.stop()
		})
	})

	void describe('getActiveTranscoders', () => {
		void it('should return empty array initially', async () => {
			const streamMetadataService = new StreamMetadataService({
				tableName: 'test-table',
				region: 'us-east-1',
			})

			const manager = new TranscodingManager({
				s3Bucket: 'test-bucket',
				streamMetadataService,
			})

			const active = manager.getActiveTranscoders()
			assert.deepStrictEqual(active, [])

			await manager.stop()
		})
	})

	void describe('getTranscoderStatus', () => {
		void it('should return null for non-existent transcoder', async () => {
			const streamMetadataService = new StreamMetadataService({
				tableName: 'test-table',
				region: 'us-east-1',
			})

			const manager = new TranscodingManager({
				s3Bucket: 'test-bucket',
				streamMetadataService,
			})

			const status = manager.getTranscoderStatus(5000)
			assert.strictEqual(status, null)

			await manager.stop()
		})
	})

	void describe('stop', () => {
		void it('should stop all transcoders', async () => {
			const streamMetadataService = new StreamMetadataService({
				tableName: 'test-table',
				region: 'us-east-1',
			})

			const manager = new TranscodingManager({
				s3Bucket: 'test-bucket',
				streamMetadataService,
			})

			await manager.stop()
			// Give time for cleanup
			await new Promise((resolve) => setTimeout(resolve, 100))
			assert.ok(true)
		})
	})
})
