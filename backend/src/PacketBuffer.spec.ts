import assert from 'node:assert'
import fs from 'node:fs/promises'
import path from 'node:path'
import { after, before, describe, it } from 'node:test'
import { PacketBuffer } from './PacketBuffer.ts'

const TEST_OUTPUT_DIR = '/tmp/packet-buffer-test'

void describe('PacketBuffer', () => {
	before(async () => {
		// Clean up test directory before tests
		await fs.rm(TEST_OUTPUT_DIR, { recursive: true, force: true }).catch(() => {
			// Ignore if directory doesn't exist
		})
	})

	void after(async () => {
		// Clean up test directory after tests
		await fs.rm(TEST_OUTPUT_DIR, { recursive: true, force: true }).catch(() => {
			// Ignore errors
		})
	})

	void describe('Packet buffering', () => {
		void it('should buffer packets in memory', async () => {
			const buffer = new PacketBuffer({
				bufferSize: 1024,
				flushInterval: 10000, // Long interval to prevent auto-flush
				outputDirectory: TEST_OUTPUT_DIR,
			})

			const testData = Buffer.from('test packet 1')
			await buffer.addPacket(5000, testData, new Date())

			// Buffer should not flush yet
			const files = await fs.readdir(TEST_OUTPUT_DIR).catch(() => [])
			assert.strictEqual(files.length, 0)

			await buffer.stop()
		})

		void it('should flush buffer to disk when size threshold reached', async () => {
			const buffer = new PacketBuffer({
				bufferSize: 20, // Small buffer to trigger flush
				flushInterval: 10000,
				outputDirectory: TEST_OUTPUT_DIR,
			})

			// Add packets that together exceed buffer size
			const packet1 = Buffer.from('packet1')
			const packet2 = Buffer.from('packet2_large')

			await buffer.addPacket(5001, packet1, new Date())
			await buffer.addPacket(5001, packet2, new Date()) // This should trigger flush

			// Wait a moment for async flush to complete
			await new Promise((resolve) => setTimeout(resolve, 50))

			// Buffer should have flushed
			const files = await fs.readdir(TEST_OUTPUT_DIR)
			assert.ok(files.length > 0)

			const flushedFile = files.find((f) => f.includes('port_5001'))
			assert.ok(
				flushedFile !== undefined,
				'Expected to find a file for port 5001',
			)

			await buffer.stop()
		})

		void it('should flush buffer to disk on time interval', async () => {
			const buffer = new PacketBuffer({
				bufferSize: 1024,
				flushInterval: 100, // Short interval to trigger flush
				outputDirectory: TEST_OUTPUT_DIR,
			})

			const testData = Buffer.from('test packet')
			await buffer.addPacket(5002, testData, new Date())

			// Wait for flush interval
			await new Promise((resolve) => setTimeout(resolve, 150))

			const files = await fs.readdir(TEST_OUTPUT_DIR)
			const flushedFile = files.find((f) => f.includes('port_5002'))
			assert.ok(flushedFile !== undefined)

			await buffer.stop()
		})

		void it('should combine multiple packets into single file', async () => {
			const buffer = new PacketBuffer({
				bufferSize: 1024,
				flushInterval: 10000,
				outputDirectory: TEST_OUTPUT_DIR,
			})

			const packet1 = Buffer.from('packet 1')
			const packet2 = Buffer.from('packet 2')
			const packet3 = Buffer.from('packet 3')

			await buffer.addPacket(5003, packet1, new Date())
			await buffer.addPacket(5003, packet2, new Date())
			await buffer.addPacket(5003, packet3, new Date())

			await buffer.flush(5003)

			const files = await fs.readdir(TEST_OUTPUT_DIR)
			const flushedFile = files.find((f) => f.includes('port_5003'))
			assert.ok(flushedFile !== undefined)

			if (flushedFile !== undefined) {
				const content = await fs.readFile(
					path.join(TEST_OUTPUT_DIR, flushedFile),
				)
				const expected = Buffer.concat([packet1, packet2, packet3])
				assert.deepStrictEqual(content, expected)
			}

			await buffer.stop()
		})
	})

	void describe('Buffer overflow handling', () => {
		void it('should drop oldest packets on buffer overflow (FIFO)', async () => {
			const buffer = new PacketBuffer({
				bufferSize: 30, // Small buffer
				flushInterval: 10000,
				outputDirectory: TEST_OUTPUT_DIR,
			})

			const packet1 = Buffer.from('packet1') // 7 bytes
			const packet2 = Buffer.from('packet2') // 7 bytes
			const packet3 = Buffer.from('packet3') // 7 bytes
			const packet4 = Buffer.from('packet4_large_data') // 18 bytes - will cause overflow

			await buffer.addPacket(5004, packet1, new Date())
			await buffer.addPacket(5004, packet2, new Date())
			await buffer.addPacket(5004, packet3, new Date())
			await buffer.addPacket(5004, packet4, new Date()) // This should trigger overflow

			await buffer.flush(5004)

			const files = await fs.readdir(TEST_OUTPUT_DIR)
			const flushedFile = files.find((f) => f.includes('port_5004'))
			assert.ok(flushedFile !== undefined)

			if (flushedFile !== undefined) {
				const content = await fs.readFile(
					path.join(TEST_OUTPUT_DIR, flushedFile),
				)

				// Should contain packet4 and possibly packet2/packet3, but not packet1 (oldest)
				assert.ok(!content.includes(packet1))
				assert.ok(content.includes(packet4))
			}

			await buffer.stop()
		})
	})

	void describe('Multiple ports', () => {
		void it('should handle multiple ports independently', async () => {
			const buffer = new PacketBuffer({
				bufferSize: 1024,
				flushInterval: 10000,
				outputDirectory: TEST_OUTPUT_DIR,
			})

			await buffer.addPacket(5005, Buffer.from('port 5005 data'), new Date())
			await buffer.addPacket(5006, Buffer.from('port 5006 data'), new Date())

			await buffer.flushAll()

			const files = await fs.readdir(TEST_OUTPUT_DIR)
			const port5005File = files.find((f) => f.includes('port_5005'))
			const port5006File = files.find((f) => f.includes('port_5006'))

			assert.ok(port5005File !== undefined)
			assert.ok(port5006File !== undefined)

			await buffer.stop()
		})
	})

	void describe('Filename generation', () => {
		void it('should generate filenames with timestamp and port', async () => {
			const buffer = new PacketBuffer({
				bufferSize: 1024,
				flushInterval: 10000,
				outputDirectory: TEST_OUTPUT_DIR,
			})

			const timestamp = new Date('2024-01-15T14:30:45Z')
			await buffer.addPacket(5007, Buffer.from('test'), timestamp)
			await buffer.flush(5007)

			const files = await fs.readdir(TEST_OUTPUT_DIR)
			const generatedFile = files.find((f) => f.includes('port_5007'))

			assert.ok(generatedFile !== undefined)
			if (generatedFile !== undefined) {
				assert.ok(generatedFile.includes('port_5007'))
				assert.ok(generatedFile.endsWith('.ts'))
			}

			await buffer.stop()
		})
	})
})
