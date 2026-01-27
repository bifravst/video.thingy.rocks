import assert from 'node:assert'
import dgram from 'node:dgram'
import { describe, it } from 'node:test'
import { UDPListener, type PacketHandler } from './UDPListener.ts'

void describe('UDPListener', () => {
	void describe('Port binding on startup', () => {
		void it('should bind to all ports in the configured range', async () => {
			const listener = new UDPListener({
				portRange: { start: 5000, end: 5002 },
				bufferSize: 1024,
				flushInterval: 1000,
				outputDirectory: '/tmp/test',
			})

			await listener.start()
			const activeStreams = listener.getActiveStreams()

			assert.strictEqual(activeStreams.length, 3)
			assert.ok(activeStreams.includes(5000))
			assert.ok(activeStreams.includes(5001))
			assert.ok(activeStreams.includes(5002))

			await listener.stop()
		})

		void it('should handle port binding failures with retry', async () => {
			// First listener occupies the port
			const blocker = new UDPListener({
				portRange: { start: 5003, end: 5003 },
				bufferSize: 1024,
				flushInterval: 1000,
				outputDirectory: '/tmp/test',
			})
			await blocker.start()

			// Second listener tries to bind to same port - should fail after retries
			const listener = new UDPListener({
				portRange: { start: 5003, end: 5003 },
				bufferSize: 1024,
				flushInterval: 1000,
				outputDirectory: '/tmp/test',
			})

			await assert.rejects(
				async () => await listener.start(),
				(err: Error) => {
					const hasError =
						err.message.includes('EADDRINUSE') ||
						('code' in err && err.code === 'EADDRINUSE')
					assert.ok(hasError)
					return true
				},
			)

			await blocker.stop()
		})
	})

	void describe('Packet reception and buffering', () => {
		void it('should receive UDP packets and call packet handler', async () => {
			const listener = new UDPListener({
				portRange: { start: 5004, end: 5004 },
				bufferSize: 1024,
				flushInterval: 1000,
				outputDirectory: '/tmp/test',
			})

			let receivedPackets = 0
			let receivedPort = 0
			let receivedData: Buffer | null = null

			const handler: PacketHandler = {
				onPacket: async (port, data) => {
					receivedPackets++
					receivedPort = port
					receivedData = data
				},
				onStreamStart: async () => Promise.resolve(),
				onStreamStop: async () => Promise.resolve(),
			}

			listener.setPacketHandler(handler)
			await listener.start()

			// Send a test packet
			const client = dgram.createSocket('udp4')
			const testData = Buffer.from('test packet data')

			await new Promise<void>((resolve, reject) => {
				client.send(testData, 5004, 'localhost', (err) => {
					if (err !== null && err !== undefined) {
						reject(err)
					} else {
						resolve()
					}
				})
			})

			// Wait for packet to be received
			await new Promise((resolve) => setTimeout(resolve, 100))

			assert.strictEqual(receivedPackets, 1)
			assert.strictEqual(receivedPort, 5004)
			assert.ok(receivedData !== null)
			if (receivedData !== null) {
				assert.strictEqual(
					(receivedData as Buffer).toString(),
					'test packet data',
				)
			}

			client.close()
			await listener.stop()
		})

		void it('should discard malformed packets (empty packets)', async () => {
			const listener = new UDPListener({
				portRange: { start: 5005, end: 5005 },
				bufferSize: 1024,
				flushInterval: 1000,
				outputDirectory: '/tmp/test',
			})

			let receivedPackets = 0

			const handler: PacketHandler = {
				onPacket: async () => {
					receivedPackets++
				},
				onStreamStart: async () => Promise.resolve(),
				onStreamStop: async () => Promise.resolve(),
			}

			listener.setPacketHandler(handler)
			await listener.start()

			// Send an empty packet
			const client = dgram.createSocket('udp4')
			const emptyData = Buffer.alloc(0)

			await new Promise<void>((resolve, reject) => {
				client.send(emptyData, 5005, 'localhost', (err) => {
					if (err !== null && err !== undefined) {
						reject(err)
					} else {
						resolve()
					}
				})
			})

			// Wait for packet processing
			await new Promise((resolve) => setTimeout(resolve, 100))

			// Empty packet should be discarded, so no packets received
			assert.strictEqual(receivedPackets, 0)

			client.close()
			await listener.stop()
		})
	})

	void describe('Graceful shutdown', () => {
		void it('should close all sockets on stop', async () => {
			const listener = new UDPListener({
				portRange: { start: 5006, end: 5008 },
				bufferSize: 1024,
				flushInterval: 1000,
				outputDirectory: '/tmp/test',
			})

			await listener.start()
			assert.strictEqual(listener.getActiveStreams().length, 3)

			await listener.stop()
			assert.strictEqual(listener.getActiveStreams().length, 0)
		})
	})
})
