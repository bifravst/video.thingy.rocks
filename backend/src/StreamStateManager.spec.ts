import assert from 'node:assert'
import { describe, it } from 'node:test'
import { StreamStateManager } from './StreamStateManager.ts'

void describe('StreamStateManager', () => {
	void describe('Stream state transitions', () => {
		void it('should detect stream start on first packet', () => {
			const manager = new StreamStateManager({ inactivityTimeout: 60000 })

			let streamStarted = false
			let startedPort = 0

			manager.on('streamStart', (port: number) => {
				streamStarted = true
				startedPort = port
			})

			manager.onPacketReceived(5000, new Date())

			assert.strictEqual(streamStarted, true)
			assert.strictEqual(startedPort, 5000)

			const state = manager.getStreamState(5000)
			assert.ok(state !== undefined)
			if (state !== undefined) {
				assert.strictEqual(state.status, 'active')
				assert.strictEqual(state.packetCount, 1)
			}

			manager.stop()
		})

		void it('should detect stream stop after inactivity timeout', async () => {
			const manager = new StreamStateManager({ inactivityTimeout: 100 }) // 100ms timeout

			let streamStopped = false
			let stoppedPort = 0

			manager.on('streamStop', (port: number) => {
				streamStopped = true
				stoppedPort = port
			})

			manager.onPacketReceived(5001, new Date())

			// Wait for inactivity timeout
			await new Promise((resolve) => setTimeout(resolve, 150))

			assert.strictEqual(streamStopped, true)
			assert.strictEqual(stoppedPort, 5001)

			const state = manager.getStreamState(5001)
			assert.ok(state !== undefined)
			if (state !== undefined) {
				assert.strictEqual(state.status, 'inactive')
			}

			manager.stop()
		})

		void it('should detect stream resume after being inactive', async () => {
			const manager = new StreamStateManager({ inactivityTimeout: 100 })

			let streamResumed = false

			manager.on('streamResume', () => {
				streamResumed = true
			})

			// Start stream
			manager.onPacketReceived(5002, new Date())

			// Wait for inactivity
			await new Promise((resolve) => setTimeout(resolve, 150))

			const stateAfterTimeout = manager.getStreamState(5002)
			assert.ok(stateAfterTimeout !== undefined)
			if (stateAfterTimeout !== undefined) {
				assert.strictEqual(stateAfterTimeout.status, 'inactive')
			}

			// Resume stream
			manager.onPacketReceived(5002, new Date())

			assert.strictEqual(streamResumed, true)

			const stateAfterResume = manager.getStreamState(5002)
			assert.ok(stateAfterResume !== undefined)
			if (stateAfterResume !== undefined) {
				assert.strictEqual(stateAfterResume.status, 'active')
			}

			manager.stop()
		})

		void it('should track packet count correctly', () => {
			const manager = new StreamStateManager({ inactivityTimeout: 60000 })

			manager.onPacketReceived(5003, new Date())
			manager.onPacketReceived(5003, new Date())
			manager.onPacketReceived(5003, new Date())

			const state = manager.getStreamState(5003)
			assert.ok(state !== undefined)
			if (state !== undefined) {
				assert.strictEqual(state.packetCount, 3)
			}

			manager.stop()
		})
	})

	void describe('Active stream tracking', () => {
		void it('should return correct active stream count', () => {
			const manager = new StreamStateManager({ inactivityTimeout: 60000 })

			manager.onPacketReceived(5000, new Date())
			manager.onPacketReceived(5001, new Date())
			manager.onPacketReceived(5002, new Date())

			assert.strictEqual(manager.getActiveStreamCount(), 3)

			const activeStreams = manager.getActiveStreams()
			assert.strictEqual(activeStreams.length, 3)
			assert.ok(activeStreams.includes(5000))
			assert.ok(activeStreams.includes(5001))
			assert.ok(activeStreams.includes(5002))

			manager.stop()
		})

		void it('should not count inactive streams in active count', async () => {
			const manager = new StreamStateManager({ inactivityTimeout: 100 })

			manager.onPacketReceived(5000, new Date())
			manager.onPacketReceived(5001, new Date())

			assert.strictEqual(manager.getActiveStreamCount(), 2)

			// Wait for one stream to become inactive
			await new Promise((resolve) => setTimeout(resolve, 150))

			assert.strictEqual(manager.getActiveStreamCount(), 0)

			manager.stop()
		})
	})

	void describe('Multiple concurrent streams', () => {
		void it('should handle multiple streams independently', () => {
			const manager = new StreamStateManager({ inactivityTimeout: 60000 })

			const timestamp1 = new Date('2024-01-01T10:00:00Z')
			const timestamp2 = new Date('2024-01-01T10:01:00Z')

			manager.onPacketReceived(5000, timestamp1)
			manager.onPacketReceived(5001, timestamp2)

			const state1 = manager.getStreamState(5000)
			const state2 = manager.getStreamState(5001)

			assert.ok(state1 !== undefined)
			assert.ok(state2 !== undefined)
			if (state1 !== undefined && state2 !== undefined) {
				assert.strictEqual(state1.port, 5000)
				assert.strictEqual(state2.port, 5001)
				assert.strictEqual(
					state1.lastPacketTime?.toISOString(),
					timestamp1.toISOString(),
				)
				assert.strictEqual(
					state2.lastPacketTime?.toISOString(),
					timestamp2.toISOString(),
				)
			}

			manager.stop()
		})
	})
})
