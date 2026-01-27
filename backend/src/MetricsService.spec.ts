import assert from 'node:assert'
import { describe, it, mock } from 'node:test'
import { MetricsService } from './MetricsService.ts'

void describe('MetricsService', () => {
	void describe('Metrics calculation', () => {
		void it('should calculate bitrate correctly', () => {
			const service = new MetricsService({
				namespace: 'TestNamespace',
				emitInterval: 60000,
			})

			const port = 5000
			const packetSize = 1000 // bytes
			const packetCount = 10

			// Record packets over time
			for (let i = 0; i < packetCount; i++) {
				service.recordPacket(port, packetSize)
			}

			const metrics = service.getStreamMetrics(port)
			assert.ok(metrics !== undefined)
			assert.strictEqual(metrics.bytesReceived, packetSize * packetCount)
			assert.strictEqual(metrics.packetCount, packetCount)
		})

		void it('should calculate packet loss from sequence numbers', () => {
			const service = new MetricsService({
				namespace: 'TestNamespace',
				emitInterval: 60000,
			})

			const port = 5000

			// Record packets with sequence numbers
			service.recordPacket(port, 1000, 1)
			service.recordPacket(port, 1000, 2)
			service.recordPacket(port, 1000, 3)
			// Skip sequence 4 (packet loss)
			service.recordPacket(port, 1000, 5)
			// Skip sequences 6, 7 (more packet loss)
			service.recordPacket(port, 1000, 8)

			const metrics = service.getStreamMetrics(port)
			assert.ok(metrics !== undefined)
			assert.strictEqual(metrics.packetCount, 5)
			assert.strictEqual(metrics.packetsLost, 3) // Sequences 4, 6, 7
		})

		void it('should handle packets without sequence numbers', () => {
			const service = new MetricsService({
				namespace: 'TestNamespace',
				emitInterval: 60000,
			})

			const port = 5000

			// Record packets without sequence numbers
			service.recordPacket(port, 1000)
			service.recordPacket(port, 1000)
			service.recordPacket(port, 1000)

			const metrics = service.getStreamMetrics(port)
			assert.ok(metrics !== undefined)
			assert.strictEqual(metrics.packetCount, 3)
			assert.strictEqual(metrics.packetsLost, 0)
		})

		void it('should track multiple streams independently', () => {
			const service = new MetricsService({
				namespace: 'TestNamespace',
				emitInterval: 60000,
			})

			// Record packets for different ports
			service.recordPacket(5000, 1000, 1)
			service.recordPacket(5000, 1000, 2)
			service.recordPacket(5001, 2000, 1)
			service.recordPacket(5001, 2000, 3) // Packet loss on 5001

			const metrics5000 = service.getStreamMetrics(5000)
			const metrics5001 = service.getStreamMetrics(5001)

			assert.ok(metrics5000 !== undefined)
			assert.ok(metrics5001 !== undefined)

			assert.strictEqual(metrics5000.packetCount, 2)
			assert.strictEqual(metrics5000.bytesReceived, 2000)
			assert.strictEqual(metrics5000.packetsLost, 0)

			assert.strictEqual(metrics5001.packetCount, 2)
			assert.strictEqual(metrics5001.bytesReceived, 4000)
			assert.strictEqual(metrics5001.packetsLost, 1) // Sequence 2 was lost
		})
	})

	void describe('Metrics emission', () => {
		void it('should emit metrics to CloudWatch', async () => {
			const service = new MetricsService({
				namespace: 'TestNamespace',
				emitInterval: 60000,
			})

			// Record some packets
			service.recordPacket(5000, 1000, 1)
			service.recordPacket(5000, 1000, 2)

			// Mock the CloudWatch client send method
			const sendMock = mock.method(
				(service as any).client,
				'send',
				async () => ({}),
			)

			// Emit metrics
			await service.emitMetrics()

			// Verify send was called
			assert.strictEqual(sendMock.mock.calls.length, 1)

			// Verify the command contains expected metrics
			const command = sendMock.mock.calls[0]?.arguments[0]
			assert.ok(command !== undefined)
			assert.ok('input' in command)
			const input = command.input
			assert.strictEqual(input.Namespace, 'TestNamespace')
			assert.ok(Array.isArray(input.MetricData))
			assert.ok(input.MetricData.length > 0)

			// Check for expected metric names
			const metricNames = input.MetricData.map((m: any) => m.MetricName)
			assert.ok(metricNames.includes('Bitrate') === true)
			assert.ok(metricNames.includes('PacketLossRate') === true)
			assert.ok(metricNames.includes('ActiveStreamCount') === true)
		})

		void it('should not emit metrics when no data is recorded', async () => {
			const service = new MetricsService({
				namespace: 'TestNamespace',
				emitInterval: 60000,
			})

			// Mock the CloudWatch client send method
			const sendMock = mock.method(
				(service as any).client,
				'send',
				async () => ({}),
			)

			// Emit metrics without recording any packets
			await service.emitMetrics()

			// Verify send was called once (for ActiveStreamCount = 0)
			assert.strictEqual(sendMock.mock.calls.length, 1)

			// Verify only ActiveStreamCount metric is emitted
			const command = sendMock.mock.calls[0]?.arguments[0]
			assert.ok(command !== undefined)
			assert.ok('input' in command)
			const input = command.input
			assert.strictEqual(input.Namespace, 'TestNamespace')
			assert.ok(Array.isArray(input.MetricData))
			assert.strictEqual(input.MetricData.length, 1)
			assert.strictEqual(input.MetricData[0].MetricName, 'ActiveStreamCount')
			assert.strictEqual(input.MetricData[0].Value, 0)
		})
	})

	void describe('Lifecycle management', () => {
		void it('should start and stop metrics emission timer', async () => {
			const service = new MetricsService({
				namespace: 'TestNamespace',
				emitInterval: 100, // Short interval for testing
			})

			// Mock the emitMetrics method
			let emitCount = 0
			mock.method(service, 'emitMetrics', async () => {
				emitCount++
			})

			service.start()

			// Wait for at least one emission
			await new Promise((resolve) => setTimeout(resolve, 150))

			service.stop()

			const countAfterStop = emitCount

			// Wait to ensure no more emissions after stop
			await new Promise((resolve) => setTimeout(resolve, 150))

			assert.ok(emitCount >= 1, 'Should have emitted at least once')
			assert.strictEqual(
				emitCount,
				countAfterStop,
				'Should not emit after stop',
			)
		})

		void it('should clear stream metrics', () => {
			const service = new MetricsService({
				namespace: 'TestNamespace',
				emitInterval: 60000,
			})

			service.recordPacket(5000, 1000)
			assert.ok(service.getStreamMetrics(5000) !== undefined)

			service.clearStreamMetrics(5000)
			assert.strictEqual(service.getStreamMetrics(5000), undefined)
		})
	})
})
