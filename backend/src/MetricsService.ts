import {
	CloudWatchClient,
	PutMetricDataCommand,
} from '@aws-sdk/client-cloudwatch'

export type MetricsServiceConfig = {
	namespace: string
	region?: string
	emitInterval?: number // milliseconds (default: 60000 = 1 minute)
}

export type StreamMetrics = {
	port: number
	packetCount: number
	bytesReceived: number
	packetsLost: number
	lastSequenceNumber?: number
	startTime: Date
	lastUpdateTime: Date
}

export class MetricsService {
	private readonly config: MetricsServiceConfig
	private readonly client: CloudWatchClient
	private readonly streamMetrics: Map<number, StreamMetrics> = new Map()
	private emitTimer?: NodeJS.Timeout

	constructor(config: MetricsServiceConfig) {
		this.config = {
			emitInterval: 60000,
			...config,
		}
		this.client = new CloudWatchClient({
			region: this.config.region ?? process.env.AWS_REGION ?? 'us-east-1',
		})
	}

	start(): void {
		if (this.emitTimer) {
			return
		}

		this.emitTimer = setInterval(() => {
			void this.emitMetrics().catch((err) => {
				console.error('[MetricsService] Error emitting metrics:', err)
			})
		}, this.config.emitInterval)
	}

	stop(): void {
		if (this.emitTimer) {
			clearInterval(this.emitTimer)
			this.emitTimer = undefined
		}
	}

	recordPacket(port: number, dataSize: number, sequenceNumber?: number): void {
		const now = new Date()
		let metrics = this.streamMetrics.get(port)

		if (!metrics) {
			metrics = {
				port,
				packetCount: 0,
				bytesReceived: 0,
				packetsLost: 0,
				lastSequenceNumber: undefined,
				startTime: now,
				lastUpdateTime: now,
			}
			this.streamMetrics.set(port, metrics)
		}

		// Update packet count and bytes
		metrics.packetCount++
		metrics.bytesReceived += dataSize

		// Calculate packet loss if sequence numbers are available
		if (
			sequenceNumber !== undefined &&
			metrics.lastSequenceNumber !== undefined
		) {
			const expectedSequence = metrics.lastSequenceNumber + 1
			if (sequenceNumber > expectedSequence) {
				const lost = sequenceNumber - expectedSequence
				metrics.packetsLost += lost
			}
		}

		metrics.lastSequenceNumber = sequenceNumber
		metrics.lastUpdateTime = now
	}

	getStreamMetrics(port: number): StreamMetrics | undefined {
		return this.streamMetrics.get(port)
	}

	clearStreamMetrics(port: number): void {
		this.streamMetrics.delete(port)
	}

	async emitMetrics(): Promise<void> {
		const metricData: any[] = []
		const now = new Date()

		for (const [port, metrics] of this.streamMetrics.entries()) {
			const durationSeconds =
				(metrics.lastUpdateTime.getTime() - metrics.startTime.getTime()) / 1000

			// Calculate bitrate (bits per second)
			const bitrate =
				durationSeconds > 0 ? (metrics.bytesReceived * 8) / durationSeconds : 0

			// Calculate packet loss rate (percentage)
			const totalPackets = metrics.packetCount + metrics.packetsLost
			const packetLossRate =
				totalPackets > 0 ? (metrics.packetsLost / totalPackets) * 100 : 0

			// Emit bitrate metric
			metricData.push({
				MetricName: 'Bitrate',
				Value: bitrate,
				Unit: 'Bits/Second',
				Timestamp: now,
				Dimensions: [
					{
						Name: 'Port',
						Value: port.toString(),
					},
				],
			})

			// Emit packet loss rate metric
			metricData.push({
				MetricName: 'PacketLossRate',
				Value: packetLossRate,
				Unit: 'Percent',
				Timestamp: now,
				Dimensions: [
					{
						Name: 'Port',
						Value: port.toString(),
					},
				],
			})
		}

		// Emit active stream count
		metricData.push({
			MetricName: 'ActiveStreamCount',
			Value: this.streamMetrics.size,
			Unit: 'Count',
			Timestamp: now,
		})

		// Send metrics to CloudWatch if there's data
		if (metricData.length > 0) {
			const command = new PutMetricDataCommand({
				Namespace: this.config.namespace,
				MetricData: metricData,
			})

			await this.client.send(command)
		}
	}
}
