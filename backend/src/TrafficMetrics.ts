import {
	CloudWatchClient,
	PutMetricDataCommand,
} from '@aws-sdk/client-cloudwatch'
import { Logger } from './Logger.ts'
import {
	RECEIVED_BYTES_METRIC,
	SERVING_METRIC,
	SRTP_TRANSPORT,
	TRANSPORT_DIMENSION,
	UNENCRYPTED_TRANSPORT,
	trafficMetricNamespace,
} from './TrafficMetricNames.ts'

/**
 * Publishes the per-transport traffic metrics the stack's zero-ingestion alarms read.
 *
 * Two metrics, one dimension:
 *
 * - `ReceivedBytes`, summed by the alarm over a minute: the per-transport
 *   counterpart to the load balancer's ProcessedBytes_UDP, which AWS only
 *   publishes per load balancer, so it cannot tell one transport's ports from
 *   another's - and an alarm that asks "is anything arriving anywhere?" against
 *   "is this transport reaching Kinesis?" is decided by the wrong transport.
 * - `TransportServing`, minimum over a minute: 1 while the transport is bound and
 *   serving, 0 while it is not. A transport that never bound receives nothing, so
 *   its byte count is a steady zero that is indistinguishable from an idle
 *   transport - and "configured but never bound" is exactly the failure the
 *   additive-transport isolation exists to survive, so it is the one failure no
 *   traffic-gated alarm can see.
 *
 * Both are published every interval - including explicit zeros - so that a gap in
 * the data means "this instance is not reporting", which is the health check's
 * problem, rather than "no traffic", which would quietly suppress alerts.
 *
 * Everything here is best effort: metrics keep the alarms honest, but losing one
 * must never take ingest down.
 */
export class TrafficMetrics {
	private readonly client: CloudWatchClient | undefined
	private readonly namespace: string
	private readonly logger = new Logger('TrafficMetrics')
	private readonly received = new Map<string, number>()
	private readonly serving = new Map<string, boolean>()
	private readonly intervalMs = 60_000
	private timer: NodeJS.Timeout | undefined
	private publishCount = 0

	constructor(config: { region: string; stackName?: string }) {
		if (config.stackName === undefined || config.stackName === '') {
			this.logger.warn(
				'STACK_NAME is not set; the per-transport traffic metrics will not be published and the zero-ingestion alarms will see no data',
			)
			this.client = undefined
		} else {
			this.client = new CloudWatchClient({ region: config.region })
		}
		this.namespace = trafficMetricNamespace(config.stackName ?? 'local')
		for (const transport of [UNENCRYPTED_TRANSPORT, SRTP_TRANSPORT]) {
			this.received.set(transport, 0)
			this.serving.set(transport, false)
		}
	}

	start(): void {
		if (this.timer !== undefined || this.client === undefined) return
		void this.publish()
		this.timer = setInterval(() => {
			void this.publish()
		}, this.intervalMs)
		this.timer.unref?.()
	}

	async stop(): Promise<void> {
		if (this.timer !== undefined) {
			clearInterval(this.timer)
			this.timer = undefined
		}
		return this.publish()
	}

	recordReceived(transport: string, bytes: number): void {
		this.received.set(transport, (this.received.get(transport) ?? 0) + bytes)
	}

	setServing(transport: string, serving: boolean): void {
		this.serving.set(transport, serving)
	}

	private async publish(): Promise<void> {
		if (this.client === undefined) return
		const now = new Date()
		try {
			await this.client.send(
				new PutMetricDataCommand({
					Namespace: this.namespace,
					MetricData: [...this.received.entries()].flatMap(
						([transport, bytes]) => [
							{
								MetricName: RECEIVED_BYTES_METRIC,
								Dimensions: [{ Name: TRANSPORT_DIMENSION, Value: transport }],
								Timestamp: now,
								Value: bytes,
								Unit: 'Bytes',
							},
							{
								MetricName: SERVING_METRIC,
								Dimensions: [{ Name: TRANSPORT_DIMENSION, Value: transport }],
								Timestamp: now,
								Value: (this.serving.get(transport) ?? false) ? 1 : 0,
								Unit: 'None',
							},
						],
					),
				}),
			)
			this.publishCount++
			// Zero the received counters after every publish, so each datapoint is the
			// bytes of its own interval and the alarm's Sum is that interval's traffic.
			for (const [transport] of this.received) this.received.set(transport, 0)
		} catch (err) {
			// Best effort by design, but a metric that silently never publishes makes
			// the alarms watch nothing, so it is logged and counted.
			this.logger.error(
				'Could not publish traffic metrics',
				err instanceof Error ? err : new Error(String(err)),
			)
		}
	}
}
