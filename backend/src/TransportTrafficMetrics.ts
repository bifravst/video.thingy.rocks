import {
	CloudWatchClient,
	PutMetricDataCommand,
	type PutMetricDataCommandInput,
} from '@aws-sdk/client-cloudwatch'

import { Logger } from './Logger.ts'
import {
	RECEIVED_BYTES_METRIC,
	SERVING_METRIC,
	TRANSPORT_DIMENSION,
	trafficMetricNamespace,
} from './TrafficMetricNames.ts'

/** How often the counters are published, matching the alarms' one-minute period. */
const DEFAULT_INTERVAL_MS = 60_000

export type TransportTrafficSample = {
	transport: string
	bytes: number
	/** 1 while this transport's listener is serving, 0 while it is not. */
	serving: number
	at: Date
}

export type TransportTrafficPublisher = (
	samples: TransportTrafficSample[],
) => Promise<void>

export type TransportTrafficMetricsOptions = {
	/** Every transport to report on, whether or not it has seen traffic. */
	transports: string[]
	publish: TransportTrafficPublisher
	intervalMs?: number
	logger?: Logger
}

/**
 * Counts received bytes per transport and reports them to CloudWatch every minute.
 *
 * This exists because the restart automation needs to know whether traffic is arriving
 * on *one* transport. The load balancer's ProcessedBytes_UDP is published per load
 * balancer only, so it cannot tell 5000-5009 from 6000-6009: an alarm built on it
 * compares "is anything arriving anywhere" against "is this transport reaching
 * Kinesis", and the answer changes with traffic on the other transport.
 *
 * A zero is published for a transport that saw nothing, which is the property the
 * alarms depend on. Publishing only non-zero samples would make "no devices are
 * sending" and "the backend is not running" the same observation - a gap in the metric
 * - and an alarm cannot then treat missing data as a fault without firing on an idle
 * fleet.
 */
export class TransportTrafficMetrics {
	private readonly options: TransportTrafficMetricsOptions
	private readonly logger: Logger
	private readonly bytes = new Map<string, number>()
	private readonly serving = new Set<string>()
	private timer?: NodeJS.Timeout

	constructor(options: TransportTrafficMetricsOptions) {
		this.options = options
		this.logger = options.logger ?? new Logger('TransportTrafficMetrics')
		for (const transport of options.transports) this.bytes.set(transport, 0)
	}

	/**
	 * Adds a datagram to a transport's counter.
	 *
	 * On the packet path, so it does no more than one map read and write. Anything that
	 * could throw or await here would be on every datagram of every port.
	 */
	record(transport: string, byteCount: number): void {
		const current = this.bytes.get(transport)
		if (current === undefined) return
		this.bytes.set(transport, current + byteCount)
	}

	/**
	 * Records whether a transport's listener is bound and serving.
	 *
	 * Reported separately from the byte count because the byte count cannot express
	 * it: a transport that never bound receives nothing, and a steady zero looks the
	 * same as an idle transport. Until a transport says otherwise it counts as not
	 * serving, so one that is configured and never starts is reported rather than
	 * silently absent.
	 */
	setServing(transport: string, serving: boolean): void {
		if (!this.bytes.has(transport)) return
		if (serving) this.serving.add(transport)
		else this.serving.delete(transport)
	}

	start(): void {
		if (this.timer !== undefined) return
		this.timer = setInterval(() => {
			void this.publishNow()
		}, this.options.intervalMs ?? DEFAULT_INTERVAL_MS)
	}

	stop(): void {
		if (this.timer === undefined) return
		clearInterval(this.timer)
		this.timer = undefined
	}

	/**
	 * Publishes one period's counters and resets them.
	 *
	 * The counters are reset whether or not the publish succeeds. They describe a
	 * period rather than a running total, so carrying a failed period into the next one
	 * would report a burst of traffic that never happened - and the alarms read this as
	 * a rate.
	 *
	 * Never rejects: a CloudWatch call failing is not a reason to disturb ingest.
	 */
	async publishNow(): Promise<void> {
		const at = new Date()
		const samples = this.options.transports.map((transport) => {
			const bytes = this.bytes.get(transport) ?? 0
			this.bytes.set(transport, 0)
			// Not reset: serving is a state, not a count of what happened this period.
			return {
				transport,
				bytes,
				serving: this.serving.has(transport) ? 1 : 0,
				at,
			}
		})
		if (samples.length === 0) return

		try {
			await this.options.publish(samples)
		} catch (err) {
			this.logger.warn('Could not publish transport traffic metrics', {
				error: err instanceof Error ? err.message : String(err),
			})
		}
	}
}

/**
 * The exact PutMetricData request these samples become.
 *
 * Separated from the call so that the CDK spec can compare what the backend publishes
 * against what the alarms query, without a CloudWatch client. That comparison is the
 * point: namespace, metric name and dimension *value* all have to match across the two
 * sides, and a mismatch in any of them produces an alarm watching a metric nobody
 * publishes, which reads as "no traffic" and so fails towards silence.
 */
export const trafficMetricRequest = (
	samples: TransportTrafficSample[],
	stackName: string,
): PutMetricDataCommandInput => ({
	Namespace: trafficMetricNamespace(stackName),
	MetricData: samples.flatMap((sample) => {
		const Dimensions = [{ Name: TRANSPORT_DIMENSION, Value: sample.transport }]
		return [
			{
				MetricName: RECEIVED_BYTES_METRIC,
				Value: sample.bytes,
				Unit: 'Bytes' as const,
				Timestamp: sample.at,
				Dimensions,
			},
			{
				MetricName: SERVING_METRIC,
				Value: sample.serving,
				Unit: 'None' as const,
				Timestamp: sample.at,
				Dimensions,
			},
		]
	}),
})

/** Publishes to CloudWatch, in the namespace the stack's alarms read. */
export const cloudWatchTrafficPublisher = (options: {
	stackName: string
	region: string
}): TransportTrafficPublisher => {
	const client = new CloudWatchClient({ region: options.region })
	return async (samples) => {
		await client.send(
			new PutMetricDataCommand(
				trafficMetricRequest(samples, options.stackName),
			),
		)
	}
}
