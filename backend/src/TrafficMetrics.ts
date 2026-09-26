import {
	CloudWatchClient,
	PutMetricDataCommand,
} from '@aws-sdk/client-cloudwatch'
import { Logger } from './Logger.ts'
import { SRTP_TRANSPORT, UNENCRYPTED_TRANSPORT } from './TrafficMetricNames.ts'
import {
	trafficMetricRequest,
	type TransportTrafficSample,
} from './TrafficMetricRequest.ts'

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
	/** The stack the request builder derives the namespace from. */
	private readonly stackName: string
	private readonly logger = new Logger('TrafficMetrics')
	private readonly received = new Map<string, number>()
	private readonly serving = new Map<string, boolean>()
	private readonly intervalMs: number
	private timer: NodeJS.Timeout | undefined
	private publishCount = 0
	/**
	 * Publishes are serialized through this chain: a publish that overlaps the
	 * previous one's in-flight request would otherwise read a half-accumulated
	 * snapshot (see publishNow for the other half of that fix).
	 */
	private publishChain: Promise<void> = Promise.resolve()

	constructor(config: {
		region: string
		stackName?: string
		/** Injected CloudWatch client, for tests; production omits it. */
		client?: CloudWatchClient
		/** Publish interval, for tests; production uses the default. */
		intervalMs?: number
	}) {
		if (config.stackName === undefined || config.stackName === '') {
			this.logger.warn(
				'STACK_NAME is not set; the per-transport traffic metrics will not be published and the zero-ingestion alarms will see no data',
			)
			this.client = undefined
		} else {
			this.client =
				config.client ?? new CloudWatchClient({ region: config.region })
		}
		this.stackName = config.stackName ?? 'local'
		this.intervalMs = config.intervalMs ?? 60_000
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

	/** Serializes one publish behind any in-flight one. */
	private async publish(): Promise<void> {
		const run = this.publishChain.then(async (): Promise<void> =>
			this.publishNow(),
		)
		// The chain itself never rejects: a failed publish is logged and its
		// snapshot restored, and the next interval publishes again.
		this.publishChain = run.catch((): undefined => undefined)
		return run
	}

	/**
	 * Publishes one interval's traffic, as the exact request
	 * trafficMetricRequest builds - the same module cdk/StreamingStack.spec.ts
	 * cross-checks the stack's alarms against, so a namespace, metric name or
	 * dimension that drifts here fails that test rather than arming a dead
	 * alarm.
	 *
	 * The snapshot is taken and the counters cleared BEFORE the request is
	 * awaited: bytes that arrive while the request is in flight belong to the
	 * next interval, and clearing after the await would erase them - they were
	 * never in the submitted request, and a slow or overlapping publish would
	 * silently under-report traffic, which suppresses the alarms that exist to
	 * see it. On failure the snapshot is restored instead, so a lost request
	 * costs the traffic a delay in the metric rather than the metric the
	 * traffic.
	 */
	private async publishNow(): Promise<void> {
		if (this.client === undefined) return
		const at = new Date()
		const samples: TransportTrafficSample[] = [...this.received.entries()].map(
			([transport, bytes]) => ({
				transport,
				bytes,
				serving: (this.serving.get(transport) ?? false) ? 1 : 0,
				at,
			}),
		)
		for (const [transport] of this.received) this.received.set(transport, 0)
		try {
			await this.client.send(
				new PutMetricDataCommand(trafficMetricRequest(samples, this.stackName)),
			)
			this.publishCount++
		} catch (err) {
			for (const { transport, bytes } of samples) {
				if (bytes > 0) {
					this.received.set(
						transport,
						(this.received.get(transport) ?? 0) + bytes,
					)
				}
			}
			// Best effort by design, but a metric that silently never publishes makes
			// the alarms watch nothing, so it is logged and counted.
			this.logger.error(
				'Could not publish traffic metrics',
				err instanceof Error ? err : new Error(String(err)),
			)
		}
	}
}
