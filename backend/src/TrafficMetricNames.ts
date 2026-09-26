/**
 * The names of the per-transport traffic metric, shared by its publisher and its alarms.
 *
 * The backend publishes this metric and cdk/StreamingStack.ts alarms on it, so the two
 * have to agree on a namespace, a metric name, a dimension and the dimension's values.
 * They are here rather than written out at both ends because a typo in any of them
 * produces an alarm that watches a metric nobody publishes - which looks exactly like
 * "no traffic", and so fails silently in the direction that suppresses alerts.
 *
 * This module deliberately imports nothing: the CDK reads it at synth time, and the
 * backend ships to the instance without the CDK.
 */

/** Namespace for one stack's ingest metrics, kept apart so two stacks cannot collide. */
export const trafficMetricNamespace = (stackName: string): string =>
	`${stackName}/ingest`

/**
 * Bytes of UDP payload accepted on a transport during the period.
 *
 * The per-transport counterpart to the load balancer's ProcessedBytes_UDP, which AWS
 * only publishes per load balancer - there is no per-listener or per-target-group
 * variant, which is why this has to come from the backend at all.
 */
export const RECEIVED_BYTES_METRIC = 'ReceivedBytes'

/**
 * 1 while a transport's listener is bound and serving, 0 while it is not.
 *
 * The byte count cannot answer this. A transport whose listener never bound receives
 * nothing, so it reports a steady zero that is indistinguishable from an idle
 * transport - which makes the failure the additive-transport isolation exists to
 * survive, an unreachable parameter store, the one failure no traffic-gated alarm can
 * see.
 */
export const SERVING_METRIC = 'TransportServing'

/** Dimension carrying the transport name; its values are the constants below. */
export const TRANSPORT_DIMENSION = 'Transport'

/**
 * Transport names, which are also the dimension values.
 *
 * Used for the transports themselves too, so the name an alarm looks for cannot drift
 * from the name the service gave the transport.
 */
export const UNENCRYPTED_TRANSPORT = 'unencrypted'
export const SRTP_TRANSPORT = 'srtp'
