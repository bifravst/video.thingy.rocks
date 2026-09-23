/**
 * The exact PutMetricData request per-transport traffic samples become.
 *
 * Its own module, with no AWS SDK import, because both sides need it: the backend
 * publishes it, and cdk/StreamingStack.spec.ts builds it to check the alarms query
 * what is actually published. The CDK is installed from the repository root, which
 * does not carry the backend's AWS SDK clients, so anything the CDK side imports has
 * to load without them - which is also why the request is shaped here as a plain
 * object rather than typed against the SDK.
 *
 * That comparison is the point: namespace, metric name and dimension *value* all have
 * to match across the two sides, and a mismatch in any of them produces an alarm
 * watching a metric nobody publishes, which reads as "no traffic" and so fails towards
 * silence.
 */

import {
	RECEIVED_BYTES_METRIC,
	SERVING_METRIC,
	TRANSPORT_DIMENSION,
	trafficMetricNamespace,
} from './TrafficMetricNames.ts'

export type TransportTrafficSample = {
	transport: string
	bytes: number
	/** 1 while this transport's listener is serving, 0 while it is not. */
	serving: number
	at: Date
}

export type TrafficMetricRequest = {
	Namespace: string
	MetricData: {
		MetricName: string
		Value: number
		Unit: 'Bytes' | 'None'
		Timestamp: Date
		Dimensions: { Name: string; Value: string }[]
	}[]
}

export const trafficMetricRequest = (
	samples: TransportTrafficSample[],
	stackName: string,
): TrafficMetricRequest => ({
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
