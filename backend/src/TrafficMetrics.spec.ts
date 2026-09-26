import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import type {
	CloudWatchClient,
	PutMetricDataCommand,
} from '@aws-sdk/client-cloudwatch'
import { SRTP_TRANSPORT, UNENCRYPTED_TRANSPORT } from './TrafficMetricNames.ts'
import { trafficMetricRequest } from './TrafficMetricRequest.ts'
import { TrafficMetrics } from './TrafficMetrics.ts'

/**
 * A CloudWatch client the tests control: records every command, and can hold a
 * publish in flight or fail it on demand.
 *
 * Cast through unknown: the class type has more surface than send, but send is
 * all TrafficMetrics calls.
 */
const makeClient = (options?: {
	/** Holds every send in flight until released. */
	gate?: Promise<void>
	/** Makes the next send reject. */
	failNext?: boolean
}): {
	client: CloudWatchClient
	commands: PutMetricDataCommand[]
} => {
	const commands: PutMetricDataCommand[] = []
	let failNext = options?.failNext ?? false
	const client = {
		send: async (command: PutMetricDataCommand): Promise<unknown> => {
			commands.push(command)
			if (failNext) {
				failNext = false
				throw new Error('cloudwatch unavailable')
			}
			await options?.gate
			return {}
		},
	}
	return { client: client as unknown as CloudWatchClient, commands }
}

const waitFor = async (condition: () => boolean): Promise<void> => {
	const deadline = Date.now() + 5_000
	while (!condition()) {
		if (Date.now() > deadline) throw new Error('timed out waiting')
		await new Promise((resolve) => setTimeout(resolve, 10))
	}
}

/** The bytes of one transport in one published request. */
const bytesFor = (
	command: PutMetricDataCommand,
	transport: string,
): number | undefined =>
	(command.input.MetricData ?? []).find(
		(d) =>
			d.Dimensions?.[0]?.Value === transport &&
			d.MetricName === 'ReceivedBytes',
	)?.Value

void describe('TrafficMetrics', () => {
	void it('publishes exactly the request the alarms are cross-checked against', async () => {
		// The CDK spec builds trafficMetricRequest and asserts every alarm
		// resolves to something in it; the publisher must emit that request, not
		// a hand-rolled copy of it, or the two sides can drift with a green test
		// on each side of the gap.
		const { client, commands } = makeClient()
		const metrics = new TrafficMetrics({
			region: 'eu-central-1',
			stackName: 'test-stack',
			client,
		})
		metrics.recordReceived(UNENCRYPTED_TRANSPORT, 1234)
		metrics.setServing(UNENCRYPTED_TRANSPORT, true)
		await metrics.stop()
		assert.strictEqual(commands.length, 1)
		const input = commands[0]?.input as {
			MetricData: { Timestamp: Date }[]
		}
		const at = input.MetricData[0]?.Timestamp as Date
		assert.ok(at instanceof Date)
		assert.deepStrictEqual(
			commands[0]?.input,
			trafficMetricRequest(
				[
					{ transport: UNENCRYPTED_TRANSPORT, bytes: 1234, serving: 1, at },
					{ transport: SRTP_TRANSPORT, bytes: 0, serving: 0, at },
				],
				'test-stack',
			),
		)
	})

	void it('does not lose bytes recorded while a publish is in flight', async () => {
		let release: (() => void) | undefined
		const gate = new Promise<void>((resolve) => {
			release = resolve
		})
		const { client, commands } = makeClient({ gate })
		const metrics = new TrafficMetrics({
			region: 'eu-central-1',
			stackName: 'test-stack',
			client,
		})
		metrics.recordReceived(UNENCRYPTED_TRANSPORT, 100)
		const first = metrics.stop()
		// Let the publish take its snapshot and clear the counter - only then
		// does traffic arrive "during" the request.
		await waitFor(() => commands.length === 1)
		metrics.recordReceived(UNENCRYPTED_TRANSPORT, 60)
		assert.strictEqual(
			bytesFor(commands[0] as PutMetricDataCommand, UNENCRYPTED_TRANSPORT),
			100,
		)
		// The in-flight publish settles; a second one must carry the 60 that
		// arrived during it, and nothing that was already submitted.
		release?.()
		await first
		await metrics.stop()
		assert.strictEqual(commands.length, 2)
		assert.strictEqual(
			bytesFor(commands[1] as PutMetricDataCommand, UNENCRYPTED_TRANSPORT),
			60,
		)
	})

	void it('restores the snapshot of a publish that failed', async () => {
		const { client, commands } = makeClient({ failNext: true })
		const metrics = new TrafficMetrics({
			region: 'eu-central-1',
			stackName: 'test-stack',
			client,
		})
		metrics.recordReceived(UNENCRYPTED_TRANSPORT, 500)
		await metrics.stop() // fails; the snapshot must go back, not away
		await metrics.stop() // succeeds; the 500 must be in it
		assert.strictEqual(commands.length, 2)
		assert.strictEqual(
			bytesFor(commands[1] as PutMetricDataCommand, UNENCRYPTED_TRANSPORT),
			500,
		)
	})
})
