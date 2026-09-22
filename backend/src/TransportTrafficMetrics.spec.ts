import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { Logger, type LogContext } from './Logger.ts'
import {
	TransportTrafficMetrics,
	type TransportTrafficSample,
} from './TransportTrafficMetrics.ts'

const delay = async (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms))

class CapturingLogger extends Logger {
	readonly warnings: { message: string; context?: LogContext }[] = []
	constructor() {
		super('TransportTrafficMetricsSpec')
	}
	override warn(message: string, context?: LogContext): void {
		this.warnings.push({ message, context })
	}
}

/** Collects what would have gone to CloudWatch, and can be made to fail. */
class PublisherFake {
	readonly batches: TransportTrafficSample[][] = []
	failWith?: Error

	readonly publish = async (
		samples: TransportTrafficSample[],
	): Promise<void> => {
		if (this.failWith !== undefined) throw this.failWith
		this.batches.push(samples)
	}

	/** Bytes reported for a transport, one entry per published period. */
	bytesFor(transport: string): number[] {
		return this.batches.map(
			(batch) => batch.find((s) => s.transport === transport)?.bytes ?? -1,
		)
	}
}

const metrics = (
	publisher: PublisherFake,
	logger?: Logger,
): TransportTrafficMetrics =>
	new TransportTrafficMetrics({
		transports: ['unencrypted', 'srtp'],
		publish: publisher.publish,
		logger,
	})

void describe('TransportTrafficMetrics', () => {
	void it('reports bytes per transport', async () => {
		const publisher = new PublisherFake()
		const m = metrics(publisher)

		m.record('unencrypted', 1200)
		m.record('unencrypted', 300)
		m.record('srtp', 40)
		await m.publishNow()

		assert.deepStrictEqual(publisher.bytesFor('unencrypted'), [1500])
		assert.deepStrictEqual(publisher.bytesFor('srtp'), [40])
	})

	/**
	 * The property the alarms are built on.
	 *
	 * A transport that saw nothing still reports a zero, so a gap in the metric means
	 * "this instance is not reporting" rather than "it is idle". Publishing only
	 * non-zero samples would make those two indistinguishable, and an alarm could then
	 * only treat a gap as healthy - which is what stopped the SRTP alarm from being
	 * able to report a real SRTP failure.
	 */
	void it('reports a zero for a transport that saw nothing', async () => {
		const publisher = new PublisherFake()
		const m = metrics(publisher)

		m.record('unencrypted', 1200)
		await m.publishNow()

		assert.deepStrictEqual(publisher.bytesFor('srtp'), [0])
	})

	void it('reports zeros when nothing has arrived at all', async () => {
		const publisher = new PublisherFake()
		await metrics(publisher).publishNow()

		assert.deepStrictEqual(
			publisher.batches[0]?.map((s) => s.bytes),
			[0, 0],
		)
	})

	// Each period stands alone: the alarms read this as a rate, so carrying a period
	// forward would report traffic that never arrived.
	void it('starts each period from zero', async () => {
		const publisher = new PublisherFake()
		const m = metrics(publisher)

		m.record('unencrypted', 1000)
		await m.publishNow()
		await m.publishNow()
		m.record('unencrypted', 7)
		await m.publishNow()

		assert.deepStrictEqual(publisher.bytesFor('unencrypted'), [1000, 0, 7])
	})

	void it('drops rather than accumulates a period it could not publish', async () => {
		const publisher = new PublisherFake()
		const logger = new CapturingLogger()
		const m = metrics(publisher, logger)

		publisher.failWith = new Error('Throttled')
		m.record('unencrypted', 5000)
		await m.publishNow()

		assert.strictEqual(publisher.batches.length, 0)
		assert.deepStrictEqual(
			logger.warnings.map((w) => w.message),
			['Could not publish transport traffic metrics'],
		)

		publisher.failWith = undefined
		m.record('unencrypted', 11)
		await m.publishNow()
		assert.deepStrictEqual(
			publisher.bytesFor('unencrypted'),
			[11],
			'the failed period must not be added to the next one',
		)
	})

	// Ingest must not depend on CloudWatch being reachable.
	void it('never rejects when publishing fails', async () => {
		const publisher = new PublisherFake()
		publisher.failWith = new Error('Network down')
		await assert.doesNotReject(
			metrics(publisher, new CapturingLogger()).publishNow(),
		)
	})

	void it('ignores a transport it was not told about', async () => {
		const publisher = new PublisherFake()
		const m = metrics(publisher)

		m.record('nonsense', 999)
		await m.publishNow()

		assert.deepStrictEqual(
			publisher.batches[0]?.map((s) => s.transport),
			['unencrypted', 'srtp'],
		)
		assert.deepStrictEqual(
			publisher.batches[0]?.map((s) => s.bytes),
			[0, 0],
		)
	})

	void describe('the reporting interval', () => {
		void it('publishes on its own once started', async () => {
			const publisher = new PublisherFake()
			const m = new TransportTrafficMetrics({
				transports: ['unencrypted'],
				publish: publisher.publish,
				intervalMs: 15,
			})

			m.start()
			try {
				const deadline = Date.now() + 2000
				while (publisher.batches.length < 2 && Date.now() < deadline) {
					await delay(5)
				}
			} finally {
				m.stop()
			}

			assert.ok(
				publisher.batches.length >= 2,
				`expected repeated publishes, got ${String(publisher.batches.length)}`,
			)
		})

		void it('publishes nothing more once stopped', async () => {
			const publisher = new PublisherFake()
			const m = new TransportTrafficMetrics({
				transports: ['unencrypted'],
				publish: publisher.publish,
				intervalMs: 10,
			})

			m.start()
			m.stop()
			await delay(60)

			assert.deepStrictEqual(publisher.batches, [])
		})
	})
})
