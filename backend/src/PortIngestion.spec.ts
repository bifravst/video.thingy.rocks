import assert from 'node:assert/strict'
import { beforeEach, describe, it } from 'node:test'

import { PortIngestion, type PortIngestionConfig } from './PortIngestion.ts'
import type { OwnedWriteResult } from './StreamMetadataService.ts'

const PORT = 5000
const INSTANCE = 'i-test'
const MIN_BYTES = 100

const datagram = (size: number, fill = 0x41): Buffer => Buffer.alloc(size, fill)

/** A gate a test can hold open to stop time inside an awaited call. */
const gate = (): { promise: Promise<void>; open: () => void } => {
	let open: () => void = () => undefined
	const promise = new Promise<void>((resolve) => {
		open = resolve
	})
	return { promise, open }
}

type LockCall = 'acquire' | 'release' | 'heartbeat'

class LockStoreFake {
	readonly calls: LockCall[] = []
	/** True while this instance has acquired and not released - what ownsSlot mirrors. */
	acquiredByUs = false
	/** Set when a conditional write proved the row was taken by someone else. */
	lostSinceAcquire = false
	/** Set if release is ever called while the producer is still alive. */
	releasedWhileProducing = false
	producerIsActive: () => boolean = () => false
	acquireOutcomes: ('ok' | 'refused' | 'throw')[] = []
	heartbeatOutcomes: OwnedWriteResult[] = []
	acquireGate?: Promise<void>
	releaseGate?: Promise<void>

	async tryAcquireKinesisLock(port: number, instanceId: string) {
		assert.strictEqual(port, PORT)
		assert.strictEqual(instanceId, INSTANCE)
		this.calls.push('acquire')
		if (this.acquireGate !== undefined) await this.acquireGate
		const outcome = this.acquireOutcomes.shift() ?? 'ok'
		if (outcome === 'throw') throw new Error('dynamo unavailable')
		if (outcome === 'refused') return false
		this.acquiredByUs = true
		this.lostSinceAcquire = false
		return true
	}

	async releaseKinesisLock(port: number) {
		assert.strictEqual(port, PORT)
		// The ordering that matters: another instance may acquire the moment this
		// returns, so the producer must already be dead. This is the check that fails
		// if the release-before-stop ordering is ever reintroduced.
		if (this.producerIsActive()) this.releasedWhileProducing = true
		this.calls.push('release')
		if (this.releaseGate !== undefined) await this.releaseGate
		this.acquiredByUs = false
	}

	async updateLastPacketTime(): Promise<OwnedWriteResult> {
		this.calls.push('heartbeat')
		const outcome = this.heartbeatOutcomes.shift() ?? 'ok'
		// The row belongs to another instance from here on, so leaving the owning
		// states without releasing is correct - see lostSinceAcquire in the I1 check.
		if (outcome === 'lostLock') this.lostSinceAcquire = true
		return outcome
	}
}

class ProducerFake {
	readonly startCalls: Buffer[][] = []
	readonly written: Buffer[] = []
	stopCount = 0
	active = false
	startGate?: Promise<void>
	startBehaviour: 'ok' | 'throw' | 'inactive' = 'ok'
	/** Datagrams start() "did not send", handed back on a failed start. */
	unsent: Buffer[] = []
	takeUnsentCount = 0

	async start(port: number, datagrams: Buffer[]) {
		assert.strictEqual(port, PORT)
		this.startCalls.push(datagrams)
		if (this.startGate !== undefined) await this.startGate
		if (this.startBehaviour === 'throw') throw new Error('spawn failed')
		this.active = this.startBehaviour === 'ok'
	}

	async stop() {
		this.stopCount += 1
		this.active = false
	}

	writePacket(port: number, data: Buffer) {
		assert.strictEqual(port, PORT)
		this.written.push(data)
	}

	isActive() {
		return this.active
	}

	takeUnsentDatagrams() {
		this.takeUnsentCount += 1
		const unsent = this.unsent
		this.unsent = []
		return unsent
	}

	/** Models a producer dying on its own: it stops being active before we hear about it. */
	simulateExit() {
		this.active = false
	}
}

/** Mirrors StreamStateManager's semantics without its timers. */
class ActivityFake {
	status: 'active' | 'inactive' | undefined = undefined
	onPacketReceived() {
		this.status = 'active'
	}
	getStreamState() {
		return this.status === undefined ? undefined : { status: this.status }
	}
	goInactive() {
		this.status = 'inactive'
	}
}

type Harness = {
	machine: PortIngestion
	locks: LockStoreFake
	producer: ProducerFake
	activity: ActivityFake
	states: string[]
	errors: Error[]
	advance: (ms: number) => void
	send: (size?: number) => void
	settle: () => Promise<void>
	/** Waits until the machine reaches a state, for pinning a mid-flight interleaving. */
	waitForState: (name: string) => Promise<void>
}

const harness = (config: Partial<PortIngestionConfig> = {}): Harness => {
	const locks = new LockStoreFake()
	const producer = new ProducerFake()
	locks.producerIsActive = () => producer.isActive()
	const activity = new ActivityFake()
	const states: string[] = []
	const errors: Error[] = []
	let clock = 1_000_000

	const machine = new PortIngestion(
		{
			port: PORT,
			instanceId: INSTANCE,
			minBytesBeforeStart: MIN_BYTES,
			startRetryBackoffMs: 30_000,
			restartDelayMs: 10_000,
			...config,
		},
		{
			locks,
			producer,
			activity,
			now: () => clock,
			// A violated invariant must fail the test, not end up in a log: the queue
			// deliberately keeps running after a handler throws.
			onError: (err) => errors.push(err),
			onTransition: (_from, to) => {
				states.push(to)
				// I1: the machine only claims the slot while it has acquired and not
				// released the row, and it only stops claiming it after releasing -
				// unless a conditional write proved the row was taken, which is the one
				// case where abandoning without releasing is correct.
				if (machine.ownsSlot) {
					assert.ok(
						locks.acquiredByUs,
						`I1 violated entering ${to}: claims the slot without holding the row`,
					)
				} else {
					assert.ok(
						!locks.acquiredByUs || locks.lostSinceAcquire,
						`I1 violated entering ${to}: holds the row without claiming the slot`,
					)
				}
				// The producer must never outlive the release.
				assert.strictEqual(
					locks.releasedWhileProducing,
					false,
					`released the lock while the producer was still alive (entering ${to})`,
				)
				// I2: an active producer only exists in these states.
				if (producer.isActive()) {
					assert.ok(
						['Provisional', 'Running', 'Stopping'].includes(to),
						`I2 violated: producer active in ${to}`,
					)
				}
				// I3: while producing, nothing is left buffered.
				if (to === 'Running' || to === 'Provisional') {
					assert.strictEqual(
						machine.bufferedBytes,
						0,
						`I3 violated: ${String(machine.bufferedBytes)} bytes buffered in ${to}`,
					)
				}
			},
		},
	)

	const settle = async (): Promise<void> => {
		await machine.drained()
		const [firstError] = errors
		if (firstError !== undefined) throw firstError
	}

	return {
		machine,
		locks,
		producer,
		activity,
		states,
		errors,
		advance: (ms) => {
			clock += ms
		},
		send: (size = MIN_BYTES) => machine.offer(datagram(size), new Date()),
		settle,
		waitForState: async (name) => {
			const deadline = Date.now() + 2000
			while (machine.stateName !== name) {
				const [firstError] = errors
				if (firstError !== undefined) throw firstError
				if (Date.now() > deadline) {
					throw new Error(
						`timed out waiting for ${name}; still ${machine.stateName}`,
					)
				}
				await new Promise((resolve) => setImmediate(resolve))
			}
		},
	}
}

void describe('PortIngestion', () => {
	let h: Harness
	beforeEach(() => {
		h = harness()
	})

	void describe('happy path', () => {
		void it('buffers without touching the lock below the threshold', async () => {
			h.send(10)
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Buffering')
			assert.deepStrictEqual(h.locks.calls, [])
			assert.strictEqual(h.machine.bufferedBytes, 10)
		})

		void it('acquires and starts once the threshold is reached', async () => {
			h.send(MIN_BYTES)
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Running')
			assert.deepStrictEqual(h.locks.calls, ['acquire'])
			assert.strictEqual(h.producer.startCalls.length, 1)
			assert.strictEqual(h.machine.ownsSlot, true)
		})

		void it('hands the buffered datagrams to start() in arrival order', async () => {
			h.machine.offer(datagram(40, 1), new Date())
			h.machine.offer(datagram(40, 2), new Date())
			h.machine.offer(datagram(40, 3), new Date())
			await h.settle()
			const handed = h.producer.startCalls[0] ?? []
			assert.strictEqual(handed.length, 3)
			assert.deepStrictEqual(
				handed.map((d) => d[0]),
				[1, 2, 3],
			)
		})

		void it('relays subsequent datagrams and refreshes the lease', async () => {
			h.send()
			await h.settle()
			h.machine.offer(datagram(5, 9), new Date())
			await h.settle()
			assert.strictEqual(h.producer.written.length, 1)
			assert.strictEqual(h.producer.written[0]?.[0], 9)
			assert.deepStrictEqual(h.locks.calls, ['acquire', 'heartbeat'])
		})

		// Releasing before the producer dies lets another instance start writing to the
		// same stream while this one is still alive.
		void it('stops the producer BEFORE releasing the lock on inactivity', async () => {
			h.send()
			await h.settle()
			h.activity.goInactive()
			h.machine.onInactive()
			await h.settle()

			assert.strictEqual(h.machine.stateName, 'Idle')
			assert.strictEqual(h.producer.stopCount, 1)
			assert.deepStrictEqual(h.locks.calls, ['acquire', 'release'])
			assert.strictEqual(h.machine.ownsSlot, false)
		})

		void it('can start again after going idle', async () => {
			h.send()
			await h.settle()
			h.activity.goInactive()
			h.machine.onInactive()
			await h.settle()
			h.send()
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Running')
			assert.strictEqual(h.producer.startCalls.length, 2)
		})
	})

	void describe('acquisition failures', () => {
		void it('keeps the buffer intact when acquisition is refused', async () => {
			h.locks.acquireOutcomes = ['refused']
			h.send()
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Cooldown')
			assert.strictEqual(h.machine.bufferedBytes, MIN_BYTES)
			assert.strictEqual(h.producer.startCalls.length, 0)
			assert.deepStrictEqual(h.locks.calls, ['acquire'])
		})

		void it('keeps the buffer intact when acquisition throws', async () => {
			h.locks.acquireOutcomes = ['throw']
			h.send()
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Cooldown')
			assert.strictEqual(h.machine.bufferedBytes, MIN_BYTES)
			assert.strictEqual(h.machine.ownsSlot, false)
		})

		void it('does not retry before the cooldown expires', async () => {
			h.locks.acquireOutcomes = ['refused']
			h.send()
			await h.settle()
			h.advance(29_999)
			h.send()
			await h.settle()
			assert.deepStrictEqual(h.locks.calls, ['acquire'])
		})

		// The port must not be stranded: once the cooldown passes, a packet retries.
		void it('retries once the cooldown has expired', async () => {
			h.locks.acquireOutcomes = ['refused']
			h.send()
			await h.settle()
			h.advance(30_000)
			h.send()
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Running')
			assert.deepStrictEqual(h.locks.calls, ['acquire', 'acquire'])
		})
	})

	void describe('start failures', () => {
		void it('tears down and re-arms the datagrams when start throws', async () => {
			h.producer.startBehaviour = 'throw'
			h.producer.unsent = [datagram(30, 7), datagram(30, 8)]
			h.send()
			await h.settle()

			assert.strictEqual(h.machine.stateName, 'Cooldown')
			assert.strictEqual(h.producer.stopCount, 1)
			assert.deepStrictEqual(h.locks.calls, ['acquire', 'release'])
			assert.strictEqual(h.machine.bufferedBytes, 60)
		})

		void it('treats a start that leaves no active producer as a failure', async () => {
			h.producer.startBehaviour = 'inactive'
			h.send()
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Cooldown')
			assert.deepStrictEqual(h.locks.calls, ['acquire', 'release'])
		})

		void it('re-arms the datagrams start() never sent, oldest first', async () => {
			h.producer.startBehaviour = 'throw'
			h.producer.unsent = [datagram(10, 1), datagram(10, 2)]
			h.send()
			await h.settle()

			h.advance(30_000)
			h.machine.offer(datagram(MIN_BYTES, 3), new Date())
			await h.settle()
			const handed = h.producer.startCalls[1] ?? []
			assert.deepStrictEqual(
				handed.map((d) => d[0]),
				[1, 2, 3],
			)
		})

		// This is the finding that stranded a port: the buffer was deleted before the
		// outcome was known, so no later packet could re-enter the start path.
		void it('still retries after a failed start', async () => {
			h.producer.startBehaviour = 'throw'
			h.send()
			await h.settle()
			h.producer.startBehaviour = 'ok'
			h.advance(30_000)
			h.send()
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Running')
		})

		void it('never remains in Starting after a failure', async () => {
			h.producer.startBehaviour = 'throw'
			h.send()
			await h.settle()
			assert.ok(!h.states.slice(-1).includes('Starting'))
		})
	})

	void describe('ownership loss', () => {
		void it('stops the producer without releasing when the lease is lost', async () => {
			h.send()
			await h.settle()
			h.locks.heartbeatOutcomes = ['lostLock']
			h.machine.offer(datagram(5), new Date())
			await h.settle()

			assert.strictEqual(h.machine.stateName, 'Cooldown')
			assert.strictEqual(h.producer.stopCount, 1)
			// No release: the row belongs to the new owner now.
			assert.deepStrictEqual(h.locks.calls, ['acquire', 'heartbeat'])
		})

		// A transient DynamoDB error says nothing about ownership, and the lease has not
		// expired - relinquishing would only churn the pipeline.
		void it('keeps producing through a transient write error', async () => {
			h.send()
			await h.settle()
			h.locks.heartbeatOutcomes = ['writeError']
			h.machine.offer(datagram(5), new Date())
			await h.settle()

			assert.strictEqual(h.machine.stateName, 'Running')
			assert.strictEqual(h.producer.stopCount, 0)
		})
	})

	void describe('producer restart', () => {
		void it('keeps the lock when the producer exits', async () => {
			h.send()
			await h.settle()
			h.producer.simulateExit()
			h.machine.onProducerExited(h.machine.currentEpoch)
			await h.settle()

			assert.strictEqual(h.machine.stateName, 'Restarting')
			assert.strictEqual(h.machine.ownsSlot, true)
			assert.deepStrictEqual(h.locks.calls, ['acquire'])
		})

		void it('buffers during the restart delay and does not start early', async () => {
			h.send()
			await h.settle()
			h.producer.simulateExit()
			h.machine.onProducerExited(h.machine.currentEpoch)
			await h.settle()

			h.advance(9_999)
			h.machine.offer(datagram(20, 4), new Date())
			await h.settle()
			assert.strictEqual(h.producer.startCalls.length, 1)
			assert.strictEqual(h.machine.bufferedBytes, 20)
		})

		void it('restarts on the first packet past the delay, with the buffer', async () => {
			h.send()
			await h.settle()
			h.producer.simulateExit()
			h.machine.onProducerExited(h.machine.currentEpoch)
			await h.settle()

			h.advance(10_000)
			h.machine.offer(datagram(20, 4), new Date())
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Running')
			assert.strictEqual(h.producer.startCalls.length, 2)
			assert.strictEqual((h.producer.startCalls[1] ?? []).length, 1)
		})

		// The central finding: a late event from a previous ownership lifetime must not
		// tear down the current one.
		void it('drops an exit event from a previous epoch', async () => {
			h.send()
			await h.settle()
			const staleEpoch = h.machine.currentEpoch - 1
			h.machine.onProducerExited(staleEpoch)
			await h.settle()

			assert.strictEqual(h.machine.stateName, 'Running')
			assert.strictEqual(h.producer.stopCount, 0)
		})

		void it('ignores an exit event once the port went idle', async () => {
			h.send()
			await h.settle()
			const epoch = h.machine.currentEpoch
			h.activity.goInactive()
			h.machine.onInactive()
			await h.settle()

			h.machine.onProducerExited(epoch)
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Idle')
			assert.strictEqual(h.machine.ownsSlot, false)
		})

		void it('releases the port if inactivity arrives while restarting', async () => {
			h.send()
			await h.settle()
			h.producer.simulateExit()
			h.machine.onProducerExited(h.machine.currentEpoch)
			await h.settle()

			h.activity.goInactive()
			h.machine.onInactive()
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Idle')
			assert.deepStrictEqual(h.locks.calls, ['acquire', 'release'])
		})

		// A repeatedly failing restart must back off on the long clock, not retry every
		// 10 seconds forever.
		void it('falls back to the start cooldown when a restart fails', async () => {
			h.send()
			await h.settle()
			h.producer.simulateExit()
			h.machine.onProducerExited(h.machine.currentEpoch)
			await h.settle()

			h.producer.startBehaviour = 'throw'
			h.advance(10_000)
			h.send()
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Cooldown')

			h.advance(10_001)
			h.send()
			await h.settle()
			// Still cooling down: the restart delay is not the retry interval.
			assert.strictEqual(h.producer.startCalls.length, 2)
		})
	})

	void describe('interleavings', () => {
		void it('delivers packets that arrive during a start after the start, in order', async () => {
			const g = gate()
			h.producer.startGate = g.promise
			h.send()
			// Wait until start() is actually in flight. Datagrams offered before this
			// point legitimately belong to the initial batch - they arrived before the
			// producer existed - so only these two must arrive as live traffic.
			await h.waitForState('Starting')
			h.machine.offer(datagram(5, 1), new Date())
			h.machine.offer(datagram(5, 2), new Date())
			g.open()
			await h.settle()

			assert.strictEqual(h.machine.stateName, 'Running')
			assert.deepStrictEqual(
				h.producer.written.map((d) => d[0]),
				[1, 2],
			)
			// They were not smuggled into the initial batch.
			assert.strictEqual((h.producer.startCalls[0] ?? []).length, 1)
		})

		void it('includes datagrams that arrived before the producer existed in the initial batch', async () => {
			const g = gate()
			h.locks.acquireGate = g.promise
			h.machine.offer(datagram(MIN_BYTES, 1), new Date())
			h.machine.offer(datagram(5, 2), new Date())
			g.open()
			await h.settle()

			// Nothing is lost and the order holds; which side of the boundary a datagram
			// falls on depends only on whether a producer existed yet.
			assert.deepStrictEqual(
				[...(h.producer.startCalls[0] ?? []), ...h.producer.written].map(
					(d) => d[0],
				),
				[1, 2],
			)
		})

		void it('tears down after a held start rather than during it', async () => {
			const g = gate()
			h.producer.startGate = g.promise
			h.send()
			await h.waitForState('Starting')
			h.activity.goInactive()
			h.machine.onInactive()
			g.open()
			await h.settle()

			// The producer is stopped only once the start it raced has completed, so a
			// spawned child is never left behind by a teardown that overtook it.
			assert.strictEqual(h.machine.stateName, 'Idle')
			assert.strictEqual(h.producer.stopCount, 1)
			assert.deepStrictEqual(h.locks.calls, ['acquire', 'release'])
			assert.strictEqual(h.producer.written.length, 0)
		})

		// Acquiring a lock for a stream that has already stopped must not spawn a
		// producer; it must hand the slot straight back.
		void it('releases without starting when the stream stops during acquisition', async () => {
			const g = gate()
			h.locks.acquireGate = g.promise
			h.send()
			h.activity.goInactive()
			g.open()
			await h.settle()

			assert.strictEqual(h.producer.startCalls.length, 0)
			assert.deepStrictEqual(h.locks.calls, ['acquire', 'release'])
			assert.strictEqual(h.machine.stateName, 'Idle')
		})

		void it('shuts down cleanly in the middle of a start', async () => {
			const g = gate()
			h.producer.startGate = g.promise
			h.send()
			const shutdown = h.machine.shutdown()
			g.open()
			await shutdown

			assert.strictEqual(h.machine.stateName, 'Terminated')
			assert.strictEqual(h.producer.stopCount, 1)
			assert.deepStrictEqual(h.locks.calls, ['acquire', 'release'])
		})

		void it('drops packets and exit events after shutdown', async () => {
			h.send()
			await h.settle()
			const epoch = h.machine.currentEpoch
			await h.machine.shutdown()

			const startsBefore = h.producer.startCalls.length
			h.send()
			h.machine.onProducerExited(epoch)
			await h.settle()
			assert.strictEqual(h.machine.stateName, 'Terminated')
			assert.strictEqual(h.producer.startCalls.length, startsBefore)
		})

		void it('makes no DynamoDB call when shut down while cooling down', async () => {
			h.locks.acquireOutcomes = ['refused']
			h.send()
			await h.settle()
			const calls = h.locks.calls.length
			await h.machine.shutdown()
			assert.strictEqual(h.locks.calls.length, calls)
			assert.strictEqual(h.machine.stateName, 'Terminated')
		})
	})

	void describe('queue bounds', () => {
		void it('drops the oldest datagrams over the cap and keeps the newest', async () => {
			// Eviction is observable while the port is held off from starting, which is
			// the situation the cap exists for: another instance owns the slot. Sizes
			// are MTU-shaped because the cap has a floor of one maximum datagram above
			// the start threshold (see PortIngestion's constructor).
			const mtu = 1400
			const h2 = harness({ minBytesBeforeStart: mtu })
			h2.locks.acquireOutcomes = ['refused']
			h2.send(mtu)
			await h2.settle()
			assert.strictEqual(h2.machine.stateName, 'Cooldown')

			const needed = Math.ceil(h2.machine.queueCapBytes / mtu) + 20
			for (let i = 0; i < needed; i++) {
				h2.machine.offer(datagram(mtu, 1), new Date())
			}
			await h2.settle()
			assert.ok(
				h2.machine.bufferedBytes <= h2.machine.queueCapBytes,
				`${String(h2.machine.bufferedBytes)} > ${String(h2.machine.queueCapBytes)}`,
			)

			h2.advance(30_000)
			h2.machine.offer(datagram(mtu, 99), new Date())
			await h2.settle()

			const handed = h2.producer.startCalls[0] ?? []
			// The newest datagram is always kept, and the buffer was trimmed to the cap
			// rather than growing without bound.
			assert.strictEqual(handed.at(-1)?.[0], 99)
			assert.ok(
				handed.length < needed,
				`nothing was evicted: ${String(handed.length)} of ${String(needed)}`,
			)
		})

		// There are no transition markers to lose to eviction - the state is the marker.
		void it('still starts after overflowing the cap while idle', async () => {
			const h2 = harness({ minBytesBeforeStart: 40, maxQueuedBytes: 80 })
			for (let i = 0; i < 10; i++) h2.machine.offer(datagram(40), new Date())
			await h2.settle()
			assert.strictEqual(h2.machine.stateName, 'Running')
		})

		// A cap below the start threshold would keep the buffer under the threshold
		// forever, so the port would buffer and never start.
		void it('never lets the cap fall below the start threshold', async () => {
			const h2 = harness({ minBytesBeforeStart: 1_000, maxQueuedBytes: 10 })
			for (let i = 0; i < 30; i++) h2.machine.offer(datagram(40), new Date())
			await h2.settle()
			assert.strictEqual(h2.machine.stateName, 'Running')
		})

		void it('starts on the first packet when the threshold is zero', async () => {
			const h2 = harness({ minBytesBeforeStart: 0 })
			h2.machine.offer(datagram(1), new Date())
			await h2.settle()
			assert.strictEqual(h2.machine.stateName, 'Running')
			assert.strictEqual((h2.producer.startCalls[0] ?? []).length, 1)
		})
	})

	void describe('admission', () => {
		void it('ignores datagrams the filter rejects while unowned', async () => {
			const h2 = harness({ admit: () => false })
			h2.send()
			await h2.settle()
			assert.strictEqual(h2.machine.stateName, 'Idle')
			assert.strictEqual(h2.machine.bufferedBytes, 0)
			assert.deepStrictEqual(h2.locks.calls, [])
			assert.strictEqual(h2.activity.status, undefined)
		})

		void it('bypasses the filter once the port is owned', async () => {
			let admit = true
			const h2 = harness({ admit: () => admit })
			h2.send()
			await h2.settle()
			assert.strictEqual(h2.machine.stateName, 'Running')

			admit = false
			h2.machine.offer(datagram(5, 3), new Date())
			await h2.settle()
			assert.strictEqual(h2.producer.written.length, 1)
		})
	})

	void describe('provisional phase', () => {
		const provisional = (): Harness =>
			harness({ provisionalTimeoutMs: 20_000, provisionalCooldownMs: 60_000 })

		void it('enters Provisional instead of Running when authentication is required', async () => {
			const h2 = provisional()
			h2.send()
			await h2.settle()
			assert.strictEqual(h2.machine.stateName, 'Provisional')
			assert.strictEqual(h2.machine.ownsSlot, true)
		})

		// I7: unauthenticated traffic earns no lease refresh.
		void it('refreshes no lease while provisional', async () => {
			const h2 = provisional()
			h2.send()
			await h2.settle()
			h2.machine.offer(datagram(5), new Date())
			await h2.settle()
			assert.deepStrictEqual(h2.locks.calls, ['acquire'])
		})

		void it('relays datagrams while provisional, so they can be authenticated', async () => {
			const h2 = provisional()
			h2.send()
			await h2.settle()
			h2.machine.offer(datagram(5, 2), new Date())
			await h2.settle()
			assert.strictEqual(h2.producer.written.length, 1)
		})

		void it('becomes Running on the first authentication', async () => {
			const h2 = provisional()
			h2.send()
			await h2.settle()
			h2.machine.onAuthenticated()
			await h2.settle()
			assert.strictEqual(h2.machine.stateName, 'Running')

			h2.machine.offer(datagram(5), new Date())
			await h2.settle()
			assert.deepStrictEqual(h2.locks.calls, ['acquire', 'heartbeat'])
		})

		void it('releases the port and cools down when nothing authenticates', async () => {
			const h2 = provisional()
			h2.send()
			await h2.settle()
			h2.advance(20_000)
			h2.machine.offer(datagram(5), new Date())
			await h2.settle()

			assert.strictEqual(h2.machine.stateName, 'Cooldown')
			assert.strictEqual(h2.producer.stopCount, 1)
			assert.deepStrictEqual(h2.locks.calls, ['acquire', 'release'])

			// And the cooldown is the longer one, so an unauthenticated sender cannot
			// hold the slot in a loop.
			h2.advance(30_000)
			h2.send()
			await h2.settle()
			assert.strictEqual(h2.producer.startCalls.length, 1)
			h2.advance(30_000)
			h2.send()
			await h2.settle()
			assert.strictEqual(h2.producer.startCalls.length, 2)
		})

		void it('ignores an authentication report when not provisional', async () => {
			const h2 = provisional()
			h2.machine.onAuthenticated()
			await h2.settle()
			assert.strictEqual(h2.machine.stateName, 'Idle')
		})
	})

	void describe('port independence', () => {
		void it('does not let one port block another', async () => {
			const slow = harness()
			const fast = harness()
			const g = gate()
			slow.producer.startGate = g.promise

			slow.send()
			fast.send()
			await fast.settle()

			assert.strictEqual(fast.machine.stateName, 'Running')
			assert.strictEqual(slow.machine.stateName, 'Starting')
			g.open()
			await slow.settle()
			assert.strictEqual(slow.machine.stateName, 'Running')
		})
	})
})
