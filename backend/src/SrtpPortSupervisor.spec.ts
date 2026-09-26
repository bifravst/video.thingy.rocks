import assert from 'node:assert/strict'
import { afterEach, describe, it } from 'node:test'

import type { SrtpHelperInit, SrtpHelperMessage } from './SrtpHelperProtocol.ts'
import { SRTP_HELPER_PROTOCOL_VERSION } from './SrtpHelperProtocol.ts'
import type { SrtpPortKey } from './SrtpKeyStore.ts'
import {
	SrtpPortSupervisor,
	type HelperProcess,
	type SrtpPortSupervisorConfig,
	type SrtpSupervisorState,
} from './SrtpPortSupervisor.ts'

/**
 * A stand-in for the helper child the supervisor can be given in tests.
 *
 * It records every command line written to its stdin, lets a test emit frames on
 * its stdout, and can die (by exit or by signal) on demand. Nothing here is
 * asynchronous by accident: every emit happens synchronously, the way a real
 * child's events do.
 */
class FakeHelper implements HelperProcess {
	exitCode: number | null = null
	signalCode: NodeJS.Signals | null = null
	written: string[] = []
	ended = false
	private readonly exitListeners: (() => void)[] = []
	private readonly errorListeners: (() => void)[] = []
	private readonly dataListeners: ((chunk: string) => void)[] = []
	private readonly stdinErrorListeners: ((err: Error) => void)[] = []
	/** Lets a test script replies to commands, like the real helper would. */
	onCommand?: (command: { type: string }) => void

	on(event: 'exit' | 'error', listener: () => void): this {
		;(event === 'exit' ? this.exitListeners : this.errorListeners).push(
			listener,
		)
		return this
	}

	removeListener(event: 'exit' | 'error', listener: () => void): this {
		const list = event === 'exit' ? this.exitListeners : this.errorListeners
		const index = list.indexOf(listener)
		if (index !== -1) list.splice(index, 1)
		return this
	}

	kill(signal?: NodeJS.Signals): boolean {
		if (this.exitCode !== null || this.signalCode !== null) return false
		this.signalCode = signal ?? 'SIGTERM'
		for (const listener of [...this.exitListeners]) listener()
		return true
	}

	stdin = {
		write: (line: string): void => {
			this.written.push(line)
			try {
				const command = JSON.parse(line) as { type: string }
				this.onCommand?.(command)
			} catch {
				// Not a command this test cares about.
			}
		},
		end: (): void => {
			this.ended = true
		},
		on: (event: 'error', listener: (err: Error) => void): void => {
			if (event === 'error') this.stdinErrorListeners.push(listener)
		},
	}

	stdout = {
		on: (event: 'data', listener: (chunk: string) => void): void => {
			if (event === 'data') this.dataListeners.push(listener)
		},
	}

	stderr = {
		on: (event: 'data', listener: (chunk: string) => void): void => {
			void event
			void listener
		},
	}

	emitFrame(message: SrtpHelperMessage): void {
		for (const listener of [...this.dataListeners]) {
			listener(`${JSON.stringify(message)}\n`)
		}
	}

	emitStdinError(): void {
		for (const listener of [...this.stdinErrorListeners]) {
			listener(new Error('EPIPE'))
		}
	}

	/** Fires the child's 'error' event without the child dying, as a failed
	 * signal delivery does. */
	emitError(): void {
		for (const listener of [...this.errorListeners]) listener()
	}

	exit(code = 0): void {
		if (this.exitCode !== null || this.signalCode !== null) return
		this.exitCode = code
		for (const listener of [...this.exitListeners]) listener()
	}

	commands(): { type: string }[] {
		return this.written.map((line) => JSON.parse(line) as { type: string })
	}
}

type LockLog = {
	acquires: { port: number; instanceId: string }[]
	releases: { port: number; instanceId: string }[]
	heartbeats: number[]
	/** What the next acquisition attempt returns; defaults to true. */
	nextAcquire: boolean
	/** What the next heartbeat returns. */
	nextHeartbeat: 'ok' | 'lostLock' | 'writeError'
}

const makeLocks = () => {
	const log: LockLog = {
		acquires: [],
		releases: [],
		heartbeats: [],
		nextAcquire: true,
		nextHeartbeat: 'ok',
	}
	return {
		log,
		locks: {
			tryAcquireKinesisLock: async (port: number, id: string) => {
				log.acquires.push({ port, instanceId: id })
				return log.nextAcquire
			},
			releaseKinesisLock: async (port: number, id: string) => {
				log.releases.push({ port, instanceId: id })
			},
			updateLastPacketTime: async (port: number, id: string) => {
				void id
				log.heartbeats.push(port)
				return log.nextHeartbeat
			},
		},
	}
}

const makeFloors = () => {
	const log = {
		reads: 0,
		/** Fail the next read; an unreadable floor must not look like a missing one. */
		failNextRead: false,
		raised: [] as number[],
		floor: undefined as number | undefined,
	}
	return {
		log,
		floors: {
			getSrtpIndexFloor: async () => {
				log.reads++
				if (log.failNextRead) {
					log.failNextRead = false
					throw new Error('dynamodb unavailable')
				}
				return log.floor
			},
			raiseSrtpIndexFloor: async (_port: number, index: number) => {
				log.raised.push(index)
			},
		},
	}
}

const key: SrtpPortKey = {
	keyHex: 'ab'.repeat(30),
	ssrc: 42,
	cipher: 'aes-128-icm',
	auth: 'hmac-sha1-80',
	keyFingerprint: '0123456789abcdef',
	generation: 0,
}

const waitFor = async (
	condition: () => boolean,
	timeoutMs = 5_000,
	what = 'a condition',
): Promise<void> => {
	const deadline = Date.now() + timeoutMs
	while (!condition()) {
		if (Date.now() > deadline) {
			throw new Error(`timed out waiting for ${what}`)
		}
		await new Promise((resolve) => setTimeout(resolve, 10))
	}
}

/** The supervisors created by a test, stopped however the test ends. */
const active: SrtpPortSupervisor[] = []

afterEach(async () => {
	for (const supervisor of active.splice(0)) {
		await supervisor.stop()
	}
})

const makeSupervisor = () => {
	// A fresh fake per spawn, like a real child: a respawned session is a different
	// process, and its predecessor's late frames must be distinguishable from its.
	const helper = new FakeHelper()
	const locks = makeLocks()
	const floors = makeFloors()
	const spawned: FakeHelper[] = []
	const transitions: { from: SrtpSupervisorState; to: SrtpSupervisorState }[] =
		[]
	const supervisor = new SrtpPortSupervisor({
		port: 6000,
		instanceId: 'i-test',
		key,
		streamName: 'test-video-6000',
		spawnHelper: (_init: SrtpHelperInit) => {
			void _init
			const child = spawned.length === 0 ? helper : new FakeHelper()
			spawned.push(child)
			return child
		},
		locks: locks.locks,
		floors: floors.floors,
		timeouts: {
			tickMs: 15,
			readyMs: 400,
			acquireRetryMs: 60,
			stoppedAckMs: 400,
			frameStallMs: 200,
			restartMs: 10,
			restartMaxMs: 50,
		},
		onTransition: (from, to) => {
			transitions.push({ from, to })
			// The invariant, checked after every single transition the machine makes.
			const status = supervisor.status()
			assert.deepStrictEqual(
				status.lockHeld,
				status.state === 'producing' || status.state === 'stopping',
				`lock held must mean producing (after ${from} -> ${to})`,
			)
		},
	})
	active.push(supervisor)
	return { supervisor, helper, locks, floors, spawned, transitions }
}

/**
 * A supervisor built from parts the fixed makeSupervisor stubs cannot express:
 * spawners that fail or defer, floors that hang. Tracks every spawned fake the
 * same way makeSupervisor does.
 */
const bareSupervisor = (
	spawnHelper: SrtpPortSupervisorConfig['spawnHelper'],
	over: Partial<SrtpPortSupervisorConfig> = {},
): { supervisor: SrtpPortSupervisor; spawned: FakeHelper[] } => {
	const locks = makeLocks()
	const floors = makeFloors()
	const spawned: FakeHelper[] = []
	const supervisor = new SrtpPortSupervisor({
		port: 6000,
		instanceId: 'i-test',
		key,
		streamName: 'test-video-6000',
		...over,
		locks: over.locks ?? locks.locks,
		floors: over.floors ?? floors.floors,
		timeouts: {
			tickMs: 15,
			readyMs: 400,
			acquireRetryMs: 60,
			stoppedAckMs: 400,
			frameStallMs: 200,
			restartMs: 10,
			restartMaxMs: 50,
		},
		spawnHelper: (
			init: SrtpHelperInit,
		): HelperProcess | Promise<HelperProcess> => {
			const result = spawnHelper(init)
			if (result instanceof Promise) {
				return result.then((child) => {
					spawned.push(child as FakeHelper)
					return child
				})
			}
			spawned.push(result as FakeHelper)
			return result
		},
	})
	active.push(supervisor)
	return { supervisor, spawned }
}

const ready = {
	t: 'ready',
	v: SRTP_HELPER_PROTOCOL_VERSION,
	port: 6000,
} as const
const authOkFirst = (roc = 0) =>
	({ t: 'auth', status: 'ok', first: true, roc, trials: 1 }) as const
const statsWith = (authenticated: number) =>
	({
		t: 'stats',
		inputs: 10,
		inputBytes: 100,
		authenticated,
		aus: 1,
		roc: 0,
		drops: 0,
	}) as const

/**
 * Drives a helper through spawn and ready to the searching state.
 *
 * The init frame being written is the sync point: it happens at the end of
 * spawnSession, after every listener is attached, so a test that waits for it
 * cannot emit a frame nobody is listening for yet.
 */
const toSearching = async (
	supervisor: SrtpPortSupervisor,
	helper: FakeHelper,
): Promise<void> => {
	await waitFor(() => helper.written.length > 0, 5_000, 'the init frame')
	helper.emitFrame(ready)
	await waitFor(
		() => supervisor.currentState === 'searching',
		5_000,
		'searching after ready',
	)
}

void describe('SrtpPortSupervisor', () => {
	void it('sends the key in the init frame and searches once the helper is ready', async () => {
		const { supervisor, helper } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		const init = JSON.parse(helper.written[0] ?? '{}') as {
			type: string
			key: string
			ssrc: number
		}
		assert.strictEqual(init.type, 'init')
		assert.strictEqual(init.key, key.keyHex)
		assert.strictEqual(init.ssrc, 42)
	})

	void it('acquires the lock only after authentication, and grants production', async () => {
		const { supervisor, helper, locks } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		// No frames yet, so nothing was attempted.
		assert.strictEqual(locks.log.acquires.length, 0)
		helper.emitFrame(authOkFirst())
		await waitFor(
			() => supervisor.currentState === 'producing',
			5_000,
			'producing after authentication',
		)
		assert.strictEqual(locks.log.acquires.length, 1)
		assert.ok(
			helper.commands().some((c) => c.type === 'start'),
			'the producing grant must be sent',
		)
		// Authenticated traffic refreshes the lease.
		helper.emitFrame(statsWith(5))
		await waitFor(() => locks.log.heartbeats.length > 0, 5_000, 'a heartbeat')
	})

	void it('keeps trying the lock while another instance holds it', async () => {
		const { supervisor, helper, locks } = makeSupervisor()
		locks.log.nextAcquire = false
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame(authOkFirst())
		await waitFor(
			() => supervisor.currentState === 'acquiring',
			5_000,
			'acquiring',
		)
		assert.strictEqual(locks.log.acquires.length, 1)
		// Traffic that keeps authenticating keeps the retry alive; the retry cadence
		// is the tick's, so several stats frames pass before the next attempt.
		for (let i = 0; i < 20 && locks.log.acquires.length < 2; i++) {
			helper.emitFrame(statsWith(10 + i))
			await new Promise((r) => setTimeout(r, 20))
		}
		assert.ok(locks.log.acquires.length >= 2, 'must retry the acquisition')
		// Never a grant, never a release.
		assert.ok(!helper.commands().some((c) => c.type === 'start'))
		assert.strictEqual(locks.log.releases.length, 0)
	})

	void it('auth loss while producing releases the lock and returns to searching', async () => {
		const { supervisor, helper, locks } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame(authOkFirst())
		await waitFor(() => supervisor.currentState === 'producing')
		helper.emitFrame({ t: 'auth', status: 'lost', sinceMs: 4000 })
		await waitFor(() => supervisor.currentState === 'searching')
		assert.strictEqual(locks.log.releases.length, 1)
	})

	void it('a lost lease stops the producer and does not release what is not ours', async () => {
		const { supervisor, helper, locks } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame(authOkFirst())
		await waitFor(() => supervisor.currentState === 'producing')
		locks.log.nextHeartbeat = 'lostLock'
		helper.emitFrame(statsWith(5))
		await waitFor(() => supervisor.currentState === 'stopping')
		assert.ok(
			helper.commands().some((c) => c.type === 'stop'),
			'the producer must be stopped',
		)
		helper.emitFrame({ t: 'stopped', index: 5000 })
		await waitFor(() => supervisor.currentState === 'searching')
		// The row belongs to whoever took it: no release.
		assert.strictEqual(locks.log.releases.length, 0)
	})

	void it('a helper exit releases the lock and restarts the session', async () => {
		const { supervisor, helper, locks, spawned } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame(authOkFirst())
		await waitFor(() => supervisor.currentState === 'producing')
		helper.exit(4)
		await waitFor(() => supervisor.currentState === 'cooldown')
		assert.strictEqual(locks.log.releases.length, 1)
		// The cooldown expires and a fresh session is spawned.
		await waitFor(() => spawned.length >= 2, 5_000, 'a respawn')
		await waitFor(() => supervisor.currentState === 'starting')
	})

	void it('a non-retryable fatal disables the port', async () => {
		const { supervisor, helper } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame({
			t: 'fatal',
			reason: 'missing-element',
			message: 'kvssink is not installed',
		})
		helper.exit(2)
		await waitFor(() => supervisor.currentState === 'disabled')
	})

	void it('an unreadable floor does not start a helper', async () => {
		const { supervisor, floors, spawned } = makeSupervisor()
		floors.log.failNextRead = true
		supervisor.start()
		await waitFor(() => supervisor.currentState === 'cooldown')
		assert.strictEqual(spawned.length, 0)
		// The read is retried when the cooldown expires.
		await waitFor(() => spawned.length === 1, 5_000, 'the retried start')
	})

	void it('raises the floor from index frames, even from a dying session', async () => {
		const { supervisor, helper, floors } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame({ t: 'index', index: 12345 })
		await waitFor(() => floors.log.raised.includes(12345))
		// After the helper is gone, its last index report still counts.
		helper.exit(0)
		await waitFor(() => supervisor.currentState === 'cooldown')
		helper.emitFrame({ t: 'index', index: 54321 })
		await waitFor(() => floors.log.raised.includes(54321))
	})

	void it('carries the search position into the next session', async () => {
		const { supervisor, helper, spawned } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame({
			t: 'auth',
			status: 'fail',
			candidate: 5,
			inputs: 10,
			drops: 5,
			searchFrom: 2000,
		})
		helper.exit(0)
		await waitFor(() => spawned.length === 2, 5_000, 'the second session')
		await toSearching(supervisor, spawned[1] as FakeHelper)
		const init = JSON.parse((spawned[1] as FakeHelper).written[0] ?? '{}') as {
			searchFrom?: number
		}
		assert.strictEqual(init.searchFrom, 2000)
	})

	void it('shuts down by stopping the producer, releasing the lock, and ending the helper', async () => {
		const { supervisor, helper, locks, floors } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame(authOkFirst())
		await waitFor(() => supervisor.currentState === 'producing')
		// The helper answers the stop command with the ack, as the real one does -
		// and, like the real one, it stays alive afterwards (the stop only
		// retracts production; the process lives on for the next grant).
		helper.onCommand = (command) => {
			if (command.type === 'stop') {
				helper.emitFrame({ t: 'stopped', index: 6000 })
			}
		}
		const before = Date.now()
		await supervisor.stop()
		const shutdownMs = Date.now() - before
		assert.strictEqual(supervisor.currentState, 'terminated')
		assert.strictEqual(locks.log.releases.length, 1)
		assert.ok(
			helper.ended || helper.exitCode !== null || helper.signalCode !== null,
		)
		// The ack must actually end the wait, not the timeout: a shutdown that
		// polls its own serialized queue for the ack can only ever time out (the
		// frame is queued behind the shutdown itself), costing the full ack
		// window on every graceful stop and killing the helper anyway.
		assert.ok(
			shutdownMs < 300,
			`the acked stop must resolve promptly, took ${String(shutdownMs)}ms`,
		)
		// The ack's index is the helper's last word on the floor; a shutdown that
		// only sees the acked frame as a post-terminated queue entry drops it.
		assert.ok(
			floors.log.raised.includes(6000),
			'the acked stopped frame must raise the floor',
		)
		// A frame after termination changes nothing.
		const acquiresBefore = locks.log.acquires.length
		helper.emitFrame(authOkFirst())
		await new Promise((r) => setTimeout(r, 50))
		assert.strictEqual(supervisor.currentState, 'terminated')
		assert.strictEqual(locks.log.acquires.length, acquiresBefore)
	})

	void it('ends a helper that never becomes ready', async () => {
		const { supervisor, helper } = makeSupervisor()
		supervisor.start()
		// No ready frame: the tick's deadline must end the session.
		await waitFor(() => supervisor.currentState === 'cooldown')
		assert.ok(
			helper.exitCode !== null || helper.signalCode !== null,
			'the silent helper must be killed',
		)
	})

	void it('ends a helper that goes silent while producing', async () => {
		const { supervisor, helper, locks } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame(authOkFirst())
		await waitFor(() => supervisor.currentState === 'producing')
		// No further frames at all: the stall deadline must end the session and
		// release the lock via the exit path.
		await waitFor(
			() => supervisor.currentState === 'cooldown',
			3_000,
			'the stall teardown',
		)
		assert.strictEqual(locks.log.releases.length, 1)
	})

	void it('ends a helper that goes silent before it ever authenticates', async () => {
		const { supervisor, helper, spawned } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		// No further frames at all, and no lock to lose: a wedged pre-
		// authentication helper holds the bound port with nothing to show for
		// it, so the same stall deadline must end the session here too - not
		// only in the producing state.
		await waitFor(
			() => supervisor.currentState === 'cooldown',
			3_000,
			'the stall teardown',
		)
		assert.ok(
			helper.exitCode !== null || helper.signalCode !== null,
			'the silent helper must be killed',
		)
		await waitFor(() => spawned.length === 2, 5_000, 'a respawn')
	})

	void it('ends a helper that goes silent while waiting for the lock', async () => {
		const { supervisor, helper, locks } = makeSupervisor()
		locks.log.nextAcquire = false
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame(authOkFirst())
		await waitFor(() => supervisor.currentState === 'acquiring')
		// Silence while acquiring: the stall deadline must end this session too.
		await waitFor(
			() => supervisor.currentState === 'cooldown',
			3_000,
			'the stall teardown',
		)
		assert.ok(
			helper.exitCode !== null || helper.signalCode !== null,
			'the silent helper must be killed',
		)
	})

	void it('never spawns a helper after shutdown, even when the floor read was still pending', async () => {
		let resolveRead: ((floor: number | undefined) => void) | undefined
		let reads = 0
		const { supervisor, spawned } = bareSupervisor(() => new FakeHelper(), {
			floors: {
				getSrtpIndexFloor: async () => {
					reads++
					return new Promise<number | undefined>((resolve) => {
						resolveRead = resolve
					})
				},
				raiseSrtpIndexFloor: async () => {},
			},
		})
		supervisor.start()
		await waitFor(() => reads === 1, 5_000, 'the floor read to start')
		// Shutdown arrives while the startup is still awaiting its floor read.
		// Startup must be part of the serialized queue for this to be safe: a
		// detached startup lets shutdown complete first, then resumes, spawns a
		// helper into the terminated port, and flips it back to 'starting'.
		const stopping = supervisor.stop()
		resolveRead?.(undefined)
		await stopping
		await waitFor(
			() => supervisor.currentState === 'terminated',
			5_000,
			'terminated',
		)
		await new Promise((r) => setTimeout(r, 300))
		assert.strictEqual(supervisor.currentState, 'terminated')
		assert.ok(
			spawned.every((h) => h.exitCode !== null || h.signalCode !== null),
			'no helper may be left alive by a shutdown that raced its startup',
		)
	})

	void it('contains a spawner that fails, and retries it on cooldown', async () => {
		let attempts = 0
		const { supervisor, spawned } = bareSupervisor(() => {
			attempts++
			if (attempts === 1) throw new Error('no credentials for the helper')
			return new FakeHelper()
		})
		supervisor.start()
		// The spawner is the transport's seam (it resolves the helper's AWS
		// credentials); its failure is a failed start like any other, never a
		// rejection thrown into the queue.
		await waitFor(
			() => supervisor.currentState === 'cooldown',
			5_000,
			'the failed spawn contained',
		)
		await waitFor(() => spawned.length === 1, 5_000, 'the retried spawn')
		assert.strictEqual(attempts, 2)
	})

	void it('carries no search position across a floor the index width moved', async () => {
		const { supervisor, helper, floors, spawned } = makeSupervisor()
		// A 48-bit packet index is (roc << 16) | seq with a uint32 roc; this
		// helper builds one arithmetically, the way the floor store numbers it.
		const indexAt = (roc: number, seq: number): number => roc * 65536 + seq
		// The floor sits at rollover 2; a session climbs and reports its far
		// position, which is carried against that base.
		floors.log.floor = indexAt(2, 1000)
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame({
			t: 'auth',
			status: 'fail',
			candidate: 5,
			inputs: 10,
			drops: 5,
			searchFrom: 10,
		})
		helper.exit(0)
		// The floor then moves to rollover 65538 - an index no int32 shift can
		// address, and whose base a `floor >> 16` truncates to exactly 2, the
		// base the carry was stored against. The carry belongs to the old floor
		// and must be dropped: a new one only costs a repeated climb, but a
		// wrongly-kept one validates a search position against a floor that no
		// longer exists.
		floors.log.floor = indexAt(65538, 1000)
		await waitFor(() => spawned.length === 2, 5_000, 'the second session')
		await toSearching(supervisor, spawned[1] as FakeHelper)
		const init = JSON.parse((spawned[1] as FakeHelper).written[0] ?? '{}') as {
			floor?: number
			searchFrom?: number
		}
		assert.strictEqual(init.floor, indexAt(65538, 1000))
		assert.strictEqual(
			init.searchFrom,
			undefined,
			'a carry from a different floor must not survive the floor moving',
		)
	})

	void it('never releases the lock on an error event alone', async () => {
		const { supervisor, helper, locks } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		helper.emitFrame(authOkFirst())
		await waitFor(() => supervisor.currentState === 'producing')
		// The child errors while very much alive - a signal that could not be
		// delivered, say. Releasing the lock here would hand the stream to
		// another writer while this one may still be flushing: the child must
		// be ended, its death verified, and only then the teardown.
		helper.emitError()
		await waitFor(
			() => supervisor.currentState === 'cooldown',
			5_000,
			'the teardown after a verified end',
		)
		assert.ok(
			helper.exitCode !== null || helper.signalCode !== null,
			'the helper must actually be dead before the lock goes',
		)
		assert.strictEqual(locks.log.releases.length, 1)
	})

	void it('a late frame from a replaced session cannot act', async () => {
		const { supervisor, helper, locks, spawned } = makeSupervisor()
		supervisor.start()
		await toSearching(supervisor, helper)
		// Session one dies and is replaced.
		helper.exit(0)
		await waitFor(() => spawned.length === 2, 5_000, 'the second session')
		const second = spawned[1] as FakeHelper
		await toSearching(supervisor, second)
		// The old session reports an authentication. It must not acquire anything.
		const acquiresBefore = locks.log.acquires.length
		helper.emitFrame(authOkFirst())
		await new Promise((r) => setTimeout(r, 100))
		assert.strictEqual(locks.log.acquires.length, acquiresBefore)
		assert.strictEqual(supervisor.currentState, 'searching')
		// The new session's authentication still can.
		second.emitFrame(authOkFirst())
		await waitFor(() => supervisor.currentState === 'producing')
	})
})
