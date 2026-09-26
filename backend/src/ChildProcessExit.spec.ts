import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { EventEmitter } from 'node:events'
import { describe, it } from 'node:test'

import { endChildProcess, type ExitingChild } from './ChildProcessExit.ts'

const delay = async (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms))

/**
 * A child process that can be told to survive signals.
 *
 * The real thing surviving SIGTERM is the case that matters here - a kvssink still
 * uploading, or a GStreamer pipeline wedged in a flush - and it is not reproducible
 * with a process that exits when asked.
 */
class ChildFake extends EventEmitter implements ExitingChild {
	readonly signals: NodeJS.Signals[] = []
	exitCode: number | null = null
	signalCode: NodeJS.Signals | null = null
	/** Signals this process ignores; anything else ends it. */
	survives: NodeJS.Signals[] = []
	/**
	 * Makes the child become gone as a listener is attached, emitting nothing.
	 *
	 * Models an ExitingChild whose exit fields and exit event are not set in the same
	 * turn - the interleaving a check taken only before attaching would miss. Keyed to
	 * a signal so it lands during one chosen wait rather than the first, since a flip
	 * during an earlier wait would be caught by the next wait's opening check and
	 * prove nothing.
	 */
	goneWhenListenedToAfter?: NodeJS.Signals

	override on(
		event: string | symbol,
		listener: (...args: unknown[]) => void,
	): this {
		if (
			this.goneWhenListenedToAfter !== undefined &&
			this.signals.includes(this.goneWhenListenedToAfter)
		) {
			this.exitCode = 0
		}
		return super.on(event, listener)
	}

	kill(signal: NodeJS.Signals = 'SIGTERM'): boolean {
		this.signals.push(signal)
		if (!this.survives.includes(signal)) this.exit(signal)
		return true
	}

	exit(signal?: NodeJS.Signals): void {
		if (this.exitCode !== null || this.signalCode !== null) return
		if (signal === undefined) this.exitCode = 0
		else this.signalCode = signal
		this.emit('exit')
	}

	/**
	 * A spawn that never happened, exactly as Node reports one.
	 *
	 * Verified against the real thing: exitCode is set as 'error' is emitted, and
	 * 'exit' is never emitted at all.
	 */
	failSpawn(): void {
		this.exitCode = -2
		this.emit('error', new Error('spawn ENOENT'))
	}
}

/** Resolves to true only if the promise is still pending after `ms`. */
const stillPending = async (
	promise: Promise<unknown>,
	ms: number,
): Promise<boolean> => {
	const pending = Symbol('pending')
	const result = await Promise.race([promise, delay(ms).then(() => pending)])
	return result === pending
}

void describe('endChildProcess', () => {
	// The whole reason this waits: PortIngestion releases the port's DynamoDB lock as
	// soon as the producer's stop() resolves, so another instance may acquire the same
	// Kinesis stream from that moment. Resolving while the child is still alive makes
	// two writers to one stream.
	void it('resolves only once the child has actually exited', async () => {
		const child = new ChildFake()
		child.survives = ['SIGTERM', 'SIGKILL']

		const ended = endChildProcess(child, { sigkillAfterMs: 20 })

		assert.strictEqual(
			await stillPending(ended, 120),
			true,
			'resolved while the child was still running',
		)
		assert.deepStrictEqual(
			child.signals,
			['SIGTERM', 'SIGKILL'],
			'the escalation has to happen even though neither signal worked',
		)

		child.survives = []
		child.exit('SIGKILL')
		await ended
	})

	void it('escalates to SIGKILL only after SIGTERM has had its window', async () => {
		const child = new ChildFake()
		child.survives = ['SIGTERM']

		const ended = endChildProcess(child, { sigkillAfterMs: 80 })

		await delay(30)
		assert.deepStrictEqual(
			child.signals,
			['SIGTERM'],
			'SIGKILL must not arrive before SIGTERM has had its window',
		)

		await ended
		assert.deepStrictEqual(child.signals, ['SIGTERM', 'SIGKILL'])
	})

	void it('never signals a child that stops on its own', async () => {
		const child = new ChildFake()

		const ended = endChildProcess(child, { sigkillAfterMs: 1000 })
		await ended

		assert.deepStrictEqual(child.signals, ['SIGTERM'])
	})

	void it('resolves without signalling a child that is already gone', async () => {
		const child = new ChildFake()
		child.exit()

		await endChildProcess(child, { sigkillAfterMs: 1000 })

		assert.deepStrictEqual(child.signals, [])
	})

	// The Kinesis pipeline stops by closing GStreamer's input, so it needs a window in
	// which nothing is signalled at all: kvssink uploads what it is holding when the
	// stream ends, and SIGTERM during that would cut the tail off the recording.
	void describe('the unsignalled drain', () => {
		void it('lets a child that exits during the drain go unsignalled', async () => {
			const child = new ChildFake()
			const ended = endChildProcess(child, {
				drainMs: 1000,
				sigkillAfterMs: 1000,
			})

			await delay(20)
			child.exit()
			await ended

			assert.deepStrictEqual(
				child.signals,
				[],
				'a child that finished flushing must not be signalled',
			)
		})

		void it('signals a child that outlasts the drain', async () => {
			const child = new ChildFake()
			child.survives = ['SIGTERM']

			const ended = endChildProcess(child, {
				drainMs: 30,
				sigkillAfterMs: 30,
			})

			assert.strictEqual(
				await stillPending(ended, 15),
				true,
				'nothing may be signalled while the drain window is open',
			)
			assert.deepStrictEqual(child.signals, [])

			await ended
			assert.deepStrictEqual(child.signals, ['SIGTERM', 'SIGKILL'])
		})
	})

	// Each window that expires rather than firing has to take its own listener with it,
	// or a child stopped after several escalations accumulates them.
	void it('leaves no exit listener behind on the windows that expire', async () => {
		const child = new ChildFake()
		child.survives = ['SIGTERM']

		await endChildProcess(child, { drainMs: 20, sigkillAfterMs: 20 })

		assert.deepStrictEqual(child.signals, ['SIGTERM', 'SIGKILL'])
		assert.strictEqual(child.listenerCount('exit'), 0)
		assert.strictEqual(child.listenerCount('error'), 0)
	})

	/**
	 * Waiting for an event that is never coming is the one way this can hang, and the
	 * final wait has no deadline - so it would hang for the life of the process, with
	 * the port's lock never released.
	 */
	void describe('a child that never emits an exit', () => {
		/**
		 * A spawn that failed, which is the case Node never emits 'exit' for.
		 *
		 * The failure arrives after SIGTERM's window has already closed, so the wait
		 * that has to notice it is the last one - the one with no deadline of its own.
		 * `timeout` is what turns the regression into a failure rather than a hung
		 * suite; in production it would be a port whose lock is never released.
		 */
		void it(
			'settles when the spawn failure arrives',
			{ timeout: 5000 },
			async () => {
				const child = new ChildFake()
				child.survives = ['SIGTERM', 'SIGKILL']
				setTimeout(() => child.failSpawn(), 60)

				await endChildProcess(child, { sigkillAfterMs: 20 })

				assert.deepStrictEqual(child.signals, ['SIGTERM', 'SIGKILL'])
			},
		)

		/**
		 * The defensive half: ExitingChild is a shape, not one class.
		 *
		 * A real ChildProcess sets its exit fields in the same turn it emits 'exit',
		 * so a check taken just before attaching cannot miss it. Anything that sets
		 * the fields first and emits later would be waited on for an event already
		 * past, which is why the check is repeated with the listeners in place.
		 */
		void it(
			'settles for a child gone by the time it is listened to',
			{ timeout: 5000 },
			async () => {
				const child = new ChildFake()
				child.survives = ['SIGTERM', 'SIGKILL']
				// Gone by the final wait, which is the one with no deadline to fall
				// back on.
				child.goneWhenListenedToAfter = 'SIGKILL'

				await endChildProcess(child, { sigkillAfterMs: 20 })
			},
		)

		// And the same against the real thing, so the semantics the fake models are
		// Node's rather than mine.
		void it(
			'matches how Node reports a spawn that failed',
			{ timeout: 10_000 },
			async () => {
				const child = spawn('definitely-not-a-real-binary-4f3a9c', [])
				const events: string[] = []
				child.on('error', () => events.push('error'))
				child.on('exit', () => events.push('exit'))

				await endChildProcess(child, { sigkillAfterMs: 50 })

				assert.deepStrictEqual(
					events,
					['error'],
					'no exit is ever emitted, so waiting only for one would never end',
				)
				assert.notStrictEqual(child.exitCode, null)
			},
		)
	})
})
