import assert from 'node:assert/strict'
import type { ChildProcess, spawn as nodeSpawn } from 'node:child_process'
import { EventEmitter } from 'node:events'
import * as fs from 'node:fs'
import { tmpdir } from 'node:os'
import { PassThrough } from 'node:stream'
import { describe, it } from 'node:test'

import { KinesisIngestionPipeline } from './KinesisIngestionPipeline.ts'

// kvssink's credentials are resolved before anything is spawned; these satisfy that
// without touching a real account.
process.env.AWS_ACCESS_KEY_ID = 'AKIAEXAMPLEEXAMPLE00'
process.env.AWS_SECRET_ACCESS_KEY = 'example-secret-for-tests-only'

const delay = async (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms))

type Behaviour =
	/** Exits without ever opening its input: kvssink missing, a parse error. */
	| 'dies before opening its input'
	/** Neither opens its input nor exits: wedged starting up. */
	| 'never opens its input'
	/** Opens its input and reads it until the other end closes, then exits. */
	| 'opens its input'
	/** Opens its input, then closes it and exits before reading anything. */
	| 'opens its input and dies'

/**
 * Stands in for gst-launch-1.0, behind the `sh -c` the pipeline spawns.
 *
 * The one thing it has to get right is the FIFO: GStreamer's filesrc opening it for
 * reading is what completes the pipeline's own write-open, and not opening it is what
 * used to leave that write-open blocked for good.
 */
class GstFake extends EventEmitter {
	readonly stdout = new PassThrough()
	readonly stderr = new PassThrough()
	readonly signals: NodeJS.Signals[] = []
	exitCode: number | null = null
	signalCode: NodeJS.Signals | null = null
	received = ''

	constructor(
		command: string,
		private readonly behaviour: Behaviour,
	) {
		super()
		const fifo = /filesrc location="([^"]+)"/.exec(command)?.[1]
		assert.ok(fifo !== undefined, `no FIFO in ${command}`)
		setTimeout(() => this.run(fifo), 10)
	}

	private run(fifo: string): void {
		switch (this.behaviour) {
			case 'dies before opening its input':
				this.exit(1)
				return
			case 'never opens its input':
				return
			case 'opens its input': {
				const input = fs.createReadStream(fifo)
				input.on('data', (chunk) => {
					this.received += chunk.toString()
				})
				input.on('end', () => this.exit(0))
				return
			}
			case 'opens its input and dies':
				fs.open(fifo, 'r', (err, fd) => {
					if (err === null) fs.closeSync(fd)
					this.exit(1)
				})
				return
		}
	}

	kill(signal: NodeJS.Signals = 'SIGTERM'): boolean {
		this.signals.push(signal)
		this.exit(null, signal)
		return true
	}

	private exit(
		code: number | null,
		signal: NodeJS.Signals | null = null,
	): void {
		if (this.exitCode !== null || this.signalCode !== null) return
		this.exitCode = code
		this.signalCode = signal
		this.emit('exit', code, signal)
	}
}

const pipeline = (
	behaviour: Behaviour,
	options: { inputOpenTimeoutMs?: number } = {},
): { kinesis: KinesisIngestionPipeline; children: GstFake[] } => {
	const children: GstFake[] = []
	const kinesis = new KinesisIngestionPipeline({
		streamNamePrefix: 'test-video',
		region: 'eu-central-1',
		portRange: { start: 5000, end: 5009 },
		stopDrainMs: 1_000,
		stopSigkillAfterMs: 1_000,
		inputOpenTimeoutMs: options.inputOpenTimeoutMs,
		spawn: ((_command: string, args: string[]) => {
			const child = new GstFake(args[1] ?? '', behaviour)
			children.push(child)
			return child as unknown as ChildProcess
		}) as unknown as typeof nodeSpawn,
	})
	return { kinesis, children }
}

/** FIFOs this process has left behind for a port. */
const fifosFor = (port: number): string[] =>
	fs
		.readdirSync(tmpdir())
		.filter((name) =>
			name.startsWith(`kinesis-${String(port)}-${String(process.pid)}-`),
		)

void describe('KinesisIngestionPipeline', () => {
	/**
	 * A start has to end, however GStreamer fails.
	 *
	 * The pipeline spawns GStreamer and then opens the FIFO for writing, which blocks
	 * until GStreamer's filesrc opens the other end. Every listener on the child used
	 * to be attached only after that open, so a GStreamer that failed before opening
	 * its input - kvssink missing, a pipeline that does not parse - had its exit
	 * missed and left the open blocked for good: the port stuck in Starting with its
	 * lock held, and shutdown queued behind it.
	 */
	void describe('starting', () => {
		void it(
			'fails when GStreamer exits before opening its input',
			{ timeout: 10_000 },
			async () => {
				const { kinesis } = pipeline('dies before opening its input')
				await assert.rejects(
					async () => kinesis.start(5000, Buffer.from('ts')),
					/exited before opening its input/,
				)
				assert.strictEqual(kinesis.isActive(5000), false)
				assert.deepStrictEqual(fifosFor(5000), [], 'its FIFO is removed')
			},
		)

		/**
		 * The part that reached past one port.
		 *
		 * The blocked open sits on one of libuv's threadpool threads, four by default,
		 * shared with everything else that uses the pool - dns.lookup among them, which
		 * every AWS SDK call goes through. Four failed starts used to stall the whole
		 * process. Measured before the change: four stuck opens, and both fs.readFile
		 * and dns.lookup stalled.
		 */
		void it(
			'leaves the shared threadpool usable after several failures',
			{ timeout: 10_000 },
			async () => {
				const { kinesis } = pipeline('dies before opening its input')
				const ports = [5000, 5001, 5002, 5003, 5004]
				const results = await Promise.allSettled(
					ports.map(async (port) => kinesis.start(port)),
				)
				assert.ok(results.every((r) => r.status === 'rejected'))
				for (const port of ports) {
					assert.deepStrictEqual(
						fifosFor(port),
						[],
						`port ${String(port)}'s FIFO`,
					)
				}

				const read = fs.promises.readFile('/proc/self/stat')
				const outcome = await Promise.race([
					read.then(() => 'read'),
					delay(2_000).then(() => 'stalled'),
				])
				assert.strictEqual(outcome, 'read', 'a threadpool thread is still held')
			},
		)

		void it(
			'fails, and ends GStreamer, when it never opens its input',
			{ timeout: 10_000 },
			async () => {
				const { kinesis, children } = pipeline('never opens its input', {
					inputOpenTimeoutMs: 200,
				})
				await assert.rejects(
					async () => kinesis.start(5000),
					/did not open its input .* within 200ms/,
				)
				// A GStreamer that opened its input later would be a pipeline nobody owns.
				assert.deepStrictEqual(children[0]?.signals, ['SIGTERM'])
				assert.strictEqual(kinesis.isActive(5000), false)
				assert.deepStrictEqual(fifosFor(5000), [])
			},
		)

		/**
		 * GStreamer dying after it opened its input, before the start has finished.
		 *
		 * Two failures used to follow from this. The initial write fails with EPIPE, and
		 * the input stream had no 'error' listener yet, so Node rethrew it and the
		 * process ended. And if the write had got in first, the child's exit was
		 * handled as expected - the port was not registered yet - so a dead pipeline
		 * was registered as running with nothing to report it.
		 */
		void it(
			'fails, and does not crash, when GStreamer dies right after opening its input',
			{ timeout: 10_000 },
			async () => {
				const { kinesis } = pipeline('opens its input and dies')
				await assert.rejects(async () =>
					kinesis.start(5000, Buffer.alloc(64 * 1024)),
				)
				assert.strictEqual(kinesis.isActive(5000), false)
				assert.deepStrictEqual(fifosFor(5000), [])
			},
		)

		// The ordinary case, so the new deadline and checks are known not to get in the way.
		void it(
			'starts, and delivers the initial data, when GStreamer opens its input',
			{ timeout: 10_000 },
			async () => {
				const { kinesis, children } = pipeline('opens its input')
				await kinesis.start(5000, Buffer.from('first-segment'))
				try {
					assert.strictEqual(kinesis.isActive(5000), true)
					await delay(50)
					assert.strictEqual(children[0]?.received, 'first-segment')
				} finally {
					await kinesis.stop(5000)
				}
				assert.deepStrictEqual(fifosFor(5000), [])
			},
		)
	})
})
