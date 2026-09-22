import assert from 'node:assert/strict'
import type { ChildProcess, spawn as nodeSpawn } from 'node:child_process'
import dgram from 'node:dgram'
import { EventEmitter } from 'node:events'
import { PassThrough } from 'node:stream'
import { describe, it } from 'node:test'

import { Logger, type LogContext } from './Logger.ts'
import { SRTP_HELPER_PROTOCOL_VERSION } from './SrtpHelperProtocol.ts'
import type { SrtpKeyStore } from './SrtpKeyStore.ts'
import { SrtpProducer } from './SrtpProducer.ts'

const PORT = 6000
const KEY = '000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d'

const delay = async (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms))

class CapturingLogger extends Logger {
	readonly warnings: { message: string; context?: LogContext }[] = []
	constructor() {
		super('SrtpProducerSpec')
	}
	override warn(message: string, context?: LogContext): void {
		this.warnings.push({ message, context })
	}
}

/**
 * Stands in for srtp_pipeline.py: it speaks the same line protocol over stdout and,
 * unlike the real helper, can be told to ignore SIGTERM.
 */
class HelperFake extends EventEmitter {
	readonly stdin = new PassThrough()
	readonly stdout = new PassThrough()
	readonly stderr = new PassThrough()
	readonly signals: string[] = []
	exitCode: number | null = null
	signalCode: NodeJS.Signals | null = null
	/** Models a helper wedged in a flush: signals arrive, the process stays. */
	ignoreSignals = false

	constructor(private readonly relayPort: number) {
		super()
		// The real helper reports readiness once it has the key, so readiness is
		// triggered by the init frame here too.
		this.stdin.on('data', () => this.reportReady())
	}

	kill(signal?: NodeJS.Signals): boolean {
		this.signals.push(signal ?? 'SIGTERM')
		if (!this.ignoreSignals) this.exit(0)
		return true
	}

	exit(code: number): void {
		if (this.exitCode !== null) return
		this.exitCode = code
		this.emit('exit', code, null)
	}

	private reportReady(): void {
		this.stdout.write(
			`${JSON.stringify({
				t: 'ready',
				v: SRTP_HELPER_PROTOCOL_VERSION,
				relayPort: this.relayPort,
				pid: 4242,
			})}\n`,
		)
	}
}

/**
 * A bound loopback socket standing in for the helper's udpsrc.
 *
 * Only the ordering tests need one - the rest are happier with nothing listening, so
 * that a relayed datagram draws the asynchronous error they are about.
 */
class RelayFake {
	readonly received: Buffer[] = []
	private readonly socket = dgram.createSocket('udp4')

	async bind(): Promise<number> {
		this.socket.on('message', (data) => this.received.push(data))
		await new Promise<void>((resolve) =>
			this.socket.bind(0, '127.0.0.1', resolve),
		)
		return this.socket.address().port
	}

	close(): void {
		this.socket.close()
	}

	/** Resolves once `count` datagrams have arrived, or throws on timeout. */
	async waitFor(count: number, timeoutMs = 5000): Promise<void> {
		const deadline = Date.now() + timeoutMs
		while (this.received.length < count) {
			if (Date.now() > deadline)
				throw new Error(
					`only ${String(this.received.length)} of ${String(count)} datagrams arrived`,
				)
			await delay(5)
		}
	}
}

const start = async (
	options: {
		ignoreSignals?: boolean
		stopSigkillAfterMs?: number
		relayPort?: number
		/** Startup buffer handed to start(), as the state machine would. */
		datagrams?: Buffer[]
		/** Leaves the start pending, so a test can act during the replay. */
		awaitStart?: boolean
	} = {},
): Promise<{
	producer: SrtpProducer
	helper: HelperFake
	logger: CapturingLogger
	started: Promise<void>
}> => {
	// Nothing is bound to the default relay port: the fake helper only has to report
	// one, and most tests do not depend on the datagrams arriving anywhere.
	const helper = new HelperFake(options.relayPort ?? 45_454)
	helper.ignoreSignals = options.ignoreSignals ?? false
	const logger = new CapturingLogger()
	const producer = new SrtpProducer({
		keyStore: {
			getKeyForPort: () => ({
				keyHex: KEY,
				ssrc: 42,
				cipher: 'aes-128-icm',
				auth: 'hmac-sha1-80',
				keyFingerprint: 'abcdef0123456789',
			}),
		} as unknown as SrtpKeyStore,
		hints: {
			getSrtpRocHint: async () => undefined,
			putSrtpRocHint: async () => undefined,
		},
		region: 'eu-central-1',
		streamNameForPort: (port) => `test-video-${String(port)}`,
		kvsLogConfigPath: '/dev/null',
		stopSigkillAfterMs: options.stopSigkillAfterMs,
		spawn: (() =>
			helper as unknown as ChildProcess) as unknown as typeof nodeSpawn,
		logger,
	})
	const started = producer.start(PORT, options.datagrams ?? [], { epoch: 1 })
	if (options.awaitStart !== false) await started
	return { producer, helper, logger, started }
}

void describe('SrtpProducer', () => {
	// PortIngestion releases the port's lock the moment stop() returns, so another
	// instance can acquire the Kinesis stream from then on. A helper that has been
	// signalled but not yet reaped is still a writer to it.
	void describe('stopping', () => {
		void it('resolves only once the helper has exited', async () => {
			const { producer, helper } = await start({
				ignoreSignals: true,
				stopSigkillAfterMs: 20,
			})

			let resolved = false
			const stopping = producer.stop(PORT).then(() => {
				resolved = true
			})

			await delay(150)
			assert.deepStrictEqual(
				helper.signals,
				['SIGTERM', 'SIGKILL'],
				'the kill escalation still has to happen',
			)
			assert.strictEqual(
				resolved,
				false,
				'stop() resolved while the helper was still running',
			)

			helper.exit(137)
			await stopping
			assert.strictEqual(resolved, true)
			assert.strictEqual(producer.isActive(PORT), false)
		})

		void it('resolves without signalling a helper that is already gone', async () => {
			const { producer, helper } = await start()
			helper.exit(0)
			await producer.stop(PORT)
			assert.deepStrictEqual(helper.signals, [])
		})
	})

	/**
	 * The startup buffer only helps if it reaches the helper in order.
	 *
	 * SRTP rejects a packet whose sequence number falls outside its replay window, so
	 * one live datagram arriving ahead of the buffer makes libsrtp discard the older
	 * ones behind it - taking the keyframe the buffer was held for with them.
	 */
	void describe('replaying the startup buffer', () => {
		void it('sends live traffic behind the buffer, not ahead of it', async () => {
			const relay = new RelayFake()
			const relayPort = await relay.bind()
			// More than one batch, so the replay has to pause - and it is during a
			// pause that a live datagram used to overtake the rest of the buffer.
			const buffered = Array.from({ length: 180 }, (_, i) =>
				Buffer.from(`buffered-${String(i).padStart(3, '0')}`),
			)

			const { producer, started } = await start({
				relayPort,
				datagrams: buffered,
				awaitStart: false,
			})
			try {
				// Mid-replay: the first batch is out and the loop is in a pause.
				await relay.waitFor(1)
				producer.writePacket(PORT, Buffer.from('live'))

				await started
				await relay.waitFor(buffered.length + 1)

				assert.deepStrictEqual(
					relay.received.map((d) => d.toString()),
					[...buffered.map((d) => d.toString()), 'live'],
					'the live datagram must arrive after every buffered one',
				)
			} finally {
				await producer.stop(PORT)
				relay.close()
			}
		})

		void it('only reports itself active once the buffer is drained', async () => {
			const relay = new RelayFake()
			const relayPort = await relay.bind()
			const buffered = Array.from({ length: 180 }, (_, i) =>
				Buffer.from(`buffered-${String(i)}`),
			)

			const { producer, started } = await start({
				relayPort,
				datagrams: buffered,
				awaitStart: false,
			})
			try {
				await relay.waitFor(1)
				assert.strictEqual(
					producer.isActive(PORT),
					false,
					'a session mid-replay is not yet passing traffic through',
				)

				await started
				assert.strictEqual(producer.isActive(PORT), true)
			} finally {
				await producer.stop(PORT)
				relay.close()
			}
		})
	})

	/**
	 * Every pipe of the child, not only the ones that are read.
	 *
	 * An unhandled 'error' on a stream is rethrown and ends the Node process, so one
	 * pipe without a listener lets an SRTP-only failure take unencrypted ingest with
	 * it - the same failure the relay socket's handler exists for. stdin is the one
	 * that gets there in practice: the init frame is written to it immediately, and a
	 * helper that exits first turns that write into EPIPE.
	 */
	void describe('the helper streams', () => {
		for (const stream of ['stdin', 'stdout', 'stderr'] as const) {
			void it(`reports an error on ${stream} instead of letting it escape`, async () => {
				const { producer, helper, logger } = await start()
				try {
					// EventEmitter rethrows an 'error' nobody is listening for, so this
					// throws here and ends the process in production without a handler.
					assert.doesNotThrow(() =>
						helper[stream].emit('error', new Error('EPIPE')),
					)
					assert.deepStrictEqual(
						logger.warnings
							.filter((w) => w.message === 'SRTP helper stream error')
							.map((w): unknown[] => [w.context?.port, w.context?.stream]),
						[[PORT, stream]],
					)
					// A stream error is not the helper exiting, so the session stands.
					assert.strictEqual(producer.isActive(PORT), true)
				} finally {
					await producer.stop(PORT)
				}
			})
		}
	})

	void describe('the relay socket', () => {
		// Relaying to a port nothing is listening on is the real case: the helper's
		// udpsrc is gone, so the loopback datagram draws an ICMP port-unreachable and
		// the connected socket reports it asynchronously. An unhandled 'error' event on
		// a dgram socket ends the process, which would let an SRTP-only failure take
		// unencrypted ingest down with it - so a regression here kills the whole suite
		// rather than failing this one test.
		void it('reports asynchronous errors instead of letting them escape', async () => {
			const { producer, logger } = await start()
			try {
				producer.writePacket(PORT, Buffer.from('relayed'))
				const deadline = Date.now() + 2000
				while (
					!logger.warnings.some((w) => w.message.includes('relay socket')) &&
					Date.now() < deadline
				) {
					await delay(10)
				}
				assert.deepStrictEqual(
					logger.warnings
						.filter((w) => w.message.includes('relay socket'))
						.map((w) => w.context?.port),
					[PORT],
				)
				// And the session is still there, because a socket error is not the
				// helper exiting.
				assert.strictEqual(producer.isActive(PORT), true)
			} finally {
				await producer.stop(PORT)
			}
		})
	})
})
