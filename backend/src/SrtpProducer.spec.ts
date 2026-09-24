import assert from 'node:assert/strict'
import type { ChildProcess, spawn as nodeSpawn } from 'node:child_process'
import dgram from 'node:dgram'
import { EventEmitter } from 'node:events'
import { PassThrough } from 'node:stream'
import { describe, it } from 'node:test'

import { Logger, type LogContext } from './Logger.ts'
import { SRTP_HELPER_PROTOCOL_VERSION } from './SrtpHelperProtocol.ts'
import type { SrtpKeyStore } from './SrtpKeyStore.ts'
import { AUTH_LOSS_MS, SrtpProducer } from './SrtpProducer.ts'

const PORT = 6000
const KEY = '000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d'

const delay = async (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms))

class CapturingLogger extends Logger {
	readonly infos: { message: string; context?: LogContext }[] = []
	readonly warnings: { message: string; context?: LogContext }[] = []
	readonly errors: { message: string; context?: LogContext }[] = []
	constructor() {
		super('SrtpProducerSpec')
	}
	override info(message: string, context?: LogContext): void {
		this.infos.push({ message, context })
	}
	override warn(message: string, context?: LogContext): void {
		this.warnings.push({ message, context })
	}
	override error(message: string, _error?: Error, context?: LogContext): void {
		this.errors.push({ message, context })
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
	/** Cleared to model a helper that dies before it can report a relay port. */
	reportReadyOnInit = true

	constructor(private readonly relayPort: number) {
		super()
		// The real helper reports readiness once it has the key, so readiness is
		// triggered by the init frame here too.
		this.stdin.on('data', () => {
			if (this.reportReadyOnInit) this.reportReady()
		})
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
		/** Rollover counter this key and SSRC previously reached, if any. */
		rocHint?: number
		/** Cleared to model a helper that dies before its readiness is handled. */
		reportReady?: boolean
	} = {},
): Promise<{
	producer: SrtpProducer
	helper: HelperFake
	logger: CapturingLogger
	started: Promise<void>
	/** The arguments the helper was started with. */
	helperArgs: string[]
	/** Every authentication the producer reported, as the port it named. */
	authenticated: number[]
	/** Every rollover counter the producer persisted. */
	hintsWritten: number[]
}> => {
	// Nothing is bound to the default relay port: the fake helper only has to report
	// one, and most tests do not depend on the datagrams arriving anywhere.
	const helper = new HelperFake(options.relayPort ?? 45_454)
	helper.ignoreSignals = options.ignoreSignals ?? false
	helper.reportReadyOnInit = options.reportReady ?? true
	const helperArgs: string[] = []
	const authenticated: number[] = []
	const hintsWritten: number[] = []
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
			getSrtpRocHint: async () => options.rocHint,
			putSrtpRocHint: async (_port, roc) => {
				hintsWritten.push(roc)
			},
		},
		region: 'eu-central-1',
		streamNameForPort: (port) => `test-video-${String(port)}`,
		kvsLogConfigPath: '/dev/null',
		stopSigkillAfterMs: options.stopSigkillAfterMs,
		onAuthenticated: (port) => {
			authenticated.push(port)
		},
		spawn: ((_command: string, args: string[]) => {
			helperArgs.push(...args)
			return helper as unknown as ChildProcess
		}) as unknown as typeof nodeSpawn,
		logger,
	})
	const started = producer.start(PORT, options.datagrams ?? [], { epoch: 1 })
	if (options.awaitStart !== false) await started
	return {
		producer,
		helper,
		logger,
		started,
		helperArgs,
		authenticated,
		hintsWritten,
	}
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
	 * A session that is over can still be heard, but no longer act.
	 *
	 * The stdout handler holds its session for as long as frames arrive, and frames
	 * can arrive after the session is over - Node documents that stdio may still be
	 * open when 'exit' fires, and the exit handler closes the session at once. I could
	 * not get Node to deliver them in that order here in 600 tries, so the fake does
	 * it: it exits, then writes. Acting on such a frame could promote the lifetime that
	 * replaced the session, or overwrite its rollover counter.
	 */
	void describe('frames from a session that is over', () => {
		const authOk = (helper: HelperFake): void => {
			helper.stdout.write(
				`${JSON.stringify({ t: 'auth', status: 'ok', first: true, roc: 7, trials: 1 })}\n`,
			)
		}

		void it('neither reports authentication nor persists a counter after exit', async () => {
			const { helper, authenticated, hintsWritten } = await start()
			helper.exit(1)
			authOk(helper)
			await delay(20)
			assert.deepStrictEqual(authenticated, [])
			assert.deepStrictEqual(hintsWritten, [])
		})

		// A helper that reports ready and dies, with the exit handled first. Its relay
		// socket is closed by then, and connecting it throws synchronously - from the
		// stdout handler, where nothing would catch it.
		void it('ignores a ready that arrives after the helper has exited', async () => {
			const { helper, started } = await start({
				reportReady: false,
				awaitStart: false,
			})
			await delay(5)
			helper.exit(1)
			await assert.rejects(async () => started)
			helper.stdout.write(
				`${JSON.stringify({ t: 'ready', v: SRTP_HELPER_PROTOCOL_VERSION, relayPort: 45_455 })}\n`,
			)
			await delay(20)
		})

		void it('does not act for a session that is stopping', async () => {
			const { producer, helper, authenticated, hintsWritten } = await start({
				ignoreSignals: true,
				stopSigkillAfterMs: 60_000,
			})
			const stopping = producer.stop(PORT)
			try {
				authOk(helper)
				await delay(20)
				assert.deepStrictEqual(authenticated, [])
				assert.deepStrictEqual(hintsWritten, [])
			} finally {
				// The helper ignores signals here, so stop() waits for this; skipping it
				// on a failed assertion would leave the suite hanging on SIGKILL's timer.
				helper.exit(0)
				await stopping
			}
		})

		void it('still logs what a dying helper says', async () => {
			const { helper, logger } = await start()
			helper.exit(1)
			helper.stdout.write(
				`${JSON.stringify({ t: 'warning', element: 'kvssink', message: 'last words' })}\n`,
			)
			await delay(20)
			assert.ok(
				logger.warnings.some(
					(w) =>
						w.message === 'SRTP pipeline warning' &&
						w.context?.message === 'last words',
				),
			)
		})

		// The ordinary case, so the guard is known not to swallow it.
		void it('acts for the current session as before', async () => {
			const { producer, helper, authenticated, hintsWritten } = await start()
			try {
				authOk(helper)
				await delay(20)
				assert.deepStrictEqual(authenticated, [PORT])
				assert.deepStrictEqual(hintsWritten, [7])
			} finally {
				await producer.stop(PORT)
			}
		})
	})

	/**
	 * The loss window is passed, not left to the helper's default.
	 *
	 * It bounds how long unauthenticated traffic can keep a running port's lease
	 * fresh, because PortIngestion refreshes on every datagram and relies on the
	 * helper ending once authentication stops. A bound that rests on a default in
	 * another process is one nobody sees change.
	 */
	void it('passes the helper its authentication loss window', async () => {
		const { producer, helperArgs } = await start()
		try {
			const flag = helperArgs.indexOf('--auth-loss-ms')
			assert.notStrictEqual(flag, -1, `not passed: ${helperArgs.join(' ')}`)
			assert.strictEqual(helperArgs[flag + 1], String(AUTH_LOSS_MS))
		} finally {
			await producer.stop(PORT)
		}
	})

	/**
	 * The helper's stats, where an operator can read them.
	 *
	 * The troubleshooting guide tells an operator whose stream never authenticates to
	 * read "inputs climbing with authenticated at 0" off these lines. They were being
	 * dropped, so that advice pointed at nothing; they are now logged until something
	 * authenticates, and not after, so a healthy stream adds nothing to the log.
	 */
	void describe('stats', () => {
		const stats = (helper: HelperFake): void => {
			helper.stdout.write(
				`${JSON.stringify({ t: 'stats', inputs: 12, authenticated: 0, aus: 0, roc: null, drops: 12 })}\n`,
			)
		}
		const logged = (logger: CapturingLogger) =>
			logger.infos.filter((i) => i.message === 'SRTP pipeline stats')

		void it('logs them while nothing has authenticated', async () => {
			const { producer, helper, logger } = await start()
			try {
				stats(helper)
				await delay(20)
				assert.deepStrictEqual(
					logged(logger).map((i): unknown[] => [
						i.context?.inputs,
						i.context?.authenticated,
						i.context?.drops,
					]),
					[[12, 0, 12]],
				)
			} finally {
				await producer.stop(PORT)
			}
		})

		void it('stops logging them once traffic has authenticated', async () => {
			const { producer, helper, logger } = await start()
			try {
				helper.stdout.write(
					`${JSON.stringify({ t: 'auth', status: 'ok', first: true, roc: 0, trials: 1 })}\n`,
				)
				await delay(20)
				stats(helper)
				await delay(20)
				assert.deepStrictEqual(logged(logger), [])
			} finally {
				await producer.stop(PORT)
			}
		})
	})

	/**
	 * A sender that rewinds its packet index under a key it has already used.
	 *
	 * The keys here are static and the SSRC fixed per port, so SRTP's keystream
	 * depends only on those and the packet index. Restarting the index reuses the
	 * keystream, and two packets sharing one reveal the XOR of their plaintexts. Only
	 * provisioning can prevent it; the receiver can at least say it happened, which
	 * the persisted rollover counter makes cheap.
	 */
	void describe('a reused keystream', () => {
		const confirm = (helper: HelperFake, roc: number): void => {
			helper.stdout.write(
				`${JSON.stringify({
					t: 'auth',
					status: 'ok',
					first: true,
					roc,
					trials: 1,
					candidate: roc,
					authenticated: 1,
				})}\n`,
			)
		}

		const rewinds = async (
			rocHint: number | undefined,
			confirmedRoc: number,
		): Promise<CapturingLogger> => {
			const { producer, helper, logger } = await start({ rocHint })
			try {
				confirm(helper, confirmedRoc)
				await delay(20)
				return logger
			} finally {
				await producer.stop(PORT)
			}
		}

		void it('reports a counter below the one this key already reached', async () => {
			const logger = await rewinds(7, 0)
			assert.deepStrictEqual(
				logger.errors.map((e): unknown[] => [
					e.context?.previousRoc,
					e.context?.roc,
				]),
				[[7, 0]],
			)
		})

		void it('says nothing when the counter carried on from where it was', async () => {
			assert.deepStrictEqual((await rewinds(7, 7)).errors, [])
			assert.deepStrictEqual((await rewinds(7, 9)).errors, [])
		})

		// A key this port has never confirmed anything under cannot have been reused.
		void it('says nothing when there is no counter to compare against', async () => {
			assert.deepStrictEqual((await rewinds(undefined, 0)).errors, [])
		})

		void it('keeps ingesting, because refusing would not undo the reuse', async () => {
			const { producer, helper } = await start({ rocHint: 7 })
			try {
				confirm(helper, 0)
				await delay(20)
				assert.strictEqual(producer.isActive(PORT), true)
			} finally {
				await producer.stop(PORT)
			}
		})
	})

	/**
	 * A failed start must hand the startup buffer back, not drop it.
	 *
	 * PortIngestion puts whatever the producer never sent at the front of the buffer
	 * so a retry still begins with the oldest datagrams, which is normally the
	 * keyframe. Everything that fails a start also closes the session - the readiness
	 * rejection stops the helper, and the child's exit handler closes it before
	 * rejecting - so reading the datagrams off the session returned nothing.
	 */
	void describe('a start that fails', () => {
		const startFailing = async (
			fail: (helper: HelperFake) => void,
			datagrams: Buffer[],
		): Promise<SrtpProducer> => {
			const helper = new HelperFake(45_454)
			// Readiness never arrives, so the start fails however `fail` chooses.
			helper.reportReadyOnInit = false
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
				readyTimeoutMs: 60,
				spawn: (() =>
					helper as unknown as ChildProcess) as unknown as typeof nodeSpawn,
				logger: new CapturingLogger(),
			})

			const starting = producer.start(PORT, datagrams, { epoch: 1 })
			await delay(5)
			fail(helper)
			await assert.rejects(async () => starting)
			return producer
		}

		const buffered = [Buffer.from('keyframe'), Buffer.from('rest')]

		/**
		 * An unusable relay port fails the start, not the process.
		 *
		 * dgram.connect throws synchronously for a port it cannot use, and it runs in
		 * the helper's stdout handler, so the throw is uncaught. Here the test runner
		 * catches it and fails the test with ERR_SOCKET_BAD_PORT; in the backend
		 * nothing does, and the process exits - taking the unencrypted path with it.
		 */
		for (const relayPort of [0, 1.5, 70000]) {
			void it(`fails the start when the helper reports relay port ${String(relayPort)}`, async () => {
				const producer = await startFailing((helper) => {
					helper.stdout.write(
						`${JSON.stringify({ t: 'ready', v: SRTP_HELPER_PROTOCOL_VERSION, relayPort })}\n`,
					)
				}, buffered)
				assert.deepStrictEqual(
					producer.takeUnsentDatagrams(PORT).map((d) => d.toString()),
					['keyframe', 'rest'],
				)
			})
		}

		void it('hands the buffer back when the helper exits during startup', async () => {
			const producer = await startFailing((helper) => helper.exit(1), buffered)
			assert.deepStrictEqual(
				producer.takeUnsentDatagrams(PORT).map((d) => d.toString()),
				['keyframe', 'rest'],
			)
		})

		void it('hands the buffer back when readiness times out', async () => {
			const producer = await startFailing(() => undefined, buffered)
			assert.deepStrictEqual(
				producer.takeUnsentDatagrams(PORT).map((d) => d.toString()),
				['keyframe', 'rest'],
			)
		})

		void it('hands them back only once', async () => {
			const producer = await startFailing((helper) => helper.exit(1), buffered)
			producer.takeUnsentDatagrams(PORT)
			assert.deepStrictEqual(producer.takeUnsentDatagrams(PORT), [])
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
