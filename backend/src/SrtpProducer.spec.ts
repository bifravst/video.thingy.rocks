import assert from 'node:assert/strict'
import type { ChildProcess, spawn as nodeSpawn } from 'node:child_process'
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

const start = async (
	options: { ignoreSignals?: boolean; stopSigkillAfterMs?: number } = {},
): Promise<{
	producer: SrtpProducer
	helper: HelperFake
	logger: CapturingLogger
}> => {
	// Nothing is bound to the relay port: the fake helper only has to report one, and
	// no test depends on the datagrams arriving anywhere.
	const helper = new HelperFake(45_454)
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
	await producer.start(PORT, [], { epoch: 1 })
	return { producer, helper, logger }
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
