import assert from 'node:assert/strict'
import {
	spawn,
	spawnSync,
	type ChildProcessWithoutNullStreams,
} from 'node:child_process'
import dgram from 'node:dgram'
import { describe, it } from 'node:test'

import {
	SrtpHelperProtocol,
	type SrtpHelperMessage,
} from './SrtpHelperProtocol.ts'
import { h264Payload, srtpPacket } from './testing/srtpSender.ts'

const HELPER = 'backend/src/srtp_pipeline.py'
const KEY = '000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d'
const SSRC = 42

/**
 * The helper needs the GStreamer Python bindings and the srtp plugin, which are not
 * present everywhere. Skip rather than fail where they are missing, so this suite can
 * run in the same command as the pure unit tests.
 */
const capability = spawnSync(
	'python3',
	[
		'-c',
		"import gi; gi.require_version('Gst','1.0')\n" +
			'from gi.repository import Gst\n' +
			'Gst.init(None)\n' +
			"raise SystemExit(0 if Gst.ElementFactory.find('srtpdec') else 1)",
	],
	{ timeout: 30_000 },
)
const available = capability.status === 0
const skip = available
	? false
	: 'requires python3 GStreamer bindings with the srtp plugin'

type Helper = {
	child: ChildProcessWithoutNullStreams
	messages: SrtpHelperMessage[]
	relayPort: number
	send: (packet: Buffer) => void
	waitFor: (
		predicate: (m: SrtpHelperMessage) => boolean,
		timeoutMs?: number,
	) => Promise<SrtpHelperMessage>
	stop: () => Promise<number | null>
	stderr: () => string
}

const startHelper = async (
	options: { rocHint?: number; extraArgs?: string[] } = {},
): Promise<Helper> => {
	const child = spawn(
		'python3',
		[
			HELPER,
			'--ssrc',
			String(SSRC),
			'--fake-sink',
			'--stats-interval-ms',
			'250',
			'--auth-loss-ms',
			'100000',
			'--trial-drops',
			'4',
			'--trial-timeout-ms',
			'400',
			...(options.extraArgs ?? []),
		],
		{ stdio: ['pipe', 'pipe', 'pipe'] },
	)

	const protocol = new SrtpHelperProtocol()
	const messages: SrtpHelperMessage[] = []
	child.stdout.setEncoding('utf8')
	child.stdout.on('data', (chunk: string) => {
		messages.push(...protocol.push(chunk))
	})
	let stderr = ''
	child.stderr.setEncoding('utf8')
	child.stderr.on('data', (chunk: string) => {
		stderr += chunk
	})

	const waitFor = async (
		predicate: (m: SrtpHelperMessage) => boolean,
		timeoutMs = 10_000,
	): Promise<SrtpHelperMessage> => {
		const deadline = Date.now() + timeoutMs
		for (;;) {
			const found = messages.find(predicate)
			if (found !== undefined) return found
			if (Date.now() > deadline) {
				throw new Error(
					`timed out; messages so far: ${JSON.stringify(messages)}\nstderr: ${stderr}`,
				)
			}
			await new Promise((resolve) => setTimeout(resolve, 20))
		}
	}

	child.stdin.write(
		`${JSON.stringify({
			type: 'init',
			v: 1,
			key: KEY,
			ssrc: SSRC,
			cipher: 'aes-128-icm',
			auth: 'hmac-sha1-80',
			...(options.rocHint === undefined ? {} : { rocHint: options.rocHint }),
		})}\n`,
	)

	const ready = await waitFor((m) => m.t === 'ready')
	const relayPort = ready.t === 'ready' ? ready.relayPort : 0
	const socket = dgram.createSocket('udp4')

	return {
		child,
		messages,
		relayPort,
		send: (packet) => socket.send(packet, relayPort, '127.0.0.1'),
		waitFor,
		stop: async () => {
			socket.close()
			// Already gone: 'exit' has fired and will not fire again, so waiting for it
			// would just burn the timeout.
			if (child.exitCode !== null || child.signalCode !== null) {
				return child.exitCode
			}
			child.kill('SIGTERM')
			return new Promise<number | null>((resolve) => {
				const timer = setTimeout(() => {
					child.kill('SIGKILL')
					resolve(child.exitCode)
				}, 8000)
				child.once('exit', (code) => {
					clearTimeout(timer)
					resolve(code)
				})
			})
		},
		stderr: () => stderr,
	}
}

const packetAt = (roc: number, seq: number): Buffer =>
	srtpPacket({ keyHex: KEY, ssrc: SSRC, seq, roc, payload: h264Payload() })

/** Sends `count` authentic packets at `roc`, starting from `startSeq`. */
const sendBurst = (
	helper: Helper,
	roc: number,
	startSeq: number,
	count: number,
): number => {
	let seq = startSeq
	for (let i = 0; i < count; i++) {
		helper.send(packetAt(roc, seq & 0xffff))
		seq += 1
	}
	return seq
}

const confirmation = (
	m: SrtpHelperMessage,
): { roc: number; trials?: number } | undefined =>
	m.t === 'auth' && m.status === 'ok' && m.first ? m : undefined

/** The confirmed rollover counter, or -1 for any other message. */
const rocOf = (m: SrtpHelperMessage): number =>
	m.t === 'auth' && m.status === 'ok' ? m.roc : -1

void describe('srtp_pipeline.py', { skip }, () => {
	void describe('readiness', () => {
		void it('reports the relay port the OS actually assigned', async () => {
			const helper = await startHelper()
			try {
				// Not a fixed offset from the ingest port: the port is whatever bind
				// returned, read back off udpsrc, so relay ports cannot collide.
				assert.ok(helper.relayPort > 0)
				assert.ok(helper.relayPort <= 65535)
			} finally {
				await helper.stop()
			}
		})

		void it('refuses to run with key-shaped material in argv', async () => {
			const result = spawnSync(
				'python3',
				[HELPER, '--ssrc', String(SSRC), '--fake-sink', '--aws-region', KEY],
				{ encoding: 'utf8', timeout: 30_000, input: '' },
			)
			assert.match(result.stdout, /"reason": "key-in-argv"/)
		})

		void it('refuses a GST_DEBUG level that would log the key', async () => {
			const result = spawnSync(
				'python3',
				[HELPER, '--ssrc', String(SSRC), '--fake-sink'],
				{
					encoding: 'utf8',
					timeout: 30_000,
					input: '',
					env: { ...process.env, GST_DEBUG: '5' },
				},
			)
			assert.match(result.stdout, /"reason": "unsafe-debug-env"/)
		})
	})

	void describe('rollover counter confirmation', () => {
		void it('confirms on the first trial when the hint is exact', async () => {
			const helper = await startHelper({ rocHint: 0 })
			try {
				sendBurst(helper, 0, 1000, 20)
				const confirmed = await helper.waitFor(
					(m) => confirmation(m) !== undefined,
				)
				assert.deepStrictEqual(
					{
						roc: rocOf(confirmed),
						trials: confirmation(confirmed)?.trials,
					},
					{ roc: 0, trials: 1 },
				)
			} finally {
				await helper.stop()
			}
		})

		// A real encoder always starts at ROC 0, so only a synthetic sender can put a
		// stream above the hint - which is exactly the case a restart after a wrap
		// produces.
		void it('climbs to a rollover counter above the hint', async () => {
			const helper = await startHelper({ rocHint: 0 })
			try {
				const timer = setInterval(() => sendBurst(helper, 2, 1000, 10), 60)
				try {
					const confirmed = await helper.waitFor(
						(m) => confirmation(m) !== undefined,
					)
					assert.strictEqual(rocOf(confirmed), 2)
				} finally {
					clearInterval(timer)
				}
			} finally {
				await helper.stop()
			}
		})

		// The regression test for a search that only counts upwards: a hint left over
		// from a longer previous session, or a sender that restarted its session, is
		// *below* the hint and would otherwise never be reached.
		void it('recovers when the hint is far too high', async () => {
			const helper = await startHelper({ rocHint: 9 })
			try {
				const timer = setInterval(() => sendBurst(helper, 0, 2000, 10), 60)
				try {
					const confirmed = await helper.waitFor(
						(m) => confirmation(m) !== undefined,
					)
					assert.strictEqual(rocOf(confirmed), 0)
					// Second candidate tried: the hint, then zero.
					assert.strictEqual(confirmation(confirmed)?.trials, 2)
				} finally {
					clearInterval(timer)
				}
			} finally {
				await helper.stop()
			}
		})

		void it('reports a rollover while running', async () => {
			const helper = await startHelper({ rocHint: 0 })
			try {
				sendBurst(helper, 0, 65_500, 30)
				await helper.waitFor((m) => confirmation(m) !== undefined)
				// Crossing 65535 -> 0 continues the packet index into the next rollover.
				sendBurst(helper, 1, 0, 30)
				const rollover = await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && !m.first,
				)
				assert.strictEqual(rocOf(rollover), 1)
			} finally {
				await helper.stop()
			}
		})
	})

	void describe('traffic that does not hold the key', () => {
		void it('never confirms for forged packets, and counts them as dropped', async () => {
			const helper = await startHelper({ rocHint: 0 })
			try {
				// Correct RTP version and SSRC - both public - with a random payload and
				// tag. This is what anyone able to reach the public port can produce.
				const timer = setInterval(() => {
					for (let i = 0; i < 10; i++) {
						const forged = Buffer.alloc(86)
						forged[0] = 0x80
						forged[1] = 96
						forged.writeUInt16BE(1234 + i, 2)
						forged.writeUInt32BE(SSRC, 8)
						helper.send(forged)
					}
				}, 50)
				try {
					const stats = await helper.waitFor(
						(m) => m.t === 'stats' && m.drops > 0,
					)
					assert.strictEqual(
						stats.t === 'stats' ? stats.authenticated : -1,
						0,
						'forged traffic must never authenticate',
					)
					// And no rollover counter is ever reported, so none can be persisted.
					assert.strictEqual(stats.t === 'stats' ? stats.roc : -1, null)
					assert.ok(
						!helper.messages.some((m) => confirmation(m) !== undefined),
						'forged traffic must never produce a confirmation',
					)
				} finally {
					clearInterval(timer)
				}
			} finally {
				await helper.stop()
			}
		})

		// Stepping on a timer alone would burn through the candidate list whenever a
		// port is simply idle.
		void it('does not step candidates while no traffic arrives', async () => {
			const helper = await startHelper({ rocHint: 5 })
			try {
				await new Promise((resolve) => setTimeout(resolve, 1500))
				const searches = helper.messages.filter((m) => m.t === 'searching')
				const failures = helper.messages.filter(
					(m) => m.t === 'auth' && m.status === 'fail',
				)
				assert.deepStrictEqual(
					{ searches: searches.length, failures: failures.length },
					{ searches: 0, failures: 0 },
				)
			} finally {
				await helper.stop()
			}
		})
	})

	void describe('lifecycle', () => {
		void it('ends the stream and exits cleanly on SIGTERM', async () => {
			const helper = await startHelper({ rocHint: 0 })
			sendBurst(helper, 0, 1000, 10)
			await helper.waitFor((m) => confirmation(m) !== undefined)
			const code = await helper.stop()
			assert.strictEqual(code, 0)
			// EOS rather than a kill, so the sink can flush what it already holds.
			assert.ok(helper.messages.some((m) => m.t === 'eos'))
		})

		// If the parent dies without stopping the child, the child must not survive
		// holding the Kinesis stream.
		void it('exits when its parent closes stdin', async () => {
			const helper = await startHelper({ rocHint: 0 })
			try {
				helper.child.stdin.end()
				const code = await new Promise<number | null>((resolve) => {
					const timer = setTimeout(() => resolve(-1), 8000)
					helper.child.once('exit', (c) => {
						clearTimeout(timer)
						resolve(c)
					})
				})
				assert.strictEqual(code, 0)
			} finally {
				await helper.stop()
			}
		})
	})
})
