import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { describe, it } from 'node:test'

import type { SrtpHelperMessage } from './SrtpHelperProtocol.ts'
import {
	HELPER,
	hasGstElements,
	startHelper as startHelperFor,
	type Helper,
} from './testing/srtpHelper.ts'
import { h264Payload, srtpPacket } from './testing/srtpSender.ts'

const KEY = '000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d'
const SSRC = 42

const skip = hasGstElements(['srtpdec'])
	? false
	: 'requires python3 GStreamer bindings with the srtp plugin'

const startHelper = async (
	options: { rocHint?: number; extraArgs?: string[] } = {},
): Promise<Helper> => startHelperFor({ key: KEY, ssrc: SSRC, ...options })

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

		// The other direction of the same regression: a hint *one* above the actual
		// counter is what a value left over from a longer previous session looks like,
		// and it is not reachable by counting up from zero either. The band is narrowed
		// to two so a search that only walks up from the hint and up from zero runs out
		// of reachable candidates quickly and never offers four.
		void it('recovers when the hint is just above the counter', async () => {
			const helper = await startHelper({
				rocHint: 5,
				extraArgs: ['--search-max-offset', '2'],
			})
			try {
				const timer = setInterval(() => sendBurst(helper, 4, 3000, 10), 60)
				try {
					const confirmed = await helper.waitFor(
						(m) => confirmation(m) !== undefined,
					)
					assert.strictEqual(rocOf(confirmed), 4)
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

		// Reordering across the wrap must not inflate the counter: the persisted hint
		// would then be above the stream, and the next start would pay for a search.
		void it('counts one rollover when packets arrive reordered across the wrap', async () => {
			const helper = await startHelper({ rocHint: 0 })
			try {
				sendBurst(helper, 0, 65_530, 5)
				await helper.waitFor((m) => confirmation(m) !== undefined)

				// 65534, 0, 65535, 1: the last pre-wrap packet arrives behind the first
				// post-wrap one, which is ordinary on any network.
				helper.send(packetAt(1, 0))
				helper.send(packetAt(0, 65_535))
				helper.send(packetAt(1, 1))

				const rollover = await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && !m.first,
				)
				assert.strictEqual(rocOf(rollover), 1)

				// Every one of the eight packets held the key, the late one included -
				// without that this would pass even if the reorder never reached the
				// counter.
				const stats = await helper.waitFor(
					(m) => m.t === 'stats' && m.authenticated === 8,
				)
				assert.strictEqual(stats.t === 'stats' ? stats.roc : -1, 1)
				assert.ok(
					!helper.messages.some((m) => rocOf(m) > 1),
					`a reorder was counted as a second rollover: ${JSON.stringify(helper.messages)}`,
				)
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

		/**
		 * Other SSRCs are counted, not reported one by one.
		 *
		 * The SSRC is in the clear and chosen by whoever sends, and srtpdec asks for a
		 * key for every datagram of one it does not know - so a line per datagram let
		 * anyone who could reach the port write to the logs at line rate, varying the
		 * SSRC or not. Measured before the change: 200 datagrams, 200 lines, whether the
		 * SSRC was fixed or new each time.
		 */
		void it('reports datagrams for other SSRCs as a count per interval', async () => {
			const helper = await startHelper({ rocHint: 0 })
			try {
				const started = Date.now()
				for (let i = 0; i < 200; i++) {
					helper.send(
						srtpPacket({
							keyHex: KEY,
							// Fixed for half, new each time for the rest: both used to cost
							// one line apiece.
							ssrc: i < 100 ? 99 : 1_000 + i,
							seq: i,
							roc: 0,
							payload: h264Payload(),
						}),
					)
				}
				// Long enough for the count to be reported.
				await new Promise((resolve) => setTimeout(resolve, 800))
				const elapsedIntervals = Math.ceil((Date.now() - started) / 250)

				const reports = helper.messages.filter(
					(m) => m.t === 'warning' && m.message.includes('SSRCs other than'),
				)
				const counted = reports.reduce(
					(sum, m) =>
						sum +
						Number(
							m.t === 'warning' ? /ignored (\d+)/.exec(m.message)?.[1] : 0,
						),
					0,
				)
				assert.strictEqual(counted, 200, 'every one is still accounted for')
				assert.ok(
					reports.length <= elapsedIntervals + 1,
					`at most one line per stats interval, not per datagram: got ${String(reports.length)}`,
				)
				assert.ok(
					!reports.some((m) => m.t === 'warning' && /\b99\b/.test(m.message)),
					'the SSRCs themselves are the sender’s choice and are not repeated',
				)
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
