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
	options: { floor?: number; extraArgs?: string[] } = {},
): Promise<Helper> => startHelperFor({ key: KEY, ssrc: SSRC, ...options })

const packetAt = (roc: number, seq: number): Buffer =>
	srtpPacket({ keyHex: KEY, ssrc: SSRC, seq, roc, payload: h264Payload() })

/** The packet index of RFC 3711: rollover counter above, sequence number below. */
const indexAt = (roc: number, seq: number): number => roc * 65_536 + seq

/** How many authenticated datagrams the helper has said it dropped as stale. */
const staleDropped = (helper: Helper): number =>
	helper.messages.reduce(
		(sum, m) =>
			sum +
			Number(
				m.t === 'warning'
					? (/dropped (\d+) authenticated/.exec(m.message)?.[1] ?? 0)
					: 0,
			),
		0,
	)

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
		void it('confirms on the first trial with no floor', async () => {
			const helper = await startHelper()
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

		void it('confirms on the first trial when the stream is in the floor’s rollover', async () => {
			const helper = await startHelper({ floor: indexAt(3, 999) })
			try {
				sendBurst(helper, 3, 1000, 20)
				const confirmed = await helper.waitFor(
					(m) => confirmation(m) !== undefined,
				)
				assert.deepStrictEqual(
					{
						roc: rocOf(confirmed),
						trials: confirmation(confirmed)?.trials,
					},
					{ roc: 3, trials: 1 },
				)
			} finally {
				await helper.stop()
			}
		})

		// A sender that kept counting while ingestion was down is above the floor by
		// however many times it wrapped. A real encoder always starts at ROC 0, so only
		// a synthetic sender can put a stream there.
		void it('climbs to a rollover counter above the floor', async () => {
			const helper = await startHelper({ floor: indexAt(0, 500) })
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

		/**
		 * Below the floor is not searched, by design.
		 *
		 * The search used to walk down from its starting point and fall back to zero, so
		 * that a sender which restarted its packet index under the same key recovered on
		 * its own. That is precisely a stream below the highest index already accepted,
		 * and so is a recording of earlier traffic: the two cannot be told apart, and
		 * neither may be accepted. The band is narrowed so the search cycles quickly.
		 */
		void it('never searches below the floor', async () => {
			const helper = await startHelper({
				floor: indexAt(5, 0),
				extraArgs: ['--search-max-offset', '2'],
			})
			try {
				const timer = setInterval(() => sendBurst(helper, 4, 3000, 10), 60)
				try {
					await helper.waitFor(
						(m) =>
							helper.messages.filter((n) => n.t === 'searching').length >= 6 &&
							m.t === 'searching',
					)
				} finally {
					clearInterval(timer)
				}
				const tried = helper.messages.flatMap((m) =>
					m.t === 'searching' ? [m.candidate] : [],
				)
				assert.ok(
					tried.every((candidate) => candidate >= 5),
					`tried ${JSON.stringify(tried)}`,
				)
				assert.ok(
					!helper.messages.some((m) => confirmation(m) !== undefined),
					'a stream below the floor must never be confirmed',
				)
			} finally {
				await helper.stop()
			}
		})

		void it('reports a rollover while running', async () => {
			const helper = await startHelper()
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

		// Reordering across the wrap must not inflate the counter: the persisted floor
		// would then be above the stream, and the next start would never find it.
		void it('counts one rollover when packets arrive reordered across the wrap', async () => {
			const helper = await startHelper()
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

	/**
	 * Authenticated is not the same as fresh.
	 *
	 * libsrtp keeps its replay window in the helper's memory, and it starts empty in
	 * every new helper - so a recording of traffic this port already accepted used to
	 * authenticate again after any restart, promote the port, persist its counter and
	 * go into the stream as if it were live. The floor is the highest index ever
	 * accepted under this key, and nothing at or below it gets through.
	 */
	void describe('traffic that was already accepted', () => {
		void it('rejects a recording of an earlier session after a restart', async () => {
			const recording = Array.from({ length: 30 }, (_, i) =>
				packetAt(0, 1000 + i),
			)

			const first = await startHelper()
			let floor: number
			try {
				for (const packet of recording) first.send(packet)
				await first.waitFor((m) => confirmation(m) !== undefined)
				const reported = await first.waitFor((m) => m.t === 'index')
				floor = reported.t === 'index' ? reported.index : -1
			} finally {
				await first.stop()
			}
			// The last word on the way out, which is what the parent persists.
			const last = first.messages.filter((m) => m.t === 'index').at(-1)
			assert.deepStrictEqual(last, { t: 'index', index: indexAt(0, 1029) })
			floor = last.t === 'index' ? last.index : floor

			const second = await startHelper({ floor })
			try {
				for (const packet of recording) second.send(packet)
				await second.waitFor(() => staleDropped(second) === 30)
				const stats = await second.waitFor(
					(m) => m.t === 'stats' && m.inputs >= 30,
				)
				assert.strictEqual(
					stats.t === 'stats' ? stats.authenticated : -1,
					0,
					'none of it counts as authenticated',
				)
				assert.ok(
					!second.messages.some((m) => confirmation(m) !== undefined),
					`the recording was accepted: ${JSON.stringify(second.messages)}`,
				)

				// The sender carrying on from where it was is still welcome...
				sendBurst(second, 0, 1030, 10)
				const confirmed = await second.waitFor(
					(m) => confirmation(m) !== undefined,
				)
				assert.strictEqual(rocOf(confirmed), 0)

				// ...and the recording is still refused alongside it.
				for (const packet of recording) second.send(packet)
				const after = await second.waitFor(
					(m) => m.t === 'stats' && m.inputs >= 70,
				)
				assert.strictEqual(after.t === 'stats' ? after.authenticated : -1, 10)
			} finally {
				await second.stop()
			}
		})

		void it('accepts only what is above the floor within one burst', async () => {
			const helper = await startHelper({ floor: indexAt(0, 1009) })
			try {
				sendBurst(helper, 0, 1000, 20)
				await helper.waitFor((m) => confirmation(m) !== undefined)
				await helper.waitFor(() => staleDropped(helper) === 10)
				const stats = await helper.waitFor(
					(m) => m.t === 'stats' && m.authenticated === 10,
				)
				assert.ok(stats.t === 'stats')
				const index = await helper.waitFor((m) => m.t === 'index')
				assert.deepStrictEqual(index, { t: 'index', index: indexAt(0, 1019) })
			} finally {
				await helper.stop()
			}
		})
	})

	void describe('traffic that does not hold the key', () => {
		void it('never confirms for forged packets, and counts them as dropped', async () => {
			const helper = await startHelper()
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
		 * The producer's half of the bound on unauthenticated lease refresh.
		 *
		 * Once a port runs, PortIngestion refreshes its lease on every datagram,
		 * authenticated or not, and relies on this: the helper ending once nothing has
		 * authenticated for its loss window, even while forged traffic keeps arriving.
		 * The flood uses the right SSRC, since it is public, so it is exactly what
		 * anyone reaching the port can send. The window is shortened here; the parent
		 * passes the production one, which SrtpProducer.spec.ts checks. The state
		 * machine's half - no lease once this happens - is in PortIngestion.spec.ts.
		 */
		void it('ends within its loss window while forged traffic keeps arriving', async () => {
			const lossMs = 600
			const helper = await startHelper({
				extraArgs: ['--auth-loss-ms', String(lossMs)],
			})
			try {
				sendBurst(helper, 0, 0, 10)
				await helper.waitFor((m) => confirmation(m) !== undefined)
				const confirmedAt = Date.now()

				const exited = new Promise<number | null>((resolve) => {
					helper.child.once('exit', (code) => resolve(code))
				})
				let forgedSent = 0
				const flood = setInterval(() => {
					for (let i = 0; i < 10; i++) {
						const forged = Buffer.alloc(86)
						forged[0] = 0x80
						forged[1] = 96
						forged.writeUInt16BE(20 + forgedSent, 2)
						forged.writeUInt32BE(SSRC, 8)
						helper.send(forged)
						forgedSent += 1
					}
				}, 20)
				try {
					const lost = await helper.waitFor(
						(m) => m.t === 'auth' && m.status === 'lost',
						10_000,
					)
					const code = await exited
					const elapsed = Date.now() - confirmedAt

					assert.strictEqual(
						code,
						5,
						'it exits, which is what restarts the port',
					)
					assert.ok(
						lost.t === 'auth' &&
							lost.status === 'lost' &&
							(lost.sinceMs ?? 0) >= lossMs,
						`not before the window: ${JSON.stringify(lost)}`,
					)
					// The window plus the supervisor tick and scheduling, not open-ended.
					assert.ok(
						elapsed < lossMs + 1_500,
						`it ended ${String(elapsed)} ms after authentication stopped`,
					)
					assert.ok(forgedSent > 0, 'the flood was running throughout')
				} finally {
					clearInterval(flood)
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
			const helper = await startHelper()
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
			const helper = await startHelper({ floor: indexAt(5, 0) })
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

	/**
	 * The search across the moment it switches candidate.
	 *
	 * The supervisor tick decides when to step on; srtpdec's streaming thread asks for
	 * keys and authenticates packets. A packet can authenticate on either side of the
	 * switch, and both sides have been bugs: a baseline snapshotted from the tick
	 * always leaves a gap on one side or the other. Authentication is now credited to
	 * the key srtpdec actually holds.
	 *
	 * These orderings cannot be forced from outside a running pipeline, which is why
	 * this was once shipped as "reasoned rather than tested". They can be forced from
	 * inside: srtp_pipeline_scenarios.py drives the helper's own methods with a stand-in
	 * for srtpdec whose remove-key runs the streaming thread's side at the exact point
	 * in question. Against the previous helper, both switch cases fail.
	 */
	void describe('the search across a candidate switch', () => {
		type Outcome = {
			confirmed: boolean
			reportedCandidate: number | null
			reportedRoc: number | null
			handedToSrtpdec: number[]
			searchingAt: number
		}
		const outcomes = (): Record<string, Outcome> => {
			const result = spawnSync(
				'python3',
				['backend/src/testing/srtp_pipeline_scenarios.py'],
				{ encoding: 'utf8', timeout: 30_000 },
			)
			assert.strictEqual(result.status, 0, result.stderr)
			assert.ok(
				!result.stdout.toLowerCase().includes(KEY),
				'the scenarios must not print the key',
			)
			return JSON.parse(result.stdout) as Record<string, Outcome>
		}
		const all = outcomes()

		// Copilot's case: the new candidate authenticates before its trial is set up.
		void it('credits a new key that authenticates before its trial begins', () => {
			const o = all.newKeyAuthenticatesBeforeTheTrialBaseline
			assert.strictEqual(o?.confirmed, true)
			assert.strictEqual(o.reportedCandidate, 6)
			assert.strictEqual(o.reportedRoc, 6)
		})

		// The other side, which the earlier fix claimed and did not hold: the right key
		// authenticates after the search has decided to leave it.
		void it('keeps an old key that authenticates after the search stepped on', () => {
			const o = all.oldKeyAuthenticatesAfterTheSwitch
			assert.strictEqual(o?.confirmed, true)
			assert.strictEqual(o.reportedCandidate, 5, 'the key that authenticated')
			assert.deepStrictEqual(
				o.handedToSrtpdec,
				[5, 5],
				'srtpdec is given that key back, not the candidate stepped to',
			)
		})

		void it('confirms a live stream as before', () => {
			const o = all.liveStream
			assert.strictEqual(o?.confirmed, true)
			assert.strictEqual(o.reportedCandidate, 5)
		})

		void it('never confirms noise, and keeps searching through it', () => {
			const o = all.nothingAuthenticates
			assert.strictEqual(o?.confirmed, false)
			assert.strictEqual(o.reportedCandidate, null)
			assert.ok(
				o.handedToSrtpdec.length > 1,
				`the search has to move: ${JSON.stringify(o.handedToSrtpdec)}`,
			)
		})
	})

	void describe('lifecycle', () => {
		void it('ends the stream and exits cleanly on SIGTERM', async () => {
			const helper = await startHelper()
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
			const helper = await startHelper()
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
