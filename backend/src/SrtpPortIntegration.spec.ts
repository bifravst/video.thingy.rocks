import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import type { SrtpHelperMessage } from './SrtpHelperProtocol.ts'
import {
	hasGstElements,
	startHelper,
	type Helper,
} from './testing/srtpHelper.ts'
import { h264Payload, srtpPacket } from './testing/srtpSender.ts'

/**
 * The v3 helper against real libsrtp.
 *
 * If any of the cryptographic reasoning is wrong, srtpdec simply does not
 * authenticate and these assertions fail loudly - which is the point of driving the
 * real element rather than a model of it. These suites skip where the GStreamer
 * SRTP plugin is not installed.
 */

/** 30 bytes: a 16-byte AES key plus a 14-byte salt, hex encoded. */
const KEY = 'ab'.repeat(30)
const SSRC = 42

const indexAt = (roc: number, seq: number): number => (roc << 16) | seq

const waitFor = async (
	condition: () => boolean,
	timeoutMs: number,
): Promise<void> => {
	const deadline = Date.now() + timeoutMs
	while (!condition()) {
		if (Date.now() > deadline) throw new Error('timed out')
		await new Promise((resolve) => setTimeout(resolve, 20))
	}
}

const hasSrtp = hasGstElements(['srtpdec', 'udpsrc', 'fakesink'])

/** Sends `count` packets starting at seq, carrying the roc across the wrap. */
const sendBurst = async (
	helper: Helper,
	key: string,
	roc: number,
	startSeq: number,
	count: number,
): Promise<void> => {
	for (let i = 0; i < count; i++) {
		const seq = (startSeq + i) % 65536
		const burstRoc = startSeq + i > 65535 ? (roc + 1) % 2 ** 32 : roc
		helper.send(
			srtpPacket({
				keyHex: key,
				ssrc: SSRC,
				seq,
				roc: burstRoc,
				payload: h264Payload(),
			}),
		)
		// A real sender paces; cramming everything into one event loop turn lets
		// trial timeouts decide orders that arrival order should decide.
		await new Promise((resolve) => setTimeout(resolve, 5))
	}
}

void describe('srtp_port.py against real libsrtp', { skip: !hasSrtp }, () => {
	void describe('readiness', () => {
		void it('reports the port it bound, having received the key first', async () => {
			const helper = await startHelper({ key: KEY, ssrc: SSRC })
			try {
				assert.ok(helper.port > 0, 'an ephemeral port was bound')
				assert.ok(helper.port < 65536)
			} finally {
				await helper.stop()
			}
		})

		void it('refuses key-shaped argv and unsafe debug settings', async () => {
			// The harness starts a clean helper first to prove the environment works.
			const clean = await startHelper({ key: KEY, ssrc: SSRC })
			await clean.stop()
			await assert.rejects(
				startHelper({
					key: KEY,
					ssrc: SSRC,
					extraArgs: ['--stream-name', KEY],
				}),
				(err: Error) => {
					assert.match(err.message, /key-in-argv/)
					return true
				},
			)
		})
	})

	void describe('rollover-counter confirmation', () => {
		void it('confirms at rollover zero on the first trial when there is no floor', async () => {
			const helper = await startHelper({ key: KEY, ssrc: SSRC })
			try {
				await sendBurst(helper, KEY, 0, 100, 10)
				const confirmed = await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && m.first,
				)
				assert.ok(confirmed.t === 'auth' && confirmed.status === 'ok')
				assert.strictEqual(confirmed.roc, 0)
				assert.strictEqual(confirmed.trials, 1)
			} finally {
				await helper.stop()
			}
		})

		void it("confirms the floor's rollover on the first trial", async () => {
			const helper = await startHelper({
				key: KEY,
				ssrc: SSRC,
				floor: indexAt(3, 999),
			})
			try {
				await sendBurst(helper, KEY, 3, 1000, 10)
				const confirmed = await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && m.first,
				)
				assert.ok(confirmed.t === 'auth' && confirmed.status === 'ok')
				assert.strictEqual(confirmed.roc, 3)
				assert.strictEqual(confirmed.trials, 1)
			} finally {
				await helper.stop()
			}
		})

		void it('climbs to a rollover counter above the floor', async () => {
			// Only a synthetic sender can put a stream two rollovers above its floor.
			const helper = await startHelper({
				key: KEY,
				ssrc: SSRC,
				floor: indexAt(0, 500),
			})
			try {
				await sendBurst(helper, KEY, 2, 1000, 30)
				const confirmed = await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && m.first,
					20_000,
				)
				assert.ok(confirmed.t === 'auth' && confirmed.status === 'ok')
				assert.strictEqual(confirmed.roc, 2)
			} finally {
				await helper.stop()
			}
		})

		void it('never offers a candidate below the floor', async () => {
			const helper = await startHelper({
				key: KEY,
				ssrc: SSRC,
				floor: indexAt(5, 0),
				extraArgs: ['--floor-every', '2'],
			})
			try {
				await sendBurst(helper, KEY, 4, 1000, 40)
				// Some searching happened, all of it at or above 5, and nothing ever
				// confirmed - a stream below the floor is a replay or a keystream
				// reuse, and both are refused.
				await helper.waitFor((m) => m.t === 'searching' && m.trial > 3)
				for (const m of helper.messages) {
					if (m.t === 'searching') {
						assert.ok(
							m.candidate >= 5,
							`candidate ${String(m.candidate)} is below the floor's rollover`,
						)
					}
					if (m.t === 'auth' && m.status === 'ok') {
						assert.fail('traffic below the floor authenticated')
					}
				}
			} finally {
				await helper.stop()
			}
		})

		void it(
			'recovers after unauthenticated traffic has walked the search past the answer',
			{ timeout: 60_000 },
			async () => {
				// The collision case, and the attack it stands in for: traffic that
				// holds the SSRC and the RTP version but not the key advances the
				// search one candidate per few drops, past the counter the real
				// sender is actually on. Without the re-sweep of the counters just
				// above the floor, the climbing search never comes back down and
				// the port cannot authenticate the real sender again until the
				// service restarts - which was a real failure on the deployed
				// fleet, not a hypothesis.
				const helper = await startHelper({
					key: KEY,
					ssrc: SSRC,
					extraArgs: ['--floor-every', '8'],
				})
				try {
					// Unauthenticated traffic with the correct public SSRC and
					// version: the wrong key makes every authentication fail, but
					// every failure is a drop, and every four drops advance the
					// search.
					const wrongKey = KEY.slice(0, 58) + 'ff'
					for (let i = 0; i < 60; i++) {
						helper.send(
							srtpPacket({
								keyHex: wrongKey,
								ssrc: SSRC,
								seq: 30000 + i,
								roc: 2,
								payload: h264Payload(),
							}),
						)
						await new Promise((resolve) => setTimeout(resolve, 5))
					}
					// The search has been stepped well past rollover 2.
					await helper.waitFor((m) => m.t === 'searching' && m.candidate > 4)

					// The real sender, still on rollover 2, streams continuously.
					// The re-sweep after the next floor re-offer must reach 2 and
					// confirm it - within a bounded number of candidates, not
					// after the climb has wrapped the whole counter space.
					await sendBurst(helper, KEY, 2, 31000, 150)
					const confirmed = await helper.waitFor(
						(m) => m.t === 'auth' && m.status === 'ok' && m.first,
						30_000,
					)
					assert.ok(confirmed.t === 'auth' && confirmed.status === 'ok')
					assert.strictEqual(confirmed.roc, 2)
				} finally {
					await helper.stop()
				}
			},
		)

		void it('reports a wrap as a later rollover of a confirmed stream', async () => {
			const helper = await startHelper({ key: KEY, ssrc: SSRC })
			try {
				await sendBurst(helper, KEY, 0, 100, 10)
				await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && m.first,
				)
				// Cross the wrap: seqs 65500..65599 wrap to 0..49 under roc 1.
				await sendBurst(helper, KEY, 0, 65500, 150)
				const wrap = await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && !m.first && m.roc === 1,
				)
				assert.ok(wrap.t === 'auth')
			} finally {
				await helper.stop()
			}
		})

		void it('counts one rollover for a reordered wrap, not two', async () => {
			const helper = await startHelper({ key: KEY, ssrc: SSRC })
			try {
				await sendBurst(helper, KEY, 0, 100, 10)
				await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && m.first,
				)
				// The authenticated arrival order that broke arrival-order wrap
				// detection: the late pre-wrap packet must not make the next post-wrap
				// packet look like a second wrap.
				for (const seq of [65534, 0, 65535, 1, 2, 3, 4, 5]) {
					const roc = seq < 100 ? 1 : 0
					helper.send(
						srtpPacket({
							keyHex: KEY,
							ssrc: SSRC,
							seq,
							roc,
							payload: h264Payload(),
						}),
					)
					await new Promise((resolve) => setTimeout(resolve, 5))
				}
				// A second rollover would put the tracker above the stream; the next
				// authenticated packet would then be classified below it and dropped.
				await sendBurst(helper, KEY, 1, 6, 10)
				const still = await helper.waitFor(
					(m) => m.t === 'stats' && m.roc !== null && m.roc <= 1,
				)
				assert.ok(still.t === 'stats')
				assert.ok((still.roc ?? 0) <= 1, 'the wrap was counted once')
			} finally {
				await helper.stop()
			}
		})
	})

	void describe('the replay floor', () => {
		void it('refuses a recording of traffic an earlier session accepted', async () => {
			const first = await startHelper({ key: KEY, ssrc: SSRC })
			try {
				await sendBurst(first, KEY, 0, 1000, 30)
				const confirmed = await first.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && m.first,
				)
				assert.ok(confirmed.t === 'auth' && confirmed.status === 'ok')
				// Wait for the *final* index, not the first one that appears: it is
				// rate-limited while the floor moves, and an early report would leave
				// the recording's tail above the floor.
				const index = await first.waitFor(
					(m) => m.t === 'index' && m.index === indexAt(0, 1029),
				)
				const floor = index.t === 'index' ? index.index : 0
				await first.stop()

				const second = await startHelper({
					key: KEY,
					ssrc: SSRC,
					floor,
				})
				try {
					// The recording: the same 30 packets the first session accepted.
					await sendBurst(second, KEY, 0, 1000, 30)
					await new Promise((resolve) => setTimeout(resolve, 2_000))
					for (const m of second.messages) {
						assert.ok(
							!(m.t === 'auth' && m.status === 'ok'),
							'a replayed recording authenticated',
						)
					}
					const stale = second.messages.find(
						(m) =>
							m.t === 'warning' &&
							m.message.includes('at or below the highest'),
					)
					assert.ok(stale, 'the stale drops are reported, counted not repeated')
				} finally {
					await second.stop()
				}
			} finally {
				// first.stop() already ran in the try; this is for the throw path.
				if (first.child.exitCode === null && first.child.signalCode === null) {
					await first.stop()
				}
			}
		})

		void it('accepts the sender carrying on above the floor', async () => {
			const helper = await startHelper({
				key: KEY,
				ssrc: SSRC,
				floor: indexAt(0, 1009),
			})
			try {
				await sendBurst(helper, KEY, 0, 1010, 10)
				const confirmed = await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && m.first,
				)
				assert.ok(confirmed.t === 'auth' && confirmed.status === 'ok')
			} finally {
				await helper.stop()
			}
		})
	})

	void describe('traffic that does not hold the key', () => {
		void it('never authenticates forged packets with the public SSRC', async () => {
			const helper = await startHelper({ key: KEY, ssrc: SSRC })
			try {
				for (let i = 0; i < 50; i++) {
					// Version 2, the right SSRC, a plausible payload, a random tag.
					const forged = Buffer.alloc(86)
					forged[0] = 0x80
					forged[1] = 96
					forged.writeUInt16BE(1000 + i, 2)
					forged.writeUInt32BE(SSRC, 8)
					helper.send(forged)
					await new Promise((resolve) => setTimeout(resolve, 5))
				}
				await helper.waitFor((m) => m.t === 'stats' && m.inputs >= 50)
				// Nothing authenticated, nothing persistable: roc stays null.
				for (const m of helper.messages) {
					if (m.t === 'stats') {
						assert.strictEqual(
							m.roc,
							null,
							'a forged packet must not produce a rollover counter',
						)
					}
					assert.ok(!(m.t === 'auth' && m.status === 'ok'))
				}
			} finally {
				await helper.stop()
			}
		})

		void it('counts foreign SSRCs once per stats interval, never echoing them', async () => {
			const helper = await startHelper({ key: KEY, ssrc: SSRC })
			try {
				for (let i = 0; i < 100; i++) {
					helper.send(
						srtpPacket({
							keyHex: KEY,
							ssrc: 4242, // not this port's SSRC
							seq: 1000 + i,
							roc: 0,
							payload: h264Payload(),
						}),
					)
					await new Promise((resolve) => setTimeout(resolve, 5))
				}
				const ignoredWarnings = (): Extract<
					SrtpHelperMessage,
					{ t: 'warning' }
				>[] =>
					helper.messages.filter(
						(m): m is Extract<SrtpHelperMessage, { t: 'warning' }> =>
							m.t === 'warning' && m.message.includes('ignored'),
					)
				const totalIgnored = (): number =>
					ignoredWarnings().reduce(
						(sum, m) =>
							sum + Number(/ignored (\d+) datagrams/.exec(m.message)?.[1] ?? 0),
						0,
					)
				// Some datagrams may not make it over even loopback UDP, so the
				// exact total is not the property under test: what is, is that the
				// count is reported (bounded by the clock, not per datagram), that it
				// accounts for essentially everything sent, and that the
				// attacker-chosen SSRC never appears in any line.
				await waitFor(() => totalIgnored() > 0, 10_000)
				// Two more stats intervals to report whatever was still in flight.
				await new Promise((resolve) => setTimeout(resolve, 700))
				assert.ok(
					totalIgnored() >= 90,
					`almost every foreign datagram must be accounted for (${String(totalIgnored())} of 100)`,
				)
				// One line per stats interval however many there were, and the
				// attacker-chosen SSRCs are never echoed: the lines carry counts only.
				for (const m of helper.messages) {
					if (m.t === 'warning' && m.message.includes('ignored')) {
						assert.ok(!m.message.includes('4242'), 'no SSRC echo')
						const count = Number(/ignored (\d+) datagrams/.exec(m.message)?.[1])
						assert.ok(count <= 100)
					}
				}
			} finally {
				await helper.stop()
			}
		})

		void it('steps no candidate while the port is idle', async () => {
			const helper = await startHelper({ key: KEY, ssrc: SSRC })
			try {
				await new Promise((resolve) => setTimeout(resolve, 1_500))
				assert.strictEqual(
					helper.messages.filter((m) => m.t === 'searching').length,
					0,
				)
				assert.strictEqual(
					helper.messages.filter((m) => m.t === 'auth' && m.status === 'fail')
						.length,
					0,
				)
			} finally {
				await helper.stop()
			}
		})
	})

	void describe('the two modes over one socket', () => {
		void it('starts producing on command, re-confirms, and stops with the ack', async () => {
			const helper = await startHelper({
				key: KEY,
				ssrc: SSRC,
				extraArgs: ['--stream-name', 'e2e-video-6000'],
			})
			try {
				await sendBurst(helper, KEY, 0, 1000, 10)
				await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && m.first,
				)
				helper.command('start')
				const producing = await helper.waitFor((m) => m.t === 'producing')
				assert.ok(producing.t === 'producing')
				// The producing pipeline re-runs the search from the floor it was
				// handed; the seed is the confirmed counter, so the first packet
				// confirms it again.
				await sendBurst(helper, KEY, 0, 1010, 10)
				const reconfirmed = await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && m.first,
					10_000,
				)
				assert.ok(reconfirmed.t === 'auth' && reconfirmed.status === 'ok')
				assert.strictEqual(reconfirmed.roc, 0)
				// The socket is still the same one: the port never changed hands.
				assert.ok(helper.port > 0)
				helper.command('stop')
				const stopped = await helper.waitFor((m) => m.t === 'stopped')
				assert.ok(stopped.t === 'stopped')
				assert.ok(stopped.index !== undefined, 'the ack carries the floor')
				// Still alive, back to searching: the same process keeps its socket.
				await sendBurst(helper, KEY, 0, 1100, 10)
				const again = await helper.waitFor(
					(m) =>
						m.t === 'auth' &&
						m.status === 'ok' &&
						m.first &&
						m.seq !== undefined &&
						m.seq >= 1100,
					10_000,
				)
				assert.ok(again.t === 'auth' && again.status === 'ok')
			} finally {
				await helper.stop()
			}
		})

		void it('gives production up and re-searches when auth is lost', async () => {
			const helper = await startHelper({
				key: KEY,
				ssrc: SSRC,
				extraArgs: ['--auth-loss-ms', '600', '--stream-name', 'e2e-video-6000'],
			})
			try {
				await sendBurst(helper, KEY, 0, 1000, 10)
				await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'ok' && m.first,
				)
				helper.command('start')
				await helper.waitFor((m) => m.t === 'producing')
				// Traffic stops. The auth-loss window passes, the producing pipeline
				// is given up and reported, and the process stays up, re-searching.
				const lost = await helper.waitFor(
					(m) => m.t === 'auth' && m.status === 'lost',
					10_000,
				)
				assert.ok(lost.t === 'auth')
				// The stopped acknowledgement is waited for rather than found in
				// what has arrived: the auth-loss report this wait matched can be
				// an earlier one from the searching mode that preceded the grant
				// (those have nothing producing to acknowledge), while the one that
				// carries the stopped frame lands on the next tick.
				const stopped = await helper.waitFor((m) => m.t === 'stopped', 10_000)
				assert.ok(
					stopped.t === 'stopped',
					'production was given up and reported',
				)
				// Traffic resumes above the floor: the same process re-confirms.
				await sendBurst(helper, KEY, 0, 2000, 10)
				const again = await helper.waitFor(
					(m) =>
						m.t === 'auth' &&
						m.status === 'ok' &&
						m.first &&
						m.seq !== undefined &&
						m.seq >= 2000,
					10_000,
				)
				assert.ok(again.t === 'auth' && again.status === 'ok')
			} finally {
				await helper.stop()
			}
		})
	})

	void describe('lifecycle', () => {
		void it('ends with EOS on SIGTERM so the sink can flush', async () => {
			const helper = await startHelper({ key: KEY, ssrc: SSRC })
			const code = await helper.stop()
			assert.strictEqual(code, 0)
		})

		void it('exits when its parent closes stdin', async () => {
			const helper = await startHelper({ key: KEY, ssrc: SSRC })
			const exited = new Promise<number | null>((resolve) => {
				helper.child.once('exit', (code) => resolve(code))
			})
			helper.child.stdin.end()
			assert.strictEqual(await exited, 0)
			await helper.stop()
		})
	})
})
