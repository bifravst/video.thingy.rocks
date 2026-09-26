import assert from 'node:assert/strict'
import dgram from 'node:dgram'
import { readFileSync } from 'node:fs'
import { describe, it } from 'node:test'

import { hasGstElements, startHelper } from '../src/testing/srtpHelper.ts'
import { E2eSender, parseAnnexB } from './sender.ts'

/**
 * The committed fixture, parsed the way the sender paces it.
 *
 * No GStreamer needed: this pins the access-unit detection against the bytes
 * that actually ship. The fixture's keyframes are three IDR slices and its P
 * frames three P slices each (measured, not assumed), so anything weaker than
 * first_mb_in_slice parsing mis-frames it - every IDR slice became its own
 * "frame" and a whole GOP's P frames glued into one access unit sharing a
 * single timestamp and marker, which is not the per-frame pacing the sender
 * claims.
 */
void describe('the e2e fixture parses into real access units', () => {
	const fixture = parseAnnexB(
		readFileSync(new URL('./fixtures/testsrc.h264', import.meta.url)),
	)

	void it('starts one access unit per frame, not per IDR slice', () => {
		// 20 one-second GOPs of 16 frames each: one keyframe and 15 P frames.
		assert.strictEqual(fixture.accessUnitStarts.length, 320)
		const startTypes = fixture.accessUnitStarts.map(
			(i) => fixture.nals[i]?.type as number,
		)
		const keyframes = startTypes.filter((t) => t === 7).length
		const idrFirstSlices = startTypes.filter((t) => t === 5).length
		const pFirstSlices = startTypes.filter((t) => t === 1).length
		assert.strictEqual(keyframes, 20)
		assert.strictEqual(idrFirstSlices, 20)
		assert.strictEqual(pFirstSlices, 280)
	})

	void it("keeps a frame's continuation slices inside its access unit", () => {
		// No access unit is a single stray continuation slice, and none is a
		// whole GOP: every frame is 1-3 NALs (its slices, plus the parameter
		// sets on a keyframe), never the ~45 a "until the next SPS" grouping
		// produced.
		for (let k = 0; k < fixture.accessUnitStarts.length; k++) {
			const from = fixture.accessUnitStarts[k] as number
			const to =
				k + 1 < fixture.accessUnitStarts.length
					? (fixture.accessUnitStarts[k + 1] as number)
					: fixture.nals.length
			const size = to - from
			assert.ok(
				size >= 1 && size <= 5,
				`access unit ${String(k)} is ${String(size)} NALs`,
			)
			const first = fixture.nals[from]?.data as Buffer
			const type = first[0] as number
			if (type === 1 || type === 5) {
				assert.ok(
					(first[1] as number) & 0x80,
					"an access unit starting at a slice must be that frame's first slice",
				)
			}
		}
	})
})

/**
 * The e2e sender against the real helper and real libsrtp.
 *
 * This is the dress rehearsal for the e2e suite: the same real H.264 fixture,
 * the same packetizer, the same crypto - stopped just short of kvssink and the
 * network. It proves the sender's video is depacketizable and parseable (access
 * units reach the sink, not just authenticated RTP headers), which is what makes
 * "the stream reached Kinesis" mean real fragments rather than a byte counter.
 */
void describe(
	'E2eSender against the real helper',
	{ skip: !hasGstElements(['srtpdec', 'h264parse', 'rtph264depay']) },
	() => {
		void it(
			'streams real H.264 that authenticates and depacketizes into access units',
			{ timeout: 90_000 },
			async () => {
				const key = 'cd'.repeat(30)
				const ssrc = 4242
				const helper = await startHelper({ key, ssrc })
				try {
					const sender = new E2eSender({
						host: '127.0.0.1',
						port: helper.port,
						keyHex: key,
						ssrc,
						roc: 0,
						seq: 60000,
						fps: 15,
						durationS: 3,
					})
					await sender.run()

					const confirmed = await helper.waitFor(
						(m) => m.t === 'auth' && m.status === 'ok' && m.first,
						20_000,
					)
					assert.ok(confirmed.t === 'auth' && confirmed.status === 'ok')
					assert.strictEqual(confirmed.roc, 0)

					// Searching mode counts nothing as an access unit: its
					// pipeline ends at a fakesink straight after srtpdec, and
					// the aus stat is only ever attached to the producing
					// pipeline's sink - after the depayloader and the parser -
					// so it can never be satisfied by authenticated packets
					// alone.
					const whileSearching = await helper.waitFor((m) => m.t === 'stats')
					assert.ok(
						whileSearching.t === 'stats' && whileSearching.aus === 0,
						`searching must not count packets as access units (aus: ${String(whileSearching.t === 'stats' ? whileSearching.aus : '?')})`,
					)

					// The producer's tail is granted, so the depayloader and the
					// parser run: access units, not just authenticated headers.
					helper.command('start')
					await helper.waitFor((m) => m.t === 'producing')
					sender.stop()
					const sender2 = new E2eSender({
						host: '127.0.0.1',
						port: helper.port,
						keyHex: key,
						ssrc,
						roc: 0,
						seq: sender.currentState.seq,
						timestamp: sender.currentState.timestamp,
						fps: 15,
						durationS: 3,
					})
					await sender2.run()

					const stats = await helper.waitFor(
						(m) => m.t === 'stats' && m.aus > 0,
						20_000,
					)
					assert.ok(stats.t === 'stats')
					assert.ok(
						stats.aus > 0,
						'real access units must reach the sink for the video to be ingestable',
					)
				} finally {
					await helper.stop()
				}
			},
		)

		void it(
			'carries the sequence number across the wrap while streaming',
			{ timeout: 90_000 },
			async () => {
				const key = 'cd'.repeat(30)
				const ssrc = 4242
				const helper = await startHelper({ key, ssrc })
				try {
					const sender = new E2eSender({
						host: '127.0.0.1',
						port: helper.port,
						keyHex: key,
						ssrc,
						roc: 0,
						seq: 65400, // crosses the wrap within ~10 frames
						fps: 15,
						durationS: 3,
					})
					await sender.run()
					// The run starts at 65400 and sends hundreds of packets, so the
					// sequence number must have wrapped and carried the counter.
					assert.ok(
						sender.currentState.seq < 32768,
						`the sequence number wrapped during the run (now ${String(sender.currentState.seq)})`,
					)
					assert.strictEqual(sender.currentState.roc, 1)
					const wrap = await helper.waitFor(
						(m) =>
							m.t === 'auth' && m.status === 'ok' && !m.first && m.roc === 1,
						20_000,
					)
					assert.ok(wrap.t === 'auth')
				} finally {
					await helper.stop()
				}
			},
		)
	},
)

/**
 * The sender's network resilience, with no GStreamer needed.
 *
 * The restart-recovery case keeps a sender streaming while the backend is
 * deliberately restarted beneath it. While nothing is bound to the port, the
 * target answers every datagram with ICMP port unreachable, which a connected
 * UDP socket delivers as an 'error' event on the receive path. The sender must
 * absorb that and keep sending - before it did, the first ICMP error killed
 * the whole suite as an unhandled 'error' event, mid-case, on 2026-09-25
 * (`recvmsg ECONNREFUSED` at UDP.onMessage).
 */
void describe('E2eSender network resilience', () => {
	void it(
		'keeps streaming through ICMP port-unreachable feedback',
		{ timeout: 30_000 },
		async () => {
			// A port that is guaranteed closed: bind one, read the number the
			// OS picked, close it again.
			const probe = dgram.createSocket('udp4')
			await new Promise<void>((resolve) => {
				probe.bind(0, '127.0.0.1', () => resolve())
			})
			const closedPort = probe.address().port
			probe.close()

			const sender = new E2eSender({
				host: '127.0.0.1',
				port: closedPort,
				keyHex: 'ab'.repeat(30),
				ssrc: 4242,
				roc: 0,
				fps: 15,
				// Plenty of datagrams into the closed port; on Linux every one
				// of them comes back as ICMP to a connected socket.
				durationS: 2,
			})
			// Before the fix this call did not reject - the process died
			// inside it, on the first 'error' event nobody was listening for.
			await sender.run()
			assert.ok(
				sender.networkErrorCount > 0,
				'the ICMP feedback must arrive and be counted, not be fatal',
			)
		},
	)
})
