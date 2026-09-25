import assert from 'node:assert/strict'
import dgram from 'node:dgram'
import { describe, it } from 'node:test'

import { hasGstElements, startHelper } from '../src/testing/srtpHelper.ts'
import { E2eSender } from './sender.ts'

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
