import assert from 'node:assert'
import { spawn } from 'node:child_process'
import dgram from 'node:dgram'
import { describe, it } from 'node:test'
import {
	advanceSrtpRoc,
	KinesisIngestionPipeline,
	parseAuthenticRtpSequenceNumber,
	srtpdecSeedRoc,
	type SrtpRocState,
} from './KinesisIngestionPipeline.ts'
import type { SrtpKeyStore } from './SrtpKeyStore.ts'

const TEST_SSRC = 0xdeadbeef

/** Builds a minimal 12-byte RTP/SRTP header: version 2, the given sequence number, and the
 * given SSRC (defaults to TEST_SSRC) - the rest of the header/payload is irrelevant to
 * parseAuthenticRtpSequenceNumber. */
const rtpHeader = (seq: number, ssrc: number = TEST_SSRC): Buffer => {
	const buf = Buffer.alloc(12)
	buf[0] = 0x80 // version 2, no padding/extension/CSRC
	buf.writeUInt16BE(seq, 2)
	buf.writeUInt32BE(ssrc, 8)
	return buf
}

void describe('KinesisIngestionPipeline', () => {
	void describe('parseAuthenticRtpSequenceNumber', () => {
		void it('reads the sequence number when version and SSRC match', () => {
			assert.strictEqual(
				parseAuthenticRtpSequenceNumber(rtpHeader(1234), TEST_SSRC),
				1234,
			)
			assert.strictEqual(
				parseAuthenticRtpSequenceNumber(rtpHeader(65535), TEST_SSRC),
				65535,
			)
			assert.strictEqual(
				parseAuthenticRtpSequenceNumber(rtpHeader(0), TEST_SSRC),
				0,
			)
		})

		void it('returns undefined for a datagram shorter than a full RTP header', () => {
			assert.strictEqual(
				parseAuthenticRtpSequenceNumber(Buffer.alloc(11), TEST_SSRC),
				undefined,
			)
			assert.strictEqual(
				parseAuthenticRtpSequenceNumber(Buffer.alloc(0), TEST_SSRC),
				undefined,
			)
		})

		void it('returns undefined for a mismatched SSRC (unrelated/spoofed traffic)', () => {
			assert.strictEqual(
				parseAuthenticRtpSequenceNumber(rtpHeader(100, 0x11111111), TEST_SSRC),
				undefined,
			)
		})

		void it('returns undefined for a non-v2 RTP version', () => {
			const buf = rtpHeader(100)
			buf[0] = 0x00 // version 0
			assert.strictEqual(
				parseAuthenticRtpSequenceNumber(buf, TEST_SSRC),
				undefined,
			)
		})
	})

	void describe('advanceSrtpRoc', () => {
		void it('starts at ROC 0 for the first observed packet', () => {
			const state = advanceSrtpRoc(undefined, 100)
			assert.deepStrictEqual(state, { highestSeq: 100, roc: 0 })
		})

		void it('classifies any first sequence from a {0,0} baseline as ROC 0, matching a never-seen port', () => {
			// A {roc: 0, highestSeq: 0} state can arrive from a cleared or
			// identity-mismatched persisted item (see
			// StreamMetadataService.getSrtpRocState) and must classify the first observed
			// datagram exactly like the uninitialized (undefined-state) branch does - or
			// a fresh sender whose randomized starting sequence is in the upper half of
			// the 16-bit space would be misclassified as already one rollover ahead,
			// persisted as such, and seed the *next* restart one ROC too high (which
			// srtpdec cannot decrypt until the sender wraps again).
			for (const seq of [1, 0x7fff, 0x8000, 40000, 0xffff]) {
				assert.deepStrictEqual(advanceSrtpRoc({ highestSeq: 0, roc: 0 }, seq), {
					highestSeq: seq,
					roc: 0,
				})
			}
		})

		void it('does not increment ROC for ordinary forward progress', () => {
			let state = advanceSrtpRoc(undefined, 100)
			state = advanceSrtpRoc(state, 101)
			state = advanceSrtpRoc(state, 5000)
			assert.strictEqual(state.roc, 0)
		})

		void it('increments ROC on a genuine sequence-number rollover', () => {
			let state = advanceSrtpRoc(undefined, 0xfffe)
			state = advanceSrtpRoc(state, 0xffff)
			assert.strictEqual(state.roc, 0)
			state = advanceSrtpRoc(state, 0x0000)
			assert.strictEqual(state.roc, 1)
			state = advanceSrtpRoc(state, 0x0005)
			assert.strictEqual(state.roc, 1)
		})

		void it('does not false-trigger on ordinary reordering near the midpoint', () => {
			// A small backward step (not a wrap) must not be mistaken for a rollover.
			let state = advanceSrtpRoc(undefined, 40000)
			state = advanceSrtpRoc(state, 39990)
			assert.strictEqual(state.roc, 0)
		})

		void it('increments across multiple rollovers over a long session of small forward steps', () => {
			// Realistic operation: trackSrtpRoc is called for every datagram, so consecutive
			// deltas are always small (bounded by however many packets were lost since the
			// last one seen) - never a jump anywhere near half the 16-bit range in one call,
			// which is the regime any ROC-guessing heuristic (including libsrtp's own) can't
			// disambiguate. Walk forward in steps of 100 through just over two full 16-bit
			// cycles and confirm exactly two rollovers are detected.
			let state: SrtpRocState | undefined = undefined
			for (let i = 0; i <= 65536 * 2 + 1000; i += 100) {
				state = advanceSrtpRoc(state, i % 0x10000)
			}
			assert.strictEqual(state?.roc, 2)
		})

		void it('does not let a late/reordered pre-wrap duplicate corrupt tracking after a real wrap', () => {
			// Regression test: a naive "compare only to the last-seen seq" implementation
			// mistakes the late 65535 for "no wrap yet" (since it's compared against 0, not
			// against the highest extended index seen), which then makes the next genuinely
			// forward packet (seq 1) look like a *second* wrap. Comparing against the highest
			// extended index instead means the late duplicate is simply ignored.
			let state = advanceSrtpRoc(undefined, 0xfffe)
			state = advanceSrtpRoc(state, 0xffff)
			state = advanceSrtpRoc(state, 0x0000) // genuine wrap -> ROC 1
			assert.strictEqual(state.roc, 1)

			state = advanceSrtpRoc(state, 0xffff) // late/reordered duplicate from before the wrap
			assert.strictEqual(state.roc, 1, 'late duplicate must not change ROC')

			state = advanceSrtpRoc(state, 0x0001) // next genuinely forward packet
			assert.strictEqual(
				state.roc,
				1,
				'ROC must not be double-incremented by the late duplicate',
			)
		})

		void it('ignores an out-of-order packet that arrives after a higher one', () => {
			let state = advanceSrtpRoc(undefined, 100)
			state = advanceSrtpRoc(state, 200)
			const beforeStale = state
			state = advanceSrtpRoc(state, 150) // stale/reordered, lower than highest seen
			assert.deepStrictEqual(state, beforeStale)
		})
	})

	void describe('srtpdecSeedRoc', () => {
		void it('seeds the raw roc when the tracked sequence is in the lower half', () => {
			assert.strictEqual(srtpdecSeedRoc({ roc: 5, highestSeq: 100 }), 5)
			assert.strictEqual(srtpdecSeedRoc({ roc: 5, highestSeq: 0x7fff }), 5)
		})

		void it('seeds the raw roc when the tracked sequence is in the upper half', () => {
			// libsrtp >= 2.3 (any version with srtp_set_stream_roc, which srtpdec's caps
			// "roc" field requires) does NOT re-guess the first packet's ROC against a
			// roc*65536+0 baseline: srtp_set_stream_roc sets stream->pending_roc, the first
			// packet's extended index is computed directly as (seededRoc << 16) | seq, and
			// both halves of the rollover state are then pinned from that estimate. Verified
			// against the libsrtp source and empirically (GStreamer 1.28 + libsrtp): a sender
			// pinned to ROC 0 with sequence numbers starting at 40000 decrypts 151/151
			// packets when seeded with the raw roc=0, and 0/151 when seeded roc=1. The
			// "seed roc+1 in the upper half" compensation this test used to assert breaks
			// decryption for up to ~32768 sequence numbers instead of fixing it.
			assert.strictEqual(srtpdecSeedRoc({ roc: 5, highestSeq: 0x8000 }), 5)
			assert.strictEqual(srtpdecSeedRoc({ roc: 5, highestSeq: 40000 }), 5)
			assert.strictEqual(srtpdecSeedRoc({ roc: 5, highestSeq: 0xffff }), 5)
		})

		void it('seeds 0 for a never-observed stream', () => {
			assert.strictEqual(srtpdecSeedRoc({ roc: 0, highestSeq: 0 }), 0)
		})
	})

	void describe('streamSlotForPort / streamNameForPort / pairedPortFor', () => {
		const keyStore = {} as SrtpKeyStore

		const makePipeline = (): KinesisIngestionPipeline =>
			new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
				srtp: {
					portRange: { start: 6000, end: 6009 },
					keyStore,
				},
			})

		void it('maps unencrypted and SRTP ports at the same offset to the same slot', () => {
			const pipeline = makePipeline()
			assert.strictEqual(pipeline.streamSlotForPort(5000), 1)
			assert.strictEqual(pipeline.streamSlotForPort(6000), 1)
			assert.strictEqual(pipeline.streamSlotForPort(5003), 4)
			assert.strictEqual(pipeline.streamSlotForPort(6003), 4)
			assert.strictEqual(pipeline.streamSlotForPort(5009), 10)
			assert.strictEqual(pipeline.streamSlotForPort(6009), 10)
		})

		void it('names the stream from the slot, shared across transports', () => {
			const pipeline = makePipeline()
			assert.strictEqual(
				pipeline.streamNameForPort(5003),
				pipeline.streamNameForPort(6003),
			)
			assert.match(pipeline.streamNameForPort(5003), /-4$/)
		})

		void it('resolves the paired port across transports', () => {
			const pipeline = makePipeline()
			assert.strictEqual(pipeline.pairedPortFor(5003), 6003)
			assert.strictEqual(pipeline.pairedPortFor(6003), 5003)
		})

		void it('has no paired port when SRTP is not configured', () => {
			const pipeline = new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
			})
			assert.strictEqual(pipeline.pairedPortFor(5003), undefined)
		})
	})

	void describe('constructor port-range validation', () => {
		const keyStore = {} as SrtpKeyStore

		void it('accepts matching-length port ranges', () => {
			new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
				srtp: { portRange: { start: 6000, end: 6009 }, keyStore },
			})
			new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
			})
		})

		void it('throws when the SRTP range covers more ports than the main range', () => {
			// streamSlotForPort derives the slot from the port's offset within its own
			// range, so extra SRTP ports would map to slots beyond the Kinesis streams the
			// CDK stack creates - fail fast at construction instead.
			assert.throws(
				() =>
					new KinesisIngestionPipeline({
						region: 'eu-central-1',
						portRange: { start: 5000, end: 5009 },
						srtp: { portRange: { start: 6000, end: 6019 }, keyStore },
					}),
				/must cover exactly as many ports/,
			)
		})

		void it('throws when the SRTP range covers fewer ports than the main range', () => {
			assert.throws(
				() =>
					new KinesisIngestionPipeline({
						region: 'eu-central-1',
						portRange: { start: 5000, end: 5009 },
						srtp: { portRange: { start: 6000, end: 6005 }, keyStore },
					}),
				/must cover exactly as many ports/,
			)
		})

		void it('throws for an inverted main port range', () => {
			assert.throws(
				() =>
					new KinesisIngestionPipeline({
						region: 'eu-central-1',
						portRange: { start: 5009, end: 5000 },
					}),
				/portRange must be non-empty/,
			)
		})

		void it('throws when the SRTP range overlaps the main range', () => {
			// Overlapping ports would be received by the unencrypted listener but
			// classified as SRTP by isSrtpPort() - routing them to the wrong handler and
			// pipeline - while the SRTP listener could never bind them (EADDRINUSE).
			// Both a fully-inside overlap and a partial overlap must be rejected.
			for (const srtpRange of [
				{ start: 5000, end: 5009 }, // identical
				{ start: 5005, end: 5014 }, // straddling the end (same length)
				{ start: 4995, end: 5004 }, // straddling the start (same length)
				{ start: 5000, end: 5002 }, // fully inside (rejected by the length check first)
			]) {
				assert.throws(
					() =>
						new KinesisIngestionPipeline({
							region: 'eu-central-1',
							portRange: { start: 5000, end: 5009 },
							srtp: { portRange: srtpRange, keyStore },
						}),
					/must not overlap|must cover exactly as many ports/,
				)
			}
		})

		void it('accepts adjacent but disjoint ranges', () => {
			new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
				srtp: { portRange: { start: 4990, end: 4999 }, keyStore },
			})
			new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
				srtp: { portRange: { start: 5010, end: 5019 }, keyStore },
			})
		})
	})

	void describe('getConfiguredSrtpSsrc', () => {
		void it('returns undefined when SRTP is not configured', () => {
			const pipeline = new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
			})
			assert.strictEqual(pipeline.getConfiguredSrtpSsrc(6000), undefined)
		})

		void it('returns undefined when no key is loaded for the port', () => {
			const keyStore = {
				getKeyForPort: () => undefined,
			} as unknown as SrtpKeyStore
			const pipeline = new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
				srtp: { portRange: { start: 6000, end: 6009 }, keyStore },
			})
			assert.strictEqual(pipeline.getConfiguredSrtpSsrc(6000), undefined)
		})

		void it('returns the configured key ssrc for the port', () => {
			const keyStore = {
				getKeyForPort: (port: number) =>
					port === 6003
						? { keyHex: 'a'.repeat(60), ssrc: 42, cipher: 'x', auth: 'y' }
						: undefined,
			} as unknown as SrtpKeyStore
			const pipeline = new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
				srtp: { portRange: { start: 6000, end: 6009 }, keyStore },
			})
			assert.strictEqual(pipeline.getConfiguredSrtpSsrc(6003), 42)
		})
	})

	void describe('seedSrtpRoc / getSrtpRoc / getSrtpRocState', () => {
		const makePipeline = (): KinesisIngestionPipeline =>
			new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
			})

		void it('defaults to {roc: 0, highestSeq: 0} for a port never observed or seeded', () => {
			const pipeline = makePipeline()
			assert.strictEqual(pipeline.getSrtpRoc(6000), 0)
			assert.deepStrictEqual(pipeline.getSrtpRocState(6000), {
				roc: 0,
				highestSeq: 0,
			})
		})

		void it('seeds an initial state', () => {
			const pipeline = makePipeline()
			pipeline.seedSrtpRoc(6000, { roc: 5, highestSeq: 40000 })
			assert.strictEqual(pipeline.getSrtpRoc(6000), 5)
			assert.deepStrictEqual(pipeline.getSrtpRocState(6000), {
				roc: 5,
				highestSeq: 40000,
			})
		})

		void it('never regresses a higher tracked extended index to a lower seeded one', () => {
			const pipeline = makePipeline()
			pipeline.seedSrtpRoc(6000, { roc: 5, highestSeq: 100 })
			pipeline.seedSrtpRoc(6000, { roc: 3, highestSeq: 100 })
			assert.strictEqual(pipeline.getSrtpRoc(6000), 5)
		})

		void it('raises the tracked state when the seed has a higher extended index', () => {
			const pipeline = makePipeline()
			pipeline.seedSrtpRoc(6000, { roc: 5, highestSeq: 100 })
			pipeline.seedSrtpRoc(6000, { roc: 10, highestSeq: 100 })
			assert.strictEqual(pipeline.getSrtpRoc(6000), 10)
		})

		void it('restores the full extended index, not just the ROC, so a live seq far above 0 is classified correctly', () => {
			// Regression test: seeding ROC 5 while inventing highestSeq: 0 would make the next
			// packet's candidate-ROC search compare against extended index 5*65536+0, so a
			// packet with a high sequence number (e.g. 40000, which the sender was actually at
			// when this ROC was persisted) looks closer to ROC 4 than ROC 5 and gets
			// misclassified/ignored - seeding the real highestSeq avoids that.
			const pipeline = makePipeline()
			pipeline.seedSrtpRoc(6000, { roc: 5, highestSeq: 40000 })
			// Advancing with a nearby, still-forward sequence number must not roll ROC back.
			const state = pipeline.getSrtpRocState(6000)
			assert.strictEqual(state.roc, 5)
			assert.strictEqual(state.highestSeq, 40000)
		})

		void it('tracks a fresh sender at ROC 0 after seeding the {0,0} fresh-session sentinel', () => {
			// End-to-end guard for the sentinel StreamMetadataService.getSrtpRocState
			// returns for a cleared/identity-mismatched persisted item: seeding it must
			// leave a fresh sender's first datagram (with a randomized starting sequence,
			// here well above the midpoint) classified at ROC 0 - never one rollover ahead,
			// which would later be persisted and make the next restart undecryptable.
			// seedSrtpRoc deliberately does not install {0,0} as a tracked baseline (the
			// first packet takes advanceSrtpRoc's genuine first-packet branch instead),
			// and the spec above locks in that the heuristic agrees even if it did.
			const keyStore = {
				getKeyForPort: () => ({
					keyHex: 'a'.repeat(60),
					ssrc: 42,
					cipher: 'x',
					auth: 'y',
				}),
			} as unknown as SrtpKeyStore
			const pipeline = new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
				srtp: { portRange: { start: 6000, end: 6009 }, keyStore },
			})
			pipeline.seedSrtpRoc(6000, { roc: 0, highestSeq: 0 })
			const datagram = Buffer.alloc(12)
			datagram[0] = 0x80 // RTP version 2
			datagram.writeUInt32BE(42, 8)
			datagram.writeUInt16BE(40000, 2)
			pipeline.trackSrtpRoc(6000, datagram)
			assert.deepStrictEqual(pipeline.getSrtpRocState(6000), {
				highestSeq: 40000,
				roc: 0,
			})
		})
	})

	void describe('SRTP pipeline lifecycle (injected child process)', () => {
		const port = 6000
		// port + DEFAULT_SRTP_RELAY_PORT_OFFSET (10000) - must stay in sync manually.
		const relayPort = 16000
		const keyHex = 'ab'.repeat(30)
		const keyStore = {
			getKeyForPort: (p: number) =>
				p === port
					? {
							keyHex,
							ssrc: 42,
							cipher: 'aes-128-icm',
							auth: 'hmac-sha1-80',
							keyFingerprint: 'fp',
						}
					: undefined,
		} as unknown as SrtpKeyStore

		/** A minimal valid SRTP datagram for the configured SSRC (RTP v2). */
		const datagram = (seq: number, size = 12): Buffer => {
			const buf = Buffer.alloc(size)
			buf[0] = 0x80 // RTP version 2
			buf.writeUInt16BE(seq, 2)
			buf.writeUInt32BE(42, 8)
			return buf
		}

		const waitUntil = async (
			predicate: () => boolean,
			timeoutMs = 3_000,
		): Promise<void> => {
			const deadline = Date.now() + timeoutMs
			while (!predicate() && Date.now() < deadline) {
				await new Promise((r) => setTimeout(r, 10))
			}
			assert.ok(predicate(), 'condition not met within timeout')
		}

		/**
		 * Builds a pipeline whose "GStreamer" children are real Node processes (spawned via
		 * the injectable spawn dependency), so exit/kill/stdio behave like the real thing.
		 * The default child script prints GStreamer's readiness line; pass a custom script
		 * (e.g. one that never prints it) to exercise the readiness-grace fallback.
		 */
		const makeLifecycle = (opts?: {
			pendingQueueMaxBytes?: number
			childScript?: string
			/** Awaited inside the injected credential resolution - lets a test hold a
			 * start in its pre-spawn window and then release it. */
			envGate?: Promise<void>
		}) => {
			const spawnedCalls: {
				command: string
				argv: string[]
				env: NodeJS.ProcessEnv
			}[] = []
			const children: ReturnType<typeof spawn>[] = []
			const fakeSpawn = ((
				command: string,
				args: readonly string[],
				options: { env?: NodeJS.ProcessEnv },
			): ReturnType<typeof spawn> => {
				spawnedCalls.push({
					command,
					argv: [...args],
					env: options.env ?? {},
				})
				const child = spawn(
					process.execPath,
					[
						'-e',
						opts?.childScript ??
							"console.log('Setting pipeline to PLAYING ...'); setInterval(() => {}, 1000);",
					],
					{ stdio: ['ignore', 'pipe', 'pipe'] },
				)
				children.push(child)
				return child
			}) as unknown as typeof spawn
			const pipeline = new KinesisIngestionPipeline(
				{
					region: 'eu-central-1',
					portRange: { start: 5000, end: 5009 },
					srtp: {
						portRange: { start: 6000, end: 6009 },
						keyStore,
						pendingQueueMaxBytes: opts?.pendingQueueMaxBytes,
					},
				},
				{
					spawn: fakeSpawn,
					gstEnv: async () => {
						if (opts?.envGate !== undefined) await opts.envGate
						return { GST_ENV_STUB: 'yes' }
					},
				},
			)
			return { pipeline, spawnedCalls, children }
		}

		/** Binds a receiver on the relay port - plays the role of GStreamer's udpsrc. */
		const makeReceiver = async (): Promise<{
			received: Buffer[]
			close: () => void
		}> => {
			const received: Buffer[] = []
			const socket = dgram.createSocket('udp4')
			await new Promise<void>((resolve) =>
				socket.bind(relayPort, '127.0.0.1', resolve),
			)
			socket.on('message', (msg: Buffer) => received.push(msg))
			return { received, close: () => socket.close() }
		}

		void it('spawns with the expected argv/caps, replays initial datagrams in order, and relays live traffic', async () => {
			const receiver = await makeReceiver()
			const { pipeline, spawnedCalls, children } = makeLifecycle()
			try {
				// A seeded ROC must reach srtpdec's caps unmodified (see srtpdecSeedRoc).
				pipeline.seedSrtpRoc(port, { roc: 2, highestSeq: 500 })
				const initial = [datagram(100), datagram(101), datagram(102)]
				await pipeline.start(port, initial)

				assert.strictEqual(pipeline.isActive(port), true)
				assert.strictEqual(spawnedCalls.length, 1)
				const call = spawnedCalls[0]!
				assert.strictEqual(call.command, 'gst-launch-1.0')
				assert.ok(call.argv.includes('udpsrc'))
				assert.ok(call.argv.includes(`port=${relayPort}`))
				const caps = call.argv.find((token) => token.startsWith('caps='))
				assert.ok(caps !== undefined)
				assert.ok(caps.includes('ssrc=(uint)42'))
				assert.ok(caps.includes(`srtp-key=(buffer)${keyHex}`))
				assert.ok(caps.includes('srtp-cipher=(string)aes-128-icm'))
				assert.ok(caps.includes('srtp-auth=(string)hmac-sha1-80'))
				assert.ok(caps.includes('roc=(uint)2'))
				// The env resolved for kvssink (injected here) is passed to the child.
				assert.strictEqual(call.env.GST_ENV_STUB, 'yes')

				// The buffered datagrams are replayed in order, as individual datagrams.
				await waitUntil(() => receiver.received.length === 3)
				assert.deepStrictEqual(receiver.received, initial)

				// Live datagrams relay after readiness, in order.
				pipeline.writePacket(port, datagram(103))
				pipeline.writePacket(port, datagram(104))
				await waitUntil(() => receiver.received.length === 5)
				assert.deepStrictEqual(receiver.received.slice(3), [
					datagram(103),
					datagram(104),
				])

				// stop() terminates the child (its actual exit is awaited) and clears the port.
				await pipeline.stop(port)
				assert.strictEqual(pipeline.isActive(port), false)
				assert.ok(
					children[0]!.signalCode === 'SIGTERM' ||
						children[0]!.exitCode !== null,
				)
				// Live traffic after stop is not relayed anywhere.
				const receivedAtStop = receiver.received.length
				pipeline.writePacket(port, datagram(105))
				await new Promise((r) => setTimeout(r, 50))
				assert.strictEqual(receiver.received.length, receivedAtStop)
			} finally {
				await pipeline.shutdown()
				receiver.close()
			}
		})

		void it('queues live datagrams that arrive before readiness, in order, dropping the oldest past pendingQueueMaxBytes', async () => {
			const receiver = await makeReceiver()
			// The child never prints the readiness line, so start() waits out the
			// SRTP_STARTUP_GRACE_MS (~300ms) fallback - a long not-ready window to queue in.
			const { pipeline } = makeLifecycle({
				childScript: 'setInterval(() => {}, 1000)',
				pendingQueueMaxBytes: 2_500,
			})
			try {
				const initial = [datagram(10), datagram(11)]
				const startPromise = pipeline.start(port, initial)
				// Wait for the pipeline to register (spawn happens after the injected env
				// resolution), then queue live datagrams while the startup replay is still
				// in flight.
				await waitUntil(() => pipeline.isActive(port))
				// Each queued datagram is 1100 bytes; four exceed the 2500-byte cap, so the
				// two oldest are dropped and the two newest retained, in order.
				pipeline.writePacket(port, datagram(50, 1100))
				pipeline.writePacket(port, datagram(51, 1100))
				pipeline.writePacket(port, datagram(52, 1100))
				pipeline.writePacket(port, datagram(53, 1100))
				await startPromise

				// Initial replay first, then the retained live datagrams, in order.
				await waitUntil(() => receiver.received.length === 4)
				assert.deepStrictEqual(receiver.received, [
					datagram(10),
					datagram(11),
					datagram(52, 1100),
					datagram(53, 1100),
				])
			} finally {
				await pipeline.shutdown()
				receiver.close()
			}
		})

		void it('holds SRTP datagrams while no pipeline is active and replays them in order on restart', async () => {
			const receiver = await makeReceiver()
			const { pipeline, children } = makeLifecycle()
			try {
				await pipeline.start(port, [datagram(1)])
				await waitUntil(() => receiver.received.length === 1)

				// Simulate an unexpected exit (crash/OOM): kill the child out from under the
				// still-registered pipeline.
				children[0]!.kill('SIGKILL')
				await waitUntil(() => !pipeline.isActive(port))

				// Datagrams arriving while no pipeline is active are held, not dropped -
				// losing the first post-crash keyframe/SPS/PPS would delay recovery until
				// the sender's next keyframe.
				pipeline.writePacket(port, datagram(2))
				pipeline.writePacket(port, datagram(3))

				// The replacement pipeline (the throttled restart in index.ts) replays them,
				// in order, as its first datagrams.
				await pipeline.start(port, [])
				await waitUntil(() => receiver.received.length === 3)
				assert.deepStrictEqual(receiver.received.slice(1), [
					datagram(2),
					datagram(3),
				])

				// A datagram held while no pipeline is active is dropped once the port is
				// intentionally stopped - the stop clears the hold (there is no replacement
				// pipeline to replay into), so the next start begins from live traffic
				// instead of stale pre-stop datagrams.
				children[1]!.kill('SIGKILL')
				await waitUntil(() => !pipeline.isActive(port))
				pipeline.writePacket(port, datagram(4)) // held while down
				await pipeline.stop(port) // intentional stop clears the hold
				await pipeline.start(port, [])
				await new Promise((r) => setTimeout(r, 100))
				assert.strictEqual(receiver.received.length, 3)
			} finally {
				await pipeline.shutdown()
				receiver.close()
			}
		})

		void it('aborts a pending start that a stop() invalidates during credential resolution', async () => {
			let releaseEnv!: () => void
			const envGate = new Promise<void>((resolve) => {
				releaseEnv = resolve
			})
			const { pipeline, spawnedCalls } = makeLifecycle({ envGate })
			const startPromise = pipeline.start(port, [datagram(1)])
			// Let the start reach (and stall in) the injected credential resolution - it
			// has not spawned anything yet.
			await new Promise((r) => setTimeout(r, 20))

			// A stop() while nothing is registered used to be a silent no-op: the pending
			// start would have registered a producer *after* the teardown returned.
			await pipeline.stop(port)
			releaseEnv()
			await startPromise

			assert.strictEqual(pipeline.isActive(port), false)
			// The invalidated start never spawned a child - there is nothing to clean up.
			assert.strictEqual(spawnedCalls.length, 0)
		})

		void it('restores unsent held datagrams to the hold when the pipeline dies during startup', async () => {
			const receiver = await makeReceiver()
			// Both children never print the readiness line: each start waits out the
			// SRTP_STARTUP_GRACE_MS (~300ms) before its readiness resolves.
			const { pipeline, children } = makeLifecycle({
				childScript: 'setInterval(() => {}, 1000)',
			})
			try {
				// First pipeline survives its grace period and starts ingesting.
				await pipeline.start(port, [datagram(1)])
				await waitUntil(() => receiver.received.length === 1)

				// It crashes: the port is left without a pipeline.
				children[0]!.kill('SIGKILL')
				await waitUntil(() => !pipeline.isActive(port))

				// Datagrams arriving in the no-pipeline window are held, and the next
				// start seeds them into its pendingQueue at registration.
				pipeline.writePacket(port, datagram(2))
				pipeline.writePacket(port, datagram(3))

				// The replacement pipeline dies *during* its readiness window - the
				// unsent remainder of its seeded queue must go back to the hold instead
				// of being dropped (the failed-start path comes looking for it via
				// takeHeldDatagrams).
				const startPromise = pipeline.start(port, [])
				await waitUntil(() => pipeline.isActive(port))
				children[1]!.kill('SIGKILL')
				await startPromise

				assert.strictEqual(pipeline.isActive(port), false)
				assert.deepStrictEqual(pipeline.takeHeldDatagrams(port), [
					datagram(2),
					datagram(3),
				])
			} finally {
				await pipeline.shutdown()
				receiver.close()
			}
		})

		void it('shutdown() refuses further starts and waits for still-running children to exit', async () => {
			const { pipeline, spawnedCalls, children } = makeLifecycle()
			await pipeline.start(port, [])
			assert.strictEqual(pipeline.isActive(port), true)
			await pipeline.shutdown()
			assert.strictEqual(pipeline.isActive(port), false)
			// The real child actually exited - killAndWait waits for the exit event.
			assert.ok(
				children[0]!.exitCode !== null || children[0]!.signalCode !== null,
			)
			// Further starts are refused without spawning again.
			await pipeline.start(port, [])
			assert.strictEqual(spawnedCalls.length, 1)
			assert.strictEqual(pipeline.isActive(port), false)
		})
	})
})
