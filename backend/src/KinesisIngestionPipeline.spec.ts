import assert from 'node:assert'
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
	})
})
