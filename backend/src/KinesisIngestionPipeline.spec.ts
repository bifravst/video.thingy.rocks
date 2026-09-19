import assert from 'node:assert'
import { describe, it } from 'node:test'
import {
	advanceSrtpRoc,
	KinesisIngestionPipeline,
	parseRtpSequenceNumber,
} from './KinesisIngestionPipeline.ts'
import type { SrtpKeyStore } from './SrtpKeyStore.ts'

/** Builds a minimal 12-byte RTP/SRTP header with the given sequence number; the rest of
 * the header/payload is irrelevant to parseRtpSequenceNumber. */
const rtpHeader = (seq: number): Buffer => {
	const buf = Buffer.alloc(12)
	buf.writeUInt16BE(seq, 2)
	return buf
}

void describe('KinesisIngestionPipeline', () => {
	void describe('parseRtpSequenceNumber', () => {
		void it('reads the sequence number from bytes 2-3', () => {
			assert.strictEqual(parseRtpSequenceNumber(rtpHeader(1234)), 1234)
			assert.strictEqual(parseRtpSequenceNumber(rtpHeader(65535)), 65535)
			assert.strictEqual(parseRtpSequenceNumber(rtpHeader(0)), 0)
		})

		void it('returns undefined for a datagram shorter than a full RTP header', () => {
			assert.strictEqual(parseRtpSequenceNumber(Buffer.alloc(11)), undefined)
			assert.strictEqual(parseRtpSequenceNumber(Buffer.alloc(0)), undefined)
		})
	})

	void describe('advanceSrtpRoc', () => {
		void it('starts at ROC 0 for the first observed packet', () => {
			const state = advanceSrtpRoc(undefined, 100)
			assert.deepStrictEqual(state, { lastSeq: 100, roc: 0 })
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

		void it('increments across multiple rollovers over a long session', () => {
			let state = advanceSrtpRoc(undefined, 0xfffe)
			state = advanceSrtpRoc(state, 0x0002) // rollover 1
			assert.strictEqual(state.roc, 1)
			state = advanceSrtpRoc(state, 0xfffd)
			state = advanceSrtpRoc(state, 0x0001) // rollover 2
			assert.strictEqual(state.roc, 2)
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

	void describe('seedSrtpRoc / getSrtpRoc', () => {
		const makePipeline = (): KinesisIngestionPipeline =>
			new KinesisIngestionPipeline({
				region: 'eu-central-1',
				portRange: { start: 5000, end: 5009 },
			})

		void it('defaults to 0 for a port never observed or seeded', () => {
			const pipeline = makePipeline()
			assert.strictEqual(pipeline.getSrtpRoc(6000), 0)
		})

		void it('seeds an initial value', () => {
			const pipeline = makePipeline()
			pipeline.seedSrtpRoc(6000, 5)
			assert.strictEqual(pipeline.getSrtpRoc(6000), 5)
		})

		void it('never regresses a higher tracked value to a lower seeded one', () => {
			const pipeline = makePipeline()
			pipeline.seedSrtpRoc(6000, 5)
			pipeline.seedSrtpRoc(6000, 3)
			assert.strictEqual(pipeline.getSrtpRoc(6000), 5)
		})

		void it('raises the tracked value when the seed is higher', () => {
			const pipeline = makePipeline()
			pipeline.seedSrtpRoc(6000, 5)
			pipeline.seedSrtpRoc(6000, 10)
			assert.strictEqual(pipeline.getSrtpRoc(6000), 10)
		})
	})
})
