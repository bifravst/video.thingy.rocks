import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import {
	createSrtpAdmissionFilter,
	plausibleRtpSequenceNumber,
} from './SrtpAdmission.ts'

const SSRC = 0xdeadbeef

const rtp = (
	overrides: {
		version?: number
		ssrc?: number
		seq?: number
		size?: number
	} = {},
): Buffer => {
	const { version = 2, ssrc = SSRC, seq = 1234, size = 60 } = overrides
	// Built at full length and then truncated, so a deliberately short datagram can be
	// produced without the builder itself running off the end.
	const packet = Buffer.alloc(Math.max(size, 12))
	packet[0] = (version << 6) & 0xff
	packet.writeUInt16BE(seq, 2)
	packet.writeUInt32BE(ssrc, 8)
	return packet.subarray(0, size)
}

void describe('plausibleRtpSequenceNumber', () => {
	void it('reads the sequence number of a matching header', () => {
		assert.strictEqual(
			plausibleRtpSequenceNumber(rtp({ seq: 4242 }), SSRC),
			4242,
		)
	})

	void it('rejects a datagram too short to hold a header', () => {
		assert.strictEqual(
			plausibleRtpSequenceNumber(rtp({ size: 11 }), SSRC),
			undefined,
		)
	})

	void it('rejects another RTP version', () => {
		assert.strictEqual(
			plausibleRtpSequenceNumber(rtp({ version: 1 }), SSRC),
			undefined,
		)
	})

	void it('rejects another SSRC', () => {
		assert.strictEqual(
			plausibleRtpSequenceNumber(rtp({ ssrc: 1 }), SSRC),
			undefined,
		)
	})

	void it('accepts sequence number zero', () => {
		assert.strictEqual(plausibleRtpSequenceNumber(rtp({ seq: 0 }), SSRC), 0)
	})
})

void describe('createSrtpAdmissionFilter', () => {
	const identity = (ssrc: number | undefined) => ({
		ssrcForPort: () => ssrc,
	})

	void it('admits a plausible datagram for a keyed port', () => {
		const admit = createSrtpAdmissionFilter(identity(SSRC), 6000)
		assert.strictEqual(admit(rtp()), true)
	})

	void it('rejects noise on a keyed port', () => {
		const admit = createSrtpAdmissionFilter(identity(SSRC), 6000)
		assert.strictEqual(admit(Buffer.from('GET / HTTP/1.1\r\n')), false)
		assert.strictEqual(admit(rtp({ ssrc: 7 })), false)
	})

	// Nothing sent to an unkeyed port could ever be decrypted, so nothing should be
	// buffered for it, and no lock attempt should be made on its behalf.
	void it('admits nothing for a port with no key', () => {
		const admit = createSrtpAdmissionFilter(identity(undefined), 6000)
		assert.strictEqual(admit(rtp()), false)
	})

	// The filter must re-read the identity, so a key that loads later takes effect.
	void it('follows the port identity if it changes', () => {
		let ssrc: number | undefined = undefined
		const admit = createSrtpAdmissionFilter({ ssrcForPort: () => ssrc }, 6000)
		assert.strictEqual(admit(rtp()), false)
		ssrc = SSRC
		assert.strictEqual(admit(rtp()), true)
	})
})
