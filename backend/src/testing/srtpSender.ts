import { createCipheriv, createHmac } from 'node:crypto'

/**
 * Minimal SRTP sender for tests (RFC 3711, AES-128-CM with HMAC-SHA1-80).
 *
 * Exists so tests can send authentic SRTP packets at an *arbitrary* rollover counter.
 * A real encoder always starts a session at ROC 0, which would mean sending 65536
 * packets to reach ROC 1 - so the cases that matter most for the rollover-counter
 * search (a stream above its replay floor, a stream that wraps) would be untestable.
 *
 * It cannot make a test pass that should fail: if any of this is wrong, srtpdec simply
 * does not authenticate and the assertions fail loudly.
 */

const MASTER_KEY_BYTES = 16
const MASTER_SALT_BYTES = 14
const SESSION_KEY_BYTES = 16
const SESSION_AUTH_BYTES = 20
const SESSION_SALT_BYTES = 14
const AUTH_TAG_BYTES = 10
const RTP_HEADER_BYTES = 12

/** Key-derivation labels from RFC 3711 section 4.3.1. */
const LABEL_RTP_ENCRYPTION = 0x00
const LABEL_RTP_AUTHENTICATION = 0x01
const LABEL_RTP_SALT = 0x02

/** AES-CM keystream: AES-128-CTR over zeroes, with `iv` as the initial counter. */
const keystream = (key: Buffer, iv: Buffer, bytes: number): Buffer => {
	const cipher = createCipheriv('aes-128-ctr', key, iv)
	return Buffer.concat([cipher.update(Buffer.alloc(bytes)), cipher.final()])
}

/**
 * Derives one session key.
 *
 * With a key-derivation rate of zero the index divided by the rate is defined as 0, so
 * key_id is just the label followed by six zero bytes. x is that XORed into the low
 * bytes of the master salt, and the AES-CM input is x shifted left by 16 bits - which,
 * for a 14-byte salt in a 16-byte block, means writing the salt at offset 0 and
 * leaving the last two bytes zero.
 */
const deriveSessionKey = (
	masterKey: Buffer,
	masterSalt: Buffer,
	label: number,
	bytes: number,
): Buffer => {
	const iv = Buffer.alloc(16)
	masterSalt.copy(iv, 0)
	// key_id occupies the 7 bytes ending at the salt's last byte; only the label is
	// non-zero, and it sits at offset 7 within the 14-byte salt.
	iv[7] = (iv[7] ?? 0) ^ label
	return keystream(masterKey, iv, bytes)
}

export type SrtpSessionKeys = {
	encryption: Buffer
	authentication: Buffer
	salt: Buffer
}

export const deriveSessionKeys = (keyHex: string): SrtpSessionKeys => {
	const material = Buffer.from(keyHex, 'hex')
	if (material.length !== MASTER_KEY_BYTES + MASTER_SALT_BYTES) {
		throw new Error(
			`expected ${String(MASTER_KEY_BYTES + MASTER_SALT_BYTES)} bytes of key material, got ${String(material.length)}`,
		)
	}
	const masterKey = material.subarray(0, MASTER_KEY_BYTES)
	const masterSalt = material.subarray(MASTER_KEY_BYTES)
	return {
		encryption: deriveSessionKey(
			masterKey,
			masterSalt,
			LABEL_RTP_ENCRYPTION,
			SESSION_KEY_BYTES,
		),
		authentication: deriveSessionKey(
			masterKey,
			masterSalt,
			LABEL_RTP_AUTHENTICATION,
			SESSION_AUTH_BYTES,
		),
		salt: deriveSessionKey(
			masterKey,
			masterSalt,
			LABEL_RTP_SALT,
			SESSION_SALT_BYTES,
		),
	}
}

/**
 * Per-packet initialisation vector:
 *   (session salt << 16) XOR (SSRC << 64) XOR (packet index << 16)
 * where the packet index is ROC * 2^16 + sequence number.
 */
const packetIv = (
	salt: Buffer,
	ssrc: number,
	roc: number,
	seq: number,
): Buffer => {
	const iv = Buffer.alloc(16)
	salt.copy(iv, 0)

	// SSRC << 64 lands in bytes 4..7.
	const ssrcBytes = Buffer.alloc(4)
	ssrcBytes.writeUInt32BE(ssrc >>> 0)
	for (let i = 0; i < 4; i++) iv[4 + i] = (iv[4 + i] ?? 0) ^ (ssrcBytes[i] ?? 0)

	// The 48-bit packet index << 16 lands in bytes 8..13.
	const index = BigInt(roc) * 65536n + BigInt(seq)
	for (let i = 0; i < 6; i++) {
		const shift = BigInt(8 * (5 - i))
		iv[8 + i] = (iv[8 + i] ?? 0) ^ Number((index >> shift) & 0xffn)
	}
	return iv
}

export type SrtpPacketOptions = {
	keyHex: string
	ssrc: number
	seq: number
	roc: number
	payload: Buffer
	payloadType?: number
	timestamp?: number
	marker?: boolean
}

/** Builds one authentic SRTP packet. */
export const srtpPacket = (options: SrtpPacketOptions): Buffer => {
	const {
		keyHex,
		ssrc,
		seq,
		roc,
		payload,
		payloadType = 96,
		timestamp = seq * 3000,
		marker = false,
	} = options
	const keys = deriveSessionKeys(keyHex)

	const header = Buffer.alloc(RTP_HEADER_BYTES)
	header[0] = 0x80 // version 2, no padding, no extension, no CSRCs
	header[1] = (marker ? 0x80 : 0x00) | (payloadType & 0x7f)
	header.writeUInt16BE(seq & 0xffff, 2)
	header.writeUInt32BE(timestamp >>> 0, 4)
	header.writeUInt32BE(ssrc >>> 0, 8)

	const iv = packetIv(keys.salt, ssrc, roc, seq & 0xffff)
	const stream = keystream(keys.encryption, iv, payload.length)
	const encrypted = Buffer.alloc(payload.length)
	for (let i = 0; i < payload.length; i++) {
		encrypted[i] = (payload[i] ?? 0) ^ (stream[i] ?? 0)
	}

	// The authentication tag covers the header, the ciphertext, and the rollover
	// counter - which is why a receiver seeded with the wrong counter cannot
	// authenticate a packet even though every byte on the wire is correct.
	const rocBytes = Buffer.alloc(4)
	rocBytes.writeUInt32BE(roc >>> 0)
	const tag = createHmac('sha1', keys.authentication)
		.update(Buffer.concat([header, encrypted, rocBytes]))
		.digest()
		.subarray(0, AUTH_TAG_BYTES)

	return Buffer.concat([header, encrypted, tag])
}

/**
 * A single H.264 NAL unit payload that rtph264depay accepts.
 *
 * Tests here assert on authentication, not on decodable video, so this only has to be
 * a well-formed single-NAL packet.
 */
export const h264Payload = (size = 64, fill = 0x42): Buffer => {
	const payload = Buffer.alloc(size, fill)
	payload[0] = 0x41 // non-IDR slice NAL header
	return payload
}
