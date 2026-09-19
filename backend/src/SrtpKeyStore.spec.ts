import assert from 'node:assert'
import { describe, it } from 'node:test'
import {
	isStoredSrtpKey,
	isValidSrtpKeyHex,
	keyFingerprint,
} from './SrtpKeyStore.ts'

void describe('SrtpKeyStore', () => {
	void describe('keyFingerprint', () => {
		void it('is stable for the same key', () => {
			const hex = 'aB1'.repeat(20)
			assert.strictEqual(keyFingerprint(hex), keyFingerprint(hex))
		})

		void it('is the same regardless of hex casing (same key bytes)', () => {
			const lower = 'ab12cd34'.repeat(7) + 'abcd'
			const upper = lower.toUpperCase()
			assert.strictEqual(keyFingerprint(lower), keyFingerprint(upper))
		})

		void it('differs for different keys', () => {
			assert.notStrictEqual(
				keyFingerprint('a'.repeat(60)),
				keyFingerprint('b'.repeat(60)),
			)
		})
	})

	void describe('isValidSrtpKeyHex', () => {
		void it('accepts a 60-character hex string (30-byte master key+salt)', () => {
			assert.strictEqual(isValidSrtpKeyHex('a'.repeat(60)), true)
			assert.strictEqual(isValidSrtpKeyHex('aA1bB2'.repeat(10)), true)
		})

		void it('rejects strings of the wrong length', () => {
			assert.strictEqual(isValidSrtpKeyHex('a'.repeat(59)), false)
			assert.strictEqual(isValidSrtpKeyHex('a'.repeat(61)), false)
			assert.strictEqual(isValidSrtpKeyHex(''), false)
		})

		void it('rejects non-hex characters', () => {
			assert.strictEqual(isValidSrtpKeyHex('g'.repeat(60)), false)
			assert.strictEqual(isValidSrtpKeyHex(`${'a'.repeat(59)};`), false)
			assert.strictEqual(
				isValidSrtpKeyHex(`${'a'.repeat(50)}$(rm -rf /)aaa`),
				false,
			)
		})
	})

	void describe('isStoredSrtpKey', () => {
		void it('accepts a valid key with no cipher/auth override', () => {
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: 12345 }),
				true,
			)
		})

		void it('accepts a valid key with the supported cipher/auth suite', () => {
			assert.strictEqual(
				isStoredSrtpKey({
					key: 'a'.repeat(60),
					ssrc: 12345,
					cipher: 'aes-128-icm',
					auth: 'hmac-sha1-80',
				}),
				true,
			)
		})

		void it('rejects a missing or non-string key', () => {
			assert.strictEqual(isStoredSrtpKey({ ssrc: 1 }), false)
			assert.strictEqual(isStoredSrtpKey({ key: 123, ssrc: 1 }), false)
		})

		void it('rejects a non-integer, negative, or out-of-range uint32 ssrc', () => {
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: 1.5 }),
				false,
			)
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: -1 }),
				false,
			)
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: 0x1_0000_0000 }),
				false,
			)
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: '12345' }),
				false,
			)
		})

		void it('accepts the boundary uint32 values for ssrc', () => {
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: 0 }),
				true,
			)
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: 0xffffffff }),
				true,
			)
		})

		void it('rejects an unsupported cipher or auth suite', () => {
			assert.strictEqual(
				isStoredSrtpKey({
					key: 'a'.repeat(60),
					ssrc: 1,
					cipher: 'aes-256-icm',
				}),
				false,
			)
			assert.strictEqual(
				isStoredSrtpKey({
					key: 'a'.repeat(60),
					ssrc: 1,
					auth: 'hmac-sha1-32',
				}),
				false,
			)
		})
	})
})
