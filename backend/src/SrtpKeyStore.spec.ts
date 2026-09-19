import assert from 'node:assert'
import { describe, it } from 'node:test'
import { isValidSrtpKeyHex } from './SrtpKeyStore.ts'

void describe('SrtpKeyStore', () => {
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
})
