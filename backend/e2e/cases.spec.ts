import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { wrongKeyOf } from './cases.ts'

/**
 * The negative cases' wrong key: the helper behind them exists because a
 * randomly generated key ends in `ff` about once per 256 runs, which used to
 * turn the "wrong key" into the real key and authenticate the negative case.
 */
void describe('wrongKeyOf', () => {
	void it('differs from the key, whatever the key ends in', () => {
		for (const end of ['ff', '00', 'ab', '5e', '0f']) {
			const keyHex = `${'ab'.repeat(29)}${end}`
			const wrong = wrongKeyOf(keyHex)
			assert.notStrictEqual(wrong, keyHex, `a key ending in ${end}`)
			assert.match(wrong, /^[0-9a-f]{60}$/)
		}
	})
})
