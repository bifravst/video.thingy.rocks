import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { parseOnlyFilter } from './args.ts'

/**
 * The fail-closed filter: every malformed invocation must throw rather than
 * expand to the whole suite, because the whole suite mutates the deployed
 * stack - keys are rotated and the fleet restarted.
 */
void describe('parseOnlyFilter', () => {
	void it('selects every case only when there are no arguments at all', () => {
		assert.strictEqual(parseOnlyFilter([]), undefined)
	})

	void it('selects one case by a non-empty substring', () => {
		assert.strictEqual(parseOnlyFilter(['--only', 'wrap']), 'wrap')
	})

	void it('rejects a bare --only, which used to select every case', () => {
		assert.throws(() => parseOnlyFilter(['--only']), /unexpected arguments/)
	})

	void it('rejects the = form, which the old parsing never saw', () => {
		assert.throws(
			() => parseOnlyFilter(['--only=wrap']),
			/unexpected arguments/,
		)
	})

	void it('rejects an empty filter value', () => {
		assert.throws(() => parseOnlyFilter(['--only', '']), /unexpected arguments/)
	})

	void it('rejects extra arguments, whatever they are', () => {
		assert.throws(
			() => parseOnlyFilter(['--only', 'wrap', 'extra']),
			/unexpected arguments/,
		)
		assert.throws(() => parseOnlyFilter(['--foo']), /unexpected arguments/)
	})
})
