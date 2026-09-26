import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { nextGeneration } from './stack.ts'

/**
 * The port-key generation counter, pinned by its two properties.
 *
 * Both came out of review: unix seconds alone let two provisions of the same
 * port in one second collide (the replay floor's rotation fence then rejected
 * every floor write of the newer key, silently leaving it without persisted
 * replay protection), and a deleted generation parameter must not restart the
 * counter below floor rows already written.
 */
void describe('nextGeneration', () => {
	void it('is strictly newer than the current one, whatever the clock says', () => {
		// Two provisions in the same second: the second must still be newer.
		const t = Math.floor(Date.now() / 1000)
		const first = nextGeneration(0, t)
		const second = nextGeneration(first, t)
		assert.strictEqual(first, t)
		assert.ok(second > first)
		assert.strictEqual(second, t + 1)
	})

	void it('never starts over below a floor row already written', () => {
		// A floor row carries generation 1730000000 while the parameter reads
		// as absent (0): the next generation must still exceed the row's, or
		// the fence would reject the new key's floor writes forever.
		const t = Math.floor(Date.now() / 1000)
		assert.ok(nextGeneration(0, t) > 1_730_000_000)
		// And a counter ahead of the clock keeps counting, not resetting.
		assert.strictEqual(nextGeneration(1_800_000_000, t), 1_800_000_001)
	})
})
