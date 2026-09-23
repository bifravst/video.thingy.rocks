import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import {
	isRetryableFatal,
	MAX_LINE_BYTES,
	SrtpHelperProtocol,
	type SrtpHelperMessage,
} from './SrtpHelperProtocol.ts'

const parse = (...chunks: string[]): SrtpHelperMessage[] => {
	const protocol = new SrtpHelperProtocol()
	return chunks.flatMap((chunk) => protocol.push(chunk))
}

const line = (value: unknown): string => `${JSON.stringify(value)}\n`

void describe('SrtpHelperProtocol', () => {
	void describe('message types', () => {
		void it('parses ready with the bound relay port', () => {
			assert.deepStrictEqual(
				parse(line({ t: 'ready', v: 1, relayPort: 49227, pid: 1234 })),
				[{ t: 'ready', v: 1, relayPort: 49227, pid: 1234 }],
			)
		})

		void it('parses a first authentication', () => {
			assert.deepStrictEqual(
				parse(
					line({
						t: 'auth',
						status: 'ok',
						first: true,
						roc: 7,
						seq: 28600,
						candidate: 7,
						trials: 1,
						authenticated: 22,
					}),
				),
				[
					{
						t: 'auth',
						status: 'ok',
						first: true,
						roc: 7,
						seq: 28600,
						candidate: 7,
						trials: 1,
						authenticated: 22,
					},
				],
			)
		})

		void it('parses a rollover as a non-first authentication', () => {
			const [message] = parse(
				line({ t: 'auth', status: 'ok', first: false, roc: 8 }),
			)
			assert.deepStrictEqual(message, {
				t: 'auth',
				status: 'ok',
				first: false,
				roc: 8,
				seq: undefined,
				candidate: undefined,
				trials: undefined,
				authenticated: undefined,
			})
		})

		void it('parses the other auth statuses', () => {
			assert.deepStrictEqual(
				parse(
					line({
						t: 'auth',
						status: 'fail',
						candidate: 3,
						inputs: 9,
						drops: 4,
					}),
					line({ t: 'auth', status: 'exhausted', trials: 1024 }),
					line({ t: 'auth', status: 'lost', sinceMs: 3100, drops: 12 }),
				),
				[
					{ t: 'auth', status: 'fail', candidate: 3, inputs: 9, drops: 4 },
					{ t: 'auth', status: 'exhausted', trials: 1024 },
					{ t: 'auth', status: 'lost', sinceMs: 3100, drops: 12 },
				],
			)
		})

		void it('parses stats, keeping an unreported rollover counter null', () => {
			assert.deepStrictEqual(
				parse(
					line({
						t: 'stats',
						inputs: 242,
						authenticated: 0,
						aus: 0,
						roc: null,
						drops: 242,
					}),
				),
				[
					{
						t: 'stats',
						inputs: 242,
						authenticated: 0,
						aus: 0,
						roc: null,
						drops: 242,
					},
				],
			)
		})

		void it('parses searching, warning, error, fatal and eos', () => {
			assert.deepStrictEqual(
				parse(
					line({ t: 'searching', candidate: 2, trial: 1 }),
					line({ t: 'warning', element: 'srtpdec', message: 'unknown ssrc' }),
					line({ t: 'error', element: 'kvssink', message: 'boom', debug: 'd' }),
					line({ t: 'fatal', reason: 'bind-failed', message: 'in use' }),
					line({ t: 'eos' }),
				),
				[
					{ t: 'searching', candidate: 2, trial: 1 },
					{ t: 'warning', element: 'srtpdec', message: 'unknown ssrc' },
					{ t: 'error', element: 'kvssink', message: 'boom', debug: 'd' },
					{ t: 'fatal', reason: 'bind-failed', message: 'in use' },
					{ t: 'eos' },
				],
			)
		})
	})

	void describe('framing', () => {
		void it('reassembles a line split across chunks', () => {
			const full = line({ t: 'ready', v: 1, relayPort: 5 })
			const messages = parse(full.slice(0, 12), full.slice(12))
			assert.deepStrictEqual(messages, [
				{ t: 'ready', v: 1, relayPort: 5, pid: undefined },
			])
		})

		void it('handles several messages in one chunk', () => {
			const messages = parse(line({ t: 'eos' }) + line({ t: 'eos' }))
			assert.strictEqual(messages.length, 2)
		})

		void it('emits nothing for a partial line', () => {
			const protocol = new SrtpHelperProtocol()
			assert.deepStrictEqual(protocol.push('{"t":"eo'), [])
			assert.deepStrictEqual(protocol.push('s"}\n'), [{ t: 'eos' }])
		})

		void it('ignores blank lines', () => {
			assert.deepStrictEqual(parse('\n\n  \n'), [])
		})

		// A child that never emits a newline must not be able to grow the buffer
		// without bound.
		void it('discards an over-long line and resumes on the next one', () => {
			const protocol = new SrtpHelperProtocol()
			const flood = protocol.push('x'.repeat(MAX_LINE_BYTES + 10))
			assert.strictEqual(flood.length, 1)
			assert.strictEqual(flood[0]?.t, 'unparsed')

			// The remainder of that line is dropped, not parsed as a new one.
			assert.deepStrictEqual(protocol.push('still the same line\n'), [])
			assert.deepStrictEqual(protocol.push(line({ t: 'eos' })), [{ t: 'eos' }])
		})
	})

	void describe('malformed input', () => {
		void it('reports non-JSON as unparsed rather than throwing', () => {
			assert.deepStrictEqual(parse('not json at all\n'), [
				{ t: 'unparsed', raw: 'not json at all' },
			])
		})

		void it('reports an unknown message type as unparsed', () => {
			const [message] = parse(line({ t: 'something-new', x: 1 }))
			assert.strictEqual(message?.t, 'unparsed')
		})

		void it('reports a non-object as unparsed', () => {
			assert.strictEqual(parse('[1,2,3]\n')[0]?.t, 'unparsed')
			assert.strictEqual(parse('42\n')[0]?.t, 'unparsed')
		})

		void it('rejects messages missing the fields they are trusted for', () => {
			// No relayPort: the caller would have nowhere to send datagrams.
			assert.strictEqual(parse(line({ t: 'ready', v: 1 }))[0]?.t, 'unparsed')
			// No roc: nothing to confirm or persist.
			assert.strictEqual(
				parse(line({ t: 'auth', status: 'ok', first: true }))[0]?.t,
				'unparsed',
			)
			assert.strictEqual(
				parse(line({ t: 'auth', status: 'nonsense' }))[0]?.t,
				'unparsed',
			)
		})

		/**
		 * A relay port that cannot be connected to is not a relay port.
		 *
		 * SrtpProducer hands it straight to dgram.connect, which throws synchronously
		 * for anything outside 1-65535 or not an integer - and it does so inside the
		 * helper's stdout handler, where a throw ends the whole process.
		 */
		for (const relayPort of [
			0,
			-1,
			1.5,
			65536,
			70000,
			Number.MAX_SAFE_INTEGER,
		]) {
			void it(`rejects a ready frame with relay port ${String(relayPort)}`, () => {
				assert.strictEqual(
					parse(line({ t: 'ready', v: 1, relayPort }))[0]?.t,
					'unparsed',
				)
			})
		}

		void it('accepts the ends of the port range', () => {
			for (const relayPort of [1, 65535]) {
				assert.strictEqual(
					parse(line({ t: 'ready', v: 1, relayPort }))[0]?.t,
					'ready',
				)
			}
		})

		// A rollover counter is persisted and handed back to the next helper as its
		// hint, so one that could not be a uint32 must not get that far.
		for (const roc of [-1, 0.5, 2 ** 32]) {
			void it(`rejects an authentication at rollover counter ${String(roc)}`, () => {
				assert.strictEqual(
					parse(line({ t: 'auth', status: 'ok', first: true, roc }))[0]?.t,
					'unparsed',
				)
			})
		}

		void it('drops an optional field that is out of range rather than keeping it', () => {
			const [message] = parse(
				line({ t: 'auth', status: 'ok', first: true, roc: 3, seq: 70000 }),
			)
			assert.ok(message?.t === 'auth' && message.status === 'ok')
			assert.strictEqual(message.roc, 3)
			assert.strictEqual(message.seq, undefined)
		})

		// A version this parser does not understand must not be treated as ready: the
		// meaning of every later line would be a guess.
		void it('rejects a ready frame with no version', () => {
			assert.strictEqual(
				parse(line({ t: 'ready', relayPort: 5 }))[0]?.t,
				'unparsed',
			)
		})

		void it('truncates the raw text it keeps', () => {
			const [message] = parse(`${'y'.repeat(500)}\n`)
			assert.strictEqual(message?.t, 'unparsed')
			assert.strictEqual(
				message.t === 'unparsed' ? message.raw.length : -1,
				200,
			)
		})
	})

	void describe('isRetryableFatal', () => {
		void it('treats environment and configuration faults as permanent', () => {
			for (const reason of [
				'bad-init',
				'key-in-argv',
				'unsafe-debug-env',
				'missing-element',
			] as const) {
				assert.strictEqual(isRetryableFatal(reason), false, reason)
			}
		})

		void it('treats a port conflict and a state change as retryable', () => {
			assert.strictEqual(isRetryableFatal('bind-failed'), true)
			assert.strictEqual(isRetryableFatal('state-change'), true)
		})
	})
})
