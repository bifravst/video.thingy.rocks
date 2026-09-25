import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import {
	isRetryableFatal,
	MAX_LINE_BYTES,
	serializeCommand,
	SRTP_HELPER_PROTOCOL_VERSION,
	SrtpHelperProtocol,
	type SrtpHelperMessage,
} from './SrtpHelperProtocol.ts'

const parse = (...chunks: string[]): SrtpHelperMessage[] => {
	const protocol = new SrtpHelperProtocol()
	return chunks.flatMap((chunk) => protocol.push(chunk))
}

const line = (value: unknown): string => `${JSON.stringify(value)}\n`

void describe('SrtpHelperProtocol', () => {
	void describe('commands', () => {
		void it('serializes the init command with the key', () => {
			const init = serializeCommand({
				type: 'init',
				v: SRTP_HELPER_PROTOCOL_VERSION,
				key: 'ab'.repeat(30),
				ssrc: 42,
			})
			assert.strictEqual(typeof init, 'string')
			const parsed = JSON.parse(init) as { type: string; v: number }
			assert.strictEqual(parsed.type, 'init')
			assert.strictEqual(parsed.v, 3)
		})

		void it('serializes start and stop without anything but the version', () => {
			assert.deepStrictEqual(
				JSON.parse(serializeCommand({ type: 'start', v: 3 })) as Record<
					string,
					unknown
				>,
				{ type: 'start', v: 3 },
			)
			assert.deepStrictEqual(
				JSON.parse(serializeCommand({ type: 'stop', v: 3 })) as Record<
					string,
					unknown
				>,
				{ type: 'stop', v: 3 },
			)
		})
	})

	void describe('message types', () => {
		void it('parses ready with the bound ingest port', () => {
			assert.deepStrictEqual(
				parse(line({ t: 'ready', v: 3, port: 6000, pid: 1234 })),
				[{ t: 'ready', v: 3, port: 6000, pid: 1234 }],
			)
		})

		void it('rejects a ready frame with a port outside 1-65535', () => {
			for (const port of [0, -1, 1.5, 65536, 70000]) {
				assert.deepStrictEqual(
					parse(line({ t: 'ready', v: 3, port })),
					[{ t: 'unparsed', raw: line({ t: 'ready', v: 3, port }).trim() }],
					`port ${String(port)}`,
				)
			}
		})

		void it('accepts the boundary ports 1 and 65535', () => {
			assert.deepStrictEqual(parse(line({ t: 'ready', v: 3, port: 1 })), [
				{ t: 'ready', v: 3, port: 1, pid: undefined },
			])
			assert.deepStrictEqual(parse(line({ t: 'ready', v: 3, port: 65535 })), [
				{ t: 'ready', v: 3, port: 65535, pid: undefined },
			])
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

		void it('rejects an authentication with an out-of-range counter', () => {
			for (const roc of [-1, 0.5, 2 ** 32]) {
				assert.deepStrictEqual(
					parse(line({ t: 'auth', status: 'ok', first: true, roc })),
					[
						{
							t: 'unparsed',
							raw: line({ t: 'auth', status: 'ok', first: true, roc }).trim(),
						},
					],
					`roc ${String(roc)}`,
				)
			}
		})

		void it('parses a failed trial with the carried search position', () => {
			assert.deepStrictEqual(
				parse(
					line({
						t: 'auth',
						status: 'fail',
						candidate: 5,
						inputs: 40,
						drops: 12,
						searchFrom: 9,
					}),
				),
				[
					{
						t: 'auth',
						status: 'fail',
						candidate: 5,
						inputs: 40,
						drops: 12,
						searchFrom: 9,
					},
				],
			)
		})

		void it('parses auth lost', () => {
			assert.deepStrictEqual(
				parse(line({ t: 'auth', status: 'lost', sinceMs: 4500, drops: 3 })),
				[{ t: 'auth', status: 'lost', sinceMs: 4500, drops: 3 }],
			)
		})

		void it('parses an index report', () => {
			assert.deepStrictEqual(parse(line({ t: 'index', index: 2 ** 40 })), [
				{ t: 'index', index: 2 ** 40 },
			])
		})

		void it('accepts an index exactly at the 48-bit limit and rejects beyond', () => {
			assert.deepStrictEqual(parse(line({ t: 'index', index: 2 ** 48 - 1 })), [
				{ t: 'index', index: 2 ** 48 - 1 },
			])
			assert.deepStrictEqual(parse(line({ t: 'index', index: 2 ** 48 })), [
				{ t: 'unparsed', raw: line({ t: 'index', index: 2 ** 48 }).trim() },
			])
		})

		void it('parses the producing and stopped mode frames', () => {
			assert.deepStrictEqual(parse(line({ t: 'producing' })), [
				{ t: 'producing' },
			])
			assert.deepStrictEqual(parse(line({ t: 'stopped', index: 12345 })), [
				{ t: 'stopped', index: 12345 },
			])
			assert.deepStrictEqual(parse(line({ t: 'stopped' })), [
				{ t: 'stopped', index: undefined },
			])
		})

		void it('rejects a stopped frame with an out-of-range index by dropping the field', () => {
			assert.deepStrictEqual(parse(line({ t: 'stopped', index: -1 })), [
				{ t: 'stopped', index: undefined },
			])
		})

		void it('parses stats with the input byte count', () => {
			assert.deepStrictEqual(
				parse(
					line({
						t: 'stats',
						inputs: 100,
						inputBytes: 12000,
						authenticated: 80,
						aus: 2,
						roc: 5,
						drops: 20,
					}),
				),
				[
					{
						t: 'stats',
						inputs: 100,
						inputBytes: 12000,
						authenticated: 80,
						aus: 2,
						roc: 5,
						drops: 20,
					},
				],
			)
		})

		void it('rejects stats without input bytes', () => {
			assert.deepStrictEqual(
				parse(
					line({
						t: 'stats',
						inputs: 100,
						authenticated: 80,
						roc: null,
						drops: 20,
					}),
				),
				[
					{
						t: 'unparsed',
						raw: line({
							t: 'stats',
							inputs: 100,
							authenticated: 80,
							roc: null,
							drops: 20,
						}).trim(),
					},
				],
			)
		})

		void it('parses warnings and errors without a debug field', () => {
			assert.deepStrictEqual(
				parse(line({ t: 'error', element: 'kvssink', message: 'boom' })),
				[{ t: 'error', element: 'kvssink', message: 'boom' }],
			)
			assert.deepStrictEqual(parse(line({ t: 'warning', message: 'w' })), [
				{ t: 'warning', element: undefined, message: 'w' },
			])
		})

		void it('parses fatal and eos', () => {
			assert.deepStrictEqual(
				parse(line({ t: 'fatal', reason: 'bind-failed', message: 'nope' })),
				[{ t: 'fatal', reason: 'bind-failed', message: 'nope' }],
			)
			assert.deepStrictEqual(parse(line({ t: 'eos' })), [{ t: 'eos' }])
		})

		void it('classifies fatal reasons', () => {
			assert.equal(isRetryableFatal('bind-failed'), true)
			assert.equal(isRetryableFatal('state-change'), true)
			assert.equal(isRetryableFatal('bad-init'), false)
			assert.equal(isRetryableFatal('key-in-argv'), false)
			assert.equal(isRetryableFatal('unsafe-debug-env'), false)
			assert.equal(isRetryableFatal('missing-element'), false)
		})
	})

	void describe('rejections', () => {
		void it('rejects non-JSON, non-objects, unknown types and missing fields', () => {
			assert.deepStrictEqual(parse('not json\n'), [
				{ t: 'unparsed', raw: 'not json' },
			])
			assert.deepStrictEqual(parse(line([1, 2])), [
				{ t: 'unparsed', raw: line([1, 2]).trim() },
			])
			assert.deepStrictEqual(parse(line({ t: 'nope' })), [
				{ t: 'unparsed', raw: line({ t: 'nope' }).trim() },
			])
			assert.deepStrictEqual(parse(line({ t: 'ready', v: 3 })), [
				{ t: 'unparsed', raw: line({ t: 'ready', v: 3 }).trim() },
			])
			assert.deepStrictEqual(parse(line({ t: 'auth', status: 'ok' })), [
				{ t: 'unparsed', raw: line({ t: 'auth', status: 'ok' }).trim() },
			])
		})

		void it('truncates the raw text of an unparsed line', () => {
			const raw = 'x'.repeat(300)
			assert.deepStrictEqual(parse(`${raw}\n`), [
				{ t: 'unparsed', raw: raw.slice(0, 200) },
			])
		})

		void it('rejects an unknown auth status', () => {
			assert.deepStrictEqual(parse(line({ t: 'auth', status: 'maybe' })), [
				{ t: 'unparsed', raw: line({ t: 'auth', status: 'maybe' }).trim() },
			])
		})
	})

	void describe('framing', () => {
		void it('reassembles a message split across chunks', () => {
			const full = line({ t: 'eos' })
			const [message] = parse(full.slice(0, 5), full.slice(5))
			assert.deepStrictEqual(message, { t: 'eos' })
		})

		void it('handles several messages in one chunk', () => {
			const messages = parse(
				line({ t: 'searching', candidate: 1, trial: 0 }) +
					line({ t: 'searching', candidate: 2, trial: 1 }),
			)
			assert.strictEqual(messages.length, 2)
		})

		void it('skips blank lines', () => {
			assert.deepStrictEqual(parse('\n\n', line({ t: 'eos' })), [{ t: 'eos' }])
		})

		void it('discards an over-long line and resumes after it', () => {
			const overlong = 'y'.repeat(MAX_LINE_BYTES + 1)
			const messages = parse(
				`${overlong}\n` + line({ t: 'eos' }),
				line({ t: 'producing' }),
			)
			assert.deepStrictEqual(messages, [
				{
					t: 'unparsed',
					raw: `discarded an over-long line (${String(MAX_LINE_BYTES + 1)} bytes)`,
				},
				{ t: 'eos' },
				{ t: 'producing' },
			])
		})

		void it('cuts a line that never ends', () => {
			const protocol = new SrtpHelperProtocol()
			const first = protocol.push('z'.repeat(MAX_LINE_BYTES + 10))
			assert.strictEqual(first.length, 1)
			assert.ok(first[0]?.t === 'unparsed')
			// The tail of the discarded line is dropped, and the next complete line
			// parses.
			const second = protocol.push('z'.repeat(100) + '\n' + line({ t: 'eos' }))
			assert.deepStrictEqual(second, [{ t: 'eos' }])
		})

		void it('parses a line exactly at the limit', () => {
			const payload = { t: 'warning', message: 'x'.repeat(100) }
			const text = JSON.stringify(payload)
			assert.ok(text.length < MAX_LINE_BYTES)
			const padded = text.padEnd(MAX_LINE_BYTES - 1, ' ') + '\n'
			assert.deepStrictEqual(parse(padded), [
				{ ...payload, element: undefined },
			])
		})
	})
})
