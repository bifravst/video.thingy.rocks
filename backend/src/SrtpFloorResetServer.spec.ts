import assert from 'node:assert/strict'
import { after, before, describe, it } from 'node:test'
import {
	MAX_CLOCK_SKEW_MS,
	signFloorReset,
	SrtpFloorResetServer,
} from './SrtpFloorResetServer.ts'
import type { SrtpPortKey } from './SrtpKeyStore.ts'

const KEY: SrtpPortKey = {
	keyHex: 'ab'.repeat(30),
	ssrc: 42,
	cipher: 'aes-128-icm',
	auth: 'hmac-sha1-80',
	keyFingerprint: '0123456789abcdef',
	generation: 1,
}

const NOW = 1_800_000_000_000

void describe('SrtpFloorResetServer', () => {
	const resets: { port: number; requestedAtMs: number; floor?: number }[] = []
	let outcome: 'reset' | 'stale' = 'reset'
	let url = ''
	const server = new SrtpFloorResetServer(
		{
			keyForPort: (port) => (port === 6000 ? KEY : undefined),
			resetFloor: async (port, reset) => {
				resets.push({ port, ...reset })
				return outcome
			},
		},
		undefined,
		() => NOW,
	)

	before(async () => {
		// Listens on `::`, and reached over 127.0.0.1 (IPv4-mapped): connecting to
		// ::1 hangs in this sandbox.
		const port = await server.start(0)
		url = `http://127.0.0.1:${port}`
	})
	after(async () => server.stop())

	const post = async (
		path: string,
		{
			timestamp = NOW,
			floor = 70000,
			signature,
			body,
		}: {
			timestamp?: number
			floor?: number
			signature?: string
			body?: string
		} = {},
	): Promise<Response> =>
		fetch(`${url}${path}`, {
			method: 'POST',
			headers: {
				'x-timestamp': String(timestamp),
				'x-signature':
					signature ?? signFloorReset(KEY.keyHex, 6000, timestamp, floor),
			},
			body: body ?? JSON.stringify({ floor }),
		})

	void it('resets to the given floor', async () => {
		resets.length = 0
		const res = await post('/srtp/6000/floor-reset', { floor: 70000 })
		assert.strictEqual(res.status, 200)
		assert.deepStrictEqual(resets, [
			{ port: 6000, requestedAtMs: NOW, floor: 70000 },
		])
	})

	void it('refuses a bad signature, an altered floor, and a wrong key alike', async () => {
		resets.length = 0
		for (const request of [
			post('/srtp/6000/floor-reset', { signature: '00'.repeat(32) }),
			// Signed for one floor, sent with another.
			post('/srtp/6000/floor-reset', {
				body: '{"floor":5}',
				signature: signFloorReset(KEY.keyHex, 6000, NOW, 70000),
			}),
			post('/srtp/6000/floor-reset', {
				signature: signFloorReset('cd'.repeat(30), 6000, NOW, 70000),
			}),
			// Signed for another port.
			post('/srtp/6000/floor-reset', {
				signature: signFloorReset(KEY.keyHex, 6001, NOW, 70000),
			}),
		]) {
			assert.strictEqual((await request).status, 401)
		}
		assert.strictEqual(resets.length, 0)
	})

	void it('does not tell an unkeyed port from a failed signature', async () => {
		const res = await post('/srtp/6003/floor-reset')
		assert.strictEqual(res.status, 401)
	})

	void it('refuses a timestamp outside the clock skew', async () => {
		resets.length = 0
		for (const timestamp of [
			NOW - MAX_CLOCK_SKEW_MS - 1,
			NOW + MAX_CLOCK_SKEW_MS + 1,
		]) {
			const res = await post('/srtp/6000/floor-reset', { timestamp })
			assert.strictEqual(res.status, 401)
		}
		assert.strictEqual(resets.length, 0)
	})

	void it('answers 409 to a request that was already used', async () => {
		outcome = 'stale'
		try {
			const res = await post('/srtp/6000/floor-reset')
			assert.strictEqual(res.status, 409)
		} finally {
			outcome = 'reset'
		}
	})

	void it('rejects a floor that is not a packet index', async () => {
		resets.length = 0
		for (const body of [
			'',
			'{}',
			'{"floor":null}',
			'{"floor":-1}',
			'{"floor":1.5}',
			'{"floor":"7"}',
			'{"floor":281474976710656}',
			'nope',
		]) {
			const res = await post('/srtp/6000/floor-reset', { body })
			assert.strictEqual(res.status, 400, body)
		}
		assert.strictEqual(resets.length, 0)
	})

	void it('serves nothing else', async () => {
		assert.strictEqual((await post('/other')).status, 404)
		assert.strictEqual(
			(await fetch(`${url}/srtp/6000/floor-reset`)).status,
			405,
		)
	})
})
