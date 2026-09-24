import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import assert from 'node:assert/strict'
import net from 'node:net'
import { after, before, describe, it } from 'node:test'

import { StreamMetadataService } from './StreamMetadataService.ts'

type SentCommand = {
	input: {
		Key?: Record<string, number>
		ConditionExpression?: string
		UpdateExpression?: string
		ExpressionAttributeValues?: Record<string, string>
	}
}

/** Minimal stand-in for the document client, recording what would have been sent. */
const fakeDocClient = (
	outcomes: ('ok' | 'conditional' | 'error')[] = [],
	item?: Record<string, unknown>,
): {
	client: DynamoDBDocumentClient
	sent: SentCommand[]
} => {
	const sent: SentCommand[] = []
	let call = 0
	const client = {
		send: async (command: SentCommand) => {
			sent.push(command)
			const outcome = outcomes[call++] ?? 'ok'
			if (outcome === 'conditional') {
				const err = new Error('The conditional request failed')
				err.name = 'ConditionalCheckFailedException'
				throw err
			}
			if (outcome === 'error') {
				const err = new Error('throughput exceeded')
				err.name = 'ProvisionedThroughputExceededException'
				throw err
			}
			return item === undefined ? {} : { Item: item }
		},
	} as unknown as DynamoDBDocumentClient
	return { client, sent }
}

const service = (
	outcomes?: ('ok' | 'conditional' | 'error')[],
	item?: Record<string, unknown>,
): {
	subject: StreamMetadataService
	sent: SentCommand[]
} => {
	const { client, sent } = fakeDocClient(outcomes, item)
	return {
		subject: new StreamMetadataService(
			{ tableName: 'StreamMetadata' },
			{ docClient: client },
		),
		sent,
	}
}

void describe('StreamMetadataService.updateLastPacketTime', () => {
	void it('refreshes the lease and reports ok', async () => {
		const { subject, sent } = service()
		assert.strictEqual(await subject.updateLastPacketTime(5000, 'i-1'), 'ok')
		assert.strictEqual(sent.length, 1)
		assert.deepStrictEqual(sent[0]?.input.Key, { port: 5000 })
		assert.strictEqual(
			sent[0]?.input.ConditionExpression,
			'kinesisOwnerInstanceId = :instanceId',
		)
		assert.strictEqual(
			sent[0]?.input.ExpressionAttributeValues?.[':instanceId'],
			'i-1',
		)
	})

	// lastPacketTime is the lock's lease, so it must carry the time the lease was
	// refreshed - not a packet's arrival time, which can lag arbitrarily behind.
	void it('writes the current time, and the same value to both fields', async () => {
		const before = new Date().toISOString()
		const { subject, sent } = service()
		await subject.updateLastPacketTime(5000, 'i-1')
		const after = new Date().toISOString()

		const values = sent[0]?.input.ExpressionAttributeValues ?? {}
		const written = values[':lastPacketTime'] ?? ''
		assert.strictEqual(values[':updatedAt'], written)
		assert.ok(written >= before && written <= after, written)
		assert.strictEqual(values[':status'], 'active')
	})

	void it('keys the lock row by the raw port', async () => {
		const { subject, sent } = service()
		await subject.updateLastPacketTime(6000, 'i-1')
		assert.deepStrictEqual(sent[0]?.input.Key, { port: 6000 })
	})

	void it('throttles to one write per 15 seconds per port', async () => {
		const { subject, sent } = service()
		assert.strictEqual(await subject.updateLastPacketTime(5000, 'i-1'), 'ok')
		assert.strictEqual(await subject.updateLastPacketTime(5000, 'i-1'), 'ok')
		assert.strictEqual(await subject.updateLastPacketTime(5000, 'i-1'), 'ok')
		assert.strictEqual(sent.length, 1)
	})

	void it('throttles each port independently', async () => {
		const { subject, sent } = service()
		await subject.updateLastPacketTime(5000, 'i-1')
		await subject.updateLastPacketTime(5001, 'i-1')
		assert.strictEqual(sent.length, 2)
	})

	void it('reports lostLock when the ownership condition fails', async () => {
		const { subject } = service(['conditional'])
		assert.strictEqual(
			await subject.updateLastPacketTime(5000, 'i-1'),
			'lostLock',
		)
	})

	void it('reports writeError for a transient failure', async () => {
		const { subject } = service(['error'])
		assert.strictEqual(
			await subject.updateLastPacketTime(5000, 'i-1'),
			'writeError',
		)
	})

	// A failed write must not be throttled out for another 15 seconds: the lease was
	// not refreshed, so the next packet has to be allowed to try again.
	void it('clears the throttle after a failure so the next call retries', async () => {
		const { subject, sent } = service(['error', 'ok'])
		assert.strictEqual(
			await subject.updateLastPacketTime(5000, 'i-1'),
			'writeError',
		)
		assert.strictEqual(await subject.updateLastPacketTime(5000, 'i-1'), 'ok')
		assert.strictEqual(sent.length, 2)
	})

	void it('clears the throttle after a lost lock too', async () => {
		const { subject, sent } = service(['conditional', 'ok'])
		assert.strictEqual(
			await subject.updateLastPacketTime(5000, 'i-1'),
			'lostLock',
		)
		assert.strictEqual(await subject.updateLastPacketTime(5000, 'i-1'), 'ok')
		assert.strictEqual(sent.length, 2)
	})
})

const FINGERPRINT = 'abc123def4567890'

void describe('StreamMetadataService SRTP rollover hint', () => {
	const stored = (overrides: Record<string, unknown> = {}) => ({
		port: 6000,
		srtpRocHint: 7,
		srtpRocSsrc: 42,
		srtpRocKeyFingerprint: FINGERPRINT,
		...overrides,
	})

	void it('round-trips a confirmed hint', async () => {
		const { subject, sent } = service(undefined, stored())
		assert.strictEqual(await subject.getSrtpRocHint(6000, 42, FINGERPRINT), 7)
		assert.deepStrictEqual(sent[0]?.input.Key, { port: 6000 })
	})

	// Zero is a real rollover counter, so "no idea" has to be a different value -
	// conflating them is what seeded fresh decoders incorrectly before.
	void it('returns undefined, not zero, when there is nothing usable', async () => {
		for (const item of [
			undefined,
			stored({ srtpRocHint: undefined }),
			stored({ srtpRocHint: 'seven' }),
			stored({ srtpRocHint: 1.5 }),
			stored({ srtpRocHint: -1 }),
		]) {
			const { subject } = service(undefined, item)
			assert.strictEqual(
				await subject.getSrtpRocHint(6000, 42, FINGERPRINT),
				undefined,
				JSON.stringify(item),
			)
		}
	})

	void it('returns a hint of zero when zero was confirmed', async () => {
		const { subject } = service(undefined, stored({ srtpRocHint: 0 }))
		assert.strictEqual(await subject.getSrtpRocHint(6000, 42, FINGERPRINT), 0)
	})

	// A different sender or a rotated key means the hint describes another session.
	void it('ignores a hint confirmed for another sender or key', async () => {
		const wrongSsrc = service(undefined, stored({ srtpRocSsrc: 43 }))
		assert.strictEqual(
			await wrongSsrc.subject.getSrtpRocHint(6000, 42, FINGERPRINT),
			undefined,
		)
		const rotated = service(
			undefined,
			stored({ srtpRocKeyFingerprint: 'other' }),
		)
		assert.strictEqual(
			await rotated.subject.getSrtpRocHint(6000, 42, FINGERPRINT),
			undefined,
		)
		const missing = service(
			undefined,
			stored({ srtpRocKeyFingerprint: undefined }),
		)
		assert.strictEqual(
			await missing.subject.getSrtpRocHint(6000, 42, FINGERPRINT),
			undefined,
		)
	})

	void it('treats a read failure as having no hint', async () => {
		const { subject } = service(['error'], stored())
		assert.strictEqual(
			await subject.getSrtpRocHint(6000, 42, FINGERPRINT),
			undefined,
		)
	})

	// No ownership condition, no throttle: losing this value costs a short search on
	// the next start and nothing else, and the machinery those guarantees needed was
	// itself a source of bugs.
	void it('writes the hint unconditionally and unthrottled', async () => {
		const { subject, sent } = service()
		await subject.putSrtpRocHint(6000, 9, 42, FINGERPRINT)
		await subject.putSrtpRocHint(6000, 10, 42, FINGERPRINT)

		assert.strictEqual(sent.length, 2)
		assert.strictEqual(sent[0]?.input.ConditionExpression, undefined)
		assert.deepStrictEqual(sent[0]?.input.Key, { port: 6000 })
		assert.strictEqual(
			sent[1]?.input.ExpressionAttributeValues?.[':roc'],
			10 as unknown as string,
		)
	})

	void it('records the identity the hint was confirmed under', async () => {
		const { subject, sent } = service()
		await subject.putSrtpRocHint(6000, 9, 42, FINGERPRINT)
		const values = sent[0]?.input.ExpressionAttributeValues ?? {}
		assert.strictEqual(values[':ssrc'], 42 as unknown as string)
		assert.strictEqual(values[':fingerprint'], FINGERPRINT)
	})

	void it('swallows a write failure, because the next start can search again', async () => {
		const { subject } = service(['error'])
		await subject.putSrtpRocHint(6000, 9, 42, FINGERPRINT)
	})
})

/**
 * No call can hang its port.
 *
 * Every method here is awaited inside a port's serialized event queue, so one request
 * that never completes holds that port - its lock, its shutdown - for as long as it
 * does not. The SDK sets no limit of its own, and this client used to be built with
 * its defaults: a request to an endpoint that never answered was still pending when
 * these tests gave up on it.
 *
 * Driven through the real client against a local endpoint that accepts connections
 * and then says nothing, or starts a response and stops. Every method on the class is
 * called, found by enumerating the prototype rather than listed here, so a method
 * added later is covered without anyone remembering to add it.
 */
void describe('StreamMetadataService against an endpoint that stops answering', () => {
	process.env.AWS_ACCESS_KEY_ID ??= 'AKIAEXAMPLEEXAMPLE00'
	process.env.AWS_SECRET_ACCESS_KEY ??= 'example-secret-for-tests-only'

	const REQUEST_TIMEOUT_MS = 200
	/** Three attempts, each bounded, plus the SDK's backoff between them. */
	const BOUND_MS = 3 * REQUEST_TIMEOUT_MS + 2_000

	const endpoints: {
		name: string
		respond: (socket: net.Socket) => void
	}[] = [
		{ name: 'never responds', respond: () => undefined },
		{
			name: 'starts a response and stalls',
			respond: (socket) => {
				socket.once('data', () => {
					socket.write(
						'HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: 100\r\n\r\n{',
					)
				})
			},
		},
	]

	for (const endpoint of endpoints) {
		void describe(endpoint.name, () => {
			const sockets = new Set<net.Socket>()
			let connections = 0
			let url = ''
			const server = net.createServer((socket) => {
				connections += 1
				sockets.add(socket)
				socket.on('error', () => undefined)
				endpoint.respond(socket)
			})
			before(async () => {
				await new Promise<void>((resolve) =>
					server.listen(0, '127.0.0.1', resolve),
				)
				const address = server.address() as net.AddressInfo
				url = `http://127.0.0.1:${String(address.port)}`
			})
			after(async () => {
				for (const socket of sockets) socket.destroy()
				await new Promise((resolve) => server.close(resolve))
			})

			const methods = Object.getOwnPropertyNames(
				StreamMetadataService.prototype,
			).filter(
				(name) =>
					name !== 'constructor' &&
					typeof (
						StreamMetadataService.prototype as unknown as Record<
							string,
							unknown
						>
					)[name] === 'function',
			)

			void it('finds the methods to call', () => {
				assert.ok(methods.includes('tryAcquireKinesisLock'), methods.join(', '))
			})

			for (const method of methods) {
				void it(
					`${method} settles`,
					{ timeout: BOUND_MS + 5_000 },
					async () => {
						const subject = new StreamMetadataService({
							tableName: 'StreamMetadata',
							region: 'eu-central-1',
							endpoint: url,
							requestTimeoutMs: REQUEST_TIMEOUT_MS,
						})
						const before = connections
						const call = (
							subject as unknown as Record<
								string,
								(...args: unknown[]) => Promise<unknown>
							>
						)[method]
						const started = Date.now()
						const outcome = await Promise.race([
							// Arguments of every shape the methods take; each one sends a
							// request whatever it makes of them.
							call!.call(subject, 6000, 'i-test', 42, 'abcdef0123456789').then(
								() => 'settled',
								() => 'settled',
							),
							new Promise((resolve) =>
								setTimeout(() => resolve('pending'), BOUND_MS),
							),
						])
						assert.strictEqual(
							outcome,
							'settled',
							`still pending after ${String(Date.now() - started)} ms`,
						)
						assert.ok(
							connections > before,
							'the call has to reach the endpoint, or this proves nothing',
						)
					},
				)
			}
		})
	}
})
