import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

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
			return {}
		},
	} as unknown as DynamoDBDocumentClient
	return { client, sent }
}

const service = (
	outcomes?: ('ok' | 'conditional' | 'error')[],
): {
	subject: StreamMetadataService
	sent: SentCommand[]
} => {
	const { client, sent } = fakeDocClient(outcomes)
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
