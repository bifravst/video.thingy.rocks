import {
	DynamoDBClient,
	ScanCommand,
	UpdateItemCommand,
} from '@aws-sdk/client-dynamodb'

const INACTIVITY_THRESHOLD_MS = 5 * 60 * 1000 // 5 minutes
const dynamo = new DynamoDBClient({})

/**
 * Sets streams to inactive when they are marked active but no frame was received
 * in the last 5 minutes. Frees the Kinesis lock so another instance can acquire.
 */
export const handler = async (): Promise<{
	cleaned: number
	total: number
}> => {
	const tableName = process.env.TABLE_NAME
	if (tableName === undefined || tableName === '') {
		throw new Error('TABLE_NAME environment variable is required')
	}

	const staleThreshold = new Date(
		Date.now() - INACTIVITY_THRESHOLD_MS,
	).toISOString()

	const scanResult = await dynamo.send(
		new ScanCommand({
			TableName: tableName,
			FilterExpression: '#status = :active',
			ExpressionAttributeNames: { '#status': 'status' },
			ExpressionAttributeValues: { ':active': { S: 'active' } },
		}),
	)

	const items = scanResult.Items ?? []
	let cleaned = 0

	for (const item of items) {
		const port = item.port?.N
		const lastPacketTime = item.lastPacketTime?.S
		if (
			port === undefined ||
			port === '' ||
			lastPacketTime === undefined ||
			lastPacketTime === ''
		) {
			continue
		}

		if (lastPacketTime >= staleThreshold) continue

		const now = new Date().toISOString()
		try {
			await dynamo.send(
				new UpdateItemCommand({
					TableName: tableName,
					Key: {
						port: { N: port },
					},
					UpdateExpression:
						'REMOVE kinesisOwnerInstanceId SET #status = :inactive, updatedAt = :now',
					ExpressionAttributeNames: { '#status': 'status' },
					ExpressionAttributeValues: {
						':inactive': { S: 'inactive' },
						':now': { S: now },
						':active': { S: 'active' },
						':staleThreshold': { S: staleThreshold },
					},
					ConditionExpression:
						'#status = :active AND lastPacketTime < :staleThreshold',
				}),
			)
			cleaned++
			console.log(
				`Cleaned stale stream for port ${port} (lastPacketTime: ${lastPacketTime})`,
			)
		} catch (err: unknown) {
			const error = err as { name?: string }
			if (error?.name === 'ConditionalCheckFailedException') {
				continue
			}
			throw err
		}
	}

	return { cleaned, total: items.length }
}
