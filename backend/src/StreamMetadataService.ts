import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import {
	DynamoDBDocumentClient,
	PutCommand,
	UpdateCommand,
} from '@aws-sdk/lib-dynamodb'

/** Stale lock threshold: if no packet received in this many ms, another instance may acquire. */
export const KINESIS_LOCK_STALE_MS = 5 * 60 * 1000 // 5 minutes

export type StreamMetadata = {
	port: number
	status: 'active' | 'inactive'
	lastPacketTime: string // ISO 8601
	/** Instance that holds the lock for sending to Kinesis. Only this instance may send video data. */
	kinesisOwnerInstanceId?: string
	lastFramePath?: string
	hlsManifestPath?: string
	rawStreamPath?: string
	createdAt: string
	updatedAt: string
}

export type StreamMetadataServiceConfig = {
	tableName: string
	region?: string
}

export class StreamMetadataService {
	private readonly docClient: DynamoDBDocumentClient
	private readonly tableName: string
	private readonly lastUpdateTimes: Map<number, number> = new Map()
	private readonly updateThrottleMs = 15_000 // 15 seconds

	constructor(config: StreamMetadataServiceConfig) {
		const client = new DynamoDBClient({
			region: config.region ?? 'eu-central-1',
		})
		this.docClient = DynamoDBDocumentClient.from(client)
		this.tableName = config.tableName
	}

	/**
	 * Tries to acquire the Kinesis ingestion lock for a port. Only the instance that holds
	 * the lock may send video data to Kinesis. Succeeds if no owner exists, this instance
	 * already owns it, or the lock is stale (no packet in KINESIS_LOCK_STALE_MS).
	 */
	async tryAcquireKinesisLock(
		port: number,
		instanceId: string,
	): Promise<boolean> {
		const now = new Date().toISOString()
		const staleThreshold = new Date(
			Date.now() - KINESIS_LOCK_STALE_MS,
		).toISOString()

		try {
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression:
						'SET kinesisOwnerInstanceId = :instanceId, lastPacketTime = :now, #status = :active, updatedAt = :now, createdAt = if_not_exists(createdAt, :now)',
					ExpressionAttributeNames: {
						'#status': 'status',
					},
					ExpressionAttributeValues: {
						':instanceId': instanceId,
						':now': now,
						':active': 'active',
						':staleThreshold': staleThreshold,
					},
					ConditionExpression:
						'attribute_not_exists(port) OR attribute_not_exists(kinesisOwnerInstanceId) OR kinesisOwnerInstanceId = :instanceId OR lastPacketTime < :staleThreshold',
				}),
			)
			console.log(
				`[StreamMetadataService] Acquired Kinesis lock for port ${port} (instance ${instanceId})`,
			)
			return true
		} catch (error: unknown) {
			if (
				error instanceof Error &&
				error.name === 'ConditionalCheckFailedException'
			) {
				return false
			}
			console.error(
				`[StreamMetadataService] Error acquiring Kinesis lock for port ${port}:`,
				error,
			)
			throw error
		}
	}

	/**
	 * Releases the Kinesis lock for a port. Only the current owner can release.
	 */
	async releaseKinesisLock(port: number, instanceId: string): Promise<void> {
		const now = new Date().toISOString()

		try {
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression:
						'REMOVE kinesisOwnerInstanceId SET #status = :inactive, updatedAt = :now',
					ExpressionAttributeNames: {
						'#status': 'status',
					},
					ExpressionAttributeValues: {
						':instanceId': instanceId,
						':inactive': 'inactive',
						':now': now,
					},
					ConditionExpression: 'kinesisOwnerInstanceId = :instanceId',
				}),
			)
			console.log(
				`[StreamMetadataService] Released Kinesis lock for port ${port} (instance ${instanceId})`,
			)
		} catch (error: unknown) {
			if (
				error instanceof Error &&
				error.name === 'ConditionalCheckFailedException'
			) {
				// Lock was already released or taken by another instance
				return
			}
			console.error(
				`[StreamMetadataService] Error releasing Kinesis lock for port ${port}:`,
				error,
			)
			throw error
		}
	}

	async updateStreamStatus(
		port: number,
		status: 'active' | 'inactive',
	): Promise<void> {
		const now = new Date().toISOString()

		try {
			// Try to update existing item
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression: 'SET #status = :status, updatedAt = :updatedAt',
					ExpressionAttributeNames: {
						'#status': 'status',
					},
					ExpressionAttributeValues: {
						':status': status,
						':updatedAt': now,
					},
					ConditionExpression: 'attribute_exists(port)',
				}),
			)
			console.log(
				`[StreamMetadataService] Updated stream status for port ${port} to ${status}`,
			)
		} catch (error: unknown) {
			// If item doesn't exist, create it
			if (
				error instanceof Error &&
				error.name === 'ConditionalCheckFailedException'
			) {
				await this.createStreamMetadata(port, status)
			} else {
				console.error(
					`[StreamMetadataService] Error updating stream status for port ${port}:`,
					error,
				)
				throw error
			}
		}
	}

	/**
	 * Updates lastPacketTime for a port. Only succeeds if this instance holds the Kinesis lock,
	 * ensuring only the designated sender refreshes the heartbeat.
	 */
	async updateLastPacketTime(
		port: number,
		timestamp: Date,
		instanceId: string,
	): Promise<void> {
		// Throttle updates to max once per 15 seconds
		const now = Date.now()
		const lastUpdate = this.lastUpdateTimes.get(port) ?? 0
		const timeSinceLastUpdate = now - lastUpdate

		if (timeSinceLastUpdate < this.updateThrottleMs) {
			return
		}

		this.lastUpdateTimes.set(port, now)

		const isoTimestamp = timestamp.toISOString()
		const isoNow = new Date().toISOString()

		try {
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression:
						'SET #status = :status, lastPacketTime = :lastPacketTime, updatedAt = :updatedAt',
					ExpressionAttributeNames: {
						'#status': 'status',
					},
					ExpressionAttributeValues: {
						':status': 'active',
						':lastPacketTime': isoTimestamp,
						':updatedAt': isoNow,
						':instanceId': instanceId,
					},
					ConditionExpression: 'kinesisOwnerInstanceId = :instanceId',
				}),
			)
		} catch (error) {
			console.error(
				`[StreamMetadataService] Error updating last packet time for port ${port}:`,
				error,
			)
			this.lastUpdateTimes.delete(port)
			throw error
		}
	}

	private async createStreamMetadata(
		port: number,
		status: 'active' | 'inactive',
	): Promise<void> {
		const now = new Date().toISOString()

		const metadata: StreamMetadata = {
			port,
			status,
			lastPacketTime: now,
			createdAt: now,
			updatedAt: now,
		}

		try {
			await this.docClient.send(
				new PutCommand({
					TableName: this.tableName,
					Item: metadata,
				}),
			)
			console.log(
				`[StreamMetadataService] Created stream metadata for port ${port}`,
			)
		} catch (error) {
			console.error(
				`[StreamMetadataService] Error creating stream metadata for port ${port}:`,
				error,
			)
			throw error
		}
	}
}
