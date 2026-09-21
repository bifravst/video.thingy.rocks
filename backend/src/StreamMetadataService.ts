import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import {
	DynamoDBDocumentClient,
	GetCommand,
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
	/**
	 * Last SRTP rollover counter that was *confirmed by authentication* for this port.
	 *
	 * A hint, deliberately: the pipeline seeds it as its first candidate and libsrtp
	 * decides whether it was right, so a stale, absent or plainly wrong value costs a
	 * few datagrams while the search converges and is then overwritten. Nothing about
	 * correctness depends on it, which is why writing it needs no ownership condition,
	 * no throttle and no fencing - and why a value that was never authenticated must
	 * never be written here.
	 */
	srtpRocHint?: number
	/** SSRC the hint was confirmed for; a different sender starts the search over. */
	srtpRocSsrc?: number
	/** Fingerprint of the key the hint was confirmed under; a rotation invalidates it. */
	srtpRocKeyFingerprint?: string
	createdAt: string
	updatedAt: string
}

export type StreamMetadataServiceConfig = {
	tableName: string
	region?: string
}

/**
 * Outcome of a write that is conditional on still owning the port's lock.
 *
 * `lostLock` and `writeError` are distinct because they demand opposite responses:
 * losing the lock means another instance owns the stream and this one must stop
 * producing immediately, whereas a transient DynamoDB error means nothing about
 * ownership - the lease is still held until it goes stale, so tearing down and
 * retrying on every failed write would churn the pipeline for no reason.
 */
export type OwnedWriteResult = 'ok' | 'lostLock' | 'writeError'

const isConditionalCheckFailed = (err: unknown): boolean =>
	err instanceof Error && err.name === 'ConditionalCheckFailedException'

export class StreamMetadataService {
	private readonly docClient: DynamoDBDocumentClient
	private readonly tableName: string
	private readonly lastUpdateTimes: Map<number, number> = new Map()
	private readonly updateThrottleMs = 15_000 // 15 seconds

	/**
	 * `deps.docClient` is injectable so this service can be tested without AWS; production
	 * callers omit it and get a real client for the configured region.
	 */
	constructor(
		config: StreamMetadataServiceConfig,
		deps?: { docClient?: DynamoDBDocumentClient },
	) {
		this.docClient =
			deps?.docClient ??
			DynamoDBDocumentClient.from(
				new DynamoDBClient({ region: config.region ?? 'eu-central-1' }),
			)
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
	/**
	 * Refreshes the port's lock lease, throttled to once per 15 seconds.
	 *
	 * Takes no timestamp on purpose. `lastPacketTime` is both the record of activity and
	 * the lock's lease (tryAcquireKinesisLock treats a row older than
	 * KINESIS_LOCK_STALE_MS as available), so it must carry the time the lease was
	 * refreshed. Writing a packet's arrival time instead would let a lease expire while
	 * the owner is still receiving traffic, whenever processing runs behind arrival -
	 * another instance could then take the slot and both would produce at once. Arrival
	 * times belong to stream-activity tracking, not here.
	 *
	 * Returns an outcome rather than throwing so the caller can distinguish losing the
	 * lock (stop producing) from a transient write failure (keep going; the lease has not
	 * expired yet). A failed write clears the throttle so the next packet retries
	 * immediately instead of waiting out the window.
	 */
	async updateLastPacketTime(
		port: number,
		instanceId: string,
	): Promise<OwnedWriteResult> {
		const now = Date.now()
		const lastUpdate = this.lastUpdateTimes.get(port) ?? 0
		if (now - lastUpdate < this.updateThrottleMs) {
			return 'ok'
		}

		this.lastUpdateTimes.set(port, now)
		const isoNow = new Date(now).toISOString()

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
						':lastPacketTime': isoNow,
						':updatedAt': isoNow,
						':instanceId': instanceId,
					},
					ConditionExpression: 'kinesisOwnerInstanceId = :instanceId',
				}),
			)
			return 'ok'
		} catch (error) {
			this.lastUpdateTimes.delete(port)
			if (isConditionalCheckFailed(error)) {
				console.warn(
					`[StreamMetadataService] Lost the Kinesis lock for port ${port}; another instance owns it`,
				)
				return 'lostLock'
			}
			console.error(
				`[StreamMetadataService] Error refreshing the lock lease for port ${port}:`,
				error,
			)
			return 'writeError'
		}
	}

	/**
	 * Reads the rollover-counter hint for a port, if one matches this sender.
	 *
	 * Not a consistent read. A stale value is corrected by the search within a few
	 * datagrams, so asking for consistency would pay for a guarantee the value does
	 * not need - and would imply the hint is authoritative, which it is not.
	 *
	 * Returns undefined rather than zero when there is nothing usable: zero is a real
	 * rollover counter, and conflating "no idea" with "the beginning of the session"
	 * is what made the previous implementation seed fresh decoders incorrectly.
	 */
	async getSrtpRocHint(
		port: number,
		expectedSsrc: number,
		expectedKeyFingerprint: string,
	): Promise<number | undefined> {
		try {
			const result = await this.docClient.send(
				new GetCommand({ TableName: this.tableName, Key: { port } }),
			)
			const item = result.Item as StreamMetadata | undefined
			if (item === undefined) return undefined
			const { srtpRocHint, srtpRocSsrc, srtpRocKeyFingerprint } = item
			if (typeof srtpRocHint !== 'number' || !Number.isInteger(srtpRocHint)) {
				return undefined
			}
			if (srtpRocHint < 0) return undefined
			// A different sender or a rotated key means the hint describes another
			// session. Starting the search at zero is then strictly better than
			// starting it somewhere unrelated.
			if (srtpRocSsrc !== expectedSsrc) return undefined
			if (srtpRocKeyFingerprint !== expectedKeyFingerprint) return undefined
			return srtpRocHint
		} catch (error) {
			console.error(
				`[StreamMetadataService] Could not read the SRTP rollover hint for port ${port}:`,
				error,
			)
			return undefined
		}
	}

	/**
	 * Stores a rollover counter that authentication has confirmed.
	 *
	 * Best effort on purpose. There is no ownership condition, no throttle and no
	 * forced final write, because losing this value costs a short search on the next
	 * start and nothing else - whereas the machinery those guarantees needed was
	 * itself a source of bugs. Callers write only on a change, which is once at
	 * confirmation and once per rollover.
	 */
	async putSrtpRocHint(
		port: number,
		roc: number,
		ssrc: number,
		keyFingerprint: string,
	): Promise<void> {
		try {
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression:
						'SET srtpRocHint = :roc, srtpRocSsrc = :ssrc, srtpRocKeyFingerprint = :fingerprint, updatedAt = :now',
					ExpressionAttributeValues: {
						':roc': roc,
						':ssrc': ssrc,
						':fingerprint': keyFingerprint,
						':now': new Date().toISOString(),
					},
				}),
			)
		} catch (error) {
			console.warn(
				`[StreamMetadataService] Could not store the SRTP rollover hint for port ${port}; the next start will search for it:`,
				error,
			)
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
