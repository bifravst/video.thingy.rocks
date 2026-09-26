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
	 * Highest SRTP packet index ever accepted on this port - authenticated by libsrtp
	 * and above the previous value - under the key and SSRC below.
	 *
	 * It is the replay floor. libsrtp's replay window starts empty in every new helper,
	 * so without it a recording of earlier traffic authenticates again after any
	 * restart; the helper drops everything at or below this, and searches for the
	 * rollover counter upwards from it. It therefore only ever rises for a given key
	 * and SSRC - see raiseSrtpIndexFloor - and a value that was never authenticated
	 * must never be written here.
	 */
	srtpIndex?: number
	/** SSRC the floor was reached by; another sender's traffic has its own. */
	srtpIndexSsrc?: number
	/**
	 * Fingerprint of the key the floor was reached under; a new key starts afresh.
	 * Needed as well as the SSRC because provisioning may replace a key and keep the
	 * SSRC, and the SSRC alone cannot tell the new key's index space from the old one's.
	 */
	srtpIndexKeyFingerprint?: string
	createdAt: string
	updatedAt: string
}

export type StreamMetadataServiceConfig = {
	tableName: string
	region?: string
	/** For tests: where requests go instead of the regional endpoint. */
	endpoint?: string
	/** For tests: replaces DYNAMODB_REQUEST_TIMEOUT_MS; see there. */
	requestTimeoutMs?: number
}

/**
 * How long one DynamoDB request may take, and how long its socket may sit idle.
 *
 * Every call here is awaited inside a port's serialized event queue, so a request that
 * never completes holds that port - its lock, its shutdown - for as long as it does not.
 * The SDK sets no limit by default: its request timeout is 0, and even a configured
 * one only logs unless throwOnRequestTimeout is set. DynamoDB answers in milliseconds,
 * so this is only reached when something is wrong, and with the SDK's three attempts a
 * call gives up after roughly three times this.
 */
const DYNAMODB_REQUEST_TIMEOUT_MS = 3_000
const DYNAMODB_CONNECTION_TIMEOUT_MS = 2_000

const createDocumentClient = (
	config: StreamMetadataServiceConfig,
): DynamoDBDocumentClient => {
	const requestTimeout = config.requestTimeoutMs ?? DYNAMODB_REQUEST_TIMEOUT_MS
	return DynamoDBDocumentClient.from(
		new DynamoDBClient({
			region: config.region ?? 'eu-central-1',
			...(config.endpoint === undefined ? {} : { endpoint: config.endpoint }),
			requestHandler: {
				connectionTimeout: Math.min(
					DYNAMODB_CONNECTION_TIMEOUT_MS,
					requestTimeout,
				),
				// Until the response headers arrive...
				requestTimeout,
				throwOnRequestTimeout: true,
				// ...and after them, while the body is read.
				socketTimeout: requestTimeout,
			},
		}),
	)
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
		this.docClient = deps?.docClient ?? createDocumentClient(config)
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
	 * Reads the replay floor for a port, if one was reached under this key and SSRC.
	 *
	 * Strongly consistent, because a stale read is a lower floor, and a lower floor is
	 * a replay window. And it throws rather than returning undefined when the read
	 * fails: undefined means "nothing was ever accepted under this key", which admits
	 * everything, so the caller must not start on a floor it could not read. That costs
	 * nothing extra, since a start already needs DynamoDB to have acquired the lock.
	 */
	async getSrtpIndexFloor(
		port: number,
		expectedSsrc: number,
		expectedKeyFingerprint: string,
	): Promise<number | undefined> {
		const result = await this.docClient.send(
			new GetCommand({
				TableName: this.tableName,
				Key: { port },
				ConsistentRead: true,
			}),
		)
		const item = result.Item as StreamMetadata | undefined
		if (item === undefined) return undefined
		const { srtpIndex, srtpIndexSsrc, srtpIndexKeyFingerprint } = item
		if (typeof srtpIndex !== 'number' || !Number.isSafeInteger(srtpIndex)) {
			return undefined
		}
		if (srtpIndex < 0) return undefined
		// Another sender, or a key provisioned since: this floor describes a different
		// index space, and a new key is exactly what lets a device start from zero.
		if (srtpIndexSsrc !== expectedSsrc) return undefined
		if (srtpIndexKeyFingerprint !== expectedKeyFingerprint) return undefined
		return srtpIndex
	}

	/**
	 * Raises the replay floor for a port to an index authentication has accepted.
	 *
	 * Conditional, so that it only ever goes up: a write that would lower it - an
	 * instance reporting less than another already recorded, or two writes arriving out
	 * of order - fails the condition and is dropped.
	 *
	 * The one way down is a different key, which is a different index space - but
	 * only for a *newer* key: the write carries the provisioned key's generation
	 * (bumped by every provisioning run, see SrtpPortKey), and identity
	 * replacement requires a strictly larger one. A stale helper still running
	 * the previous key can therefore keep raising the floor of the row it wrote
	 * (harmless - the row is superseded the moment the new key's traffic
	 * arrives) but can never overwrite the new key's floor and so reopen the
	 * indexes the new key already accepted. Rows without a generation (written
	 * before the fence) count as the lowest and are replaceable by any.
	 *
	 * Best effort otherwise, and the honest cost is a bounded replay window, not
	 * a lost one: a failed write does not lose what the helper already accepted
	 * - its own maximum stands in-process - but after a restart the persisted
	 * floor can be behind it, and datagrams already accepted in that gap then
	 * authenticate again until the floor catches up. Production is never stopped
	 * over the write; see SrtpPortSupervisor.raiseFloor for the tradeoff.
	 */
	async raiseSrtpIndexFloor(
		port: number,
		index: number,
		ssrc: number,
		keyFingerprint: string,
		generation: number,
	): Promise<void> {
		try {
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression:
						'SET srtpIndex = :index, srtpIndexSsrc = :ssrc, srtpIndexKeyFingerprint = :fingerprint, srtpKeyGeneration = :generation, updatedAt = :now',
					ConditionExpression: [
						'attribute_not_exists(srtpIndex)',
						'attribute_not_exists(srtpKeyGeneration)',
						'srtpKeyGeneration < :generation',
						'(srtpKeyGeneration = :generation AND srtpIndexSsrc = :ssrc AND srtpIndexKeyFingerprint = :fingerprint AND srtpIndex < :index)',
					].join(' OR '),
					ExpressionAttributeValues: {
						':index': index,
						':ssrc': ssrc,
						':fingerprint': keyFingerprint,
						':generation': generation,
						':now': new Date().toISOString(),
					},
				}),
			)
		} catch (error) {
			if (isConditionalCheckFailed(error)) return
			console.warn(
				`[StreamMetadataService] Could not raise the SRTP replay floor for port ${port}:`,
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
