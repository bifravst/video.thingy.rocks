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
	/**
	 * Canonical stream-slot number (see KinesisIngestionPipeline.streamSlotForPort), NOT a
	 * raw UDP port - callers must key every method below by that, not by port, so the
	 * unencrypted and SRTP transports for the same device share one lock/heartbeat row.
	 */
	port: number
	status: 'active' | 'inactive'
	lastPacketTime: string // ISO 8601
	/** Instance that holds the lock for sending to Kinesis. Only this instance may send video data. */
	kinesisOwnerInstanceId?: string
	lastFramePath?: string
	hlsManifestPath?: string
	rawStreamPath?: string
	/** Best-known SRTP rollover counter for this slot - see
	 * KinesisIngestionPipeline.getSrtpRoc/seedSrtpRoc. Only meaningful when the device uses
	 * the SRTP transport; absent/0 otherwise. */
	srtpRoc?: number
	/** The highest RTP sequence number observed under srtpRoc (see
	 * KinesisIngestionPipeline.SrtpRocState) - persisted alongside srtpRoc because restoring
	 * only the ROC and inventing a highestSeq of 0 misclassifies the next real packet
	 * whenever the sender's actual sequence number at restart time isn't near 0 (see
	 * getSrtpRocState). */
	srtpHighestSeq?: number
	/** The SSRC srtpRoc/srtpHighestSeq were recorded under. A key/device change gives the
	 * port a new SSRC, at which point persisted state from the *previous* session is
	 * meaningless (that session's sequence numbers have nothing to do with a fresh one
	 * starting near 0) and must not be used to seed it - see getSrtpRocState, which checks
	 * this before trusting srtpRoc/srtpHighestSeq. */
	srtpRocSsrc?: number
	/** Non-secret fingerprint of the SRTP key srtpRoc/srtpHighestSeq were recorded under (see
	 * KinesisIngestionPipeline.getConfiguredSrtpKeyFingerprint) - a second identity check
	 * alongside srtpRocSsrc, since a key can be rotated in SSM while the device keeps the
	 * same SSRC (the provisioning script permits this), which srtpRocSsrc alone would not
	 * catch, letting stale state from the retired key seed a session using the new one. */
	srtpRocKeyFingerprint?: string | null
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
	private readonly lastSrtpRocUpdateTimes: Map<number, number> = new Map()
	private readonly srtpRocUpdateThrottleMs = 15_000 // 15 seconds

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

	/**
	 * Reads the persisted SRTP rollover-tracking state for a stream slot: `{roc: 0,
	 * highestSeq: 0}` if never persisted, or if it was persisted under a different SSRC or
	 * key fingerprint (see srtpRocSsrc/srtpRocKeyFingerprint on StreamMetadata - either
	 * changing means state from a *previous* session/key must never seed a fresh one), or
	 * `undefined` if the read itself failed. That `undefined` case is NOT the same as a
	 * genuinely fresh session and must not be treated as one - a DynamoDB outage happening to
	 * land right as a sender has wrapped would otherwise seed a fresh srtpdec with a
	 * fabricated zero and silently break decryption; callers must treat `undefined` as
	 * "state unavailable" (see index.ts's startPipelineOrReleaseLock, which releases the lock
	 * and backs off rather than starting with a guessed value).
	 *
	 * Called once before starting an SRTP port's pipeline, to seed srtpdec correctly across
	 * process restarts (see KinesisIngestionPipeline.seedSrtpRoc) - a fresh GStreamer instance
	 * always starts at ROC 0 otherwise, which breaks decryption after any sequence number
	 * rollover in the sender's session. Restoring only the ROC (inventing highestSeq: 0) is
	 * itself unsafe - see seedSrtpRoc - so both fields are persisted and restored together.
	 * Uses a strongly consistent read: this runs right before seeding a fresh pipeline after a
	 * lock handoff, so an eventually-consistent read could still return the *previous*
	 * owner's now-stale value even after it persisted a newer one.
	 *
	 * Known limitation: matching SSRC and key fingerprint proves the same *key* is in use,
	 * not the same cryptographic *session* - a device that reboots and restarts its own RTP
	 * sequence/ROC at zero while keeping the same provisioned key and SSRC looks identical to
	 * a resumed session from here, so a high persisted ROC would incorrectly seed the fresh
	 * one and srtpdec would reject it indefinitely. Resolving this needs either a
	 * protocol-level session identifier the device sends (there isn't one in this static-key
	 * design) or a heuristic risky enough (e.g. "assume reset if the first live sequence
	 * looks too low") to introduce its own false positives; deliberately not attempted here.
	 * If this happens, the fix today is operational: clear srtpRoc/srtpHighestSeq/
	 * srtpRocSsrc/srtpRocKeyFingerprint for the affected slot's DynamoDB item.
	 */
	async getSrtpRocState(
		port: number,
		expectedSsrc: number,
		expectedKeyFingerprint: string | undefined,
	): Promise<{ roc: number; highestSeq: number } | undefined> {
		try {
			const result = await this.docClient.send(
				new GetCommand({
					TableName: this.tableName,
					Key: { port },
					ConsistentRead: true,
				}),
			)
			const roc: unknown = result.Item?.srtpRoc
			const highestSeq: unknown = result.Item?.srtpHighestSeq
			const storedSsrc: unknown = result.Item?.srtpRocSsrc
			const storedKeyFingerprint: unknown = result.Item?.srtpRocKeyFingerprint
			if (
				typeof roc !== 'number' ||
				typeof highestSeq !== 'number' ||
				storedSsrc !== expectedSsrc ||
				// Normalize both sides the same way updateSrtpRoc stores an absent
				// fingerprint (as null, since DynamoDB has no "undefined") - comparing the
				// raw values would treat null (stored) vs undefined (expected) as a mismatch
				// even when neither side ever had a fingerprint to begin with.
				storedKeyFingerprint !== (expectedKeyFingerprint ?? null)
			) {
				return { roc: 0, highestSeq: 0 }
			}
			return { roc, highestSeq }
		} catch (error) {
			console.error(
				`[StreamMetadataService] Error reading SRTP ROC state for port ${port}:`,
				error,
			)
			return undefined
		}
	}

	/**
	 * Persists the current SRTP rollover-tracking state (roc and highestSeq, tagged with the
	 * SSRC and key fingerprint they were observed under - see srtpRocSsrc/
	 * srtpRocKeyFingerprint) for a stream slot, throttled to once per srtpRocUpdateThrottleMs
	 * unless `force` is set (e.g. on stream stop, to capture the latest value before the slot
	 * potentially gets reassigned). Only succeeds if `instanceId` currently holds the Kinesis
	 * lock for this slot - without that check, a process whose heartbeat condition already
	 * failed (or that already released the slot) could overwrite the current owner's state
	 * with stale data and break a later restart. Returns false (without throwing) if the
	 * caller no longer owns the slot, or if the write otherwise fails - callers must treat
	 * that as having lost the lock and stop writing/relinquish their local pipeline, not just
	 * log and continue.
	 */
	async updateSrtpRoc(
		port: number,
		instanceId: string,
		roc: number,
		highestSeq: number,
		ssrc: number,
		keyFingerprint: string | undefined,
		force = false,
	): Promise<boolean> {
		const now = Date.now()
		const lastUpdate = this.lastSrtpRocUpdateTimes.get(port) ?? 0
		if (!force && now - lastUpdate < this.srtpRocUpdateThrottleMs) return true
		this.lastSrtpRocUpdateTimes.set(port, now)

		try {
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression:
						'SET srtpRoc = :roc, srtpHighestSeq = :highestSeq, srtpRocSsrc = :ssrc, srtpRocKeyFingerprint = :keyFingerprint, updatedAt = :updatedAt',
					ExpressionAttributeValues: {
						':roc': roc,
						':highestSeq': highestSeq,
						':ssrc': ssrc,
						':keyFingerprint': keyFingerprint ?? null,
						':updatedAt': new Date().toISOString(),
						':instanceId': instanceId,
					},
					ConditionExpression: 'kinesisOwnerInstanceId = :instanceId',
				}),
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
				`[StreamMetadataService] Error updating SRTP ROC for port ${port}:`,
				error,
			)
			this.lastSrtpRocUpdateTimes.delete(port)
			return false
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
