import { fromNodeProviderChain } from '@aws-sdk/credential-providers'
import { HealthServer } from './HealthServer.ts'
import { resolveInstanceId } from './InstanceId.ts'
import {
	KinesisIngestionPipeline,
	parseAuthenticRtpSequenceNumber,
} from './KinesisIngestionPipeline.ts'
import { SrtpKeyStore } from './SrtpKeyStore.ts'
import { StreamMetadataService } from './StreamMetadataService.ts'
import { StreamStateManager } from './StreamStateManager.ts'
import { UDPListener, type PacketHandler } from './UDPListener.ts'

/** Parses a boolean feature-flag env var: only "true"/"1" enable it. Boolean(str) would
 * treat any non-empty string - including the literal "false" or "0" - as enabled. */
const isEnvFlagEnabled = (value: string | undefined): boolean =>
	value === 'true' || value === '1'

const ensureAwsCredentials = async (): Promise<void> => {
	const credentialProvider = fromNodeProviderChain({
		timeout: 10_000,
		maxRetries: 2,
	})
	try {
		await credentialProvider()
	} catch (err) {
		const msg = err instanceof Error ? err.message : String(err)
		console.error(
			'[Main] AWS credentials could not be loaded. The service needs credentials for DynamoDB (and for Kinesis if enabled).',
		)
		console.error(
			'[Main] Locally: set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_REGION (or use AWS_PROFILE).',
		)
		console.error(
			'[Main] On EC2: ensure the instance has an IAM role and IMDS is not disabled (AWS_EC2_METADATA_DISABLED must be unset or false).',
		)
		console.error('[Main] Error:', msg)
		throw err
	}
}

/**
 * Main entry point for the UDP video ingestion service.
 *
 * This service:
 * - Listens for UDP packets on ports 5000-5009 (unencrypted MPEG-TS/H.264), and optionally
 *   6000-6009 (SRTP-encrypted RTP/H.264, static pre-shared key)
 * - Tracks stream state (active/inactive)
 * - Updates DynamoDB with stream metadata
 * - Optionally sends both to Kinesis Video Streams via kvssink (GStreamer)
 */

const KINESIS_MIN_BYTES_BEFORE_START =
	Number(process.env.KINESIS_MIN_BYTES_BEFORE_START ?? 10) * 1024 * 1024 // 10 MB default

// Configuration
const config = {
	portRange: { start: 5000, end: 5009 },
	bufferSize: 1024 * 1024, // 1MB
	flushInterval: 5000, // 5 seconds
	outputDirectory: process.env.OUTPUT_DIR ?? '/tmp/video-streams',
	transcodingOutputDirectory:
		process.env.TRANSCODING_OUTPUT_DIR ?? '/tmp/video-streams/transcoding',
	inactivityTimeout: 60000, // 1 minute
	dynamoDBTableName: process.env.TABLE_NAME ?? 'StreamMetadata',
	awsRegion: process.env.AWS_REGION ?? 'eu-central-1',
	segmentDuration: 6, // 6 seconds for HLS segments
	kinesisIngestionEnabled: isEnvFlagEnabled(
		process.env.KINESIS_INGESTION_ENABLED,
	),
	kinesisLogGstreamerOutput: isEnvFlagEnabled(
		process.env.KINESIS_INGESTION_LOG_GSTREAMER,
	),
	kinesisMinBytesBeforeStart: KINESIS_MIN_BYTES_BEFORE_START,
	srtpPortRange: {
		start: Number(process.env.SRTP_PORT_RANGE_START ?? 6000),
		end: Number(process.env.SRTP_PORT_RANGE_END ?? 6009),
	},
	srtpKeyParameterPrefix: process.env.SRTP_KEY_PARAMETER_PREFIX ?? '',
	srtpIngestionEnabled: isEnvFlagEnabled(process.env.SRTP_INGESTION_ENABLED),
}

const streamStateManager = new StreamStateManager({
	inactivityTimeout: config.inactivityTimeout,
})

const streamMetadataService = new StreamMetadataService({
	tableName: config.dynamoDBTableName,
	region: config.awsRegion,
})

const srtpEnabled =
	config.kinesisIngestionEnabled && config.srtpIngestionEnabled

const srtpKeyStore = srtpEnabled
	? new SrtpKeyStore({
			region: config.awsRegion,
			parameterPrefix: config.srtpKeyParameterPrefix,
		})
	: null

const kinesisIngestionPipeline = config.kinesisIngestionEnabled
	? new KinesisIngestionPipeline({
			region: config.awsRegion,
			portRange: config.portRange,
			logGstreamerOutput: config.kinesisLogGstreamerOutput,
			srtp:
				srtpEnabled && srtpKeyStore
					? {
							portRange: config.srtpPortRange,
							keyStore: srtpKeyStore,
						}
					: undefined,
		})
	: null

/** Ports for which this instance holds the Kinesis lock (only holder may send to Kinesis). */
const kinesisLockHeldForPorts = new Set<number>()

/**
 * Serializes "try to acquire and start ingestion for this stream slot" by slot number, so
 * the unencrypted and SRTP ports for the same device - handled by two independently
 * draining per-range packet queues, see createPacketHandler - can never both race into
 * tryAcquireKinesisLock concurrently. Without this, if both ports crossed their
 * pre-start-buffer threshold at close enough wall-clock times, both could see no local
 * holder yet, and DynamoDB's own-instance-id clause would let both acquisitions "succeed"
 * (it has no way to know two different local callers are asking) - creating two producers
 * for one Kinesis stream. This is a one-time-per-stream-start operation (not per-packet),
 * so an unbounded promise-chain mutex is fine here (no risk of the unbounded-queue problem
 * a per-packet version would have).
 */
const slotAcquisitionMutexTail = new Map<number, Promise<unknown>>()

const withSlotLock = async <T>(
	slot: number,
	fn: () => Promise<T>,
): Promise<T> => {
	const previous = slotAcquisitionMutexTail.get(slot) ?? Promise.resolve()
	const result = previous.then(fn, fn)
	slotAcquisitionMutexTail.set(
		slot,
		result.catch(() => {}),
	)
	return result
}

/**
 * Per-port ownership epoch, bumped every time this instance stops being the local owner of a
 * port (see clearLockHeld) or becomes one (see setLockHeld). startPipelineOrReleaseLock
 * captures the current value synchronously on entry and rechecks it after every await -
 * without this, a still-running (but now-stale) invocation that started before ownership
 * changed hands (lost via relinquishPort/onStreamStop, or re-acquired later as a new
 * ownership session) could resume after its await and register a pipeline or touch
 * lock/backoff state that no longer belongs to it.
 */
const portGeneration = new Map<number, number>()

const bumpPortGeneration = (port: number): void => {
	portGeneration.set(port, (portGeneration.get(port) ?? 0) + 1)
}

/** Marks this instance as the local owner of a port's lock and starts a new ownership epoch. */
const setLockHeld = (port: number): void => {
	kinesisLockHeldForPorts.add(port)
	bumpPortGeneration(port)
}

/** Clears local ownership of a port's lock and ends its ownership epoch. */
const clearLockHeld = (port: number): void => {
	kinesisLockHeldForPorts.delete(port)
	bumpPortGeneration(port)
}

/**
 * Stops the local pipeline and clears our in-process lock bookkeeping for a port, without
 * touching DynamoDB - used when a conditional write (updateSrtpRoc) reports we're no longer
 * the slot's owner there, so there is nothing left for us to release; continuing to run
 * (and write to) a pipeline we've lost the lock for is exactly the competing-producer
 * problem the lock exists to prevent.
 *
 * Stops the pipeline *before* clearing local ownership, not after - clearing first would let
 * the paired transport's cheap pairedPortHoldsLock check pass and start racing this port's
 * still-running producer immediately (this instance no longer owns the DynamoDB row either,
 * by construction, so there's nothing stopping a genuine two-producer window). Also applies
 * the standard start backoff and re-arms an empty pre-start buffer afterward if the stream
 * is still active, so a later packet can retry lock acquisition - without the backoff, a
 * *transient* write failure (updateSrtpRoc/updateLastPacketTime return false for both a
 * genuinely lost conditional lock and an ordinary DynamoDB error - see their own doc
 * comments) would otherwise repeat this buffer/start/relinquish cycle (and the GStreamer
 * spawn/kill churn that goes with it) at roughly one attempt per 10MB for as long as the
 * outage lasts; without the re-arm at all, the stream would be stranded until a full
 * inactivity/resume cycle happens on its own.
 */
const relinquishPort = async (port: number): Promise<void> => {
	console.warn(
		`[Main] Lost Kinesis lock for port ${port}; stopping local pipeline`,
	)
	if (kinesisIngestionPipeline) {
		await kinesisIngestionPipeline.stop(port)
	}
	clearLockHeld(port)
	nextStartAttemptAllowedAtByPort.set(port, Date.now() + START_RETRY_BACKOFF_MS)
	if (
		streamStateManager.getStreamState(port)?.status === 'active' &&
		!preStartBufferByPort.has(port)
	) {
		preStartBufferByPort.set(port, { chunks: [], totalBytes: 0 })
	}
}

/**
 * Per-port buffer of packets before we start GStreamer. We wait until at least
 * kinesisMinBytesBeforeStart (10 MB) to avoid treating port scans as video streams.
 */
const preStartBufferByPort = new Map<
	number,
	{ chunks: Buffer[]; totalBytes: number }
>()

/**
 * Minimum time between Kinesis start attempts for the same port after a failure. Without
 * this, a persistent failure (missing SRTP key, bad credentials, missing plugin) would
 * retry on literally the next packet after being re-armed - re-acquiring the lock, failing
 * again, releasing it, and re-arming again - hammering DynamoDB and logs at line rate.
 */
const START_RETRY_BACKOFF_MS = 30_000
const nextStartAttemptAllowedAtByPort = new Map<number, number>()

/** Resolved at startup; used by packet handler for lock acquisition and DynamoDB updates. */
let instanceId = 'local'

/**
 * True once shutdown() has begun - no new pipeline start/restart may be initiated after
 * this point, so a pending 'pipelineExited' restart timer or a fire-and-forget streamStart
 * resume can't spawn a GStreamer producer that teardown (or process.exit(), which does not
 * signal children - see KinesisIngestionPipeline.shutdown()'s orphan sweep) never stops.
 */
let shuttingDown = false

type PacketHandlerRangeConfig = {
	minBytesBeforeStart: number
	/**
	 * SRTP decryption/depayloading is inherently per-datagram (sequence numbers, rollover
	 * counters, auth tags) - the pre-start buffer must be replayed as individual datagrams,
	 * never Buffer.concat'd into one blob (that would destroy the framing SRTP needs).
	 */
	isSrtp: boolean
}

/**
 * Shared tail for startPipelineOrReleaseLock's failure paths (a failed start, or a failed
 * read of persisted SRTP ROC state): releases the lock, backs off before the next attempt,
 * and re-arms the pre-start buffer so a later packet can retry. Without the re-arm, a
 * failure here would strand the port until a full inactivity/resume cycle happens on its
 * own, since none of isFirstPacket/isResume/preStartBufferByPort.has(port) would hold once
 * the stream is already 'active' with no buffer entry.
 *
 * Releases the DynamoDB lock *before* clearing local ownership (kinesisLockHeldForPorts),
 * not after - this can run outside withSlotLock (e.g. from the 'pipelineExited' restart
 * callback), so clearing local ownership first would let the paired transport's cheap
 * pairedPortHoldsLock check pass and race tryAcquireKinesisLock while the DynamoDB row still
 * lists this instance as owner. Since tryAcquireKinesisLock's own-instance-id clause accepts
 * that, the paired port could acquire and start a pipeline moments before this release call
 * deletes kinesisOwnerInstanceId out from under it, leaving that new pipeline unowned.
 */
const releaseLockBackoffAndRearm = async (
	port: number,
	slot: number,
	initialData: Buffer | Buffer[] | undefined,
): Promise<void> => {
	try {
		await streamMetadataService.releaseKinesisLock(slot, instanceId)
	} catch (err) {
		console.error(`[Main] Error releasing Kinesis lock for port ${port}:`, err)
	}
	clearLockHeld(port)

	nextStartAttemptAllowedAtByPort.set(port, Date.now() + START_RETRY_BACKOFF_MS)

	const chunks =
		initialData === undefined
			? []
			: Array.isArray(initialData)
				? initialData
				: [initialData]
	const totalBytes = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
	preStartBufferByPort.set(port, { chunks, totalBytes })
}

/**
 * Starts (or restarts) the Kinesis ingestion pipeline for a port, and releases the lock if
 * the pipeline did not actually become active (e.g. missing SRTP key, credential resolution
 * failure - start() can return without throwing in these cases, but is also handled here if
 * it does throw). Without this, the lock would stay held while no pipeline is registered,
 * and writePacket would silently drop all traffic for the port instead of letting another
 * attempt/instance pick it up.
 */
const startPipelineOrReleaseLock = async (
	port: number,
	initialData?: Buffer | Buffer[],
): Promise<void> => {
	if (shuttingDown) return
	if (!kinesisIngestionPipeline) return
	const slot = kinesisIngestionPipeline.streamSlotForPort(port)

	// Captured synchronously (before any await below) - see portGeneration's doc comment.
	const generation = portGeneration.get(port) ?? 0
	const isStale = (): boolean =>
		(portGeneration.get(port) ?? 0) !== generation ||
		!kinesisLockHeldForPorts.has(port)

	if (kinesisIngestionPipeline.isSrtpPort(port)) {
		// Seed from persisted state before spawning - a fresh srtpdec instance always starts
		// at ROC 0 otherwise, which breaks decryption after any sequence-number rollover in
		// the sender's session (see KinesisIngestionPipeline.seedSrtpRoc/getSrtpRocState).
		// Only trusts persisted state recorded under the currently-configured SSRC *and* key
		// fingerprint - either changing (a device/key rotation) means state from the
		// *previous* session is meaningless and must not seed a genuinely fresh one.
		const ssrc = kinesisIngestionPipeline.getConfiguredSrtpSsrc(port)
		if (ssrc !== undefined) {
			const keyFingerprint =
				kinesisIngestionPipeline.getConfiguredSrtpKeyFingerprint(port)
			const persisted = await streamMetadataService.getSrtpRocState(
				slot,
				ssrc,
				keyFingerprint,
			)
			if (isStale()) return // ownership changed while awaiting DynamoDB; abandon
			if (persisted === undefined) {
				// A DynamoDB read failure is not the same as a genuinely fresh/never-persisted
				// session (see getSrtpRocState) - seeding with a fabricated zero here could
				// silently break decryption if the sender has already wrapped. Treat this like
				// a failed start rather than guessing.
				console.error(
					`[Main] Failed to read persisted SRTP ROC state for port ${port}; releasing lock`,
				)
				await releaseLockBackoffAndRearm(port, slot, initialData)
				return
			}
			kinesisIngestionPipeline.seedSrtpRoc(port, persisted)
		}
	}

	let failed: boolean
	try {
		await kinesisIngestionPipeline.start(port, initialData)
		failed = !kinesisIngestionPipeline.isActive(port)
	} catch (err) {
		console.error(
			`[Main] Error starting Kinesis ingestion pipeline for port ${port}:`,
			err,
		)
		failed = true
	}

	if (isStale()) {
		// Ownership was lost (or re-acquired as a new epoch) while start() was in flight -
		// this invocation no longer speaks for the port. Stop whatever it may have just
		// registered rather than leaving an unowned producer running, but don't touch
		// lock/backoff/buffer state below - that belongs to whichever invocation now owns
		// (or cleanly released) this port's epoch.
		if (kinesisIngestionPipeline.isActive(port)) {
			await kinesisIngestionPipeline.stop(port)
		}
		return
	}

	if (!failed) {
		nextStartAttemptAllowedAtByPort.delete(port)
		return
	}

	console.error(
		`[Main] Kinesis ingestion pipeline failed to start for port ${port}; releasing lock`,
	)
	await releaseLockBackoffAndRearm(port, slot, initialData)
}

/** A PacketHandler plus a hook shutdown() uses to drain this handler's per-port queues before
 * stopping pipelines/releasing locks (see waitForAllQueuesIdle). */
type PacketHandlerWithQueueDrain = PacketHandler & {
	waitForAllQueuesIdle: () => Promise<void>
}

const createPacketHandler = (
	rangeConfig: PacketHandlerRangeConfig,
): PacketHandlerWithQueueDrain => {
	/**
	 * For SRTP ports, whether a datagram looks like RTP for the configured SSRC
	 * (parseAuthenticRtpSequenceNumber) - a noise filter, NOT authentication (SSRC is not
	 * secret; a deliberate spoofer who knows it can pass this check trivially - see that
	 * function's own doc comment; a fully watertight fix needs per-packet auth feedback from
	 * srtpdec, which this gst-launch-1.0-based architecture doesn't expose). For unencrypted
	 * ports, or when there's no key loaded yet to validate against, always true (unchanged
	 * behavior - start() already refuses and reports a missing key separately).
	 */
	const looksPlausibleRtp = (port: number, data: Buffer): boolean => {
		if (!kinesisIngestionPipeline || !rangeConfig.isSrtp) return true
		const ssrc = kinesisIngestionPipeline.getConfiguredSrtpSsrc(port)
		if (ssrc === undefined) return true
		return parseAuthenticRtpSequenceNumber(data, ssrc) !== undefined
	}

	const processPacket = async (
		port: number,
		data: Buffer,
		timestamp: Date,
		// Snapshotted by enqueuePacket *before* it calls streamStateManager.onPacketReceived
		// for this same packet - onPacketReceived immediately flips a missing/inactive
		// stream to 'active', so deriving these from getStreamState(port) here (after the
		// queue has already delayed this packet) would always see the post-update state and
		// never observe a genuine first/resume packet, permanently breaking pre-start
		// buffering and lock acquisition for new and resumed streams.
		isFirstPacket: boolean,
		isResume: boolean,
	): Promise<void> => {
		let packetAlreadyInInitialData = false
		// Buffer packets until we have enough data, then acquire Kinesis lock and start GStreamer.
		// This prevents port scans (small random payloads) from being treated as video streams.
		// Include preStartBufferByPort.has(port) so we keep buffering (and eventually try the lock)
		// on packets 2..N until we hit the threshold; otherwise we only enter on first packet.
		//
		// For SRTP ports specifically, also require the datagram to look like RTP for the
		// configured SSRC (looksPlausibleRtp - the same noise filter ROC tracking uses, not
		// authentication - see its own doc comment for why this can't stop a deliberate
		// spoofer, only opportunistic noise) before it can count toward the threshold or
		// trigger lock acquisition at all - these ports are public, and arbitrary traffic
		// reaching 10MB would otherwise acquire the shared stream lock and start a producer
		// with garbage input, denying a legitimate sender (routed to another instance) the
		// slot.
		if (
			kinesisIngestionPipeline &&
			kinesisIngestionPipeline.isPortInRange(port) &&
			!kinesisLockHeldForPorts.has(port) &&
			(isFirstPacket || isResume || preStartBufferByPort.has(port)) &&
			looksPlausibleRtp(port, data)
		) {
			let buf = preStartBufferByPort.get(port)
			if (!buf) {
				buf = { chunks: [], totalBytes: 0 }
				preStartBufferByPort.set(port, buf)
			}
			buf.chunks.push(data)
			buf.totalBytes += data.length

			// Cap buffered bytes while a port is backing off after a failed start (see
			// startPipelineOrReleaseLock) - a long backoff window against a high-bitrate
			// source shouldn't retain unbounded memory just because we're not retrying yet.
			const maxBufferedBytes = rangeConfig.minBytesBeforeStart * 2
			while (buf.totalBytes > maxBufferedBytes && buf.chunks.length > 0) {
				const dropped = buf.chunks.shift()
				if (dropped !== undefined) buf.totalBytes -= dropped.length
			}

			// Cheap, best-effort early-exit check (see the authoritative re-check inside
			// withSlotLock below for why this alone isn't enough): if the *other* port for
			// this stream slot already holds the lock in this process, skip the mutex
			// entirely rather than queuing up behind it for nothing.
			const pairedPort = kinesisIngestionPipeline.pairedPortFor(port)
			const pairedPortHoldsLock =
				pairedPort !== undefined && kinesisLockHeldForPorts.has(pairedPort)

			const backoffUntil = nextStartAttemptAllowedAtByPort.get(port) ?? 0
			if (
				!pairedPortHoldsLock &&
				buf.totalBytes >= rangeConfig.minBytesBeforeStart &&
				Date.now() >= backoffUntil
			) {
				// Captured for startup, but the buffer itself is NOT cleared yet - only once
				// acquisition has actually succeeded, below. Deleting it unconditionally here
				// (as before) meant that if the paired-port recheck or tryAcquireKinesisLock
				// returned false/threw, nothing re-armed it (startPipelineOrReleaseLock only
				// re-arms on its own failure path, which never runs in these cases) - this
				// instance could then never retry, since the stream is already 'active' and
				// none of isFirstPacket/isResume/preStartBufferByPort.has(port) would trigger
				// buffering again on a later packet.
				const initialData: Buffer | Buffer[] = rangeConfig.isSrtp
					? [...buf.chunks]
					: Buffer.concat(buf.chunks)
				packetAlreadyInInitialData = true
				const slot = kinesisIngestionPipeline.streamSlotForPort(port)
				try {
					// The unencrypted and SRTP ports for this slot are drained by two
					// independent per-range queues (see createPacketHandler), so without this
					// mutex, both could cross their threshold at close enough wall-clock times
					// to both see no local holder and both "succeed" at tryAcquireKinesisLock
					// (DynamoDB's own-instance-id clause can't distinguish two local callers).
					// Re-check paired-port ownership *inside* the lock - the snapshot above,
					// taken before queuing for the mutex, can be stale by the time we get it.
					await withSlotLock(slot, async () => {
						const pairedPortNow = kinesisIngestionPipeline.pairedPortFor(port)
						if (
							pairedPortNow !== undefined &&
							kinesisLockHeldForPorts.has(pairedPortNow)
						) {
							return // buffer stays armed; a later packet will retry
						}
						const acquired = await streamMetadataService.tryAcquireKinesisLock(
							slot,
							instanceId,
						)
						if (!acquired) {
							// A remote instance owns this slot. Back off before the next attempt -
							// without this, every subsequent packet re-crosses the (already-met)
							// threshold and repeats this conditional DynamoDB write at packet rate
							// until the remote owner releases the slot. The buffer itself stays
							// armed (not cleared) so a later packet, once the backoff elapses,
							// retries automatically.
							nextStartAttemptAllowedAtByPort.set(
								port,
								Date.now() + START_RETRY_BACKOFF_MS,
							)
							return
						}
						preStartBufferByPort.delete(port)
						setLockHeld(port)
						await startPipelineOrReleaseLock(port, initialData)
					})
				} catch (err) {
					console.error(
						`[Main] Error acquiring Kinesis lock / starting ingestion for port ${port}:`,
						err,
					)
				}
			}
		}

		// Only update DynamoDB lastPacketTime if we hold the lock
		if (kinesisIngestionPipeline && kinesisLockHeldForPorts.has(port)) {
			const slot = kinesisIngestionPipeline.streamSlotForPort(port)
			try {
				await streamMetadataService.updateLastPacketTime(
					slot,
					timestamp,
					instanceId,
				)
			} catch (err) {
				// updateLastPacketTime uses a conditional write - ConditionalCheckFailedException
				// specifically means another instance now owns this slot's lock, not a
				// transient DynamoDB error. Continuing (as a plain log-and-continue would) lets
				// this producer keep writing after someone else has taken the lock; unlike the
				// SRTP path below, the FIFO/unencrypted path has no later ownership check to
				// catch this, so it must stop here.
				if (
					err instanceof Error &&
					err.name === 'ConditionalCheckFailedException'
				) {
					await relinquishPort(port)
					return
				}
				console.error(`[Main] Error updating DynamoDB for port ${port}:`, err)
			}

			// Persist the best-known SRTP rollover-tracking state alongside the heartbeat
			// (throttled internally, safe to call every packet) - see
			// KinesisIngestionPipeline.getSrtpRocState. If this reports we're no longer the
			// slot's owner (a condition-checked write), stop writing to Kinesis for a stream
			// we've lost the lock for instead of continuing as a competing producer.
			const ssrc = kinesisIngestionPipeline.getConfiguredSrtpSsrc(port)
			if (ssrc !== undefined) {
				const rocState = kinesisIngestionPipeline.getSrtpRocState(port)
				const keyFingerprint =
					kinesisIngestionPipeline.getConfiguredSrtpKeyFingerprint(port)
				const stillOwner = await streamMetadataService.updateSrtpRoc(
					slot,
					instanceId,
					rocState.roc,
					rocState.highestSeq,
					ssrc,
					keyFingerprint,
				)
				if (!stillOwner) {
					await relinquishPort(port)
				}
			}
		}

		// Feed packet to Kinesis only if we hold the lock. Skip if this packet was passed as initialData.
		if (
			kinesisIngestionPipeline &&
			kinesisLockHeldForPorts.has(port) &&
			!packetAlreadyInInitialData
		) {
			kinesisIngestionPipeline.writePacket(port, data)
		}
	}

	/**
	 * Bounded per-port packet queue. UDPListener dispatches onPacket() fire-and-forget (see
	 * backend/src/UDPListener.ts), so without explicit serialization, concurrent
	 * processPacket() calls for the same port can interleave: a later packet's writePacket()
	 * call can overtake an earlier one still awaiting bookkeeping (the throttled DynamoDB
	 * heartbeat) or the lock-acquire + pipeline-start sequence - corrupting SRTP packet
	 * order, or reaching writePacket() before the pipeline has actually finished starting and
	 * silently dropping the packet. Serializing fixes that, but an unbounded queue (e.g. a
	 * plain promise chain of closures) can grow without limit while a task is slow - DynamoDB,
	 * credential-resolution retries, or the paced 10MB SRTP startup replay can each take
	 * seconds - so this explicitly bounds total queued bytes per port (same 2x-threshold cap
	 * as preStartBufferByPort), dropping the oldest queued packet once exceeded rather than
	 * growing unbounded.
	 */
	const queueStateByPort = new Map<
		number,
		{
			queue: {
				data: Buffer
				timestamp: Date
				isFirstPacket: boolean
				isResume: boolean
			}[]
			totalBytes: number
			draining: boolean
		}
	>()
	const maxQueuedBytes = rangeConfig.minBytesBeforeStart * 2

	/**
	 * Resolves once this port's queue has fully drained (no items queued, nothing currently
	 * processing) - used to serialize onStreamStop/shutdown against packets that were already
	 * queued before the stop/shutdown was requested (see enqueuePacket/onStreamStop). Bounded
	 * by the same network-call timeouts processPacket's awaited work already has.
	 */
	const waitForPortQueueIdle = async (port: number): Promise<void> => {
		for (;;) {
			const state = queueStateByPort.get(port)
			if (!state || (!state.draining && state.queue.length === 0)) return
			await new Promise((resolve) => setTimeout(resolve, 20))
		}
	}

	/** Same as waitForPortQueueIdle, but across every port this handler has ever queued -
	 * used by shutdown(), which must drain both the unencrypted and SRTP handlers' queues. */
	const waitForAllQueuesIdle = async (): Promise<void> => {
		for (;;) {
			const anyBusy = Array.from(queueStateByPort.values()).some(
				(state) => state.draining || state.queue.length > 0,
			)
			if (!anyBusy) return
			await new Promise((resolve) => setTimeout(resolve, 20))
		}
	}

	const enqueuePacket = (port: number, data: Buffer, timestamp: Date): void => {
		// Snapshot first/resume status *before* onPacketReceived below flips a missing or
		// inactive stream to 'active' - once that runs, getStreamState(port) reports 'active'
		// regardless of what it was a moment ago, so deriving these later (inside
		// processPacket, once this packet's turn in the queue comes up) would never see a
		// genuine first/resume transition.
		const streamState = streamStateManager.getStreamState(port)
		const isFirstPacket = streamState === undefined
		const isResume = streamState?.status === 'inactive'

		// Record receipt immediately, not after this packet works its way through the queue -
		// streamStateManager's inactivity timer/active-state must reflect true receipt time.
		// Deferring this into processPacket (as before) meant a backed-up queue (slow
		// DynamoDB, credential retries, the paced SRTP startup replay) could delay it long
		// enough for the inactivity timeout to fire a spurious onStreamStop while packets were
		// still arriving, just not yet processed.
		//
		// Skipped for a not-yet-locked port whose datagram doesn't look like plausible RTP
		// (looksPlausibleRtp): marking the stream active here happens *before*
		// processPacket's own plausibility check further down (which only gates entry into
		// the buffering block) - without this, a single implausible datagram could consume
		// the only isFirstPacket/isResume transition and mark the stream active without ever
		// arming a pre-start buffer, leaving later genuinely-valid packets stuck (both
		// transition flags now false, no buffer entry) until an inactivity cycle. Once the
		// lock is already held, traffic is treated normally regardless - GStreamer/srtpdec's
		// own validation is the authority at that point, not this pre-lock noise filter.
		if (kinesisLockHeldForPorts.has(port) || looksPlausibleRtp(port, data)) {
			streamStateManager.onPacketReceived(port, timestamp)
		}

		let state = queueStateByPort.get(port)
		if (!state) {
			state = { queue: [], totalBytes: 0, draining: false }
			queueStateByPort.set(port, state)
		}

		state.queue.push({ data, timestamp, isFirstPacket, isResume })
		state.totalBytes += data.length

		// If eviction below drops the packet carrying the only isFirstPacket/isResume
		// marker, carry it forward onto the new front of the queue instead of just
		// discarding it - losing it entirely would leave every retained packet with both
		// transition flags false and no pre-start buffer armed, stranding the stream until
		// a full inactivity/resume cycle happens on its own (see processPacket's buffering
		// condition, which needs one of those to be true to ever re-enter).
		let carriedIsFirstPacket = false
		let carriedIsResume = false
		while (state.totalBytes > maxQueuedBytes && state.queue.length > 1) {
			const dropped = state.queue.shift()
			if (dropped === undefined) break
			state.totalBytes -= dropped.data.length
			carriedIsFirstPacket = carriedIsFirstPacket || dropped.isFirstPacket
			carriedIsResume = carriedIsResume || dropped.isResume
		}
		if (carriedIsFirstPacket || carriedIsResume) {
			const front = state.queue[0]
			if (front !== undefined) {
				front.isFirstPacket = front.isFirstPacket || carriedIsFirstPacket
				front.isResume = front.isResume || carriedIsResume
			}
		}

		if (state.draining) return
		state.draining = true
		void (async () => {
			while (state.queue.length > 0) {
				const next = state.queue.shift()
				if (!next) break
				state.totalBytes -= next.data.length
				try {
					await processPacket(
						port,
						next.data,
						next.timestamp,
						next.isFirstPacket,
						next.isResume,
					)
				} catch (err) {
					console.error(
						`[Main] Unhandled error processing packet for port ${port}:`,
						err,
					)
				}
			}
			state.draining = false
		})()
	}

	return {
		onPacket: async (port, data, timestamp) => {
			enqueuePacket(port, data, timestamp)
		},

		waitForAllQueuesIdle,

		onStreamStart: async (port) => {
			console.log(`[Main] Stream started on port ${port}`)

			// Never (re)start a producer once shutdown has begun - see shuttingDown.
			if (shuttingDown) return

			// Resume Kinesis pipeline only if we hold the lock (e.g. stream resume after brief inactivity)
			if (kinesisIngestionPipeline && kinesisLockHeldForPorts.has(port)) {
				void startPipelineOrReleaseLock(port).catch((err) => {
					console.error(
						`[Main] Error starting Kinesis ingestion for port ${port}:`,
						err,
					)
				})
			}
		},

		onStreamStop: async (port, inactivityDuration) => {
			console.log(
				`[Main] Stream stopped on port ${port} after ${inactivityDuration}ms`,
			)

			// Wait for any packets already queued (enqueued before this stop event fired) to
			// finish processing before doing anything else. onPacket only enqueues (see
			// enqueuePacket) - without this, work still sitting in the queue could keep
			// running writePacket()/lock-acquire logic *after* we stop the pipeline and
			// release the lock below, racing the handoff this ordering is meant to guarantee.
			await waitForPortQueueIdle(port)

			// A resume can race this stop: a new packet may have arrived and been fully
			// processed - including possibly re-acquiring the lock and starting a fresh
			// pipeline - while we were waiting above (streamStateManager doesn't emit a
			// distinct 'streamResume' event this could hook into instead). If the stream is
			// active again, this stop callback is now stale; tearing down below would undo
			// that legitimate resume and leave an active stream without ingestion.
			if (streamStateManager.getStreamState(port)?.status === 'active') {
				console.log(
					`[Main] Stream on port ${port} resumed before stop could complete; aborting stale stop`,
				)
				return
			}

			preStartBufferByPort.delete(port)

			// Stop the producer *before* releasing the lock (and before persisting final
			// state) - releasing first would let another instance acquire the slot and start
			// writing while this producer is still shutting down (up to its stop timeout),
			// creating two producers for the same Kinesis stream. Release is the final step
			// of the handoff, not an early one.
			if (kinesisIngestionPipeline) {
				await kinesisIngestionPipeline.stop(port)
			}

			if (kinesisIngestionPipeline && kinesisLockHeldForPorts.has(port)) {
				const slot = kinesisIngestionPipeline.streamSlotForPort(port)

				// Guarantee clearLockHeld runs even if the release call below throws - without
				// this, a thrown releaseKinesisLock left kinesisLockHeldForPorts permanently
				// (and wrongly) marked as held, since the line clearing it was never reached.
				try {
					// Force (bypass the throttle) so the freshest state is captured before this
					// slot potentially gets reassigned to a different port/instance.
					const ssrc = kinesisIngestionPipeline.getConfiguredSrtpSsrc(port)
					if (kinesisIngestionPipeline.isSrtpPort(port) && ssrc !== undefined) {
						const rocState = kinesisIngestionPipeline.getSrtpRocState(port)
						const keyFingerprint =
							kinesisIngestionPipeline.getConfiguredSrtpKeyFingerprint(port)
						await streamMetadataService.updateSrtpRoc(
							slot,
							instanceId,
							rocState.roc,
							rocState.highestSeq,
							ssrc,
							keyFingerprint,
							true,
						)
					}

					await streamMetadataService.releaseKinesisLock(slot, instanceId)
				} finally {
					clearLockHeld(port)
				}
			}

			// A packet can arrive and mark the stream active again while the teardown above
			// (particularly the DynamoDB calls) was still in flight - kinesisLockHeldForPorts
			// still reported this port as locally held at that moment, so that packet's own
			// processPacket run couldn't enter the buffering path, and writePacket() was a
			// no-op since stop() had already removed the pipeline. Re-arm an empty buffer now
			// so a later packet can still retry lock acquisition, even though it won't carry a
			// genuine isFirstPacket/isResume marker (that was already consumed by the dropped one).
			if (
				streamStateManager.getStreamState(port)?.status === 'active' &&
				!preStartBufferByPort.has(port)
			) {
				preStartBufferByPort.set(port, { chunks: [], totalBytes: 0 })
			}
		},
	}
}

const isSrtpPort = (port: number): boolean =>
	srtpEnabled &&
	port >= config.srtpPortRange.start &&
	port <= config.srtpPortRange.end

const mainPacketHandler = createPacketHandler({
	minBytesBeforeStart: config.kinesisMinBytesBeforeStart,
	isSrtp: false,
})
const srtpPacketHandler = srtpEnabled
	? createPacketHandler({
			minBytesBeforeStart: config.kinesisMinBytesBeforeStart,
			isSrtp: true,
		})
	: null

const handlerForPort = (port: number): PacketHandler =>
	isSrtpPort(port) && srtpPacketHandler ? srtpPacketHandler : mainPacketHandler

// Set up stream state event handlers
streamStateManager.on('streamStart', (port: number) => {
	void handlerForPort(port)
		.onStreamStart(port)
		.catch((err) => {
			console.error(`[Main] Error handling stream start for port ${port}:`, err)
		})
})

streamStateManager.on(
	'streamStop',
	(port: number, inactivityDuration: number) => {
		void handlerForPort(port)
			.onStreamStop(port, inactivityDuration)
			.catch((err) => {
				console.error(
					`[Main] Error handling stream stop for port ${port}:`,
					err,
				)
			})
	},
)

// Auto-restart Kinesis pipeline when GStreamer exits unexpectedly (e.g. crash, OOM, Kinesis network issues)
// Restart is throttled to avoid storms if GStreamer keeps failing
const pipelineRestartThrottleMs = 10_000 // min delay between restarts per port
const lastPipelineRestartByPort = new Map<number, number>()
if (kinesisIngestionPipeline) {
	kinesisIngestionPipeline.on(
		'pipelineExited',
		({
			port,
			code,
			signal,
		}: {
			port: number
			code: number | null
			signal: string | null
		}) => {
			// Only restart if stream is still active (still receiving packets)
			const state = streamStateManager.getStreamState(port)
			if (state?.status !== 'active') return
			if (!kinesisIngestionPipeline?.isPortInRange(port)) return

			const now = Date.now()
			const lastRestart = lastPipelineRestartByPort.get(port) ?? 0
			const delay = Math.max(0, pipelineRestartThrottleMs - (now - lastRestart))

			console.warn(
				`[Main] GStreamer exited unexpectedly for port ${port} (code=${code}, signal=${signal}). Restarting in ${delay}ms...`,
			)
			setTimeout(() => {
				// The delay above can straddle a shutdown that started after this timer was
				// scheduled - re-check so a restart can't spawn a producer teardown never stops.
				if (shuttingDown) return
				lastPipelineRestartByPort.set(port, Date.now())
				void startPipelineOrReleaseLock(port).catch((err) => {
					console.error(
						`[Main] Error restarting Kinesis ingestion for port ${port}:`,
						err,
					)
				})
			}, delay)
		},
	)
}

// Initialize UDP listener (unencrypted MPEG-TS/H.264, ports 5000-5009)
const udpListener = new UDPListener({
	portRange: config.portRange,
	bufferSize: config.bufferSize,
	flushInterval: config.flushInterval,
	outputDirectory: config.outputDirectory,
})

udpListener.setPacketHandler(mainPacketHandler)

// Second UDP listener for SRTP-encrypted RTP/H.264 (ports 6000-6009), when configured
const srtpUdpListener = srtpEnabled
	? new UDPListener({
			portRange: config.srtpPortRange,
			bufferSize: config.bufferSize,
			flushInterval: config.flushInterval,
			outputDirectory: config.outputDirectory,
		})
	: null

if (srtpUdpListener && srtpPacketHandler) {
	srtpUdpListener.setPacketHandler(srtpPacketHandler)
}

const healthServer = new HealthServer()

// Graceful shutdown handler
const shutdown = async (): Promise<void> => {
	// First thing, before any of the awaits below: from here on, no new pipeline
	// start/restart may be initiated (see shuttingDown) while teardown drains queues and
	// stops producers.
	shuttingDown = true
	console.log('[Main] Shutting down...')

	await healthServer.stop()
	await udpListener.stop()
	await srtpUdpListener?.stop()
	streamStateManager.stop()

	// Sockets are stopped above, so no new packets can be enqueued from here on - wait for
	// anything already queued (enqueued before shutdown began) to finish processing before
	// stopping pipelines/releasing locks below, for the same reason onStreamStop does: queued
	// work left running afterward could still call writePacket()/re-acquire a lock this
	// shutdown is about to release.
	await mainPacketHandler.waitForAllQueuesIdle()
	await srtpPacketHandler?.waitForAllQueuesIdle()

	preStartBufferByPort.clear()

	// Stop every producer *before* releasing any locks - see the same ordering rationale in
	// onStreamStop above. shutdown() (not stopAll()) also refuses any further start() and
	// sweeps children still alive afterwards, so a start that was in flight while the queues
	// above were draining cannot leave an orphaned producer behind at process.exit().
	if (kinesisIngestionPipeline) {
		await kinesisIngestionPipeline.shutdown()
	}

	for (const port of kinesisLockHeldForPorts) {
		const slot = kinesisIngestionPipeline?.streamSlotForPort(port) ?? port
		const ssrc = kinesisIngestionPipeline?.getConfiguredSrtpSsrc(port)
		if (
			kinesisIngestionPipeline?.isSrtpPort(port) === true &&
			ssrc !== undefined
		) {
			const rocState = kinesisIngestionPipeline.getSrtpRocState(port)
			const keyFingerprint =
				kinesisIngestionPipeline.getConfiguredSrtpKeyFingerprint(port)
			await streamMetadataService.updateSrtpRoc(
				slot,
				instanceId,
				rocState.roc,
				rocState.highestSeq,
				ssrc,
				keyFingerprint,
				true,
			)
		}
		await streamMetadataService.releaseKinesisLock(slot, instanceId)
	}
	kinesisLockHeldForPorts.clear()

	console.log('[Main] Shutdown complete')
	process.exit(0)
}

process.on('SIGINT', () => {
	void shutdown().catch((err) => {
		console.error('[Main] Error during shutdown:', err)
		process.exit(1)
	})
})
process.on('SIGTERM', () => {
	void shutdown().catch((err) => {
		console.error('[Main] Error during shutdown:', err)
		process.exit(1)
	})
})

// Start the service
const start = async (): Promise<void> => {
	instanceId = await resolveInstanceId()
	console.log('[Main] Starting UDP video ingestion service...')
	console.log(`[Main] Instance ID: ${instanceId}`)
	console.log(
		`[Main] Listening on ports ${config.portRange.start}-${config.portRange.end}`,
	)
	console.log(`[Main] Output directory: ${config.outputDirectory}`)
	console.log(`[Main] DynamoDB table: ${config.dynamoDBTableName}`)
	console.log(`[Main] AWS region: ${config.awsRegion}`)
	if (config.kinesisIngestionEnabled) {
		console.log(
			`[Main] Kinesis ingestion enabled (ports ${config.portRange.start}-${config.portRange.end} -> streams 1-${config.portRange.end - config.portRange.start + 1})`,
		)
		console.log(
			`[Main] GStreamer starts after ${config.kinesisMinBytesBeforeStart / 1024 / 1024} MB received (KINESIS_MIN_BYTES_BEFORE_START)`,
		)
	} else {
		console.log(
			'[Main] Kinesis ingestion disabled (KINESIS_INGESTION_ENABLED not set)',
		)
	}
	if (srtpEnabled) {
		console.log(
			`[Main] SRTP ingestion enabled (ports ${config.srtpPortRange.start}-${config.srtpPortRange.end} -> same streams 1-${config.srtpPortRange.end - config.srtpPortRange.start + 1})`,
		)
	} else {
		console.log(
			'[Main] SRTP ingestion disabled (SRTP_INGESTION_ENABLED not set, or Kinesis ingestion disabled)',
		)
	}

	try {
		await ensureAwsCredentials()
		if (srtpEnabled && srtpKeyStore) {
			const ports: number[] = []
			for (
				let port = config.srtpPortRange.start;
				port <= config.srtpPortRange.end;
				port++
			) {
				ports.push(port)
			}
			// Isolated from the outer try/catch: an SSM outage/throttling/permissions error
			// here must not abort startup of the unencrypted MPEG-TS path (5000-5009), which
			// does not depend on SRTP keys at all. SRTP ports will simply have no key loaded
			// (KinesisIngestionPipeline already logs and refuses ingestion per-port when that
			// happens) until this is retried on the next restart.
			try {
				await srtpKeyStore.loadPorts(ports)
			} catch (err) {
				console.error(
					'[Main] Failed to load SRTP keys from SSM; SRTP ingestion will be unavailable until this is resolved. The unencrypted MPEG-TS path is unaffected. Error:',
					err,
				)
			}
		}
		await healthServer.start()
		await udpListener.start()
		if (srtpUdpListener) {
			// Isolated from the rest of startup, same rationale as the SRTP key-loading block
			// above: UDPListener.start() rejects if any port in its range permanently fails to
			// bind (see UDPListener.bindPort), and this used to sit in the same outer
			// try/catch that exits the whole process on any failure - an SRTP-only bind
			// problem (port conflict, misconfiguration) would then take down the
			// already-started unencrypted listener too. Clean up any SRTP sockets that did
			// bind before the failure, so a later retry (this instance's whole process still
			// isn't restarting) doesn't leak them.
			try {
				await srtpUdpListener.start()
			} catch (err) {
				console.error(
					'[Main] Failed to start SRTP UDP listener; SRTP ingestion will be unavailable until this is resolved. The unencrypted MPEG-TS path is unaffected. Error:',
					err,
				)
				try {
					await srtpUdpListener.stop()
				} catch (stopErr) {
					console.error(
						'[Main] Error cleaning up partially-started SRTP UDP listener:',
						stopErr,
					)
				}
			}
		}
		console.log('[Main] Service started successfully')
	} catch (error) {
		console.error('[Main] Failed to start service:', error)
		process.exit(1)
	}
}

// Start if running as main module
if (import.meta.url === `file://${process.argv[1]}`) {
	void start().catch((err) => {
		console.error('[Main] Fatal error:', err)
		process.exit(1)
	})
}

export {
	kinesisIngestionPipeline,
	streamMetadataService,
	streamStateManager,
	udpListener,
}
