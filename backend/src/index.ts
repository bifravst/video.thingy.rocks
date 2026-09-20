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
 * Stops the local pipeline and clears our in-process lock bookkeeping for a port - used
 * when a conditional write (updateSrtpRoc 'lostLock', updateLastPacketTime's
 * ConditionalCheckFailedException) proves we're no longer the slot's owner in DynamoDB,
 * so there is nothing left for us to release; continuing to run (and write to) a pipeline
 * we've lost the lock for is exactly the competing-producer problem the lock exists to
 * prevent. A *transient* failure ('writeError') leaves ownership unknown - pass
 * `maybeStillOwnsRow` so a conditional release is attempted (see below).
 *
 * Stops the pipeline *before* clearing local ownership, not after - clearing first would let
 * the paired transport's cheap pairedPortHoldsLock check pass and start racing this port's
 * still-running producer immediately (this instance no longer owns the DynamoDB row either,
 * by construction, so there's nothing stopping a genuine two-producer window). Also applies
 * the standard start backoff and re-arms an empty pre-start buffer afterward if the stream
 * is still active, so a later packet can retry lock acquisition - without the backoff, a
 * *transient* write failure would otherwise repeat this buffer/start/relinquish cycle (and
 * the GStreamer spawn/kill churn that goes with it) at roughly one attempt per 10MB for as
 * long as the outage lasts; without the re-arm at all, the stream would be stranded until
 * a full inactivity/resume cycle happens on its own.
 */
const relinquishPort = async (
	port: number,
	// Set when this instance may *still* own the DynamoDB row for this port's slot - i.e.
	// a *transient* failure made it give up the local producer, not a conditional write
	// that proved another owner: a conditional release is then attempted, so another
	// instance can take the slot over immediately instead of waiting out the
	// KINESIS_LOCK_STALE_MS threshold with nobody ingesting. Omitted when another owner
	// was proven - there is nothing to release (releaseKinesisLock is conditional on the
	// current owner, so it no-ops harmlessly if the row was lost after all).
	maybeStillOwnsRow?: { slot: number },
): Promise<void> => {
	console.warn(
		`[Main] Lost Kinesis lock for port ${port}; stopping local pipeline`,
	)
	if (kinesisIngestionPipeline) {
		await kinesisIngestionPipeline.stop(port)
	}
	if (maybeStillOwnsRow !== undefined) {
		try {
			// Released *before* clearing local ownership (kinesisLockHeldForPorts), same
			// ordering as every other handoff.
			await streamMetadataService.releaseKinesisLock(
				maybeStillOwnsRow.slot,
				instanceId,
			)
		} catch (err) {
			console.error(
				`[Main] Error conditionally releasing Kinesis lock for port ${port}:`,
				err,
			)
		}
	}
	clearLockHeld(port)
	nextStartAttemptAllowedAtByPort.set(port, Date.now() + START_RETRY_BACKOFF_MS)
	if (
		streamStateManager.getStreamState(port)?.status === 'active' &&
		!preStartBufferByPort.has(port)
	) {
		// Merge any datagrams held while no pipeline was active (writePacket's hold)
		// into the re-armed buffer, ahead of everything buffered from here on - they are
		// the oldest pending data, and leaving them in the hold would make the next start
		// replay newer buffered chunks first and seed the older held packets into the new
		// pipeline's pendingQueue, letting newer sequence numbers overtake the older held
		// keyframe/SPS/PPS (same ordering the other re-arm paths preserve via
		// takeHeldDatagrams).
		const held = kinesisIngestionPipeline?.takeHeldDatagrams(port) ?? []
		preStartBufferByPort.set(port, {
			chunks: held,
			totalBytes: held.reduce((sum, chunk) => sum + chunk.length, 0),
		})
	}
}

/**
 * Attempts for the final, throttled-bypassing SRTP ROC persistence before handing a slot
 * over (onStreamStop/shutdown) - see persistSrtpRocForHandoff.
 */
const FINAL_ROC_PERSIST_ATTEMPTS = 3
const FINAL_ROC_PERSIST_RETRY_DELAY_MS = 1_000

/**
 * Persists the freshest SRTP rollover-tracking state for a slot one last time before this
 * instance hands the slot over (onStreamStop) or exits (shutdown), with bounded retries.
 * updateSrtpRoc returns 'writeError' for an ordinary DynamoDB failure (retrying can help)
 * and 'lostLock' when another instance owns the row (retrying is pointless, but the release
 * below is equally conditional and will no-op) - so the write is retried a few times before
 * giving up. If it still fails, the handoff does NOT silently proceed as if persistence
 * had succeeded: the next owner's strongly consistent read may return a ROC that is stale
 * by up to one rollover, and the error logged here names the operational escape hatch for
 * exactly that failure mode.
 *
 * Callers must hold the slot's mutex (withSlotLock) for the *entire* handoff - this
 * persistence AND the releaseKinesisLock that follows it in the same critical section
 * (see onStreamStop/shutdown) - and it re-reads the in-memory ROC state immediately
 * before every write attempt: onStreamStop/shutdown can reach this while the UDP
 * listener is still enqueueing packets, and a packet processed after an earlier attempt
 * failed can advance and persist a *newer* ROC - retrying with a state captured before
 * that packet would overwrite it again (both writes are conditional on this instance
 * being the lock owner, so the retry succeeds just the same), leaving the slot's next
 * owner seeded with a stale ROC if the intervening packet crossed a rollover. Held
 * across the release as well, the mutex excludes the per-packet lock-held bookkeeping
 * in processPacket (which tracks a datagram's ROC contribution and persists it) from
 * the whole handoff: a packet either fully completes before it (its state is included
 * in the re-read below and written again - never regressed, never lost to a release
 * racing in between) or runs after the release, finds kinesisLockHeldForPorts no longer
 * containing this port (cleared in the same critical section), and skips persisting
 * entirely.
 */
const persistSrtpRocForHandoff = async (
	port: number,
	slot: number,
): Promise<void> => {
	if (!kinesisIngestionPipeline) return
	const ssrc = kinesisIngestionPipeline.getConfiguredSrtpSsrc(port)
	if (!kinesisIngestionPipeline.isSrtpPort(port) || ssrc === undefined) return

	const keyFingerprint =
		kinesisIngestionPipeline.getConfiguredSrtpKeyFingerprint(port)

	let persistedOk = false
	for (let attempt = 1; attempt <= FINAL_ROC_PERSIST_ATTEMPTS; attempt++) {
		// Fence: re-read the freshest in-memory state immediately before every
		// attempt, not once before the loop - see the doc comment above.
		const rocState = kinesisIngestionPipeline.getSrtpRocState(port)
		const result = await streamMetadataService.updateSrtpRoc(
			slot,
			instanceId,
			rocState.roc,
			rocState.highestSeq,
			ssrc,
			keyFingerprint,
			true,
		)
		if (result === 'ok') {
			persistedOk = true
			break
		}
		if (result === 'lostLock') {
			// Another instance owns the row: nothing this instance can still persist
			// (every retry fails the same condition, and the equally conditional release
			// below will no-op), and the new owner tracks and persists its own state -
			// this is normal ownership loss, not a persistence gap, so it must not fall
			// through to the loud "next owner may seed a stale ROC" alarm below.
			return
		}
		if (attempt < FINAL_ROC_PERSIST_ATTEMPTS) {
			await new Promise((resolve) =>
				setTimeout(resolve, FINAL_ROC_PERSIST_RETRY_DELAY_MS),
			)
		}
	}
	if (persistedOk) return

	console.error(
		`[Main] Failed to persist final SRTP ROC state for port ${port} (slot ${slot}) after ${FINAL_ROC_PERSIST_ATTEMPTS} attempts. The next owner of this slot may seed a stale ROC - if decryption fails after a handoff, clear srtpRoc/srtpHighestSeq/srtpRocSsrc/srtpRocKeyFingerprint on the slot's StreamMetadata item and restart the backend (its in-memory ROC estimate for the port lives until the process restarts - see docs/TESTING-SRTP-INGESTION.md).`,
	)
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
 * This is a handoff exactly like onStreamStop's: the freshest SRTP ROC state is persisted
 * (forced, with bounded retries) *before* the lock is released, and both run under the
 * slot's mutex - runStartForSrtpPort folds the whole pre-start buffer (up to 10MB of
 * datagrams, possibly spanning a sequence-number rollover) into the in-memory estimate
 * before spawning, so a start that then fails must not release the slot without that state
 * or the next owner seeds srtpdec one rollover too low and cannot decrypt. The lock is
 * still released *before* clearing local ownership (kinesisLockHeldForPorts) - clearing
 * first would let the paired transport's cheap pairedPortHoldsLock check pass and race
 * tryAcquireKinesisLock while the DynamoDB row still lists this instance as owner, and the
 * paired port could acquire and start a pipeline moments before this release deletes
 * kinesisOwnerInstanceId out from under it, leaving that new pipeline unowned.
 *
 * `holdsSlotMutex` must be true when the caller already runs inside withSlotLock for this
 * slot (the buffering path in processPacket, which serializes acquire+start) - taking the
 * mutex again here would deadlock the promise chain. When it is false (the pipelineExited
 * restart and streamStart resume paths), the handoff takes the mutex itself, matching
 * onStreamStop's structure.
 *
 * `persistRoc` is false only for the failed-persisted-ROC-read path in
 * startPipelineOrReleaseLock - see that call site for why a forced {0,0} write must be
 * avoided there.
 *
 * `isStale` is the calling invocation's ownership-epoch check (see
 * startPipelineOrReleaseLock), re-run *inside* the handoff's critical section: for the
 * restart/resume callers the handoff takes the slot's mutex itself, and the mutex wait
 * can straddle a newer invocation re-acquiring this slot - an older invocation
 * resuming then would release the row and clear the local ownership the *newer* owner
 * just established. The in-section re-check abandons the handoff instead.
 */
const releaseLockBackoffAndRearm = async (
	port: number,
	slot: number,
	initialData: Buffer | Buffer[] | undefined,
	holdsSlotMutex = false,
	// False only for the failed-persisted-ROC-read path in startPipelineOrReleaseLock: on
	// a fresh process nothing has been seeded or tracked yet, so a forced persistence
	// would write {roc: 0, highestSeq: 0} over the valid nonzero state that the failed
	// read was supposed to protect - the very outcome that path exists to avoid. Final ROC
	// persistence stays enabled everywhere state was successfully loaded or observed.
	persistRoc = true,
	// The calling invocation's staleness check (undefined when the caller has no epoch
	// to protect), re-run inside the handoff's critical section - see above.
	isStale?: () => boolean,
): Promise<void> => {
	// Set when the handoff abandoned itself as stale (see below) - the outer function
	// must then skip its backoff/re-arm work entirely: this invocation no longer speaks
	// for the port, and applying its (older-epoch) backoff or overwriting
	// preStartBufferByPort with its initialData would clobber the retry state of the
	// newer local owner that took the slot.
	let handoffAbandonedAsStale = false
	const handoff = async (): Promise<void> => {
		// Re-checked under the mutex: the wait can straddle a newer ownership epoch
		// taking this slot - this invocation no longer speaks for the port, and
		// releasing the row / clearing local ownership would tear down the newer owner.
		// (undefined = no epoch to protect - proceed with the handoff.)
		if (isStale?.() === true) {
			handoffAbandonedAsStale = true
			return
		}
		try {
			// See above: persist the freshest SRTP ROC state while this instance still
			// owns the slot, then release it - one critical section, same as onStreamStop.
			// A no-op for unencrypted ports (persistSrtpRocForHandoff's own guards).
			if (persistRoc) {
				await persistSrtpRocForHandoff(port, slot)
			}
			await streamMetadataService.releaseKinesisLock(slot, instanceId)
		} finally {
			clearLockHeld(port)
		}
	}
	try {
		if (holdsSlotMutex) {
			await handoff()
		} else {
			await withSlotLock(slot, handoff)
		}
	} catch (err) {
		// clearLockHeld has already run via the finally above; log and continue so the
		// backoff/re-arm below still happens - a thrown release must not strand the port.
		console.error(`[Main] Error releasing Kinesis lock for port ${port}:`, err)
	}

	// A stale invocation must not touch the port's retry state (see
	// handoffAbandonedAsStale): the newer owner that took the slot owns the backoff
	// clock and the pre-start buffer from here on.
	if (handoffAbandonedAsStale) return

	nextStartAttemptAllowedAtByPort.set(port, Date.now() + START_RETRY_BACKOFF_MS)

	const chunks =
		initialData === undefined
			? []
			: Array.isArray(initialData)
				? initialData
				: [initialData]
	// Merge any datagrams held while no pipeline was active during the failed attempt (see
	// takeHeldDatagrams): they are older than everything the re-armed buffer accumulates
	// from here on, and leaving them in the hold would make the next start replay two
	// competing sources (initialData first, the hold seeded into the new pipeline's
	// pendingQueue second), letting newer sequence numbers overtake the older held
	// keyframe/SPS/PPS - the jitter buffer or SRTP anti-replay window then discards
	// exactly the packets recovery needs.
	if (kinesisIngestionPipeline) {
		chunks.push(...kinesisIngestionPipeline.takeHeldDatagrams(port))
	}
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
	// True when the caller already runs inside withSlotLock for this port's slot (the
	// buffering path in processPacket) - threaded through to releaseLockBackoffAndRearm so
	// its persist+release handoff runs inline instead of taking the mutex again (which
	// would deadlock the promise chain).
	holdsSlotMutex = false,
): Promise<void> => {
	if (shuttingDown) return
	if (!kinesisIngestionPipeline) return
	// Never spawn a producer this instance doesn't hold the Kinesis lock for: the
	// delayed 'pipelineExited' restart and the fire-and-forget streamStart resume both
	// validate ownership before calling, but ownership can also be lost between that
	// check and entry. Bailing here - before any DynamoDB read, credential resolution,
	// spawn, or paced replay - avoids churning a full start sequence only for the
	// post-start staleness check to stop its result again (an unowned producer writing
	// to the stream in the meantime).
	if (!kinesisLockHeldForPorts.has(port)) return
	const slot = kinesisIngestionPipeline.streamSlotForPort(port)

	// Captured synchronously (before any await below) - see portGeneration's doc comment.
	const generation = portGeneration.get(port) ?? 0
	const isStale = (): boolean =>
		(portGeneration.get(port) ?? 0) !== generation ||
		!kinesisLockHeldForPorts.has(port)

	// The local ownership set above can be stale: during a DynamoDB outage long enough for
	// the row's heartbeat to expire, another instance can acquire this slot while this
	// process still believes it holds it (the heartbeat *writes* were failing, not the
	// local bookkeeping). Before spawning another producer - most importantly from the
	// delayed 'pipelineExited' restart - confirm ownership of the actual DynamoDB row:
	// tryAcquireKinesisLock is idempotent for the current owner (and re-claims an
	// expired-but-uncontested row, refreshing its heartbeat); only a *different* owner's
	// row makes it fail. A transient DynamoDB error is treated like any other failed start
	// rather than guessing - spawning without confirmation is exactly the
	// competing-producer problem the lock exists to prevent.
	let stillOwner: boolean
	try {
		stillOwner = await streamMetadataService.tryAcquireKinesisLock(
			slot,
			instanceId,
		)
	} catch (err) {
		console.error(
			`[Main] Error confirming Kinesis lock ownership for port ${port}:`,
			err,
		)
		stillOwner = false
	}
	if (!stillOwner) {
		console.error(
			`[Main] No longer the owner of the Kinesis lock for port ${port}; abandoning pipeline start`,
		)
		// The re-arm path, NOT relinquishPort: (a) the threshold path has already
		// deleted the pre-start buffer, and its packets exist only in initialData -
		// relinquishPort would re-arm an empty buffer and drop them; (b) for the
		// restart/resume callers this runs outside the slot mutex, where an older
		// invocation could resume after a newer epoch re-acquired the slot and clear
		// that newer owner - releaseLockBackoffAndRearm re-checks this invocation's
		// staleness inside the critical section and abandons instead. Its conditional
		// release also covers the "row ownership unknown" transient-error case above
		// (a no-op when another instance owns the row), and its re-arm merges any held
		// datagrams ahead of initialData, keeping one oldest-first replay sequence.
		await releaseLockBackoffAndRearm(
			port,
			slot,
			initialData,
			holdsSlotMutex,
			true,
			isStale,
		)
		return
	}
	if (isStale()) return // ownership changed while awaiting DynamoDB; abandon

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
				// a failed start rather than guessing. The release below also skips the final
				// ROC persistence: on a fresh process nothing has been seeded or tracked yet,
				// so the forced write would use {roc: 0, highestSeq: 0} and overwrite the valid
				// nonzero state this failed read was meant to protect.
				console.error(
					`[Main] Failed to read persisted SRTP ROC state for port ${port}; releasing lock`,
				)
				await releaseLockBackoffAndRearm(
					port,
					slot,
					initialData,
					holdsSlotMutex,
					false,
					isStale,
				)
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
	await releaseLockBackoffAndRearm(
		port,
		slot,
		initialData,
		holdsSlotMutex,
		true,
		isStale,
	)
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
	 * ports, always true. For an SRTP port with no key loaded (SSM missing/failed - keys are
	 * resolved once at process start), the port can never ingest anything, so *nothing* is
	 * admitted: letting arbitrary datagrams through would mark the stream active, fill the
	 * pre-start buffer, and temporarily acquire the shared slot lock before pipeline startup
	 * rejects the missing key - denying the paired unencrypted port the slot it could
	 * legitimately use for that whole window. Datagrams on such a port are ignored
	 * entirely (no stream activity, no buffering, no lock attempts).
	 */
	const looksPlausibleRtp = (port: number, data: Buffer): boolean => {
		if (!kinesisIngestionPipeline || !rangeConfig.isSrtp) return true
		const ssrc = kinesisIngestionPipeline.getConfiguredSrtpSsrc(port)
		if (ssrc === undefined) return false
		return parseAuthenticRtpSequenceNumber(data, ssrc) !== undefined
	}

	/**
	 * Keeps a datagram whose processing observed ownership loss from being silently
	 * dropped: without this, a datagram that entered processPacket while the lock was
	 * still held - but whose turn awaited the handoff's slot-mutex section (or a
	 * relinquish) until after clearLockHeld - would be skipped by both the buffering
	 * path above (lock held when it was evaluated) and the writePacket tail below (lock
	 * no longer held), losing e.g. the first resumed keyframe/SPS/PPS. The per-port
	 * queue is strictly serial, so nothing newer was buffered meanwhile: append to the
	 * port's re-armed pre-start buffer when one exists (its chunks predate this
	 * datagram); otherwise hold it for the next start via writePacket's hold (the re-arm
	 * paths merge holds ahead of everything buffered later, so ordering is preserved
	 * either way).
	 */
	const keepOrphanedDatagram = (port: number, data: Buffer): void => {
		const buf = preStartBufferByPort.get(port)
		if (buf === undefined) {
			// No buffer armed (stream inactive edge cases): writePacket holds it when no
			// pipeline is active - and relays into a newer epoch's active pipeline if one
			// took over this slot in the meantime, which is correct ingestion either way.
			kinesisIngestionPipeline?.writePacket(port, data)
			return
		}
		buf.chunks.push(data)
		buf.totalBytes += data.length
		// Same bounded-cap eviction as the buffering block above (keeps the newest chunk).
		const maxBufferedBytes = rangeConfig.minBytesBeforeStart * 2
		while (buf.totalBytes > maxBufferedBytes && buf.chunks.length > 1) {
			const dropped = buf.chunks.shift()
			if (dropped !== undefined) buf.totalBytes -= dropped.length
		}
	}

	const processPacket = async (
		port: number,
		data: Buffer,
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
			// The newest chunk is always preserved (length > 1, like the other bounded
			// queues): with KINESIS_MIN_BYTES_BEFORE_START configured below half a
			// datagram's size - including zero - a "drop until under the cap" loop that
			// could empty the buffer would discard every packet right after buffering it,
			// so the threshold check starts the pipeline with empty initialData and the
			// first packet is lost.
			const maxBufferedBytes = rangeConfig.minBytesBeforeStart * 2
			while (buf.totalBytes > maxBufferedBytes && buf.chunks.length > 1) {
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
						// holdsSlotMutex: we are inside withSlotLock above - releaseLockBackoffAndRearm
						// must not take the slot's mutex again on its failure paths.
						await startPipelineOrReleaseLock(port, initialData, true)
					})
				} catch (err) {
					// A throw here is most plausibly tryAcquireKinesisLock's DynamoDB call
					// failing mid-outage - the buffer stays armed (it is only cleared once
					// acquisition succeeds), so without a backoff here every subsequent
					// packet would re-cross the already-met threshold and retry the
					// conditional write at packet rate, flooding DynamoDB and logs. Same
					// backoff as the acquired === false path.
					nextStartAttemptAllowedAtByPort.set(
						port,
						Date.now() + START_RETRY_BACKOFF_MS,
					)
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

			// Serialized against the final handoff persistence (persistSrtpRocForHandoff)
			// via the slot's mutex: tracking this datagram's ROC contribution and persisting
			// it must not interleave with the handoff's read+write - the handoff's retry
			// (this instance is still the lock owner, so its conditional write succeeds)
			// could otherwise overwrite the newer ROC this section just persisted, and the
			// slot's next owner would be seeded with a stale one. Re-check ownership inside:
			// the mutex wait can straddle this port losing the lock (e.g. to the handoff
			// holding the mutex), and continuing to write as a non-owner is exactly the
			// competing-producer problem the lock exists to prevent.
			await withSlotLock(slot, async () => {
				if (!kinesisLockHeldForPorts.has(port)) {
					// Ownership was lost while this packet's turn waited behind the
					// handoff's mutex section (see onStreamStop) - neither the buffering
					// path above (lock was held when it was evaluated) nor the
					// writePacket tail below (lock no longer held) would keep this
					// datagram. Route it so the first resumed keyframe/SPS/PPS is not
					// lost.
					keepOrphanedDatagram(port, data)
					return
				}

				// Track this datagram's ROC contribution BEFORE taking the snapshot persisted
				// below - otherwise a datagram that crosses the sequence-number wrap is only
				// tracked later (in writePacket), so the heartbeat persists the *previous* ROC,
				// and the 15s persistence throttle can keep "succeeding" without ever writing
				// the new one. A crash in that window would leave DynamoDB one ROC behind, and
				// the restarted pipeline would be seeded with stale state it can never
				// authenticate against (until the sender wraps again - effectively forever).
				// This is idempotent with writePacket's own tracking of the same datagram
				// (advanceSrtpRoc only ever raises the tracked state).
				const ssrc = kinesisIngestionPipeline.getConfiguredSrtpSsrc(port)
				const rocBefore =
					ssrc !== undefined
						? kinesisIngestionPipeline.getSrtpRocState(port)
						: undefined
				if (ssrc !== undefined) {
					kinesisIngestionPipeline.trackSrtpRoc(port, data)
				}

				try {
					await streamMetadataService.updateLastPacketTime(
						slot,
						// The *current* time, not this datagram's arrival time: the bounded
						// per-port queue can delay processPacket well behind actual arrival
						// (DynamoDB retries, credential resolution, the paced 10MB SRTP
						// startup replay), and lastPacketTime is the lock-freshness signal
						// another instance's tryAcquireKinesisLock compares against the
						// 5-minute staleness threshold - writing an arrival timestamp
						// older than that would let another instance take over the slot
						// while this listener is still receiving packets, briefly
						// creating two producers. True arrival-time receipt (for
						// inactivity detection) is recorded separately, immediately at
						// enqueue, via streamStateManager.onPacketReceived.
						new Date(),
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
						// This datagram arrived while the lock was held but is now
						// orphaned by the ownership loss - keep it (see
						// keepOrphanedDatagram).
						keepOrphanedDatagram(port, data)
						return
					}
					console.error(`[Main] Error updating DynamoDB for port ${port}:`, err)
				}

				// Persist the best-known SRTP rollover-tracking state alongside the heartbeat
				// (throttled internally, safe to call every packet) - see
				// KinesisIngestionPipeline.getSrtpRocState. If this reports we're no longer the
				// slot's owner (a condition-checked write), stop writing to Kinesis for a stream
				// we've lost the lock for instead of continuing as a competing producer. The one
				// event that must never wait out the throttle is a rollover counter *change*
				// (detected above, before this datagram was folded in) - force the write through
				// then, since that value is exactly what a restart needs to decrypt again.
				if (ssrc !== undefined) {
					const rocState = kinesisIngestionPipeline.getSrtpRocState(port)
					const keyFingerprint =
						kinesisIngestionPipeline.getConfiguredSrtpKeyFingerprint(port)
					const rolloverCrossed =
						rocBefore !== undefined && rocBefore.roc !== rocState.roc
					const result = await streamMetadataService.updateSrtpRoc(
						slot,
						instanceId,
						rocState.roc,
						rocState.highestSeq,
						ssrc,
						keyFingerprint,
						rolloverCrossed,
					)
					if (result === 'lostLock') {
						// Another instance owns the row - there is nothing left to release.
						await relinquishPort(port)
						// This datagram is orphaned by the ownership loss - keep it (see
						// keepOrphanedDatagram).
						keepOrphanedDatagram(port, data)
					} else if (result === 'writeError') {
						// The write failed without proving ownership loss: this producer
						// must still stop, but the row may still be ours. Unlike a lost
						// lock, the current packet's ROC advance may not have reached
						// DynamoDB - so run the same bounded, forced final persistence
						// every other handoff path uses (still under this slot's mutex -
						// the heartbeat section runs inside withSlotLock), so a transient
						// write that recovers during the handoff still lands the freshest
						// state before the release, instead of the next owner reading
						// stale state and failing SRTP decryption indefinitely.
						await persistSrtpRocForHandoff(port, slot)
						// Attempt a conditional release so another instance can take over
						// instead of waiting out the 5-minute staleness threshold with
						// nobody ingesting.
						await relinquishPort(port, { slot })
						keepOrphanedDatagram(port, data)
					}
				}
			})
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
		// Plausibility of this datagram for SRTP ports (see looksPlausibleRtp), computed
		// once and gating everything below. For a port whose lock is not held, an
		// implausible datagram is ignored *entirely*: not just for stream-activity
		// tracking, but also for the bounded per-port queue - enqueueing it would let a
		// flood of arbitrary UDP traffic consume the queue's byte cap and evict valid
		// packets, even though this path is documented as ignoring such traffic. Once
		// the lock is held, traffic is treated normally regardless - GStreamer/srtpdec's
		// own validation is the authority at that point, not this pre-lock noise filter.
		const isPlausibleRtp = looksPlausibleRtp(port, data)
		if (!isPlausibleRtp && !kinesisLockHeldForPorts.has(port)) return

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
		// still arriving, just not yet processed. (Implausible datagrams on an unlocked SRTP
		// port never reach this point - they were dropped above, before they could consume
		// the only isFirstPacket/isResume transition or queue capacity.)
		streamStateManager.onPacketReceived(port, timestamp)

		let state = queueStateByPort.get(port)
		if (!state) {
			state = { queue: [], totalBytes: 0, draining: false }
			queueStateByPort.set(port, state)
		}

		state.queue.push({ data, isFirstPacket, isResume })
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

				// Hold the slot's mutex across the *entire* handoff - the final ROC
				// persistence, the lock release, and clearing local ownership - not just
				// the persistence: the UDP listener is still active here, and a packet
				// entering processPacket() in the gap between a persistence-only mutex
				// section ending and the release landing could advance the ROC, then
				// have its own conditional updateSrtpRoc fail against the released
				// lock - losing a rollover crossing that the slot's next owner needed
				// to seed correctly. Under one critical section, a packet either fully
				// completes before the handoff (its state is folded into the
				// persistence's fence re-read and written again, never lost) or runs
				// after it, finds kinesisLockHeldForPorts no longer containing this
				// port (cleared below, in the same section), and skips persisting
				// entirely.
				//
				// clearLockHeld runs even if a call below throws - without this, a
				// thrown releaseKinesisLock left kinesisLockHeldForPorts permanently
				// (and wrongly) marked as held, since the line clearing it was never
				// reached.
				await withSlotLock(slot, async () => {
					// Ownership may have been lost while this section was queued for
					// the mutex (e.g. a relinquish inside a packet's bookkeeping
					// section) - there is then nothing left to hand off.
					if (!kinesisLockHeldForPorts.has(port)) return
					try {
						// Force (bypass the throttle) so the freshest state is
						// captured before this slot potentially gets reassigned to a
						// different port/instance, with bounded retries - handing the
						// slot off after a silently failed final write would let the
						// next owner seed a stale ROC (see persistSrtpRocForHandoff).
						await persistSrtpRocForHandoff(port, slot)

						await streamMetadataService.releaseKinesisLock(slot, instanceId)
					} finally {
						clearLockHeld(port)
					}
				})
			}

			// A packet can arrive and mark the stream active again while the teardown above
			// (particularly the DynamoDB calls) was still in flight - kinesisLockHeldForPorts
			// still reported this port as locally held at that moment, so that packet's own
			// processPacket run couldn't enter the buffering path, and writePacket() was a
			// no-op since stop() had already removed the pipeline... except that writePacket's
			// *hold* captured the datagram instead (see KinesisIngestionPipeline's
			// srtpHoldByPort). Merge that hold into the re-armed buffer, ahead of everything
			// buffered from here on: the held datagrams are the oldest pending data, and
			// leaving them in the hold would make the next start replay the newer buffered
			// chunks first and seed the older held packets into the new pipeline's
			// pendingQueue - newer sequence numbers overtake the older held keyframe/SPS/PPS,
			// which the jitter buffer/anti-replay window then discards. Same ordering the
			// failed-start path already preserves via takeHeldDatagrams. The re-armed packet
			// path retries lock acquisition even though the buffer won't carry a genuine
			// isFirstPacket/isResume marker (that was already consumed by the dropped one).
			if (
				streamStateManager.getStreamState(port)?.status === 'active' &&
				!preStartBufferByPort.has(port)
			) {
				const held = kinesisIngestionPipeline?.takeHeldDatagrams(port) ?? []
				preStartBufferByPort.set(port, {
					chunks: held,
					totalBytes: held.reduce((sum, chunk) => sum + chunk.length, 0),
				})
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
				// It can equally straddle the stream going inactive (whose stop releases
				// this port's lock) or the lock being lost outright (a failed conditional
				// write): without ownership there is nothing to restart, and proceeding
				// would spawn a producer this instance no longer holds the Kinesis lock
				// for. The post-start staleness check in startPipelineOrReleaseLock would
				// eventually stop it - but only after the full (expensive) start sequence
				// (credential resolution, spawn, paced 10MB replay), during which the
				// unowned producer is already writing to the stream, and a stale
				// invocation's stop can even take out a pipeline a newer ownership epoch
				// started. Revalidate both immediately before restarting.
				if (streamStateManager.getStreamState(port)?.status !== 'active') return
				if (!kinesisLockHeldForPorts.has(port)) return
				// ...and the delay can straddle the 30-second start backoff armed by a
				// failed start (releaseLockBackoffAndRearm): this exit path calls the
				// restart directly, so without this check an exit during startup - whose
				// failure just armed the backoff - would keep spawning and replaying on
				// the restart cadence instead of waiting it out. Persistent GStreamer
				// failures then retry no faster than the backoff allows; the re-armed
				// packet path retries automatically once it expires.
				if (Date.now() < (nextStartAttemptAllowedAtByPort.get(port) ?? 0)) {
					console.warn(
						`[Main] Skipping GStreamer restart for port ${port}: start backoff is still active`,
					)
					return
				}
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
/**
 * SRTP-transport readiness signal (see HealthServer): opened only once the SRTP keys are
 * loaded and the SRTP listener is bound, and health-checked by the SRTP target groups
 * (TCP 9998, see cdk/StreamingStack.ts). While it stays closed, SRTP target groups
 * report this instance unhealthy - for both the NLB and the fleet-cutover readiness gate.
 */
const srtpHealthServer = new HealthServer(9998)

// Graceful shutdown handler
const shutdown = async (): Promise<void> => {
	// First thing, before any of the awaits below: from here on, no new pipeline
	// start/restart may be initiated (see shuttingDown) while teardown drains queues and
	// stops producers.
	shuttingDown = true
	console.log('[Main] Shutting down...')

	await healthServer.stop()
	await srtpHealthServer.stop()
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
		// Same forced, retrying final persistence as onStreamStop's handoff - shutdown is
		// also a handoff (to whatever instance takes over the slot next) - wrapped in the
		// slot's mutex together with the release, exactly like onStreamStop's handoff:
		// sockets are stopped and queues drained by now, so nothing can contend, but
		// the two handoff paths stay structurally identical.
		await withSlotLock(slot, async () => {
			await persistSrtpRocForHandoff(port, slot)
			await streamMetadataService.releaseKinesisLock(slot, instanceId)
		})
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
		// The unencrypted listener starts first: during an SSM outage or throttling, the
		// SRTP key loading below can stall for minutes (SDK retries, connection
		// timeouts) - the existing ingest path must not wait on an additive dependency
		// of the SRTP ports alone.
		await udpListener.start()

		// Health ports open only after the transports they gate are usable (see
		// HealthServer): the NLB's target-group health checks and the fleet-cutover
		// readiness gate treat an open port as "this instance can ingest on this
		// transport", so they must never open before the paths behind them exist.
		// TCP 9999 gates the unencrypted path (listener bound above); TCP 9998
		// additionally gates the SRTP path (keys + listener below).
		await healthServer.start()

		let srtpKeysLoaded = false
		let allSrtpPortsKeyed = false
		const srtpPorts: number[] = []
		if (srtpEnabled && srtpKeyStore) {
			for (
				let port = config.srtpPortRange.start;
				port <= config.srtpPortRange.end;
				port++
			) {
				srtpPorts.push(port)
			}
			// Isolated from the outer try/catch: an SSM outage/throttling/permissions error
			// here must not abort startup of the unencrypted MPEG-TS path (5000-5009), which
			// does not depend on SRTP keys at all. SRTP ports will simply have no key loaded
			// (KinesisIngestionPipeline already logs and refuses ingestion per-port when that
			// happens - and index.ts's looksPlausibleRtp admits no traffic at all for a
			// keyless SRTP port, so nothing is buffered or lock-attempted for it) until this
			// is retried on the next restart.
			try {
				await srtpKeyStore.loadPorts(srtpPorts)
				srtpKeysLoaded = true
			} catch (err) {
				console.error(
					'[Main] Failed to load SRTP keys from SSM; SRTP ingestion will be unavailable until this is resolved. The unencrypted MPEG-TS path is unaffected. Error:',
					err,
				)
			}
			// "loadPorts did not throw" does not mean every configured port has a usable
			// key: SrtpKeyStore skips missing, non-SecureString, and malformed parameters
			// individually, and a keyless SRTP port discards every datagram it receives.
			// The shared SRTP readiness signal (port 9998, health-checked by all ten SRTP
			// target groups) may only open when EVERY configured port is keyed - otherwise
			// it would mark non-ingesting ports healthy and let the cutover gate switch
			// traffic onto them.
			allSrtpPortsKeyed =
				srtpKeysLoaded &&
				srtpPorts.every((port) => srtpKeyStore.hasKeyForPort(port))
			if (srtpKeysLoaded && !allSrtpPortsKeyed) {
				const keylessPorts = srtpPorts.filter(
					(port) => !srtpKeyStore.hasKeyForPort(port),
				)
				console.error(
					`[Main] SRTP keys are missing or invalid for ports ${keylessPorts.join(', ')} (skipped at load time); the SRTP readiness port stays closed so no SRTP traffic is routed to non-ingesting ports.`,
				)
			}
		}
		let srtpListenerUp = false
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
				srtpListenerUp = true
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

		// The SRTP readiness port opens only when the whole SRTP path is usable: keys
		// for EVERY configured port loaded (SrtpKeyStore skips bad parameters
		// individually, so "load did not throw" is not enough - see above), the SRTP
		// listener bound, and the SRTP GStreamer elements verified by user data
		// (SRTP_ELEMENTS_OK, warning-only there so an SRTP-only gap never takes the
		// unencrypted path down). If any of these failed (isolated above, so the
		// unencrypted path keeps serving), it stays closed - the SRTP target groups then
		// report this instance unhealthy, the fleet-cutover readiness gate blocks a
		// switch onto it, and the NLB never routes SRTP traffic to a non-ingesting
		// port, instead of any of them treating a Kinesis/SRTP-broken instance as
		// ingest-ready.
		if (srtpEnabled) {
			const srtpElementsOk = process.env.SRTP_ELEMENTS_OK === '1'
			if (
				srtpKeysLoaded &&
				allSrtpPortsKeyed &&
				srtpListenerUp &&
				srtpElementsOk
			) {
				await srtpHealthServer.start()
			} else {
				const reason = !srtpKeysLoaded
					? 'keys unavailable (SSM load failed)'
					: !allSrtpPortsKeyed
						? 'one or more configured ports have no usable key'
						: !srtpListenerUp
							? 'listener unavailable'
							: 'GStreamer SRTP elements missing'
				console.error(
					`[Main] SRTP ingestion is not ready (${reason}); the SRTP health port stays closed, so the SRTP target groups report this instance unhealthy.`,
				)
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
