import { fromNodeProviderChain } from '@aws-sdk/credential-providers'
import { HealthServer } from './HealthServer.ts'
import { resolveInstanceId } from './InstanceId.ts'
import { KinesisIngestionPipeline } from './KinesisIngestionPipeline.ts'
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
 * Serializes async work per port. UDPListener dispatches onPacket() fire-and-forget (see
 * backend/src/UDPListener.ts), so without this, multiple onPacket calls for the same port
 * can run concurrently and interleave: a later packet's writePacket() call can overtake an
 * earlier one still awaiting bookkeeping (the throttled DynamoDB heartbeat, or the
 * lock-acquire + pipeline-start sequence) - corrupting SRTP packet order, or reaching
 * writePacket() before the pipeline has actually finished starting and silently dropping
 * the packet (no pipeline registered yet). Running each port's tasks one at a time, in
 * arrival order, closes both.
 */
const packetQueueTailByPort = new Map<number, Promise<void>>()

const runSerializedPerPort = async (
	port: number,
	task: () => Promise<void>,
): Promise<void> => {
	const previous = packetQueueTailByPort.get(port) ?? Promise.resolve()
	const next = previous
		.catch(() => {})
		.then(async () => {
			try {
				await task()
			} catch (err) {
				console.error(
					`[Main] Unhandled error processing packet for port ${port}:`,
					err,
				)
			}
		})
	packetQueueTailByPort.set(port, next)
	return next
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
	if (!kinesisIngestionPipeline) return

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
	if (!failed) {
		nextStartAttemptAllowedAtByPort.delete(port)
		return
	}

	console.error(
		`[Main] Kinesis ingestion pipeline failed to start for port ${port}; releasing lock`,
	)
	kinesisLockHeldForPorts.delete(port)
	try {
		await streamMetadataService.releaseKinesisLock(port, instanceId)
	} catch (err) {
		console.error(`[Main] Error releasing Kinesis lock for port ${port}:`, err)
	}

	// Back off before the next attempt for this port - without this, re-arming the buffer
	// below lets a persistent failure (missing key, bad credentials, missing plugin) retry
	// on literally the next packet, hammering DynamoDB and logs at line rate.
	nextStartAttemptAllowedAtByPort.set(port, Date.now() + START_RETRY_BACKOFF_MS)

	// Re-arm the pre-start buffer so subsequent packets can trigger another attempt once the
	// backoff above elapses. onPacket only re-enters the buffering/lock-acquire path while
	// isFirstPacket/isResume/preStartBufferByPort.has(port) holds - none of which are true
	// once the stream is already 'active' with no buffer entry, so without this a failed
	// start here would strand the port until a full inactivity/resume cycle happens on its own.
	const chunks =
		initialData === undefined
			? []
			: Array.isArray(initialData)
				? initialData
				: [initialData]
	const totalBytes = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
	preStartBufferByPort.set(port, { chunks, totalBytes })
}

const createPacketHandler = (
	rangeConfig: PacketHandlerRangeConfig,
): PacketHandler => {
	const processPacket = async (
		port: number,
		data: Buffer,
		timestamp: Date,
	): Promise<void> => {
		const streamState = streamStateManager.getStreamState(port)
		const isFirstPacket = streamState === undefined
		const isResume = streamState?.status === 'inactive'

		let packetAlreadyInInitialData = false
		// Buffer packets until we have enough data, then acquire Kinesis lock and start GStreamer.
		// This prevents port scans (small random payloads) from being treated as video streams.
		// Include preStartBufferByPort.has(port) so we keep buffering (and eventually try the lock)
		// on packets 2..N until we hit the threshold; otherwise we only enter on first packet.
		if (
			kinesisIngestionPipeline &&
			kinesisIngestionPipeline.isPortInRange(port) &&
			!kinesisLockHeldForPorts.has(port) &&
			(isFirstPacket || isResume || preStartBufferByPort.has(port))
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

			const backoffUntil = nextStartAttemptAllowedAtByPort.get(port) ?? 0
			if (
				buf.totalBytes >= rangeConfig.minBytesBeforeStart &&
				Date.now() >= backoffUntil
			) {
				preStartBufferByPort.delete(port)
				const initialData: Buffer | Buffer[] = rangeConfig.isSrtp
					? buf.chunks
					: Buffer.concat(buf.chunks)
				packetAlreadyInInitialData = true
				try {
					const acquired = await streamMetadataService.tryAcquireKinesisLock(
						port,
						instanceId,
					)
					if (acquired) {
						kinesisLockHeldForPorts.add(port)
						await startPipelineOrReleaseLock(port, initialData)
					}
				} catch (err) {
					console.error(
						`[Main] Error acquiring Kinesis lock / starting ingestion for port ${port}:`,
						err,
					)
				}
			}
		}

		streamStateManager.onPacketReceived(port, timestamp)

		// Only update DynamoDB lastPacketTime if we hold the lock
		if (kinesisLockHeldForPorts.has(port)) {
			try {
				await streamMetadataService.updateLastPacketTime(
					port,
					timestamp,
					instanceId,
				)
			} catch (err) {
				console.error(`[Main] Error updating DynamoDB for port ${port}:`, err)
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

	return {
		onPacket: async (port, data, timestamp) =>
			runSerializedPerPort(port, async () =>
				processPacket(port, data, timestamp),
			),

		onStreamStart: async (port) => {
			console.log(`[Main] Stream started on port ${port}`)

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

			preStartBufferByPort.delete(port)

			if (kinesisLockHeldForPorts.has(port)) {
				await streamMetadataService.releaseKinesisLock(port, instanceId)
				kinesisLockHeldForPorts.delete(port)
			}

			if (kinesisIngestionPipeline) {
				await kinesisIngestionPipeline.stop(port)
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
	console.log('[Main] Shutting down...')

	await healthServer.stop()
	await udpListener.stop()
	await srtpUdpListener?.stop()
	streamStateManager.stop()
	preStartBufferByPort.clear()
	for (const port of kinesisLockHeldForPorts) {
		await streamMetadataService.releaseKinesisLock(port, instanceId)
	}
	kinesisLockHeldForPorts.clear()
	if (kinesisIngestionPipeline) {
		await kinesisIngestionPipeline.stopAll()
	}

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
		await srtpUdpListener?.start()
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
