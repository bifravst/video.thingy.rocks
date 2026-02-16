import { fromNodeProviderChain } from '@aws-sdk/credential-providers'
import { HealthServer } from './HealthServer.ts'
import { resolveInstanceId } from './InstanceId.ts'
import { KinesisIngestionPipeline } from './KinesisIngestionPipeline.ts'
import { StreamMetadataService } from './StreamMetadataService.ts'
import { StreamStateManager } from './StreamStateManager.ts'
import { UDPListener, type PacketHandler } from './UDPListener.ts'

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
 * - Listens for UDP packets on ports 5000-5009
 * - Tracks stream state (active/inactive)
 * - Updates DynamoDB with stream metadata
 * - Optionally sends UDP/MPEG-TS to Kinesis Video Streams (GStreamer (TS -> H.264) -> kvssink -> Kinesis Video)
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
	kinesisStreamPrefix: process.env.KINESIS_STREAM_PREFIX ?? '',
	kinesisIngestionEnabled: Boolean(process.env.KINESIS_STREAM_PREFIX),
	kinesisLogGstreamerOutput:
		process.env.KINESIS_INGESTION_LOG_GSTREAMER === 'true' ||
		process.env.KINESIS_INGESTION_LOG_GSTREAMER === '1',
	kinesisMinBytesBeforeStart: KINESIS_MIN_BYTES_BEFORE_START,
}

const streamStateManager = new StreamStateManager({
	inactivityTimeout: config.inactivityTimeout,
})

const streamMetadataService = new StreamMetadataService({
	tableName: config.dynamoDBTableName,
	region: config.awsRegion,
})

const kinesisIngestionPipeline = config.kinesisIngestionEnabled
	? new KinesisIngestionPipeline({
			streamNamePrefix: config.kinesisStreamPrefix,
			region: config.awsRegion,
			portRange: config.portRange,
			logGstreamerOutput: config.kinesisLogGstreamerOutput,
		})
	: null

/** Ports for which this instance holds the Kinesis lock (only holder may send to Kinesis). */
const kinesisLockHeldForPorts = new Set<number>()

/**
 * Per-port buffer of packets before we start GStreamer. We wait until at least
 * kinesisMinBytesBeforeStart (10 MB) to avoid treating port scans as video streams.
 */
const preStartBufferByPort = new Map<
	number,
	{ chunks: Buffer[]; totalBytes: number }
>()

/** Resolved at startup; used by packet handler for lock acquisition and DynamoDB updates. */
let instanceId = 'local'

const createPacketHandler = (): PacketHandler => ({
	onPacket: async (port, data, timestamp) => {
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

			if (buf.totalBytes >= config.kinesisMinBytesBeforeStart) {
				preStartBufferByPort.delete(port)
				const initialData = Buffer.concat(buf.chunks)
				packetAlreadyInInitialData = true
				try {
					const acquired = await streamMetadataService.tryAcquireKinesisLock(
						port,
						instanceId,
					)
					if (acquired) {
						kinesisLockHeldForPorts.add(port)
						await kinesisIngestionPipeline.start(port, initialData)
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
	},

	onStreamStart: async (port) => {
		console.log(`[Main] Stream started on port ${port}`)

		// Resume Kinesis pipeline only if we hold the lock (e.g. stream resume after brief inactivity)
		if (kinesisIngestionPipeline && kinesisLockHeldForPorts.has(port)) {
			void kinesisIngestionPipeline.start(port).catch((err) => {
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
})

const packetHandler = createPacketHandler()

// Set up stream state event handlers
streamStateManager.on('streamStart', (port: number) => {
	void packetHandler.onStreamStart(port).catch((err) => {
		console.error(`[Main] Error handling stream start for port ${port}:`, err)
	})
})

streamStateManager.on(
	'streamStop',
	(port: number, inactivityDuration: number) => {
		void packetHandler.onStreamStop(port, inactivityDuration).catch((err) => {
			console.error(`[Main] Error handling stream stop for port ${port}:`, err)
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
				void kinesisIngestionPipeline?.start(port).catch((err) => {
					console.error(
						`[Main] Error restarting Kinesis ingestion for port ${port}:`,
						err,
					)
				})
			}, delay)
		},
	)
}

// Initialize UDP listener
const udpListener = new UDPListener({
	portRange: config.portRange,
	bufferSize: config.bufferSize,
	flushInterval: config.flushInterval,
	outputDirectory: config.outputDirectory,
})

udpListener.setPacketHandler(packetHandler)

const healthServer = new HealthServer()

// Graceful shutdown handler
const shutdown = async (): Promise<void> => {
	console.log('[Main] Shutting down...')

	await healthServer.stop()
	await udpListener.stop()
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
			`[Main] Kinesis ingestion enabled (stream prefix: ${config.kinesisStreamPrefix})`,
		)
		console.log(
			`[Main] GStreamer starts after ${config.kinesisMinBytesBeforeStart / 1024 / 1024} MB received (KINESIS_MIN_BYTES_BEFORE_START)`,
		)
	} else {
		console.log(
			'[Main] Kinesis ingestion disabled (KINESIS_STREAM_PREFIX not set)',
		)
	}

	try {
		await ensureAwsCredentials()
		await healthServer.start()
		await udpListener.start()
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
