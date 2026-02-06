import { fromNodeProviderChain } from '@aws-sdk/credential-providers'
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
		})
	: null

// Set up packet handler
const packetHandler: PacketHandler = {
	onPacket: async (port, data, timestamp) => {
		// Start Kinesis pipeline on first packet; pass this packet so it is written to stdin immediately after spawn (fdsrc needs data when it first reads).
		const isFirstPacket = streamStateManager.getStreamState(port) === undefined
		if (
			kinesisIngestionPipeline &&
			isFirstPacket &&
			kinesisIngestionPipeline.isPortInRange(port)
		) {
			try {
				await kinesisIngestionPipeline.start(port, data)
			} catch (err) {
				console.error(
					`[Main] Error starting Kinesis ingestion for port ${port}:`,
					err,
				)
			}
		}

		// Update stream state
		streamStateManager.onPacketReceived(port, timestamp)

		// Update DynamoDB with last packet time
		await streamMetadataService.updateLastPacketTime(port, timestamp)

		// Feed packet to Kinesis ingestion (GStreamer -> kvssink) if enabled (first packet was already written in start(port, data)).
		if (kinesisIngestionPipeline) {
			const skipFirst =
				isFirstPacket && kinesisIngestionPipeline.isPortInRange(port)
			if (!skipFirst) {
				kinesisIngestionPipeline.writePacket(port, data)
			}
		}
	},

	onStreamStart: async (port) => {
		console.log(`[Main] Stream started on port ${port}`)

		await streamMetadataService.updateStreamStatus(port, 'active')

		// Pipeline already started on first packet; start here for stream resume
		if (kinesisIngestionPipeline) {
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

		await streamMetadataService.updateStreamStatus(port, 'inactive')

		if (kinesisIngestionPipeline) {
			await kinesisIngestionPipeline.stop(port)
		}
	},
}

// Set up stream state event handlers
streamStateManager.on('streamStart', (port: number) => {
	void packetHandler.onStreamStart(port).catch((err) => {
		console.error(`[Main] Error handling stream start for port ${port}:`, err)
	})
})

streamStateManager.on('streamResume', (port: number) => {
	// Restart Kinesis pipeline when stream resumes after inactivity
	if (kinesisIngestionPipeline) {
		void kinesisIngestionPipeline.start(port).catch((err) => {
			console.error(
				`[Main] Error starting Kinesis ingestion on resume for port ${port}:`,
				err,
			)
		})
	}
})

streamStateManager.on(
	'streamStop',
	(port: number, inactivityDuration: number) => {
		void packetHandler.onStreamStop(port, inactivityDuration).catch((err) => {
			console.error(`[Main] Error handling stream stop for port ${port}:`, err)
		})
	},
)

// Initialize UDP listener
const udpListener = new UDPListener({
	portRange: config.portRange,
	bufferSize: config.bufferSize,
	flushInterval: config.flushInterval,
	outputDirectory: config.outputDirectory,
})

udpListener.setPacketHandler(packetHandler)

// Graceful shutdown handler
const shutdown = async (): Promise<void> => {
	console.log('[Main] Shutting down...')

	await udpListener.stop()
	streamStateManager.stop()
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
	console.log('[Main] Starting UDP video ingestion service...')
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
	} else {
		console.log(
			'[Main] Kinesis ingestion disabled (KINESIS_STREAM_PREFIX not set)',
		)
	}

	try {
		await ensureAwsCredentials()
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
