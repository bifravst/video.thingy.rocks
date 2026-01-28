import { PacketBuffer } from './PacketBuffer.ts'
import { StreamMetadataService } from './StreamMetadataService.ts'
import { StreamStateManager } from './StreamStateManager.ts'
import { TranscodingManager } from './TranscodingManager.ts'
import { UDPListener, type PacketHandler } from './UDPListener.ts'

/**
 * Main entry point for the UDP video ingestion service.
 *
 * This service:
 * 1. Listens for UDP packets on ports 5000-5009
 * 2. Buffers packets and writes them to disk
 * 3. Tracks stream state (active/inactive)
 * 4. Updates DynamoDB with stream metadata
 * 5. Transcodes video to HLS format
 * 6. Uploads segments to S3
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
	s3Bucket: process.env.BUCKET_NAME ?? '',
	segmentDuration: 6, // 6 seconds for HLS segments
}

// Initialize components
const packetBuffer = new PacketBuffer({
	bufferSize: config.bufferSize,
	flushInterval: config.flushInterval,
	outputDirectory: config.outputDirectory,
})

const streamStateManager = new StreamStateManager({
	inactivityTimeout: config.inactivityTimeout,
})

const streamMetadataService = new StreamMetadataService({
	tableName: config.dynamoDBTableName,
	region: config.awsRegion,
})

// Initialize transcoding manager
const transcodingManager = new TranscodingManager({
	s3Bucket: config.s3Bucket,
	s3Region: config.awsRegion,
	streamMetadataService,
	segmentDuration: config.segmentDuration,
})

// Validate S3 bucket configuration
if (config.s3Bucket === '') {
	console.error('[Main] ERROR: BUCKET_NAME environment variable is required')
	process.exit(1)
}

// Set up packet handler
const packetHandler: PacketHandler = {
	onPacket: async (port, data, timestamp) => {
		// Buffer the packet (for backup/raw storage)
		void packetBuffer.addPacket(port, data, timestamp)

		// Send packet to transcoding manager
		transcodingManager.writeData(port, data)

		// Update stream state
		streamStateManager.onPacketReceived(port, timestamp)

		// Update DynamoDB with last packet time
		await streamMetadataService.updateLastPacketTime(port, timestamp)
	},

	onStreamStart: async (port) => {
		console.log(`[Main] Stream started on port ${port}`)

		// Start transcoding for this stream
		try {
			await transcodingManager.startTranscoding(port)
			console.log(`[Main] Transcoding started for port ${port}`)
		} catch (error) {
			console.error(
				`[Main] Failed to start transcoding for port ${port}:`,
				error,
			)
		}

		await streamMetadataService.updateStreamStatus(port, 'active')
	},

	onStreamStop: async (port, inactivityDuration) => {
		console.log(
			`[Main] Stream stopped on port ${port} after ${inactivityDuration}ms`,
		)

		// Stop transcoding for this stream
		try {
			await transcodingManager.stopTranscoding(port)
			console.log(`[Main] Transcoding stopped for port ${port}`)
		} catch (error) {
			console.error(
				`[Main] Error stopping transcoding for port ${port}:`,
				error,
			)
		}

		await streamMetadataService.updateStreamStatus(port, 'inactive')
	},
}

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

	// Stop transcoding manager (stops all pipelines)
	await transcodingManager.stop()

	await udpListener.stop()
	await packetBuffer.stop()
	streamStateManager.stop()

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
	console.log(`[Main] S3 bucket: ${config.s3Bucket}`)
	console.log(`[Main] AWS region: ${config.awsRegion}`)

	try {
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
	packetBuffer,
	streamMetadataService,
	streamStateManager,
	transcodingManager,
	udpListener,
}
