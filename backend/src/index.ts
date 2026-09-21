import { fromNodeProviderChain } from '@aws-sdk/credential-providers'

import { loadConfig } from './config.ts'
import { HealthServer } from './HealthServer.ts'
import { IngestionService, KinesisProducerAdapter } from './IngestionService.ts'
import { resolveInstanceId } from './InstanceId.ts'
import { KinesisIngestionPipeline } from './KinesisIngestionPipeline.ts'
import { Logger } from './Logger.ts'
import { StreamMetadataService } from './StreamMetadataService.ts'
import { StreamStateManager } from './StreamStateManager.ts'
import { UDPListener } from './UDPListener.ts'

/**
 * Entry point for the UDP video ingestion service.
 *
 * Receives UDP video on ports 5000-5009, tracks stream state in DynamoDB, and - when
 * a stream prefix is configured - ingests into Kinesis Video Streams through GStreamer
 * and kvssink. One port's ingestion lifecycle lives in PortIngestion; the wiring lives
 * in IngestionService. This file only resolves configuration and handles signals.
 */

const logger = new Logger('Main')

const ensureAwsCredentials = async (): Promise<void> => {
	const credentialProvider = fromNodeProviderChain({
		timeout: 10_000,
		maxRetries: 2,
	})
	try {
		await credentialProvider()
	} catch (err) {
		logger.error(
			'AWS credentials could not be loaded. The service needs credentials for DynamoDB (and for Kinesis if enabled). Locally: set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION (or use AWS_PROFILE). On EC2: ensure the instance has an IAM role and that IMDS is reachable (AWS_EC2_METADATA_DISABLED must be unset or false).',
			err instanceof Error ? err : new Error(String(err)),
		)
		throw err
	}
}

const buildService = (instanceId: string): IngestionService => {
	const config = loadConfig()

	const streamStateManager = new StreamStateManager({
		inactivityTimeout: config.inactivityTimeout,
	})
	const streamMetadataService = new StreamMetadataService({
		tableName: config.dynamoDBTableName,
		region: config.awsRegion,
	})
	const pipeline = config.kinesisIngestionEnabled
		? new KinesisIngestionPipeline({
				streamNamePrefix: config.kinesisStreamPrefix,
				region: config.awsRegion,
				portRange: config.portRange,
				logGstreamerOutput: config.kinesisLogGstreamerOutput,
			})
		: null

	logger.info('Starting UDP video ingestion service', {
		instanceId,
		ports: `${config.portRange.start}-${config.portRange.end}`,
		table: config.dynamoDBTableName,
		region: config.awsRegion,
		kinesisIngestion: config.kinesisIngestionEnabled
			? `enabled (stream prefix ${config.kinesisStreamPrefix}, starts after ${String(
					config.kinesisMinBytesBeforeStart / 1024 / 1024,
				)} MB)`
			: 'disabled (KINESIS_STREAM_PREFIX not set)',
	})

	return new IngestionService({
		config,
		instanceId,
		locks: streamMetadataService,
		activity: streamStateManager,
		producer: pipeline === null ? null : new KinesisProducerAdapter(pipeline),
		listener: new UDPListener({
			portRange: config.portRange,
			bufferSize: config.bufferSize,
			flushInterval: config.flushInterval,
			outputDirectory: config.outputDirectory,
		}),
		healthServer: new HealthServer(),
	})
}

const main = async (): Promise<void> => {
	const instanceId = await resolveInstanceId()
	const service = buildService(instanceId)

	let shuttingDown = false
	const shutdown = (signal: string): void => {
		if (shuttingDown) return
		shuttingDown = true
		logger.info('Shutting down', { signal })
		service
			.shutdown()
			.then(() => {
				process.exit(0)
			})
			.catch((err: unknown) => {
				logger.error(
					'Error during shutdown',
					err instanceof Error ? err : new Error(String(err)),
				)
				process.exit(1)
			})
	}
	process.on('SIGINT', () => shutdown('SIGINT'))
	process.on('SIGTERM', () => shutdown('SIGTERM'))

	await ensureAwsCredentials()
	await service.start()
}

if (import.meta.url === `file://${process.argv[1]}`) {
	void main().catch((err: unknown) => {
		logger.error(
			'Failed to start service',
			err instanceof Error ? err : new Error(String(err)),
		)
		process.exit(1)
	})
}
