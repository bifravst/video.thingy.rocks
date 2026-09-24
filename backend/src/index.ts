import { fromNodeProviderChain } from '@aws-sdk/credential-providers'

import { loadConfig } from './config.ts'
import { HealthServer } from './HealthServer.ts'
import {
	IngestionService,
	KinesisProducerAdapter,
	type IngestionTransport,
} from './IngestionService.ts'
import { resolveInstanceId } from './InstanceId.ts'
import { KinesisIngestionPipeline } from './KinesisIngestionPipeline.ts'
import { Logger } from './Logger.ts'
import { createSrtpAdmissionFilter } from './SrtpAdmission.ts'
import { SrtpKeyStore } from './SrtpKeyStore.ts'
import { SrtpProducer } from './SrtpProducer.ts'
import { StreamMetadataService } from './StreamMetadataService.ts'
import { StreamStateManager } from './StreamStateManager.ts'
import { SRTP_TRANSPORT, UNENCRYPTED_TRANSPORT } from './TrafficMetricNames.ts'
import {
	cloudWatchTrafficPublisher,
	TransportTrafficMetrics,
} from './TransportTrafficMetrics.ts'
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

/** Where kvssink writes its own log configuration on the instances. */
const KVS_LOG_CONFIG_PATH = '/opt/video-streaming/kvs_log_configuration'
/**
 * How long an SRTP port may keep receiving traffic without authenticating any of it.
 *
 * A pipeline has to exist before anything can be authenticated, so this window is
 * unavoidable; a port that never authenticates gives the slot back and waits out a
 * longer cooldown before trying again.
 *
 * Measured against arriving datagrams, not wall clock. PortIngestion has no timers of
 * its own, so the deadline is compared against the clock when the next datagram is
 * handled - see its class comment for why. A burst that starts a pipeline and then
 * stops is therefore released by the inactivity timeout below rather than by this
 * window, which is the longer of the two.
 */
const SRTP_PROVISIONAL_MS = 20_000
const SRTP_PROVISIONAL_COOLDOWN_MS = 60_000

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

	/**
	 * The per-transport traffic reporter, or nothing when it cannot be namespaced.
	 *
	 * Built from the final transport list so that every transport reports a value each
	 * minute, including a zero - which is what lets the alarms tell "no devices are
	 * sending" from "this instance is not reporting".
	 */
	const buildTraffic = (
		names: string[],
	): TransportTrafficMetrics | undefined => {
		if (config.stackName === undefined) {
			logger.warn(
				'STACK_NAME is not set, so per-transport traffic metrics are not reported; the zero-ingestion alarms depend on them',
			)
			return undefined
		}
		return new TransportTrafficMetrics({
			transports: names,
			publish: cloudWatchTrafficPublisher({
				stackName: config.stackName,
				region: config.awsRegion,
			}),
		})
	}

	const transports: IngestionTransport[] = [
		{
			name: UNENCRYPTED_TRANSPORT,
			portRange: config.portRange,
			listener: new UDPListener({
				portRange: config.portRange,
				bufferSize: config.bufferSize,
				flushInterval: config.flushInterval,
				outputDirectory: config.outputDirectory,
			}),
			producer: pipeline === null ? null : new KinesisProducerAdapter(pipeline),
		},
	]

	// Additive: SRTP needs its own ports, its own streams and its own keys, and
	// nothing about it can stop the transport above from running.
	if (config.srtp !== undefined && pipeline !== null) {
		const srtp = config.srtp
		const keyStore = new SrtpKeyStore({
			region: config.awsRegion,
			parameterPrefix: srtp.keyParameterPrefix,
		})
		const ports: number[] = []
		for (let port = srtp.portRange.start; port <= srtp.portRange.end; port++) {
			ports.push(port)
		}

		// The producer has to exist before the service that owns its ports, and it
		// needs to reach back into that service to report authentication - so the
		// reference is filled in once the service exists.
		const serviceRef: { current?: IngestionService } = {}
		const producer = new SrtpProducer({
			keyStore,
			hints: streamMetadataService,
			region: config.awsRegion,
			streamNameForPort: (port) =>
				`${config.kinesisStreamPrefix}-${String(port)}`,
			kvsLogConfigPath: KVS_LOG_CONFIG_PATH,
			onAuthenticated: (port: number, epoch: number) => {
				serviceRef.current?.authenticated(port, epoch)
			},
		})

		transports.push({
			name: SRTP_TRANSPORT,
			portRange: srtp.portRange,
			// Resolved when this transport starts, which is after credentials have been
			// verified and after the unencrypted listener is already serving: an
			// unreachable or throttled parameter store can stall for minutes, and the
			// existing ingest path does not depend on SRTP keys at all. A failure here
			// leaves SRTP ports unkeyed - they then admit nothing - and is isolated by
			// IngestionService, so the service keeps running.
			prepare: async () => {
				await keyStore.loadPorts(ports)
				logger.info('SRTP keys loaded', {
					configured: ports.length,
					keyed: keyStore.keyedPorts().length,
				})
			},
			listener: new UDPListener({
				portRange: srtp.portRange,
				bufferSize: config.bufferSize,
				flushInterval: config.flushInterval,
				outputDirectory: config.outputDirectory,
			}),
			producer,
			provisionalTimeoutMs: SRTP_PROVISIONAL_MS,
			provisionalCooldownMs: SRTP_PROVISIONAL_COOLDOWN_MS,
			admitFor: (port: number) =>
				createSrtpAdmissionFilter(
					{ ssrcForPort: (p) => keyStore.getKeyForPort(p)?.ssrc },
					port,
				),
		})

		serviceRef.current = new IngestionService({
			config,
			instanceId,
			locks: streamMetadataService,
			activity: streamStateManager,
			transports,
			healthServer: new HealthServer(),
			traffic: buildTraffic(transports.map((t) => t.name)),
		})
		return serviceRef.current
	}

	return new IngestionService({
		config,
		instanceId,
		locks: streamMetadataService,
		activity: streamStateManager,
		transports,
		healthServer: new HealthServer(),
		traffic: buildTraffic(transports.map((t) => t.name)),
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
