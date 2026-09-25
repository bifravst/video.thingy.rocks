import { fromNodeProviderChain } from '@aws-sdk/credential-providers'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { Logger } from './Logger.ts'
import type { SrtpHelperInit } from './SrtpHelperProtocol.ts'
import type { SrtpPortKey } from './SrtpKeyStore.ts'
import { SrtpKeyStore } from './SrtpKeyStore.ts'
import { SrtpPortSupervisor, type HelperProcess } from './SrtpPortSupervisor.ts'
import type { StreamMetadataService } from './StreamMetadataService.ts'
import { SRTP_TRANSPORT } from './TrafficMetricNames.ts'
import type { TrafficMetrics } from './TrafficMetrics.ts'

/**
 * The SRTP transport: one helper and one supervisor per keyed port.
 *
 * This is the additive transport, and everything about its lifecycle is shaped by
 * that: it is started only after the unencrypted path is serving, every failure
 * path in it can only ever disable SRTP ports, and its credentials, keys and
 * helpers are all resolved in its own isolated startup - an unreachable parameter
 * store disables this transport rather than the service.
 */

export type SrtpTransportEnv = {
	SRTP_KEY_PARAMETER_PREFIX?: string | undefined
	SRTP_PORT_RANGE_START?: string | undefined
	SRTP_PORT_RANGE_END?: string | undefined
}

export type SrtpTransportConfig = {
	/** SSM parameter prefix; full name is `{prefix}/{port}/key`. Enables the transport. */
	keyParameterPrefix: string
	portRange: { start: number; end: number }
	region: string
	instanceId: string
	/** Stream names for the SRTP ports, e.g. `{stack}-video-6000`. */
	streamNameForPort: (port: number) => string
	locks: Pick<
		StreamMetadataService,
		'tryAcquireKinesisLock' | 'releaseKinesisLock' | 'updateLastPacketTime'
	>
	floors: Pick<
		StreamMetadataService,
		'getSrtpIndexFloor' | 'raiseSrtpIndexFloor'
	>
	metrics: TrafficMetrics
	logger?: Logger
	helperPath?: string
	credentialProvider?: CredentialResolver
}

/** The minimal credential shape the helpers' kvssink needs, resolved up front. */
type ResolvedCredentials = {
	accessKeyId: string
	secretAccessKey: string
	sessionToken?: string
}

/** Anything that resolves AWS credentials, like fromNodeProviderChain(). */
type CredentialResolver = () => Promise<
	ResolvedCredentials & { expiration?: Date }
>

/** The unencrypted ports, which the SRTP range must never overlap. */
const UNENCRYPTED_PORT_RANGE = { start: 5000, end: 5009 } as const

/**
 * Resolves the SRTP transport configuration from the environment.
 *
 * The prefix being set is what enables the transport - there is deliberately no
 * separate boolean, because two sources of truth for "is this on" is how a path
 * ends up half configured.
 */
export const srtpTransportConfigFromEnv = (
	env: SrtpTransportEnv,
):
	| { keyParameterPrefix: string; portRange: { start: number; end: number } }
	| undefined => {
	const keyParameterPrefix = env.SRTP_KEY_PARAMETER_PREFIX?.trim()
	if (keyParameterPrefix === undefined || keyParameterPrefix === '')
		return undefined

	const start = Number(env.SRTP_PORT_RANGE_START ?? 6000)
	const end = Number(env.SRTP_PORT_RANGE_END ?? 6009)
	if (!Number.isInteger(start) || !Number.isInteger(end) || start > end) {
		throw new Error(
			`invalid SRTP port range ${String(start)}-${String(end)} (SRTP_PORT_RANGE_START/END)`,
		)
	}
	if (
		start <= UNENCRYPTED_PORT_RANGE.end &&
		end >= UNENCRYPTED_PORT_RANGE.start
	) {
		throw new Error(
			`the SRTP port range ${String(start)}-${String(end)} overlaps the unencrypted ports ${String(UNENCRYPTED_PORT_RANGE.start)}-${String(UNENCRYPTED_PORT_RANGE.end)}`,
		)
	}
	return { keyParameterPrefix, portRange: { start, end } }
}

const DEFAULT_HELPER_PATH = fileURLToPath(
	new URL('./srtp_port.py', import.meta.url),
)

/** Resolves AWS credentials for the helpers' kvssink, retrying like the unencrypted path. */
const resolveHelperCredentials = async (
	provider: CredentialResolver,
	logger: Logger,
): Promise<ResolvedCredentials | undefined> => {
	const maxAttempts = 3
	for (let attempt = 1; attempt <= maxAttempts; attempt++) {
		try {
			const credentials = await provider()
			return {
				accessKeyId: credentials.accessKeyId,
				secretAccessKey: credentials.secretAccessKey,
				sessionToken: credentials.sessionToken,
			}
		} catch (err) {
			logger.error(
				`Could not resolve AWS credentials for the SRTP helpers (attempt ${String(attempt)}/${String(maxAttempts)})`,
				err instanceof Error ? err : new Error(String(err)),
			)
			if (attempt === maxAttempts) return undefined
			await new Promise((resolve) => setTimeout(resolve, 2000))
		}
	}
	return undefined
}

export class SrtpTransport {
	private readonly config: SrtpTransportConfig
	private readonly logger: Logger
	private supervisors: SrtpPortSupervisor[] = []
	private started = false

	constructor(config: SrtpTransportConfig) {
		this.config = config
		this.logger = config.logger ?? new Logger('SrtpTransport')
	}

	/**
	 * Starts the transport. Every failure is contained: the return value says
	 * whether SRTP is serving, and nothing this does can take the unencrypted
	 * path, the health port, or the process down.
	 */
	async start(): Promise<boolean> {
		if (this.started) return true
		this.started = true
		try {
			const credentials = await resolveHelperCredentials(
				this.config.credentialProvider ??
					fromNodeProviderChain({ timeout: 10_000, maxRetries: 5 }),
				this.logger,
			)
			if (credentials === undefined) {
				this.logger.error(
					'SRTP transport disabled: no AWS credentials for the helpers',
				)
				return false
			}

			const keyStore = new SrtpKeyStore({
				region: this.config.region,
				parameterPrefix: this.config.keyParameterPrefix,
			})
			const ports = Array.from(
				{ length: this.config.portRange.end - this.config.portRange.start + 1 },
				(_, i) => this.config.portRange.start + i,
			)
			await keyStore.loadPorts(ports)
			const keyedPorts = keyStore.keyedPorts()
			if (keyedPorts.length === 0) {
				this.logger.warn(
					'SRTP transport has no usable keys; none of its ports will ingest (provision with scripts/provision-srtp-key.sh and restart the service)',
					{ ports },
				)
				return false
			}

			this.supervisors = keyedPorts.map((port) =>
				this.createSupervisor(
					port,
					keyStore.getKeyForPort(port) as SrtpPortKey,
					credentials,
				),
			)
			for (const supervisor of this.supervisors) supervisor.start()
			this.config.metrics.setServing(SRTP_TRANSPORT, true)
			this.logger.info('SRTP transport started', {
				keyedPorts,
				unkeyedPorts: ports.filter((p) => !keyedPorts.includes(p)),
			})
			return true
		} catch (err) {
			this.logger.error(
				'SRTP transport disabled: startup failed',
				err instanceof Error ? err : new Error(String(err)),
			)
			await this.stop()
			return false
		}
	}

	private createSupervisor(
		port: number,
		key: SrtpPortKey,
		credentials: ResolvedCredentials,
	): SrtpPortSupervisor {
		const config = this.config
		const metrics = config.metrics
		const logger = this.logger
		const lastInputBytes = new Map<number, number>()
		return new SrtpPortSupervisor({
			port,
			instanceId: config.instanceId,
			key,
			streamName: config.streamNameForPort(port),
			locks: config.locks,
			floors: config.floors,
			logger,
			onStats: (p, stats) => {
				// inputBytes is cumulative per helper session; the metric wants the
				// interval's delta.
				const last = lastInputBytes.get(p) ?? 0
				const delta =
					stats.inputBytes >= last ? stats.inputBytes - last : stats.inputBytes
				lastInputBytes.set(p, stats.inputBytes)
				if (delta > 0) metrics.recordReceived(SRTP_TRANSPORT, delta)
			},
			spawnHelper: (init: SrtpHelperInit) =>
				spawnHelperProcess(port, init, key, config, credentials, logger),
		})
	}

	/** Stops every port's supervision and waits for it to hold nothing. */
	async stop(): Promise<void> {
		this.config.metrics.setServing(SRTP_TRANSPORT, false)
		const stopping = this.supervisors.map(async (supervisor) =>
			supervisor.stop(),
		)
		const results = await Promise.allSettled(stopping)
		this.supervisors = []
		const errors = results
			.filter((r): r is PromiseRejectedResult => r.status === 'rejected')
			.map((r) => r.reason)
		if (errors.length > 0) {
			this.logger.error(
				'Errors while stopping the SRTP transport',
				new AggregateError(errors),
			)
		}
	}
}

/**
 * Spawns one helper process.
 *
 * The argument vector carries only public configuration - the port, the SSRC, the
 * stream and region - and the environment carries only what kvssink needs. The
 * key travels in the init frame on stdin, which the supervisor writes as soon as
 * the child exists, and nothing else: not argv (readable through
 * /proc/<pid>/cmdline for the pipeline's entire lifetime), not env, not logs.
 */
const spawnHelperProcess = (
	port: number,
	_init: SrtpHelperInit, // the supervisor writes it to the child's stdin itself
	key: SrtpPortKey,
	config: SrtpTransportConfig,
	credentials: ResolvedCredentials,
	logger: Logger,
): HelperProcess => {
	const helperPath = config.helperPath ?? DEFAULT_HELPER_PATH
	const child = spawn(
		'python3',
		[
			helperPath,
			'--port',
			String(port),
			'--ssrc',
			String(key.ssrc),
			'--stream-name',
			config.streamNameForPort(port),
			'--aws-region',
			config.region,
			'--kvs-log-config',
			process.env.KVS_LOG_CONFIG_PATH ??
				'/opt/video-streaming/kvs_log_configuration',
		],
		{
			stdio: ['pipe', 'pipe', 'pipe'],
			env: {
				// The helper's own environment: credentials for kvssink (the C++ SDK does
				// not use the Node provider chain), the region, and the plugin paths the
				// rest of the service already runs with. GST_DEBUG is pinned low because
				// anything above 3 makes GStreamer log caps, and this pipeline's caps carry
				// the key.
				AWS_ACCESS_KEY_ID: credentials.accessKeyId,
				AWS_SECRET_ACCESS_KEY: credentials.secretAccessKey,
				...(credentials.sessionToken === undefined
					? {}
					: { AWS_SESSION_TOKEN: credentials.sessionToken }),
				AWS_REGION: config.region,
				...(process.env.GST_PLUGIN_PATH === undefined
					? {}
					: { GST_PLUGIN_PATH: process.env.GST_PLUGIN_PATH }),
				...(process.env.LD_LIBRARY_PATH === undefined
					? {}
					: { LD_LIBRARY_PATH: process.env.LD_LIBRARY_PATH }),
				GST_DEBUG: '2',
			},
		},
	)
	// The child is a real process; the adapter exists so the supervisor depends on
	// the shape it can be given in tests, and so the nullable stdin of a failed
	// spawn is handled here rather than at every use. exitCode and signalCode are
	// getters: "is it gone" has to be asked live, not remembered from spawn time.
	const write = (line: string): void => {
		if (child.stdin === null) return
		try {
			child.stdin.write(line)
		} catch (err) {
			logger.warn('Could not write to the SRTP helper', {
				port,
				error: err instanceof Error ? err.message : String(err),
			})
		}
	}
	const helper: HelperProcess = {
		get exitCode() {
			return child.exitCode
		},
		get signalCode() {
			return child.signalCode
		},
		kill: (signal?: NodeJS.Signals) => child.kill(signal),
		on: (event: 'exit' | 'error', listener: () => void) =>
			child.on(event, listener),
		removeListener: (event: 'exit' | 'error', listener: () => void) => {
			child.removeListener(event, listener)
		},
		stdin: {
			write,
			end: () => child.stdin?.end(),
			on: (event: 'error', listener: (err: Error) => void) => {
				child.stdin?.on(event, listener)
			},
		},
		stdout: {
			on: (event: 'data', listener: (chunk: Buffer | string) => void) => {
				child.stdout?.on(event, listener)
			},
		},
		stderr: {
			on: (event: 'data', listener: (chunk: Buffer | string) => void) => {
				child.stderr?.on(event, listener)
			},
		},
	}
	logger.info('Spawned SRTP helper', { port, pid: child.pid })
	return helper
}
