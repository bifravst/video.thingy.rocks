export type PortRange = { start: number; end: number }

export type SrtpConfig = {
	/** UDP ports that expect SRTP-encrypted RTP, one stream per port. */
	portRange: PortRange
	/** SSM parameter name prefix; the full name is "{prefix}/{port}/key". */
	keyParameterPrefix: string
}

export type IngestionConfig = {
	portRange: PortRange
	bufferSize: number
	flushInterval: number
	outputDirectory: string
	transcodingOutputDirectory: string
	inactivityTimeout: number
	dynamoDBTableName: string
	awsRegion: string
	segmentDuration: number
	kinesisStreamPrefix: string
	kinesisIngestionEnabled: boolean
	kinesisLogGstreamerOutput: boolean
	kinesisMinBytesBeforeStart: number
	/** Undefined when SRTP ingestion is not configured; the path is entirely additive. */
	srtp?: SrtpConfig
}

/**
 * Parses an explicit opt-in flag.
 *
 * Boolean(envValue) is wrong for this: every non-empty string is truthy, so the
 * documented values "false" and "0" would enable the flag they are meant to disable.
 * Enablement that is derived from a value being *present* (a stream prefix, a parameter
 * prefix) is expressed by checking that value instead of adding a boolean beside it.
 */
const isEnabled = (value: string | undefined): boolean =>
	value === 'true' || value === '1'

const portCount = (range: PortRange): number => range.end - range.start + 1

const parsePort = (value: string, label: string): number => {
	if (!/^(0|[1-9][0-9]*)$/.test(value)) {
		throw new Error(
			`Invalid configuration: ${label} must be a decimal port number, got "${value}"`,
		)
	}
	const port = Number(value)
	if (port < 1 || port > 65535) {
		throw new Error(
			`Invalid configuration: ${label} must be between 1 and 65535, got ${port}`,
		)
	}
	return port
}

const rangesOverlap = (a: PortRange, b: PortRange): boolean =>
	a.start <= b.end && b.start <= a.end

/**
 * Reads the SRTP configuration, if any.
 *
 * SRTP is enabled by the presence of its key parameter prefix - there is no separate
 * boolean, because two sources of truth for "is this on" is how a path ends up half
 * configured. An invalid range throws at startup rather than misrouting packets later:
 * a range overlapping the unencrypted ports cannot bind, and would also make the
 * transport of a received datagram ambiguous.
 */
const loadSrtpConfig = (
	env: NodeJS.ProcessEnv,
	mainPortRange: PortRange,
): SrtpConfig | undefined => {
	const keyParameterPrefix = env.SRTP_KEY_PARAMETER_PREFIX ?? ''
	if (keyParameterPrefix.length === 0) return undefined

	const start = parsePort(
		env.SRTP_PORT_RANGE_START ?? '6000',
		'SRTP_PORT_RANGE_START',
	)
	const end = parsePort(
		env.SRTP_PORT_RANGE_END ?? '6009',
		'SRTP_PORT_RANGE_END',
	)
	const portRange = { start, end }

	if (end < start) {
		throw new Error(
			`Invalid configuration: SRTP_PORT_RANGE_END (${end}) must not be below SRTP_PORT_RANGE_START (${start})`,
		)
	}
	if (rangesOverlap(portRange, mainPortRange)) {
		throw new Error(
			`Invalid configuration: the SRTP port range (${start}-${end}) must not overlap the unencrypted port range (${mainPortRange.start}-${mainPortRange.end})`,
		)
	}

	return { portRange, keyParameterPrefix }
}

/**
 * Builds the service configuration from the environment.
 *
 * `env` is a parameter so this is testable without mutating process.env; production
 * callers use the default.
 */
export const loadConfig = (
	env: NodeJS.ProcessEnv = process.env,
): IngestionConfig => {
	const portRange = { start: 5000, end: 5009 }

	const minBytesRaw = env.KINESIS_MIN_BYTES_BEFORE_START ?? '10'
	const minBytesMb = Number(minBytesRaw)
	if (!Number.isFinite(minBytesMb) || minBytesMb < 0) {
		throw new Error(
			`Invalid configuration: KINESIS_MIN_BYTES_BEFORE_START must be a non-negative number of megabytes, got "${minBytesRaw}"`,
		)
	}

	const kinesisStreamPrefix = env.KINESIS_STREAM_PREFIX ?? ''

	return {
		portRange,
		bufferSize: 1024 * 1024, // 1MB
		flushInterval: 5000, // 5 seconds
		outputDirectory: env.OUTPUT_DIR ?? '/tmp/video-streams',
		transcodingOutputDirectory:
			env.TRANSCODING_OUTPUT_DIR ?? '/tmp/video-streams/transcoding',
		inactivityTimeout: 60000, // 1 minute
		dynamoDBTableName: env.TABLE_NAME ?? 'StreamMetadata',
		awsRegion: env.AWS_REGION ?? 'eu-central-1',
		segmentDuration: 6, // 6 seconds for HLS segments
		kinesisStreamPrefix,
		// Enabled by the prefix being set: the stream names are derived from it, so
		// there is nothing to ingest into without it.
		kinesisIngestionEnabled: kinesisStreamPrefix.length > 0,
		kinesisLogGstreamerOutput: isEnabled(env.KINESIS_INGESTION_LOG_GSTREAMER),
		kinesisMinBytesBeforeStart: minBytesMb * 1024 * 1024,
		srtp: loadSrtpConfig(env, portRange),
	}
}

export { portCount }
