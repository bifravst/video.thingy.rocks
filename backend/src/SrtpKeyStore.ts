import {
	GetParametersCommand,
	type GetParametersCommandOutput,
	SSMClient,
} from '@aws-sdk/client-ssm'
import { createHash } from 'node:crypto'
import { Logger } from './Logger.ts'

/** 16-byte AES key + 14-byte salt (SRTP master key+salt), hex-encoded = 60 hex chars. */
const HEX_KEY_PATTERN = /^[0-9a-fA-F]{60}$/

export const isValidSrtpKeyHex = (value: string): boolean =>
	HEX_KEY_PATTERN.test(value)

export type SrtpPortKey = {
	/**
	 * Hex-encoded SRTP master key+salt (30 bytes / 60 hex chars).
	 *
	 * The only permitted egress is the SRTP pipeline helper's in-process key callback.
	 * It must never reach a process argument vector (readable via /proc/<pid>/cmdline
	 * for the pipeline's whole lifetime), an environment variable, a GStreamer caps
	 * string built on a command line, a log line, or DynamoDB. Use keyFingerprint when
	 * an identity for the key is needed.
	 */
	keyHex: string
	ssrc: number
	cipher: string
	auth: string
	/** Non-secret fingerprint of keyHex (see keyFingerprint) - safe to persist/log, unlike
	 * keyHex itself. */
	keyFingerprint: string
}

/**
 * A non-secret, deterministic fingerprint of an SRTP key, used to detect a key rotation that
 * keeps the same SSRC (the provisioning script permits this) - see StreamMetadata's
 * srtpIndexKeyFingerprint doc comment for why SSRC alone can't catch that case. SHA-256 is a
 * one-way function, so this reveals nothing about keyHex; truncated since it only needs to
 * distinguish keys from each other, not resist adversarial collision search. Normalizes case
 * first - upper- and lower-case hex encode the same key bytes, so hashing the raw string
 * would make reprovisioning the identical key with different casing look like a rotation and
 * reset the replay floor, reopening the recording of every earlier session to replay.
 */
export const keyFingerprint = (keyHex: string): string =>
	createHash('sha256').update(keyHex.toLowerCase()).digest('hex').slice(0, 16)

export type SrtpKeyStoreConfig = {
	region?: string
	/** SSM parameter name prefix, e.g. "/{stackName}/srtp/port". Full name is "{prefix}/{port}/key". */
	parameterPrefix: string
	/**
	 * SSM client to fetch parameters with - injectable for tests (see the loadPorts spec);
	 * production callers omit it and get a real SSMClient for the configured region.
	 */
	ssmClient?: SsmParameterFetcher
}

/**
 * The minimal SSM surface loadPorts needs. A real SSMClient satisfies this structurally;
 * the spec passes a stub recording GetParametersCommand inputs instead.
 */
export type SsmParameterFetcher = {
	send: (command: GetParametersCommand) => Promise<GetParametersCommandOutput>
}

type StoredSrtpKey = {
	key: string
	ssrc: number
	cipher?: string
	auth?: string
}

/**
 * The only cipher/auth suite validated end-to-end (matches the 60-hex-char/30-byte key
 * format enforced by isValidSrtpKeyHex below - a different suite, e.g. aes-256-icm, needs a
 * different key length, so accepting an arbitrary cipher/auth string here would let a
 * mismatched key/cipher pair through and fail later inside GStreamer instead of at load time).
 */
const SUPPORTED_SRTP_CIPHER = 'aes-128-icm'
const SUPPORTED_SRTP_AUTH = 'hmac-sha1-80'

/** RTP SSRC is a 32-bit unsigned integer. */
const MAX_UINT32 = 0xffffffff

/** AWS SSM GetParameters accepts at most 10 names per request. */
const SSM_GET_PARAMETERS_MAX_NAMES = 10

export const isStoredSrtpKey = (value: unknown): value is StoredSrtpKey => {
	if (typeof value !== 'object' || value === null) return false
	const v = value as Record<string, unknown>

	if (typeof v.key !== 'string') return false

	if (
		typeof v.ssrc !== 'number' ||
		!Number.isInteger(v.ssrc) ||
		v.ssrc < 0 ||
		v.ssrc > MAX_UINT32
	) {
		return false
	}

	if (v.cipher !== undefined && v.cipher !== SUPPORTED_SRTP_CIPHER) return false
	if (v.auth !== undefined && v.auth !== SUPPORTED_SRTP_AUTH) return false

	return true
}

/**
 * Resolves static, pre-shared SRTP keys from SSM Parameter Store (SecureString), one per port.
 * Keys are provisioned out-of-band (see scripts/provision-srtp-key.sh) and resolved once at
 * process start; there is no rotation/live-reload, matching the "static key" requirement.
 */
export class SrtpKeyStore {
	private readonly client: SsmParameterFetcher
	private readonly config: SrtpKeyStoreConfig
	private readonly logger: Logger
	private readonly keysByPort: Map<number, SrtpPortKey> = new Map()

	constructor(config: SrtpKeyStoreConfig) {
		this.config = config
		this.client =
			config.ssmClient ??
			new SSMClient({ region: config.region ?? 'eu-central-1' })
		this.logger = new Logger('SrtpKeyStore')
	}

	private parameterNameForPort(port: number): string {
		return `${this.config.parameterPrefix}/${port}/key`
	}

	/**
	 * Fetches and validates keys for the given ports. Ports whose parameter is missing or
	 * invalid are logged and skipped (not thrown) so a single bad port doesn't block startup
	 * for the rest; callers should treat a port with no entry in keysByPort as unconfigured.
	 */
	async loadPorts(ports: number[]): Promise<void> {
		for (let i = 0; i < ports.length; i += SSM_GET_PARAMETERS_MAX_NAMES) {
			await this.loadPortsChunk(
				ports.slice(i, i + SSM_GET_PARAMETERS_MAX_NAMES),
			)
		}
	}

	/** Loads at most SSM_GET_PARAMETERS_MAX_NAMES ports in a single GetParameters call. */
	private async loadPortsChunk(ports: number[]): Promise<void> {
		if (ports.length === 0) return

		const names = ports.map((port) => this.parameterNameForPort(port))
		const response = await this.client.send(
			new GetParametersCommand({
				Names: names,
				WithDecryption: true,
			}),
		)

		for (const invalidName of response.InvalidParameters ?? []) {
			this.logger.warn('SRTP key parameter not found', {
				parameterName: invalidName,
			})
		}

		const paramsByName = new Map<string, string>()
		for (const parameter of response.Parameters ?? []) {
			if (parameter.Name === undefined || parameter.Value === undefined)
				continue
			// GetParameters returns plain String parameters too - WithDecryption only means
			// "decrypt if applicable", not "only return SecureStrings" - so an SRTP key that
			// was accidentally provisioned without encryption at rest must be rejected here,
			// not silently accepted and logged as loaded.
			if (parameter.Type !== 'SecureString') {
				this.logger.error(
					'SRTP key parameter is not a SecureString; refusing to use it (re-provision with scripts/provision-srtp-key.sh, which always uses --type SecureString)',
					new Error('SRTP key parameter is not a SecureString'),
					{
						parameterName: parameter.Name,
						actualType: parameter.Type ?? 'unknown',
					},
				)
				continue
			}
			paramsByName.set(parameter.Name, parameter.Value)
		}

		for (const port of ports) {
			const name = this.parameterNameForPort(port)
			const rawValue = paramsByName.get(name)
			if (rawValue === undefined) continue

			let parsed: unknown
			try {
				parsed = JSON.parse(rawValue)
			} catch {
				this.logger.error(
					'SRTP key parameter is not valid JSON',
					new Error('JSON parse failed'),
					{ port, parameterName: name },
				)
				continue
			}

			if (!isStoredSrtpKey(parsed)) {
				this.logger.error(
					`SRTP key parameter is invalid: needs a string "key", a uint32 "ssrc", and, if present, "cipher"/"auth" must be exactly "${SUPPORTED_SRTP_CIPHER}"/"${SUPPORTED_SRTP_AUTH}" (the only suite supported end-to-end)`,
					new Error('Invalid SRTP key parameter shape'),
					{ port, parameterName: name },
				)
				continue
			}

			if (!isValidSrtpKeyHex(parsed.key)) {
				this.logger.error(
					'SRTP key is not 60 hex characters (30-byte master key+salt); refusing to use it',
					new Error('Invalid SRTP key format'),
					{ port, parameterName: name },
				)
				continue
			}

			this.keysByPort.set(port, {
				keyHex: parsed.key,
				ssrc: parsed.ssrc,
				cipher: parsed.cipher ?? SUPPORTED_SRTP_CIPHER,
				auth: parsed.auth ?? SUPPORTED_SRTP_AUTH,
				keyFingerprint: keyFingerprint(parsed.key),
			})
			this.logger.info('Loaded SRTP key', { port })
		}
	}

	getKeyForPort(port: number): SrtpPortKey | undefined {
		return this.keysByPort.get(port)
	}

	hasKeyForPort(port: number): boolean {
		return this.keysByPort.has(port)
	}

	/**
	 * Ports that actually have a usable key, in ascending order.
	 *
	 * loadPorts resolving does not mean every configured port is keyed: missing,
	 * non-SecureString and malformed parameters are skipped individually. Callers that
	 * need to know which ports can ingest ask this rather than inferring it.
	 */
	keyedPorts(): number[] {
		return [...this.keysByPort.keys()].sort((a, b) => a - b)
	}
}
