import { GetParametersCommand, SSMClient } from '@aws-sdk/client-ssm'
import { Logger } from './Logger.ts'

/** 16-byte AES key + 14-byte salt (SRTP master key+salt), hex-encoded = 60 hex chars. */
const HEX_KEY_PATTERN = /^[0-9a-fA-F]{60}$/

export const isValidSrtpKeyHex = (value: string): boolean =>
	HEX_KEY_PATTERN.test(value)

export type SrtpPortKey = {
	/** Hex-encoded SRTP master key+salt (30 bytes / 60 hex chars). */
	keyHex: string
	ssrc: number
	cipher: string
	auth: string
}

export type SrtpKeyStoreConfig = {
	region?: string
	/** SSM parameter name prefix, e.g. "/{stackName}/srtp/port". Full name is "{prefix}/{port}/key". */
	parameterPrefix: string
}

type StoredSrtpKey = {
	key: string
	ssrc: number
	cipher?: string
	auth?: string
}

const DEFAULT_CIPHER = 'aes-128-icm'
const DEFAULT_AUTH = 'hmac-sha1-80'

const isStoredSrtpKey = (value: unknown): value is StoredSrtpKey => {
	if (typeof value !== 'object' || value === null) return false
	const v = value as Record<string, unknown>
	return typeof v.key === 'string' && typeof v.ssrc === 'number'
}

/**
 * Resolves static, pre-shared SRTP keys from SSM Parameter Store (SecureString), one per port.
 * Keys are provisioned out-of-band (see scripts/provision-srtp-key.sh) and resolved once at
 * process start; there is no rotation/live-reload, matching the "static key" requirement.
 */
export class SrtpKeyStore {
	private readonly client: SSMClient
	private readonly config: SrtpKeyStoreConfig
	private readonly logger: Logger
	private readonly keysByPort: Map<number, SrtpPortKey> = new Map()

	constructor(config: SrtpKeyStoreConfig) {
		this.config = config
		this.client = new SSMClient({ region: config.region ?? 'eu-central-1' })
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

		const paramsByName = new Map(
			(response.Parameters ?? []).map((p) => [p.Name, p.Value] as const),
		)

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
					'SRTP key parameter missing required fields (key, ssrc)',
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
				cipher: parsed.cipher ?? DEFAULT_CIPHER,
				auth: parsed.auth ?? DEFAULT_AUTH,
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
}
