import { spawn as nodeSpawn, type ChildProcess } from 'node:child_process'
import dgram from 'node:dgram'

import type { ExitingProducer } from './IngestionService.ts'
import { Logger } from './Logger.ts'
import {
	isRetryableFatal,
	SRTP_HELPER_PROTOCOL_VERSION,
	SrtpHelperProtocol,
	type SrtpHelperMessage,
} from './SrtpHelperProtocol.ts'
import type { SrtpKeyStore } from './SrtpKeyStore.ts'

/** How long the helper gets to report the port it bound before the start fails. */
const READY_TIMEOUT_MS = 10_000
/** How long SIGTERM gets to end the stream cleanly before SIGKILL follows it. */
const STOP_SIGKILL_AFTER_MS = 8_000
/** Datagrams held while the helper is starting, so the buffer's order survives. */
const DEFAULT_PENDING_MAX_BYTES = 8 * 1024 * 1024
/** Paced replay, so a large startup buffer does not arrive as one burst. */
const REPLAY_BATCH = 50
const REPLAY_PAUSE_MS = 10

export type SrtpRocHintStore = {
	getSrtpRocHint(
		port: number,
		expectedSsrc: number,
		expectedKeyFingerprint: string,
	): Promise<number | undefined>
	putSrtpRocHint(
		port: number,
		roc: number,
		ssrc: number,
		keyFingerprint: string,
	): Promise<void>
}

export type SrtpProducerOptions = {
	keyStore: SrtpKeyStore
	hints: SrtpRocHintStore
	region: string
	streamNameForPort: (port: number) => string
	kvsLogConfigPath: string
	jitterBufferLatencyMs?: number
	helperPath?: string
	readyTimeoutMs?: number
	pendingMaxBytes?: number
	/** How long SIGTERM gets before SIGKILL follows; see stop(). */
	stopSigkillAfterMs?: number
	/** Called when the helper reports that libsrtp authenticated traffic. */
	onAuthenticated?: (port: number) => void
	spawn?: typeof nodeSpawn
	logger?: Logger
}

type Session = {
	child: ChildProcess
	socket: dgram.Socket
	relayPort: number
	ready: boolean
	/** Datagrams not yet sent: replayed on ready, handed back on a failed start. */
	pending: Buffer[]
	pendingBytes: number
	confirmedRoc: number | undefined
	stopping: boolean
}

/**
 * Runs one SRTP pipeline helper per port and relays datagrams to it.
 *
 * Each datagram is forwarded individually over a loopback socket to the helper's own
 * udpsrc, which preserves the one-datagram-per-packet framing SRTP authentication
 * depends on. The unencrypted transport's FIFO bridge cannot be reused here: it is a
 * byte stream, which is fine for self-synchronizing MPEG-TS and destroys RTP packet
 * boundaries.
 */
export class SrtpProducer implements ExitingProducer {
	private readonly options: SrtpProducerOptions
	private readonly logger: Logger
	private readonly spawn: typeof nodeSpawn
	private readonly sessions = new Map<number, Session>()
	private readonly epochs = new Map<number, number>()
	private exitListener: (port: number) => void = () => undefined
	private closed = false

	constructor(options: SrtpProducerOptions) {
		this.options = options
		this.logger = options.logger ?? new Logger('SrtpProducer')
		this.spawn = options.spawn ?? nodeSpawn
	}

	private helperPath(): string {
		return (
			this.options.helperPath ??
			new URL('./srtp_pipeline.py', import.meta.url).pathname
		)
	}

	async start(
		port: number,
		datagrams: Buffer[],
		context: { epoch: number },
	): Promise<void> {
		if (this.closed) throw new Error('producer is shut down')
		if (this.sessions.has(port)) return
		const key = this.options.keyStore.getKeyForPort(port)
		if (key === undefined) {
			throw new Error(`no SRTP key configured for port ${String(port)}`)
		}
		this.epochs.set(port, context.epoch)

		// A hint, not a requirement: the helper confirms it by authentication and
		// searches if it was wrong, so a missing or stale value costs a short search.
		const rocHint = await this.options.hints.getSrtpRocHint(
			port,
			key.ssrc,
			key.keyFingerprint,
		)

		const child = this.spawn(
			'python3',
			[
				this.helperPath(),
				'--ssrc',
				String(key.ssrc),
				'--stream-name',
				this.options.streamNameForPort(port),
				'--aws-region',
				this.options.region,
				'--kvs-log-config',
				this.options.kvsLogConfigPath,
				'--jitter-latency-ms',
				String(this.options.jitterBufferLatencyMs ?? 200),
			],
			{
				stdio: ['pipe', 'pipe', 'pipe'],
				// GStreamer's own debug output would include the caps carrying the key.
				env: {
					...process.env,
					GST_DEBUG: '2',
					GST_DEBUG_DUMP_DOT_DIR: undefined,
				},
			},
		)

		const session: Session = {
			child,
			socket: this.createRelaySocket(port),
			relayPort: 0,
			ready: false,
			pending: [...datagrams],
			pendingBytes: datagrams.reduce((sum, d) => sum + d.length, 0),
			confirmedRoc: undefined,
			stopping: false,
		}
		this.sessions.set(port, session)

		const ready = this.attachHelper(port, session, key)

		// The key travels here and nowhere else: not in the argument vector above, not
		// in the environment, not in any log line.
		child.stdin?.write(
			`${JSON.stringify({
				type: 'init',
				v: SRTP_HELPER_PROTOCOL_VERSION,
				key: key.keyHex,
				cipher: key.cipher,
				auth: key.auth,
				ssrc: key.ssrc,
				...(rocHint === undefined ? {} : { rocHint }),
			})}\n`,
		)

		try {
			await ready
		} catch (err) {
			await this.stop(port)
			throw err
		}
		await this.replay(port, session)
	}

	/**
	 * The loopback socket datagrams are relayed over, with its errors handled.
	 *
	 * A connected UDP socket reports failures asynchronously - ECONNREFUSED once the
	 * helper's relay port is gone, for one - and an unhandled 'error' event ends the
	 * process, which would let an SRTP-only failure take unencrypted ingest with it.
	 * Logging is all that is needed here: the helper's own exit handling is what
	 * rebuilds the pipeline.
	 */
	private createRelaySocket(port: number): dgram.Socket {
		const socket = dgram.createSocket('udp4')
		socket.on('error', (err) => {
			this.logger.warn('SRTP relay socket error', {
				port,
				error: err.message,
			})
		})
		return socket
	}

	/**
	 * Wires the helper's output, and resolves once it reports the port it bound.
	 *
	 * Not async: the listeners have to be attached synchronously, before the init
	 * frame is written, or a helper that fails immediately would have nobody reading
	 * its output.
	 */
	// eslint-disable-next-line @typescript-eslint/promise-function-async
	private attachHelper(
		port: number,
		session: Session,
		key: { ssrc: number; keyFingerprint: string },
	): Promise<void> {
		const protocol = new SrtpHelperProtocol()
		let settle: ((err?: Error) => void) | undefined
		const ready = new Promise<void>((resolve, reject) => {
			const timer = setTimeout(() => {
				reject(
					new Error(
						`SRTP helper for port ${String(port)} did not report readiness within ${String(
							this.options.readyTimeoutMs ?? READY_TIMEOUT_MS,
						)}ms`,
					),
				)
			}, this.options.readyTimeoutMs ?? READY_TIMEOUT_MS)
			settle = (err) => {
				clearTimeout(timer)
				if (err === undefined) resolve()
				else reject(err)
			}
		})

		session.child.stdout?.setEncoding('utf8')
		session.child.stdout?.on('data', (chunk: string) => {
			for (const message of protocol.push(chunk)) {
				this.handleMessage(port, session, key, message, settle)
			}
		})
		session.child.stderr?.setEncoding('utf8')
		session.child.stderr?.on('data', (chunk: string) => {
			this.logger.warn('SRTP helper stderr', { port, output: chunk.trim() })
		})
		session.child.on('error', (err) => {
			this.logger.error('SRTP helper failed to run', err, { port })
			settle?.(err)
		})
		session.child.on('exit', (code, signal) => {
			const wasStopping = session.stopping
			this.closeSession(port, session)
			settle?.(
				new Error(`SRTP helper exited during startup (code ${String(code)})`),
			)
			if (wasStopping) return
			this.logger.warn('SRTP helper exited', { port, code, signal })
			this.exitListener(port)
		})
		return ready
	}

	private handleMessage(
		port: number,
		session: Session,
		key: { ssrc: number; keyFingerprint: string },
		message: SrtpHelperMessage,
		settle: ((err?: Error) => void) | undefined,
	): void {
		switch (message.t) {
			case 'ready':
				if (message.v !== SRTP_HELPER_PROTOCOL_VERSION) {
					settle?.(
						new Error(
							`SRTP helper speaks protocol ${String(message.v)}, expected ${String(SRTP_HELPER_PROTOCOL_VERSION)}`,
						),
					)
					return
				}
				session.relayPort = message.relayPort
				session.socket.connect(message.relayPort, '127.0.0.1', () => {
					settle?.()
				})
				return
			case 'auth':
				if (message.status === 'ok') {
					if (message.first) {
						this.logger.info('SRTP traffic authenticated', {
							port,
							roc: message.roc,
							trials: message.trials,
						})
						// Only now does this port count as producing: until libsrtp has
						// authenticated something, the traffic is unproven.
						this.options.onAuthenticated?.(port)
					}
					if (session.confirmedRoc !== message.roc) {
						session.confirmedRoc = message.roc
						// Written only because authentication confirmed it.
						void this.options.hints.putSrtpRocHint(
							port,
							message.roc,
							key.ssrc,
							key.keyFingerprint,
						)
					}
				} else if (message.status === 'lost') {
					this.logger.warn('SRTP traffic stopped authenticating', {
						port,
						sinceMs: message.sinceMs,
					})
				}
				return
			case 'fatal':
				this.logger.error(
					'SRTP helper cannot run',
					new Error(`${message.reason}: ${message.message}`),
					{ port, retryable: isRetryableFatal(message.reason) },
				)
				settle?.(new Error(`${message.reason}: ${message.message}`))
				return
			case 'error':
				this.logger.error('SRTP pipeline error', new Error(message.message), {
					port,
					element: message.element,
				})
				return
			case 'warning':
				this.logger.warn('SRTP pipeline warning', {
					port,
					element: message.element,
					message: message.message,
				})
				return
			case 'stats':
			case 'searching':
			case 'eos':
				return
			case 'unparsed':
				this.logger.warn('Unrecognised SRTP helper output', {
					port,
					raw: message.raw,
				})
				return
		}
	}

	/** Sends the startup buffer in paced batches, then lets live traffic through. */
	private async replay(port: number, session: Session): Promise<void> {
		const queued = session.pending
		session.pending = []
		session.pendingBytes = 0
		session.ready = true

		for (let i = 0; i < queued.length; i += REPLAY_BATCH) {
			if (!this.sessions.has(port)) return
			for (const datagram of queued.slice(i, i + REPLAY_BATCH)) {
				this.send(session, datagram)
			}
			if (i + REPLAY_BATCH < queued.length) {
				await new Promise((resolve) => setTimeout(resolve, REPLAY_PAUSE_MS))
			}
		}
	}

	private send(session: Session, datagram: Buffer): void {
		try {
			session.socket.send(datagram)
		} catch {
			// A closed or unconnected socket must not take the packet path down; the
			// helper's own exit handling covers the pipeline being gone.
		}
	}

	writePacket(port: number, data: Buffer): void {
		const session = this.sessions.get(port)
		if (session === undefined) return
		if (!session.ready) {
			// Queued rather than sent, so live traffic cannot overtake the startup
			// buffer and make the older datagrams look like replays to SRTP.
			const cap = this.options.pendingMaxBytes ?? DEFAULT_PENDING_MAX_BYTES
			session.pending.push(data)
			session.pendingBytes += data.length
			while (session.pendingBytes > cap && session.pending.length > 1) {
				const oldest = session.pending.shift()
				session.pendingBytes -= oldest?.length ?? 0
			}
			return
		}
		this.send(session, data)
	}

	isActive(port: number): boolean {
		return this.sessions.get(port)?.ready === true
	}

	/** Datagrams the helper never received, so a failed start can put them back. */
	takeUnsentDatagrams(port: number): Buffer[] {
		const session = this.sessions.get(port)
		if (session === undefined) return []
		const unsent = session.pending
		session.pending = []
		session.pendingBytes = 0
		return unsent
	}

	/**
	 * Ends the helper for a port and returns only once it is gone.
	 *
	 * Exiting is the only thing that resolves this, SIGKILL included: PortIngestion
	 * releases the port's lock as soon as stop() returns, so another instance can
	 * acquire the stream from that moment - and a helper that has been signalled but
	 * not yet reaped is still a second writer to it.
	 */
	async stop(port: number): Promise<void> {
		const session = this.sessions.get(port)
		if (session === undefined) return
		session.stopping = true
		const exited = new Promise<void>((resolve) => {
			if (
				session.child.exitCode !== null ||
				session.child.signalCode !== null
			) {
				resolve()
				return
			}
			const timer = setTimeout(() => {
				// The helper ends its stream on SIGTERM so the sink can flush; if it is
				// still alive after that, it must not outlive the lock this port holds.
				this.logger.warn('SRTP helper ignored SIGTERM; killing it', { port })
				session.child.kill('SIGKILL')
			}, this.options.stopSigkillAfterMs ?? STOP_SIGKILL_AFTER_MS)
			session.child.once('exit', () => {
				clearTimeout(timer)
				resolve()
			})
		})
		session.child.kill('SIGTERM')
		await exited
		this.closeSession(port, session)
	}

	private closeSession(port: number, session: Session): void {
		if (this.sessions.get(port) === session) this.sessions.delete(port)
		try {
			session.socket.close()
		} catch {
			// Already closed.
		}
	}

	epochForPort(port: number): number | undefined {
		return this.epochs.get(port)
	}

	onExit(listener: (port: number) => void): void {
		this.exitListener = listener
	}

	async shutdown(): Promise<void> {
		this.closed = true
		await Promise.all([...this.sessions.keys()].map(async (p) => this.stop(p)))
	}
}
