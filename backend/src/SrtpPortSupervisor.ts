import { endChildProcess, type ExitingChild } from './ChildProcessExit.ts'
import { Logger } from './Logger.ts'
import {
	SRTP_HELPER_PROTOCOL_VERSION,
	SrtpHelperProtocol,
	isRetryableFatal,
	serializeCommand,
	type SrtpHelperCommand,
	type SrtpHelperFatalReason,
	type SrtpHelperInit,
	type SrtpHelperMessage,
} from './SrtpHelperProtocol.ts'
import type { SrtpPortKey } from './SrtpKeyStore.ts'

/**
 * One SRTP port's supervision: the state machine that owns the helper process and
 * the port's Kinesis lock. It is deliberately small, and its size is the point.
 *
 * The helper (srtp_port.py) owns the UDP socket and every piece of cryptographic
 * state, and reports - already coalesced, at most a handful of lines a second -
 * what actually happened. This state machine therefore never sees a datagram, never
 * buffers one, and never reconstructs so much as a sequence number. The events it
 * reacts to are the helper's frames, the helper's exit, and a slow tick that exists
 * only to observe *absence* - a helper that has gone quiet must still be torn down
 * and its lock released.
 *
 * The invariants (asserted by a transition hook in every test):
 *
 * - The lock is held if and only if the state is Producing or Stopping. It is
 *   acquired only after the helper reported authenticated traffic, and released
 *   only after the helper reported itself stopped (or provably exited) - never
 *   while anything of ours could still be writing to the stream.
 * - One operation is in flight per port. Every event - frames, exit, tick,
 *   shutdown - is serialized through one queue, so no handler ever observes an
 *   intermediate state.
 * - A frame only acts if it came from the current helper session. A late frame
 *   from a session that has been replaced is logged and dropped, except `index`,
 *   which raises the floor from any session: a dying helper's last word is exactly
 *   the one worth keeping, and the write can only make it higher.
 */

export type SrtpSupervisorState =
	| 'idle' // constructed, not started
	| 'starting' // helper spawned, awaiting its ready frame
	| 'searching' // helper up, awaiting authenticated traffic
	| 'acquiring' // authenticated; want the lock, retrying while it is held elsewhere
	| 'producing' // lock held; helper told to produce
	| 'stopping' // stop sent; awaiting the stopped ack (or the helper's exit)
	| 'cooldown' // helper restart backoff
	| 'disabled' // a fatal reason that restarting cannot fix
	| 'terminated'

/** The helper's `stopped` frame: the producing pipeline is provably down. */
type StoppedFrame = Extract<SrtpHelperMessage, { t: 'stopped' }>

/** The parts of a helper child process the supervisor needs, so tests can fake one. */
export type HelperProcess = ExitingChild & {
	stdin: {
		write(line: string): void
		end(): void
		on(event: 'error', listener: (err: Error) => void): void
	}
	stdout: {
		on(event: 'data', listener: (chunk: Buffer | string) => void): void
	}
	stderr: {
		on(event: 'data', listener: (chunk: Buffer | string) => void): void
	}
}

/** What the supervisor needs from the lock and floor services; structural on purpose. */
export type SrtpLockService = {
	tryAcquireKinesisLock(port: number, instanceId: string): Promise<boolean>
	releaseKinesisLock(port: number, instanceId: string): Promise<void>
	updateLastPacketTime(
		port: number,
		instanceId: string,
	): Promise<'ok' | 'lostLock' | 'writeError'>
}

export type SrtpFloorStore = {
	getSrtpIndexFloor(
		port: number,
		expectedSsrc: number,
		expectedKeyFingerprint: string,
	): Promise<number | undefined>
	raiseSrtpIndexFloor(
		port: number,
		index: number,
		ssrc: number,
		keyFingerprint: string,
		generation: number,
	): Promise<void>
}

export type SrtpPortSupervisorTimeouts = {
	/** Awaiting the ready frame after spawn. */
	readyMs: number
	/** Between lock acquisition attempts while the stream stays authenticated. */
	acquireRetryMs: number
	/** Awaiting the stopped ack before escalating to signals. */
	stoppedAckMs: number
	/** A frame-less helper is wedged after this long. */
	frameStallMs: number
	/** Between helper restarts; doubles up to restartMaxMs, resets on ready. */
	restartMs: number
	restartMaxMs: number
	/** The tick that observes absence. */
	tickMs: number
}

export type SrtpPortSupervisorConfig = {
	port: number
	instanceId: string
	key: SrtpPortKey
	streamName: string
	/** Spawns the helper; receives the init frame so the key never travels elsewhere.
	 *
	 * May resolve a promise: the transport resolves the helper's AWS credentials
	 * first (they are refreshed per spawn, because a helper outlives the session
	 * token it was first given), so the child exists only once that has. */
	spawnHelper: (init: SrtpHelperInit) => HelperProcess | Promise<HelperProcess>
	locks: SrtpLockService
	floors: SrtpFloorStore
	logger?: Logger
	timeouts?: Partial<SrtpPortSupervisorTimeouts>
	/** Observes authentications for logging and metrics. */
	onAuthenticated?: (
		port: number,
		info: { roc: number; trials?: number },
	) => void
	/** Observes stats frames for the traffic metric. */
	onStats?: (
		port: number,
		stats: { inputBytes: number; authenticated: number },
	) => void
	/** Runs after every transition, in tests, to assert the invariants. */
	onTransition?: (from: SrtpSupervisorState, to: SrtpSupervisorState) => void
}

const DEFAULT_TIMEOUTS: SrtpPortSupervisorTimeouts = {
	readyMs: 10_000,
	acquireRetryMs: 30_000,
	stoppedAckMs: 15_000,
	frameStallMs: 30_000,
	restartMs: 1_000,
	restartMaxMs: 60_000,
	tickMs: 10_000,
}

/** A page of the state, for logging and tests. */
export type SrtpPortStatus = {
	port: number
	state: SrtpSupervisorState
	/** True exactly while the lock row is ours. */
	lockHeld: boolean
	helperUp: boolean
	producing: boolean
}

export class SrtpPortSupervisor {
	private readonly config: SrtpPortSupervisorConfig
	private readonly timeouts: SrtpPortSupervisorTimeouts
	private readonly logger: Logger

	private state: SrtpSupervisorState = 'idle'
	private lockHeld = false
	private helper: HelperProcess | undefined
	private protocol = new SrtpHelperProtocol()
	private queue: Promise<void> = Promise.resolve()

	// Session bookkeeping; reset whenever a helper session replaces the last.
	private startedAtMs = 0
	private stoppedSentAtMs = 0
	/** Release the lock when the stop completes (false when the row is no longer ours). */
	private releaseAfterStop = true
	private backoffMs: number
	private retryAtMs = 0
	private fatalReason: SrtpHelperFatalReason | undefined
	private lastAcquireAttemptMs = 0
	private lastFrameAtMs = 0
	private lastHeartbeatAuthenticated = 0
	/** The far-climb position reported by the last session, with the floor base it belongs to. */
	private searchFromCarry: { base: number; searchFrom: number } | undefined
	/** The floor base of the session this helper is running, to validate carries against. */
	private sessionFloorBase = 0
	/**
	 * The ack waiter for a stop written by shutdown, installed by awaitStopAck
	 * and resolved by the stdout listener outside the serialized queue.
	 */
	private stopAck:
		| { session: HelperProcess; resolve: (frame: StoppedFrame) => void }
		| undefined

	private tickTimer: NodeJS.Timeout | undefined

	constructor(config: SrtpPortSupervisorConfig) {
		this.config = config
		this.timeouts = { ...DEFAULT_TIMEOUTS, ...config.timeouts }
		this.backoffMs = this.timeouts.restartMs
		this.logger = config.logger ?? new Logger('SrtpPortSupervisor')
	}

	get port(): number {
		return this.config.port
	}

	get currentState(): SrtpSupervisorState {
		return this.state
	}

	status(): SrtpPortStatus {
		return {
			port: this.config.port,
			state: this.state,
			lockHeld: this.lockHeld,
			helperUp: this.helper !== undefined,
			producing: this.state === 'producing',
		}
	}

	/** Starts supervision. Idempotent. */
	start(): void {
		// spawnSession's promise is part of the queue: while its floor read is in
		// flight, nothing queued after it - a stop() above all - can run, so a
		// shutdown can never observe a half-started session it then has to
		// resurrect a helper into (the guard below the floor read is the second
		// lock on that door).
		void this.enqueue((): void | Promise<void> => {
			if (this.state !== 'idle') return
			return this.spawnSession()
		})
		if (this.tickTimer === undefined) {
			this.tickTimer = setInterval(() => this.tick(), this.timeouts.tickMs)
			this.tickTimer.unref?.()
		}
	}

	/**
	 * Stops supervision: stops producing (flushing), releases the lock, ends the
	 * helper, and resolves only once the port holds nothing. Idempotent.
	 */
	async stop(): Promise<void> {
		return this.enqueue(async () => this.shutdown())
	}

	// -- event plumbing ---------------------------------------------------------

	/** Serializes one step; nothing else runs while it is in flight. */
	private async enqueue(step: () => void | Promise<void>): Promise<void> {
		this.queue = this.queue.then(step, step)
		return this.queue
	}

	private tick(): void {
		void this.enqueue(async () => this.handleTick())
	}

	// -- state transitions -----------------------------------------------------

	private transition(to: SrtpSupervisorState): void {
		const from = this.state
		if (from === to) return
		this.state = to
		this.logger.info('Port state', { port: this.config.port, from, to })
		this.config.onTransition?.(from, to)
	}

	/** True once the port is permanently out of the supervisor's care. */
	private isGivenUp(): boolean {
		return this.state === 'terminated' || this.state === 'disabled'
	}

	private async spawnSession(): Promise<void> {
		this.fatalReason = undefined
		if (this.isGivenUp()) return
		let floor: number | undefined
		try {
			// A floor that cannot be read must not look like a missing one: undefined
			// means "nothing was ever accepted", which admits everything.
			floor = await this.config.floors.getSrtpIndexFloor(
				this.config.port,
				this.config.key.ssrc,
				this.config.key.keyFingerprint,
			)
		} catch (err) {
			this.logger.error(
				'Could not read the SRTP replay floor; not starting the helper',
				err instanceof Error ? err : new Error(String(err)),
				{ port: this.config.port },
			)
			this.enterCooldown()
			return
		}
		// The floor read was awaited: the state may have changed underneath it
		// in a runtime this analysis cannot see, even though the queue now
		// serializes startup against shutdown. A terminated or disabled port
		// must not gain a helper after the fact.
		if (this.isGivenUp()) return
		// Arithmetic, not bitwise: the packet index is 48 bits, and `>>` coerces
		// to int32, so any floor at or above 2^31 silently truncates its rollover
		// counter here - a carried search position then validates against the
		// wrong base. Doubles hold every integer up to 2^53 exactly.
		const floorBase = floor === undefined ? 0 : Math.floor(floor / 65536)
		const carry = this.searchFromCarry
		// The carry only applies while the floor it climbed from has not moved; a
		// mismatched base only costs a repeated climb, never a missed counter.
		const searchFrom = carry?.base === floorBase ? carry.searchFrom : undefined
		const init: SrtpHelperInit = {
			type: 'init',
			v: SRTP_HELPER_PROTOCOL_VERSION,
			key: this.config.key.keyHex,
			cipher: this.config.key.cipher,
			auth: this.config.key.auth,
			ssrc: this.config.key.ssrc,
			...(floor === undefined ? {} : { floor }),
			...(searchFrom === undefined ? {} : { searchFrom }),
		}
		this.sessionFloorBase = floorBase
		let session: HelperProcess
		try {
			// The spawner may resolve credentials before the child exists (see the
			// config type); a failure there is contained like every other failed
			// start, not thrown into the queue.
			session = await this.config.spawnHelper(init)
		} catch (err) {
			this.logger.error(
				'Could not spawn the SRTP helper',
				err instanceof Error ? err : new Error(String(err)),
				{ port: this.config.port },
			)
			this.enterCooldown()
			return
		}
		this.helper = session
		this.protocol = new SrtpHelperProtocol()
		this.startedAtMs = Date.now()
		this.lastFrameAtMs = Date.now()
		// Everything attaches synchronously with the session in hand, before
		// anything else is awaited or written: a helper that fails immediately
		// must have a reader, and an unhandled stream error anywhere in this set
		// ends the whole process.
		//
		// Every listener captures its session. A frame or exit from a session that
		// has been replaced is routed through the same handlers but cannot act (see
		// handleFrame and handleExit): Node documents that stdio can still deliver
		// buffered output after 'exit', and a late frame from a previous helper must
		// not promote or tear down the one that replaced it.
		session.stdin.on('error', (err) => {
			this.logger.warn('Helper stdin error', {
				port: this.config.port,
				error: err.message,
			})
		})
		session.stdout.on('data', (chunk) => {
			const messages = this.protocol.push(chunk.toString())
			for (const message of messages) {
				// The shutdown ack takes a shortcut around the queue. shutdown()
				// runs inside the queue, so a 'stopped' frame that went through
				// enqueue() could only be handled after shutdown has already given
				// up on it and killed the helper - which is how every graceful stop
				// used to cost the full ack timeout. See awaitStopAck.
				if (message.t === 'stopped' && this.stopAck?.session === session) {
					this.stopAck.resolve(message)
					continue
				}
				void this.enqueue(async () => this.handleFrame(message, session))
			}
		})
		session.stderr.on('data', (chunk) => {
			for (const line of chunk.toString().split('\n')) {
				if (line.trim().length > 0)
					this.logger.warn('Helper stderr', {
						port: this.config.port,
						line,
					})
			}
		})
		session.on('exit', () => {
			void this.enqueue(async () => this.handleExit(session))
		})
		session.on('error', () => {
			// An 'error' is not proof of death (see ChildProcessExit): a signal
			// that could not be delivered leaves the child alive, and possibly
			// still writing to the stream this port's lock guards. End it for
			// real - endChildProcess also settles the failed-spawn case, where
			// no 'exit' is ever emitted - and only then treat it as gone; the
			// lock is never released on an 'error' alone.
			void this.enqueue(async () => {
				await endChildProcess(session, { sigkillAfterMs: 5_000 })
				await this.handleExit(session)
			})
		})
		this.writeCommand(init)
		this.transition('starting')
	}

	private writeCommand(command: SrtpHelperCommand): void {
		const helper = this.helper
		if (helper === undefined) return
		try {
			helper.stdin.write(serializeCommand(command) + '\n')
		} catch (err) {
			// The helper died between the exit check and the write; its exit handler
			// owns what happens next.
			this.logger.warn('Could not write to the helper', {
				port: this.config.port,
				error: err instanceof Error ? err.message : String(err),
			})
		}
	}

	private async handleFrame(
		message: SrtpHelperMessage,
		session: HelperProcess,
	): Promise<void> {
		if (this.state === 'terminated' || this.state === 'idle') return
		if (this.helper !== session) {
			// A frame from a session that has been replaced. It may still raise the
			// floor - a dying helper's last word is exactly the one worth keeping,
			// and the write can only make it higher - and carry its search position,
			// but it cannot act: no promoting, no stopping, no lease.
			if (message.t === 'index') {
				await this.raiseFloor(message.index)
				return
			}
			if (
				message.t === 'auth' &&
				message.status === 'fail' &&
				message.searchFrom !== undefined
			) {
				this.searchFromCarry = {
					base: this.sessionFloorBase,
					searchFrom: message.searchFrom,
				}
			}
			return
		}
		this.lastFrameAtMs = Date.now()
		switch (message.t) {
			case 'ready': {
				if (message.v !== SRTP_HELPER_PROTOCOL_VERSION) {
					this.logger.error(
						`Helper speaks protocol version ${String(message.v)}, this supervisor supports ${String(SRTP_HELPER_PROTOCOL_VERSION)}`,
						undefined,
						{ port: this.config.port },
					)
					await this.endSession('starting')
					return
				}
				if (this.state !== 'starting') return
				this.backoffMs = this.timeouts.restartMs
				this.transition('searching')
				return
			}
			case 'auth': {
				if (message.status === 'ok') {
					if (message.first) {
						this.config.onAuthenticated?.(this.config.port, {
							roc: message.roc,
							trials: message.trials,
						})
						this.logger.info('SRTP traffic authenticated', {
							port: this.config.port,
							roc: message.roc,
							trials: message.trials ?? 0,
						})
						if (this.state === 'searching' || this.state === 'acquiring') {
							await this.attemptAcquire()
						}
						// In producing this is the re-confirmation after a grant: expected,
						// nothing to do.
					} else {
						this.logger.info('SRTP rollover', {
							port: this.config.port,
							roc: message.roc,
						})
					}
				} else if (message.status === 'fail') {
					// The far-climb position, to be carried into the next session -
					// but only while the floor it climbed from has not moved.
					if (message.searchFrom !== undefined && this.helper !== undefined) {
						this.searchFromCarry = {
							base: this.sessionFloorBase,
							searchFrom: message.searchFrom,
						}
					}
				} else if (message.status === 'lost') {
					this.logger.warn('SRTP traffic stopped authenticating', {
						port: this.config.port,
						sinceMs: message.sinceMs,
					})
					// The helper has already given producing up and rebuilt to
					// searching; whatever it reported about that is on its way too.
					await this.notProducing()
				}
				return
			}
			case 'stopped': {
				// The helper's producing pipeline is provably down; the index is its
				// last word on the floor.
				if (message.index !== undefined) await this.raiseFloor(message.index)
				if (this.state === 'stopping') {
					if (this.releaseAfterStop) {
						await this.releaseLock()
					} else {
						// The row was taken from us before the stop completed: it is the
						// new owner's, so there is nothing to release - but it is not
						// ours either, and the invariant says so.
						this.lockHeld = false
					}
					this.transition('searching')
				} else if (this.state === 'producing') {
					// Not requested by us: the helper recovered from a pipeline error.
					// It has already torn producing down, so the lock must go now.
					await this.releaseLock()
					this.transition('searching')
				} else {
					this.logger.warn('Unexpected stopped frame', {
						port: this.config.port,
						state: this.state,
					})
				}
				return
			}
			case 'producing':
				this.logger.info('SRTP producer started', { port: this.config.port })
				return
			case 'index':
				await this.raiseFloor(message.index)
				return
			case 'stats': {
				this.config.onStats?.(this.config.port, {
					inputBytes: message.inputBytes,
					authenticated: message.authenticated,
				})
				if (message.authenticated > this.lastHeartbeatAuthenticated) {
					this.lastHeartbeatAuthenticated = message.authenticated
				}
				if (this.state === 'producing') {
					if (message.authenticated > 0) await this.refreshLease()
				} else if (
					this.state === 'acquiring' &&
					message.authenticated > 0 &&
					this.now() - this.lastAcquireAttemptMs >= this.timeouts.acquireRetryMs
				) {
					await this.attemptAcquire()
				}
				return
			}
			case 'fatal': {
				this.fatalReason = message.reason
				this.logger.error(`SRTP helper fatal: ${message.message}`, undefined, {
					port: this.config.port,
					reason: message.reason,
				})
				return
			}
			case 'error':
				this.logger.error('SRTP helper pipeline error', undefined, {
					port: this.config.port,
					element: message.element,
					message: message.message,
				})
				return
			case 'warning':
				this.logger.warn('SRTP helper warning', {
					port: this.config.port,
					element: message.element,
					message: message.message,
				})
				return
			case 'searching':
			case 'eos':
				return
			case 'unparsed':
				this.logger.warn('Unparsed helper frame', {
					port: this.config.port,
					raw: message.raw,
				})
				return
		}
	}

	private async attemptAcquire(): Promise<void> {
		if (this.state !== 'searching' && this.state !== 'acquiring') return
		this.lastAcquireAttemptMs = this.now()
		this.transition('acquiring')
		let acquired: boolean
		try {
			acquired = await this.config.locks.tryAcquireKinesisLock(
				this.config.port,
				this.config.instanceId,
			)
		} catch (err) {
			this.logger.error(
				'Lock acquisition failed',
				err instanceof Error ? err : new Error(String(err)),
				{ port: this.config.port },
			)
			return
		}
		if (!acquired) {
			// Another instance owns a fresh lock for this port; it will lose it when
			// its traffic stops, and this one retries while its own traffic stays
			// authenticated.
			this.logger.info('Lock held elsewhere; will retry', {
				port: this.config.port,
			})
			return
		}
		this.lockHeld = true
		this.releaseAfterStop = true
		this.lastHeartbeatAuthenticated = 0
		this.writeCommand({ type: 'start', v: SRTP_HELPER_PROTOCOL_VERSION })
		this.transition('producing')
		this.logger.info('Producing SRTP stream', {
			port: this.config.port,
			stream: this.config.streamName,
		})
	}

	/**
	 * The port is no longer producing, by the helper's own report, and the lock
	 * must go. Used for auth loss, where the helper tears production down itself.
	 *
	 * The helper sends its `stopped` frame - the one that releases the lock -
	 * only after the producing pipeline is down, and `auth lost` after that; so
	 * by the time this runs, the stopped frame has usually already released the
	 * lock and moved on to searching, and this is the report that agrees.
	 */
	private async notProducing(): Promise<void> {
		if (this.state === 'producing' || this.state === 'stopping') {
			await this.releaseLock()
			this.transition('searching')
		} else if (this.state === 'acquiring') {
			this.transition('searching')
		}
	}

	private async refreshLease(): Promise<void> {
		if (this.state !== 'producing') return
		let outcome: 'ok' | 'lostLock' | 'writeError'
		try {
			outcome = await this.config.locks.updateLastPacketTime(
				this.config.port,
				this.config.instanceId,
			)
		} catch (err) {
			this.logger.error(
				'Lease refresh failed',
				err instanceof Error ? err : new Error(String(err)),
				{ port: this.config.port },
			)
			return
		}
		if (outcome === 'lostLock') {
			this.logger.warn('Lost the lock; stopping the producer', {
				port: this.config.port,
			})
			// The row belongs to whoever took it: stop, but never release.
			this.releaseAfterStop = false
			this.writeCommand({ type: 'stop', v: SRTP_HELPER_PROTOCOL_VERSION })
			this.stoppedSentAtMs = this.now()
			this.transition('stopping')
		}
		// writeError: the lease stands until it goes stale; keep going.
	}

	private async releaseLock(): Promise<void> {
		if (!this.lockHeld) return
		this.lockHeld = false
		try {
			await this.config.locks.releaseKinesisLock(
				this.config.port,
				this.config.instanceId,
			)
		} catch (err) {
			// The row was already taken or the write failed: the lease goes stale
			// either way, and the inactivity cleanup clears it.
			this.logger.warn('Lock release failed; the lease will go stale', {
				port: this.config.port,
				error: err instanceof Error ? err.message : String(err),
			})
		}
	}

	private async raiseFloor(index: number): Promise<void> {
		// Deliberately best effort, and the honest cost is narrower than it looks:
		// a failed write does not lose what the helper already accepted - its own
		// maximum stands in-process - but after a restart the persisted floor can
		// be behind it, and datagrams already accepted in that gap then
		// authenticate again until the floor catches up. That replay window is
		// the documented tradeoff of this design (see docs/SRTP-DESIGN.md):
		// production is never stopped over a metrics/floor write, and the write is
		// monotonic, so it converges on the next successful report.
		await this.config.floors.raiseSrtpIndexFloor(
			this.config.port,
			index,
			this.config.key.ssrc,
			this.config.key.keyFingerprint,
			this.config.key.generation,
		)
	}

	private async handleExit(session: HelperProcess): Promise<void> {
		if (this.helper !== session) return // An event from a session already replaced.
		this.helper = undefined
		if (this.state === 'terminated') return
		// Exit is verified death: whatever it was doing, it is not doing it now.
		if (this.lockHeld) await this.releaseLock()
		if (this.fatalReason !== undefined && !isRetryableFatal(this.fatalReason)) {
			this.logger.error(
				'SRTP helper cannot run here; port disabled until the service is redeployed',
				undefined,
				{ port: this.config.port, reason: this.fatalReason },
			)
			this.transition('disabled')
			return
		}
		this.enterCooldown()
	}

	private enterCooldown(): void {
		this.retryAtMs = this.now() + this.backoffMs
		this.backoffMs = Math.min(this.backoffMs * 2, this.timeouts.restartMaxMs)
		this.transition('cooldown')
	}

	/** Ends the current helper session with signals, for ready-timeout escalation. */
	private async endSession(from: SrtpSupervisorState): Promise<void> {
		const helper = this.helper
		if (helper !== undefined) {
			this.helper = undefined
			await endChildProcess(helper, { sigkillAfterMs: 5_000 })
			if (this.lockHeld) await this.releaseLock()
		}
		void from
		if (this.state !== 'terminated') this.enterCooldown()
	}

	private async handleTick(): Promise<void> {
		if (
			this.state === 'terminated' ||
			this.state === 'idle' ||
			this.state === 'disabled'
		) {
			return
		}
		const now = this.now()
		switch (this.state) {
			case 'starting':
				if (now - this.startedAtMs > this.timeouts.readyMs) {
					this.logger.error('Helper did not become ready in time', undefined, {
						port: this.config.port,
					})
					await this.endSession('starting')
				}
				return
			case 'cooldown':
				if (now >= this.retryAtMs) await this.spawnSession()
				return
			case 'stopping':
				if (now - this.stoppedSentAtMs > this.timeouts.stoppedAckMs) {
					this.logger.warn('Helper did not acknowledge the stop; ending it', {
						port: this.config.port,
					})
					await this.endSession('stopping')
				}
				return
			case 'producing': {
				// A helper that stops reporting at all - not even stats - is wedged;
				// end it, let the exit path release the lock, and restart.
				if (now - this.lastFrameAtMs > this.timeouts.frameStallMs) {
					this.logger.error(
						'Helper went silent while producing; ending it',
						undefined,
						{ port: this.config.port },
					)
					await this.endSession('producing')
				}
				return
			}
			case 'searching':
			case 'acquiring': {
				// The same absence, before there is a lock to lose: the helper
				// reports stats on its own timer whether or not any datagram is
				// arriving, so silence is never "the port is just idle" - it is a
				// wedged process holding the bound port with nothing to show for
				// it. Without this, a helper that stalls pre-authentication or
				// while waiting for the lock is never restarted at all.
				if (now - this.lastFrameAtMs > this.timeouts.frameStallMs) {
					this.logger.error(
						'Helper went silent before production; ending it',
						undefined,
						{ port: this.config.port, state: this.state },
					)
					await this.endSession(this.state)
				}
				return
			}
		}
	}

	private async shutdown(): Promise<void> {
		if (this.state === 'terminated') return
		if (this.tickTimer !== undefined) {
			clearInterval(this.tickTimer)
			this.tickTimer = undefined
		}
		const helper = this.helper
		if (helper !== undefined && !exited(helper)) {
			if (this.state === 'producing' || this.state === 'stopping') {
				// Give the producer the chance to flush: stop, and end the process
				// only if the ack does not come (see awaitStopAck for why the ack
				// cannot take the queue like every other frame).
				const acked = await this.awaitStopAck(
					helper,
					this.timeouts.stoppedAckMs,
				)
				// The acked frame never reaches handleFrame - this step is in
				// flight in the queue it would be enqueued onto - so its last word
				// on the floor is raised here, not dropped as a post-terminated
				// frame would be.
				if (acked?.index !== undefined) await this.raiseFloor(acked.index)
			}
			await endChildProcess(helper, { sigkillAfterMs: 5_000 })
			this.helper = undefined
		}
		if (this.lockHeld) await this.releaseLock()
		this.transition('terminated')
	}

	/**
	 * Writes the stop command and resolves with the helper's `stopped` frame,
	 * or undefined if neither the ack nor the helper's exit arrives in time.
	 *
	 * This runs inside the serialized queue, and the queue is exactly what an
	 * ordinary frame takes to be handled - so the ack is delivered to the waiter
	 * installed here by the stdout listener directly, and every other frame
	 * still takes the queue as before. Without that shortcut, the ack can only
	 * ever arrive after shutdown has already waited out the full timeout and
	 * killed the helper, which made every graceful stop cost the whole ack
	 * window (15 s in production) and threw away the final index report.
	 */
	private async awaitStopAck(
		helper: HelperProcess,
		timeoutMs: number,
	): Promise<StoppedFrame | undefined> {
		const acked = new Promise<StoppedFrame>((resolve) => {
			this.stopAck = { session: helper, resolve }
		})
		// The helper ending also answers the question: there will be no further
		// frame from it, and the caller ends an exited child quickly.
		const ended = new Promise<undefined>((resolve) => {
			const onExit = (): void => {
				this.stopAck = undefined
				resolve(undefined)
			}
			helper.on('exit', onExit)
			helper.on('error', onExit)
		})
		try {
			this.writeCommand({ type: 'stop', v: SRTP_HELPER_PROTOCOL_VERSION })
			return await Promise.race([
				acked,
				ended,
				sleep(timeoutMs).then(() => undefined),
			])
		} finally {
			this.stopAck = undefined
		}
	}

	private now(): number {
		return Date.now()
	}
}

const exited = (child: HelperProcess): boolean =>
	child.exitCode !== null || child.signalCode !== null

const sleep = async (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms))
