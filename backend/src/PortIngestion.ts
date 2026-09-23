import { Logger } from './Logger.ts'
import type { OwnedWriteResult } from './StreamMetadataService.ts'

/** DynamoDB operations this machine needs; narrowed so tests can substitute them. */
export type IngestionLockStore = {
	tryAcquireKinesisLock(port: number, instanceId: string): Promise<boolean>
	releaseKinesisLock(port: number, instanceId: string): Promise<void>
	updateLastPacketTime(
		port: number,
		instanceId: string,
	): Promise<OwnedWriteResult>
}

/**
 * The producer this machine owns. Datagrams are handed over as an array, never
 * concatenated: SRTP decryption depends on per-datagram framing, so only the transport
 * itself may decide to join them into a byte stream.
 */
/** Identifies the ownership lifetime a producer was started under. */
export type ProducerStartContext = { epoch: number }

export type IngestionProducer = {
	/**
	 * `context.epoch` identifies this ownership lifetime. A producer that reports its
	 * own exit must report the epoch it was started with, not whatever is current when
	 * the exit is noticed - by then the port may be on its next lifetime, and a late
	 * event would tear that one down.
	 */
	start(
		port: number,
		datagrams: Buffer[],
		context: ProducerStartContext,
	): Promise<void>
	stop(port: number): Promise<void>
	writePacket(port: number, data: Buffer): void
	isActive(port: number): boolean
	/**
	 * Datagrams handed to start() that were never sent, so a failed start can put them
	 * back rather than lose the beginning of the stream (typically the keyframe).
	 */
	takeUnsentDatagrams?(port: number): Buffer[]
}

/** The activity tracker that decides when a stream is first seen and when it stops. */
export type StreamActivityTracker = {
	onPacketReceived(port: number, timestamp: Date): void
	getStreamState(port: number): { status: 'active' | 'inactive' } | undefined
}

export type PortIngestionConfig = {
	port: number
	instanceId: string
	/** Bytes to buffer before attempting to acquire the lock and start producing. */
	minBytesBeforeStart: number
	/** Buffer cap; defaults to twice the start threshold. */
	maxQueuedBytes?: number
	/** Delay before retrying after any failed or refused start. */
	startRetryBackoffMs?: number
	/** Delay before restarting a producer that exited on its own. */
	restartDelayMs?: number
	/**
	 * When set, the producer must report authentication before it counts as running:
	 * the machine passes through Provisional, where datagrams are relayed but earn no
	 * lease refresh. Used by SRTP, where nothing can be authenticated until a pipeline
	 * exists to authenticate it.
	 *
	 * Like every other deadline here, it is evaluated when the next datagram arrives
	 * rather than on a timer, so it bounds a *stream* that never authenticates. Traffic
	 * that stops instead is released by the inactivity event, which is the slower path
	 * of the two - see the class comment.
	 */
	provisionalTimeoutMs?: number
	/** Cooldown applied when a provisional pipeline never authenticates. */
	provisionalCooldownMs?: number
	/**
	 * Syntactic anti-noise filter. Decides whether a datagram is worth *buffering* -
	 * nothing more. It is not authentication, and a datagram that passes it earns no
	 * ownership, no lease refresh and no persisted state. Once this port owns the slot
	 * the filter is bypassed, so a filter that is wrong about live traffic cannot
	 * silently starve an already-running stream.
	 */
	admit?: (data: Buffer) => boolean
}

export type PortIngestionDeps = {
	locks: IngestionLockStore
	producer: IngestionProducer
	activity: StreamActivityTracker
	now?: () => number
	logger?: Logger
	/** Called after every transition; the specs use it to assert the invariants. */
	onTransition?: (from: string, to: string) => void
	/**
	 * Called when an event handler throws. The queue always continues - a broken queue
	 * would stop the port responding to anything, including its own teardown - but a
	 * caller (and every spec) needs to see that it happened rather than find it in a log.
	 */
	onError?: (err: Error) => void
}

const DEFAULT_START_RETRY_BACKOFF_MS = 30_000
const DEFAULT_RESTART_DELAY_MS = 10_000
const DEFAULT_PROVISIONAL_COOLDOWN_MS = 60_000
const DROPPED_LOG_THROTTLE_MS = 60_000
/** Largest possible UDP payload; see maxQueuedBytes. */
const MAX_DATAGRAM_BYTES = 65_535

/**
 * Datagrams waiting for a producer.
 *
 * Eviction drops the oldest first and never the only remaining datagram, so a
 * threshold of zero still starts on the next packet. There are no "this was the first
 * packet" markers to preserve: the machine's state already says whether it is running,
 * so a transition cannot be lost to eviction.
 */
class Pending {
	private chunks: Buffer[] = []
	private bytes = 0

	get totalBytes(): number {
		return this.bytes
	}

	get length(): number {
		return this.chunks.length
	}

	push(data: Buffer): void {
		this.chunks.push(data)
		this.bytes += data.length
	}

	/** Puts datagrams back at the front, preserving their original order. */
	prependAll(datagrams: Buffer[]): void {
		if (datagrams.length === 0) return
		this.chunks = [...datagrams, ...this.chunks]
		this.bytes += datagrams.reduce((sum, d) => sum + d.length, 0)
	}

	/** Returns everything and empties the buffer in one step, so nothing is half moved. */
	take(): Buffer[] {
		const taken = this.chunks
		this.chunks = []
		this.bytes = 0
		return taken
	}

	clear(): void {
		this.take()
	}

	/** Returns the number of bytes dropped. */
	evictTo(maxBytes: number): number {
		let dropped = 0
		while (this.bytes > maxBytes && this.chunks.length > 1) {
			const oldest = this.chunks.shift()
			if (oldest === undefined) break
			this.bytes -= oldest.length
			dropped += oldest.length
		}
		return dropped
	}
}

type PortState =
	| { name: 'Idle' }
	| { name: 'Buffering' }
	| { name: 'Cooldown'; retryAt: number }
	| { name: 'Acquiring' }
	| { name: 'Starting' }
	| { name: 'Provisional'; expiresAt: number }
	| { name: 'Running' }
	| { name: 'Restarting'; restartAt: number }
	| { name: 'Stopping' }
	| { name: 'Terminated' }

/** States in which this instance holds the port's DynamoDB lock. */
const LOCK_HELD_STATES = new Set([
	'Starting',
	'Provisional',
	'Running',
	'Restarting',
	'Stopping',
])

/**
 * Owns everything mutable about one ingest port, and runs every event through one
 * serialized queue.
 *
 * The shape this replaces was N writers to M per-port maps, reached from the packet
 * path, a resume callback, a delayed restart timer, an inactivity callback and
 * shutdown - so every asynchronous step had to re-check whether the world had moved
 * under it, and each missed re-check was a bug. Here, one event runs to completion
 * before the next is dequeued, which means no handler can observe an intermediate
 * state and the re-checks are unnecessary rather than merely correct.
 *
 * Callbacks that escape the queue - a producer exiting on its own - carry the
 * ownership epoch they were created under and are dropped if it has moved on.
 *
 * There are no timers. A cooldown, a restart delay and a provisional deadline are all
 * fields compared against the clock when the next packet arrives; if no packet ever
 * arrives, the inactivity event releases the port. A timer would be a second way into
 * the state, which is what this design exists to avoid.
 *
 * So each of those deadlines bounds a stream that keeps sending, not wall clock. A port
 * whose traffic stops dead holds what it holds until the inactivity event, whatever its
 * own deadline says - which matters most for the provisional deadline, since that is
 * the one a burst of unauthenticated traffic can reach. The inactivity timeout is
 * therefore the real upper bound on holding a slot, and it is deliberately the only
 * clock this class answers to.
 */
export class PortIngestion {
	readonly port: number
	private readonly config: PortIngestionConfig
	private readonly locks: IngestionLockStore
	private readonly producer: IngestionProducer
	private readonly activity: StreamActivityTracker
	private readonly logger: Logger
	private readonly now: () => number
	private readonly onTransition?: (from: string, to: string) => void
	private readonly onError?: (err: Error) => void

	private readonly maxQueuedBytes: number
	private readonly startRetryBackoffMs: number
	private readonly restartDelayMs: number
	private readonly provisionalCooldownMs: number

	private state: PortState = { name: 'Idle' }
	private readonly pending = new Pending()
	private readonly queue: (() => Promise<void>)[] = []
	private draining = false
	/** True while a packet event is queued and has not started running; see offer(). */
	private packetEventQueued = false
	/** Bumped on every lock acquisition; identifies one ownership lifetime. */
	private epoch = 0
	private lastDroppedLog = 0
	private droppedBytes = 0

	constructor(config: PortIngestionConfig, deps: PortIngestionDeps) {
		this.config = config
		this.port = config.port
		this.locks = deps.locks
		this.producer = deps.producer
		this.activity = deps.activity
		this.logger = deps.logger ?? new Logger('PortIngestion')
		this.now = deps.now ?? Date.now
		this.onTransition = deps.onTransition
		this.onError = deps.onError
		// The cap must exceed the start threshold by at least one maximum-size datagram.
		// Eviction runs before the threshold is tested, so a cap at or below the
		// threshold would keep trimming the buffer back under it and the port would
		// buffer forever without ever starting.
		this.maxQueuedBytes = Math.max(
			config.maxQueuedBytes ?? config.minBytesBeforeStart * 2,
			config.minBytesBeforeStart + MAX_DATAGRAM_BYTES,
		)
		this.startRetryBackoffMs =
			config.startRetryBackoffMs ?? DEFAULT_START_RETRY_BACKOFF_MS
		this.restartDelayMs = config.restartDelayMs ?? DEFAULT_RESTART_DELAY_MS
		this.provisionalCooldownMs =
			config.provisionalCooldownMs ?? DEFAULT_PROVISIONAL_COOLDOWN_MS
	}

	/** Current state name; for logs, health output and assertions. */
	get stateName(): string {
		return this.state.name
	}

	/** True while this instance holds the port's lock. */
	get ownsSlot(): boolean {
		return LOCK_HELD_STATES.has(this.state.name)
	}

	get bufferedBytes(): number {
		return this.pending.totalBytes
	}

	/** The effective buffer cap, after the floor described in the constructor. */
	get queueCapBytes(): number {
		return this.maxQueuedBytes
	}

	get currentEpoch(): number {
		return this.epoch
	}

	/** Events waiting to run; the specs use it to assert the queue stays bounded. */
	get queuedEvents(): number {
		return this.queue.length
	}

	/**
	 * Accepts a datagram. Runs synchronously on the socket callback and never awaits:
	 * the UDP socket cannot be given backpressure, so the work goes on the queue.
	 *
	 * Only the datagram is unconditional. The packet event that processes it is
	 * coalesced, because the event queue is the one thing here that has no cap - see
	 * the comment below.
	 */
	offer(data: Buffer, receivedAt: Date): void {
		if (this.state.name === 'Terminated') return

		// An unowned port only buffers traffic that looks like the transport it serves;
		// once owned, everything is relayed, so a filter that misjudges live traffic
		// cannot starve a running stream. That also lets anything that reaches the port
		// count as activity and, while running, refresh the lease - bounded by the
		// producer ending when authentication stops; see runningWrite.
		if (!this.ownsSlot && this.config.admit?.(data) === false) return

		// Recorded immediately, with the real arrival time, so stream-activity and
		// inactivity detection never depend on how long processing takes.
		this.activity.onPacketReceived(this.port, receivedAt)

		this.pending.push(data)
		const dropped = this.pending.evictTo(this.maxQueuedBytes)
		if (dropped > 0) this.noteDropped(dropped)

		// One queued packet event covers every datagram buffered behind it: the handler
		// always drains the whole buffer, so a second event would find nothing left to
		// do. Appending one per datagram instead would grow the queue without bound
		// whenever an event is blocked on lock acquisition or a producer start - the
		// memory growth the bounded buffer alone does not prevent.
		if (this.packetEventQueued) return
		this.packetEventQueued = true
		void this.enqueue(async () => {
			// Cleared as the event starts, not when it ends: a datagram arriving while
			// the handler runs may land after the buffer was drained, so it has to be
			// able to queue the next event.
			this.packetEventQueued = false
			await this.handlePacket()
		})
	}

	/** Inactivity: the activity tracker saw no packets for its timeout. */
	onInactive(): void {
		void this.enqueue(async () => this.handleInactive())
	}

	/**
	 * The producer exited on its own. `epoch` is the ownership lifetime it belonged to;
	 * a late event from a previous one is dropped.
	 */
	onProducerExited(epoch: number): void {
		void this.enqueue(async () => this.handleProducerExited(epoch))
	}

	/** The producer reported that traffic authenticated (SRTP). */
	onAuthenticated(): void {
		void this.enqueue(async () => this.handleAuthenticated())
	}

	/** Stops everything and releases the lock. The machine is unusable afterwards. */
	async shutdown(): Promise<void> {
		const done = this.enqueue(async () => this.handleShutdown())
		await done
	}

	/** Resolves when every queued event has run; for tests and for shutdown ordering. */
	async drained(): Promise<void> {
		while (this.draining || this.queue.length > 0) {
			await new Promise((resolve) => setImmediate(resolve))
		}
	}

	private async enqueue(task: () => Promise<void>): Promise<void> {
		let settle: () => void = () => undefined
		const done = new Promise<void>((resolve) => {
			settle = resolve
		})
		this.queue.push(async () => {
			try {
				await task()
			} finally {
				settle()
			}
		})
		void this.pump()
		return done
	}

	private async pump(): Promise<void> {
		if (this.draining) return
		this.draining = true
		try {
			while (this.queue.length > 0) {
				const task = this.queue.shift()
				if (task === undefined) break
				try {
					await task()
				} catch (err) {
					// A handler must never break the queue: the port would stop responding
					// to every later event, including its own teardown.
					const error = err instanceof Error ? err : new Error(String(err))
					this.logger.error('Unhandled error in ingestion event', error, {
						port: this.port,
						state: this.state.name,
					})
					this.onError?.(error)
				}
			}
		} finally {
			this.draining = false
		}
	}

	private transition(next: PortState): void {
		const from = this.state.name
		this.state = next
		this.onTransition?.(from, next.name)
	}

	private noteDropped(bytes: number): void {
		this.droppedBytes += bytes
		const now = this.now()
		if (now - this.lastDroppedLog < DROPPED_LOG_THROTTLE_MS) return
		this.lastDroppedLog = now
		this.logger.warn('Dropped buffered datagrams over the queue cap', {
			port: this.port,
			droppedBytes: this.droppedBytes,
			maxQueuedBytes: this.maxQueuedBytes,
		})
		this.droppedBytes = 0
	}

	private async handlePacket(): Promise<void> {
		switch (this.state.name) {
			case 'Idle':
				this.transition({ name: 'Buffering' })
				await this.startIfThresholdReached()
				return
			case 'Buffering':
				await this.startIfThresholdReached()
				return
			case 'Cooldown':
				if (this.now() >= this.state.retryAt) {
					this.transition({ name: 'Buffering' })
					await this.startIfThresholdReached()
				}
				return
			case 'Restarting': {
				// The lock is still held, so there is no threshold to re-reach: this is a
				// producer restart, not a fresh acquisition.
				if (this.now() < this.state.restartAt) return
				this.transition({ name: 'Starting' })
				await this.startProducer(this.pending.take())
				return
			}
			case 'Provisional':
				if (this.now() >= this.state.expiresAt) {
					this.logger.warn(
						'No authenticated traffic within the provisional window; releasing the port',
						{ port: this.port },
					)
					await this.teardown([], this.provisionalCooldownMs)
					return
				}
				this.relay()
				return
			case 'Running':
				await this.runningWrite()
				return
			case 'Acquiring':
			case 'Starting':
			case 'Stopping':
			case 'Terminated':
				// The first three only exist inside a handler, so no other event can
				// observe them; Terminated is already dropped in offer().
				return
		}
	}

	private async startIfThresholdReached(): Promise<void> {
		if (this.pending.totalBytes < this.config.minBytesBeforeStart) return
		await this.attemptAcquire()
	}

	/**
	 * Acquires the lock and starts the producer.
	 *
	 * The buffer is not cleared here: if acquisition is refused or fails, the datagrams
	 * are still queued and the port simply retries after its cooldown. Deleting the
	 * buffer before the outcome was known is what used to strand a port - the packets
	 * were gone, the stream had been marked active, and no later packet could re-enter
	 * the buffering path.
	 */
	private async attemptAcquire(): Promise<void> {
		this.transition({ name: 'Acquiring' })

		let acquired: boolean
		try {
			acquired = await this.locks.tryAcquireKinesisLock(
				this.port,
				this.config.instanceId,
			)
		} catch (err) {
			this.logger.error(
				'Failed to acquire the Kinesis lock',
				err instanceof Error ? err : new Error(String(err)),
				{ port: this.port },
			)
			this.transition({
				name: 'Cooldown',
				retryAt: this.now() + this.startRetryBackoffMs,
			})
			return
		}

		if (!acquired) {
			this.transition({
				name: 'Cooldown',
				retryAt: this.now() + this.startRetryBackoffMs,
			})
			return
		}

		this.epoch += 1

		// An inactivity event queued behind this acquisition means the stream stopped
		// while we were acquiring: release at once rather than starting a producer for a
		// stream nobody is sending.
		if (this.activity.getStreamState(this.port)?.status !== 'active') {
			this.transition({ name: 'Stopping' })
			await this.releaseLock()
			this.pending.clear()
			this.transition({ name: 'Idle' })
			return
		}

		this.transition({ name: 'Starting' })
		await this.startProducer(this.pending.take())
	}

	private async startProducer(datagrams: Buffer[]): Promise<void> {
		let failure: Error | undefined
		try {
			await this.producer.start(this.port, datagrams, { epoch: this.epoch })
		} catch (err) {
			failure = err instanceof Error ? err : new Error(String(err))
		}

		if (failure === undefined && this.producer.isActive(this.port)) {
			// Datagrams that arrived while start() was in flight are still buffered.
			// Send them now, before announcing the producer, so they go out strictly
			// after the initial batch and no state claims to be producing while
			// datagrams sit unsent behind it.
			this.relay()
			if (this.config.provisionalTimeoutMs !== undefined) {
				this.transition({
					name: 'Provisional',
					expiresAt: this.now() + this.config.provisionalTimeoutMs,
				})
			} else {
				this.transition({ name: 'Running' })
			}
			return
		}

		if (failure !== undefined) {
			this.logger.error('Failed to start ingestion', failure, {
				port: this.port,
			})
		} else {
			this.logger.error(
				'Ingestion start reported no active producer',
				new Error('producer inactive after start'),
				{ port: this.port },
			)
		}

		// Whatever start() did not send goes back to the front of the buffer, so a retry
		// still begins with the oldest datagrams - normally the keyframe.
		const unsent = this.producer.takeUnsentDatagrams?.(this.port) ?? datagrams
		await this.teardown(unsent, this.startRetryBackoffMs)
	}

	/**
	 * Relays and refreshes the lease - on every datagram, authenticated or not.
	 *
	 * The invariant is that unauthenticated traffic earns no lease refresh, and this is
	 * the one state where it holds only within a bound. By the time a port runs, the
	 * filter is behind it (see offer), and this machine cannot tell which datagrams the
	 * producer will authenticate. So for a transport that requires authentication, the
	 * bound is the producer's: it must end once authentication stops, and the port then
	 * restarts into Provisional, where nothing refreshes the lease until something
	 * authenticates again. For SRTP that is AUTH_LOSS_MS in SrtpProducer - 3 seconds -
	 * after which traffic nobody can authenticate has, at most, pushed a lease that
	 * lasts minutes a few seconds further out.
	 *
	 * Keeping it here rather than tying the refresh to authenticated output keeps this
	 * machine transport-neutral; the cost is that the bound lives in the producer, so
	 * both halves of it are pinned by tests.
	 */
	private async runningWrite(): Promise<void> {
		this.relay()

		const outcome = await this.locks.updateLastPacketTime(
			this.port,
			this.config.instanceId,
		)
		if (outcome === 'lostLock') {
			// Another instance owns the stream. Stop producing, but do not release: the
			// row is theirs now, and the conditional release would be a no-op anyway.
			this.logger.warn('Lost the port lock; stopping the producer', {
				port: this.port,
			})
			this.transition({ name: 'Stopping' })
			await this.stopProducer()
			this.transition({
				name: 'Cooldown',
				retryAt: this.now() + this.startRetryBackoffMs,
			})
			return
		}
		// 'writeError' says nothing about ownership: the lease is still held until it
		// goes stale, so relinquishing here would only churn the pipeline.
	}

	/**
	 * Sends everything buffered to the producer, in order.
	 *
	 * Normally this is the single datagram that triggered the event, but a datagram can
	 * arrive while a handler is mid-flight, so it drains rather than assuming one.
	 */
	private relay(): void {
		for (const datagram of this.pending.take()) {
			this.producer.writePacket(this.port, datagram)
		}
	}

	private async handleAuthenticated(): Promise<void> {
		if (this.state.name !== 'Provisional') return
		this.logger.info('Authenticated traffic confirmed', { port: this.port })
		this.transition({ name: 'Running' })
	}

	private async handleInactive(): Promise<void> {
		// A packet processed while this event was queued means the stream came back:
		// tearing it down now would leave an active stream with no ingestion.
		if (this.activity.getStreamState(this.port)?.status === 'active') return

		switch (this.state.name) {
			case 'Idle':
				return
			case 'Buffering':
			case 'Cooldown':
				// Nothing is owned and the stream is gone; a later packet starts over.
				this.pending.clear()
				this.transition({ name: 'Idle' })
				return
			case 'Provisional':
			case 'Running':
			case 'Restarting':
				await this.teardown([], undefined)
				this.transition({ name: 'Idle' })
				return
			case 'Acquiring':
			case 'Starting':
			case 'Stopping':
			case 'Terminated':
				return
		}
	}

	private async handleProducerExited(epoch: number): Promise<void> {
		// A late event from a previous ownership lifetime must not touch this one.
		if (epoch !== this.epoch) return
		if (this.state.name !== 'Running' && this.state.name !== 'Provisional')
			return

		this.logger.warn('Producer exited; will restart on the next packet', {
			port: this.port,
			restartDelayMs: this.restartDelayMs,
		})
		// Reap the exited child and release any transport resources it held before
		// announcing Restarting: that state asserts there is no producer, so tearing
		// it down afterwards would leave a window where both were true at once.
		await this.stopProducer()
		// The lock stays held: this instance is still the owner, and releasing it here
		// would hand the stream to another instance for the length of a restart.
		this.transition({
			name: 'Restarting',
			restartAt: this.now() + this.restartDelayMs,
		})
	}

	private async handleShutdown(): Promise<void> {
		if (this.state.name === 'Terminated') return
		if (this.ownsSlot) {
			await this.teardown([], undefined)
		}
		this.pending.clear()
		this.transition({ name: 'Terminated' })
	}

	/**
	 * The one teardown path: stop the producer, then release the lock, then re-arm.
	 *
	 * The order is the point. Releasing first lets another instance acquire the port and
	 * start writing to the same Kinesis stream while this producer is still alive, so
	 * the producer always dies first.
	 */
	private async teardown(
		datagramsToRearm: Buffer[],
		cooldownMs: number | undefined,
	): Promise<void> {
		this.transition({ name: 'Stopping' })
		await this.stopProducer()
		await this.releaseLock()

		this.pending.prependAll(datagramsToRearm)
		this.pending.evictTo(this.maxQueuedBytes)

		if (cooldownMs === undefined) {
			this.pending.clear()
			this.transition({ name: 'Idle' })
			return
		}
		this.transition({ name: 'Cooldown', retryAt: this.now() + cooldownMs })
	}

	private async stopProducer(): Promise<void> {
		try {
			await this.producer.stop(this.port)
		} catch (err) {
			this.logger.error(
				'Failed to stop the producer',
				err instanceof Error ? err : new Error(String(err)),
				{ port: this.port },
			)
		}
	}

	private async releaseLock(): Promise<void> {
		try {
			await this.locks.releaseKinesisLock(this.port, this.config.instanceId)
		} catch (err) {
			this.logger.error(
				'Failed to release the Kinesis lock',
				err instanceof Error ? err : new Error(String(err)),
				{ port: this.port },
			)
		}
	}
}
