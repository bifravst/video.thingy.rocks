import type { IngestionConfig } from './config.ts'
import type { KinesisIngestionPipeline } from './KinesisIngestionPipeline.ts'
import { Logger } from './Logger.ts'
import {
	PortIngestion,
	type IngestionLockStore,
	type IngestionProducer,
	type StreamActivityTracker,
} from './PortIngestion.ts'

/** The activity tracker, plus the events and teardown the service drives. */
export type ActivitySource = StreamActivityTracker & {
	on(event: 'streamStop', listener: (port: number) => void): unknown
	stop(): void
}

/** What the service needs from a UDP listener. */
export type PacketSource = {
	setPacketHandler(handler: {
		onPacket(port: number, data: Buffer, timestamp: Date): void
		onStreamStart(port: number): Promise<void>
		onStreamStop(port: number, inactivityDuration: number): Promise<void>
	}): void
	start(): Promise<void>
	stop(): Promise<void>
}

/** What the service needs from a health port. */
export type HealthPort = {
	readonly port: number
	start(): Promise<void>
	stop(): Promise<void>
}

/**
 * Where per-transport traffic is counted, so the restart alarms can be scoped to one
 * transport. See TransportTrafficMetrics for why the load balancer's own metric cannot.
 */
export type TrafficRecorder = {
	record(transport: string, byteCount: number): void
	setServing(transport: string, serving: boolean): void
	start(): void
	stop(): void
	publishNow(): Promise<void>
}

/**
 * A producer that also announces when one of its children exits on its own.
 *
 * The exit has to be attributable to the ownership lifetime that started it, which is
 * why the adapter records the epoch at start time rather than reading it when the
 * event arrives - by then the port may already be on its next lifetime, and a late
 * event would tear that one down.
 */
export type ExitingProducer = IngestionProducer & {
	/** Epoch recorded when the currently-running producer for this port was started. */
	epochForPort(port: number): number | undefined
	onExit(listener: (port: number) => void): void
	shutdown(): Promise<void>
}

/**
 * One ingest transport: its ports, its listener, its producer, and the per-port
 * settings that differ between transports.
 *
 * Transports are independent by construction. They share the lock table, keyed by the
 * raw port, and nothing else - each port has its own Kinesis stream, so two transports
 * never contend for one destination and no cross-transport arbitration exists to get
 * wrong.
 */
export type IngestionTransport = {
	name: string
	portRange: { start: number; end: number }
	listener: PacketSource
	/** Null when this transport cannot produce; its ports then only track state. */
	producer: ExitingProducer | null
	/**
	 * Set when the producer must report authentication before a port counts as
	 * running. Until then the port relays traffic but refreshes no lease.
	 */
	provisionalTimeoutMs?: number
	provisionalCooldownMs?: number
	/** Per-port syntactic filter applied while a port is unowned. */
	admitFor?: (port: number) => (datagram: Buffer) => boolean
	/**
	 * Work this transport needs done before its listener binds - resolving keys, for
	 * instance. Runs inside the same isolation as the listener, so for an additive
	 * transport a failure here disables that transport and nothing else.
	 */
	prepare?: () => Promise<void>
}

export type IngestionServiceOptions = {
	config: IngestionConfig
	instanceId: string
	locks: IngestionLockStore
	activity: ActivitySource
	transports: IngestionTransport[]
	healthServer: HealthPort
	/** Optional: without it the per-transport traffic metric is simply not reported. */
	traffic?: TrafficRecorder
	logger?: Logger
}

/**
 * Wires one PortIngestion per port to the UDP listener, the activity tracker and the
 * producer, and owns startup and shutdown ordering.
 *
 * Everything is passed in rather than constructed at module scope: the module-level
 * singletons this replaces ran on import, which is why none of this path could be
 * tested.
 */
export class IngestionService {
	private readonly options: IngestionServiceOptions
	private readonly logger: Logger
	private readonly machines = new Map<number, PortIngestion>()

	constructor(options: IngestionServiceOptions) {
		this.options = options
		this.logger = options.logger ?? new Logger('IngestionService')

		for (const transport of options.transports) {
			this.wireTransport(transport)
		}

		options.activity.on('streamStop', (port: number) => {
			this.machines.get(port)?.onInactive()
		})
	}

	private wireTransport(transport: IngestionTransport): void {
		const { portRange, producer } = transport
		for (let port = portRange.start; port <= portRange.end; port++) {
			this.machines.set(port, this.createMachine(transport, port))
		}

		transport.listener.setPacketHandler({
			onPacket: (port: number, data: Buffer, timestamp: Date) => {
				// Counted before admission, deliberately: the metric answers "is traffic
				// arriving on this transport", which is the question the restart alarms
				// pair with "is it reaching Kinesis". Counting only admitted datagrams
				// would hide the failure where nothing is admitted.
				this.options.traffic?.record(transport.name, data.length)
				this.machines.get(port)?.offer(data, timestamp)
			},
			// The machine starts from packets, so a resume needs no separate path - the
			// fire-and-forget resume callback that used to race the threshold path is
			// gone rather than guarded.
			onStreamStart: async () => undefined,
			onStreamStop: async () => undefined,
		})

		producer?.onExit((port: number) => {
			const machine = this.machines.get(port)
			if (machine === undefined) return
			const epoch = producer.epochForPort(port)
			if (epoch === undefined) return
			machine.onProducerExited(epoch)
		})
	}

	/** Lets a transport report that a port's traffic authenticated. */
	authenticated(port: number): void {
		this.machines.get(port)?.onAuthenticated()
	}

	private createMachine(
		transport: IngestionTransport,
		port: number,
	): PortIngestion {
		return new PortIngestion(
			{
				port,
				instanceId: this.options.instanceId,
				minBytesBeforeStart: this.options.config.kinesisMinBytesBeforeStart,
				provisionalTimeoutMs: transport.provisionalTimeoutMs,
				provisionalCooldownMs: transport.provisionalCooldownMs,
				admit: transport.admitFor?.(port),
			},
			{
				locks: this.options.locks,
				producer: transport.producer ?? disabledProducer,
				activity: this.options.activity,
				logger: this.logger,
			},
		)
	}

	/** For health output and tests. */
	stateFor(port: number): string | undefined {
		return this.machines.get(port)?.stateName
	}

	/**
	 * Binds the primary transport, opens the health port, then adds the rest.
	 *
	 * The first transport is the one the health port represents and its failure is
	 * fatal; every later transport is additive, so a failure there is logged and the
	 * service keeps running without it. That asymmetry is deliberate: the health port
	 * says "this instance's backend is up", and the Auto Scaling group replaces
	 * instances that fail it - so a condition identical on every instance, like an
	 * unreachable parameter store, must not be able to close it and churn the fleet.
	 *
	 * The health port therefore opens as soon as the primary listener is serving,
	 * before any additive setup. An additive transport is explicitly allowed to stall
	 * for minutes on an unreachable dependency, and waiting for it would leave a new
	 * instance failing health checks in every target group for that whole time -
	 * which is the same fleet-wide outage in a different disguise.
	 */
	async start(): Promise<void> {
		const [primary, ...additive] = this.options.transports
		if (primary === undefined)
			throw new Error('no ingest transports configured')

		try {
			await primary.prepare?.()
			await primary.listener.start()
			await this.options.healthServer.start()
		} catch (err) {
			// A failed primary start rolls the whole service back, not just the step
			// that failed. The listener binds its ports one at a time, so a failure
			// part-way through leaves the earlier ones bound - and they are already
			// wired to their machines, so a datagram arriving on one takes a lock and
			// spawns a producer. Stopping only the listener would leave that lock held
			// and that child running, and since the caller's response to this throw is
			// to exit, the child outlives the process as an orphan still writing to a
			// stream this instance no longer owns. Two writers to one Kinesis stream is
			// exactly what the locking exists to prevent.
			await this.rollBack()
			throw err
		}
		this.logger.info('Serving', {
			transport: primary.name,
			ports: `${primary.portRange.start}-${primary.portRange.end}`,
			healthPort: this.options.healthServer.port,
		})

		// Reporting starts with the primary listener rather than at the end of start().
		// Its zeros are what tell the alarms this instance is reporting at all, so a
		// stalled additive transport must not delay them - the same reason the health
		// port opens here.
		this.options.traffic?.setServing(primary.name, true)
		this.options.traffic?.start()

		for (const transport of additive) {
			try {
				// After the primary listener is serving and the health port is open,
				// deliberately: see start()'s doc comment.
				await transport.prepare?.()
				await transport.listener.start()
				// Every transport counts as not serving until it says otherwise, so one
				// that never gets here - because its preparation threw, or never
				// returned - is reported as not serving rather than going unmentioned.
				// Nothing arrives on a listener that never bound, so its byte count is a
				// steady zero that no traffic-gated alarm can tell from an idle
				// transport; this is the only signal that distinguishes them.
				this.options.traffic?.setServing(transport.name, true)
				this.logger.info('Transport started', {
					transport: transport.name,
					ports: `${transport.portRange.start}-${transport.portRange.end}`,
				})
			} catch (err) {
				this.logger.error(
					'Additive transport could not start; continuing without it',
					err instanceof Error ? err : new Error(String(err)),
					{ transport: transport.name },
				)
				await this.abandon(transport)
			}
		}

		this.logger.info('Ingestion started', {
			transports: this.options.transports.map((t) => t.name).join(', '),
			healthPort: this.options.healthServer.port,
		})
	}

	/**
	 * Undoes a failed start, keeping the error that caused it.
	 *
	 * Shutdown is the rollback: it closes the health port, stops every listener,
	 * releases every lock and reaps every producer, and each of those steps is a no-op
	 * for something that never started. A rollback that fails itself is logged rather
	 * than thrown, because the start failure is the one that explains the exit.
	 */
	private async rollBack(): Promise<void> {
		try {
			await this.shutdown()
		} catch (err) {
			this.logger.error(
				'Could not fully roll back a failed start; something may still be bound or running',
				err instanceof Error ? err : new Error(String(err)),
			)
		}
	}

	/**
	 * Stops accepting traffic, then releases every port, then disposes the producer.
	 *
	 * The order matters: closing the health port first takes this instance out of the
	 * load balancer, and stopping the listener means no new packet can re-arm a port
	 * that is being torn down.
	 *
	 * Every phase runs whatever happened in the ones before it, and only then does a
	 * failure surface. The phases that matter most come last - releasing locks and
	 * reaping producers - so a health port or a socket that would not close used to
	 * skip exactly those, and a failed-start rollback that hit one left the lock held
	 * and the GStreamer child running: the two-writers case the rollback is for.
	 */
	async shutdown(): Promise<void> {
		const failures: unknown[] = []
		const attempt = async (phase: string, work: () => Promise<unknown>) => {
			try {
				await work()
			} catch (err) {
				failures.push(err)
				this.logger.error(
					'Shutdown step failed; carrying on with the rest',
					err instanceof Error ? err : new Error(String(err)),
					{ phase },
				)
			}
		}

		await attempt('health port', async () => this.options.healthServer.stop())
		for (const transport of this.options.transports) {
			await attempt(`${transport.name} listener`, async () =>
				transport.listener.stop(),
			)
			this.options.traffic?.setServing(transport.name, false)
		}
		// Once no listener can add to them, report the counters one last time so the
		// final period is not lost - and stop first, so the interval cannot race it.
		this.options.traffic?.stop()
		await attempt('traffic metrics', async () =>
			this.options.traffic?.publishNow(),
		)
		await attempt('activity tracker', async () => this.options.activity.stop())
		await attempt('ports', async () =>
			this.shutDownMachines([...this.machines.values()]),
		)
		for (const transport of this.options.transports) {
			await attempt(`${transport.name} producer`, async () =>
				transport.producer?.shutdown(),
			)
		}

		if (failures.length > 0) {
			throw new AggregateError(failures, 'Ingestion stopped with failures')
		}
		this.logger.info('Ingestion stopped')
	}

	/** Shuts every one down, and reports a failure only once all have been tried. */
	private async shutDownMachines(machines: PortIngestion[]): Promise<void> {
		const results = await Promise.allSettled(
			machines.map(async (machine) => machine.shutdown()),
		)
		const failed = results.filter(
			(result): result is PromiseRejectedResult => result.status === 'rejected',
		)
		if (failed.length > 0) {
			throw new AggregateError(
				failed.map((result): unknown => result.reason),
				`${String(failed.length)} of ${String(machines.length)} ports did not shut down`,
			)
		}
	}

	/**
	 * Gives up on an additive transport that failed to start, leaving nothing of it.
	 *
	 * Not just its listener. The listener binds its ports one at a time, so a failure
	 * part-way leaves the earlier ones bound - and they are wired to their machines, so
	 * a datagram on one can take a lock and start a producer before the failure
	 * arrives. Stopping only the listener leaves that lock and that producer in place,
	 * with nothing left to deliver the traffic whose absence would end them sooner
	 * than the inactivity timeout. The service carries on without this transport, so
	 * its machines and producer are shut down for good rather than reset.
	 */
	private async abandon(transport: IngestionTransport): Promise<void> {
		const steps: [string, () => Promise<unknown>][] = [
			['listener', async () => transport.listener.stop()],
			[
				'ports',
				async () => {
					const machines: PortIngestion[] = []
					for (
						let port = transport.portRange.start;
						port <= transport.portRange.end;
						port++
					) {
						const machine = this.machines.get(port)
						if (machine !== undefined) machines.push(machine)
					}
					await this.shutDownMachines(machines)
				},
			],
			['producer', async () => transport.producer?.shutdown()],
		]
		for (const [step, work] of steps) {
			try {
				await work()
			} catch (err) {
				this.logger.error(
					'Could not fully abandon an additive transport',
					err instanceof Error ? err : new Error(String(err)),
					{ transport: transport.name, step },
				)
			}
		}
	}
}

/** Stands in when Kinesis ingestion is disabled: state is tracked, nothing is produced. */
const disabledProducer: IngestionProducer = {
	start: async () => undefined,
	stop: async () => undefined,
	writePacket: () => undefined,
	isActive: () => false,
}

/**
 * Adapts KinesisIngestionPipeline to the producer interface.
 *
 * Datagrams are concatenated here because this transport is a byte stream: MPEG-TS is
 * self-synchronizing, so joining datagrams is correct for it. A transport that depends
 * on datagram boundaries must not do this, which is why the machine hands over an array
 * and leaves the decision to the transport.
 */
export class KinesisProducerAdapter implements ExitingProducer {
	private readonly pipeline: KinesisIngestionPipeline
	private readonly epochs = new Map<number, number>()

	constructor(pipeline: KinesisIngestionPipeline) {
		this.pipeline = pipeline
	}

	async start(
		port: number,
		datagrams: Buffer[],
		context: { epoch: number },
	): Promise<void> {
		this.epochs.set(port, context.epoch)
		await this.pipeline.start(
			port,
			datagrams.length > 0 ? Buffer.concat(datagrams) : undefined,
		)
	}

	async stop(port: number): Promise<void> {
		await this.pipeline.stop(port)
		this.epochs.delete(port)
	}

	writePacket(port: number, data: Buffer): void {
		this.pipeline.writePacket(port, data)
	}

	isActive(port: number): boolean {
		return this.pipeline.isActive(port)
	}

	epochForPort(port: number): number | undefined {
		return this.epochs.get(port)
	}

	onExit(listener: (port: number) => void): void {
		this.pipeline.on('pipelineExited', ({ port }: { port: number }) => {
			listener(port)
		})
	}

	async shutdown(): Promise<void> {
		await this.pipeline.stopAll()
	}
}
