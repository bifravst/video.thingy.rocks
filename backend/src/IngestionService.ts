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

export type IngestionServiceOptions = {
	config: IngestionConfig
	instanceId: string
	locks: IngestionLockStore
	activity: ActivitySource
	/** Null when Kinesis ingestion is disabled; the service then only tracks state. */
	producer: ExitingProducer | null
	listener: PacketSource
	healthServer: HealthPort
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

		const { config, producer } = options
		for (
			let port = config.portRange.start;
			port <= config.portRange.end;
			port++
		) {
			this.machines.set(port, this.createMachine(port))
		}

		options.listener.setPacketHandler({
			onPacket: (port, data, timestamp) => {
				this.machines.get(port)?.offer(data, timestamp)
			},
			// The machine starts from packets, so a resume needs no separate path - the
			// fire-and-forget resume callback that used to race the threshold path is
			// gone rather than guarded.
			onStreamStart: async () => undefined,
			onStreamStop: async () => undefined,
		})

		options.activity.on('streamStop', (port: number) => {
			this.machines.get(port)?.onInactive()
		})

		producer?.onExit((port) => {
			const machine = this.machines.get(port)
			if (machine === undefined) return
			const epoch = producer.epochForPort(port)
			if (epoch === undefined) return
			machine.onProducerExited(epoch)
		})
	}

	private createMachine(port: number): PortIngestion {
		const { config, producer } = this.options
		return new PortIngestion(
			{
				port,
				instanceId: this.options.instanceId,
				minBytesBeforeStart: config.kinesisMinBytesBeforeStart,
			},
			{
				locks: this.options.locks,
				producer: producer ?? disabledProducer,
				activity: this.options.activity,
				logger: this.logger,
			},
		)
	}

	/** For health output and tests. */
	stateFor(port: number): string | undefined {
		return this.machines.get(port)?.stateName
	}

	async start(): Promise<void> {
		await this.options.listener.start()
		// The health port opens only once the listener behind it is bound, so an
		// instance never reports itself able to ingest before it can.
		await this.options.healthServer.start()
		this.logger.info('Ingestion started', {
			ports: `${this.options.config.portRange.start}-${this.options.config.portRange.end}`,
			healthPort: this.options.healthServer.port,
		})
	}

	/**
	 * Stops accepting traffic, then releases every port, then disposes the producer.
	 *
	 * The order matters: closing the health port first takes this instance out of the
	 * load balancer, and stopping the listener means no new packet can re-arm a port
	 * that is being torn down.
	 */
	async shutdown(): Promise<void> {
		await this.options.healthServer.stop()
		await this.options.listener.stop()
		this.options.activity.stop()
		await Promise.all(
			[...this.machines.values()].map(async (m) => m.shutdown()),
		)
		await this.options.producer?.shutdown()
		this.logger.info('Ingestion stopped')
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
