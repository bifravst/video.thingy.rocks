import assert from 'node:assert/strict'
import net from 'node:net'
import { describe, it } from 'node:test'

import { loadConfig } from './config.ts'
import { HealthServer } from './HealthServer.ts'
import {
	IngestionService,
	type ActivitySource,
	type ExitingProducer,
	type HealthPort,
	type IngestionTransport,
	type PacketSource,
	type TrafficRecorder,
} from './IngestionService.ts'
import type { OwnedWriteResult } from './StreamMetadataService.ts'

const PORTS = { start: 5000, end: 5009 }

type PacketDelivery = (port: number, data: Buffer, timestamp: Date) => void

class ListenerFake implements PacketSource {
	deliver: PacketDelivery = () => undefined
	started = false
	stopped = false
	failOnStart?: Error

	setPacketHandler(handler: { onPacket: PacketDelivery }) {
		this.deliver = handler.onPacket
	}

	async start() {
		if (this.failOnStart !== undefined) throw this.failOnStart
		this.started = true
	}

	async stop() {
		this.stopped = true
	}
}

class ActivityFake implements ActivitySource {
	status = new Map<number, 'active' | 'inactive'>()
	stopped = false
	private listener: (port: number) => void = () => undefined

	onPacketReceived(port: number) {
		this.status.set(port, 'active')
	}

	getStreamState(port: number) {
		const status = this.status.get(port)
		return status === undefined ? undefined : { status }
	}

	on(_event: 'streamStop', listener: (port: number) => void) {
		this.listener = listener
		return this
	}

	stop() {
		this.stopped = true
	}

	/** Mimics the inactivity timeout firing for a port. */
	fireStop(port: number) {
		this.status.set(port, 'inactive')
		this.listener(port)
	}
}

class ProducerFake implements ExitingProducer {
	readonly startedPorts: number[] = []
	readonly stoppedPorts: number[] = []
	shutdownCalls = 0
	private readonly active = new Set<number>()
	private readonly epochs = new Map<number, number>()
	private exitListener: (port: number) => void = () => undefined

	async start(port: number, _datagrams: Buffer[], context: { epoch: number }) {
		this.startedPorts.push(port)
		this.epochs.set(port, context.epoch)
		this.active.add(port)
	}

	async stop(port: number) {
		this.stoppedPorts.push(port)
		this.active.delete(port)
	}

	writePacket() {
		/* not asserted here */
	}

	isActive(port: number) {
		return this.active.has(port)
	}

	epochForPort(port: number) {
		return this.epochs.get(port)
	}

	onExit(listener: (port: number) => void) {
		this.exitListener = listener
	}

	async shutdown() {
		this.shutdownCalls += 1
	}

	simulateExit(port: number) {
		this.active.delete(port)
		this.exitListener(port)
	}
}

class LockFake {
	readonly order: string[] = []
	async tryAcquireKinesisLock(port: number) {
		this.order.push(`acquire:${String(port)}`)
		return true
	}
	async releaseKinesisLock(port: number) {
		this.order.push(`release:${String(port)}`)
	}
	async updateLastPacketTime(): Promise<OwnedWriteResult> {
		return 'ok'
	}
}

class HealthFake implements HealthPort {
	readonly port = 9999
	started = false
	stopped = false
	failOnStart?: Error
	async start() {
		if (this.failOnStart !== undefined) throw this.failOnStart
		this.started = true
	}
	async stop() {
		this.stopped = true
	}
}

/** Records what the service counts, so the per-transport scoping can be asserted. */
class TrafficFake implements TrafficRecorder {
	readonly recorded: { transport: string; byteCount: number }[] = []
	started = false
	stopped = false
	publishCalls = 0

	record(transport: string, byteCount: number) {
		this.recorded.push({ transport, byteCount })
	}
	start() {
		this.started = true
	}
	stop() {
		this.stopped = true
	}
	async publishNow() {
		this.publishCalls += 1
	}

	bytesFor(transport: string): number {
		return this.recorded
			.filter((r) => r.transport === transport)
			.reduce((sum, r) => sum + r.byteCount, 0)
	}
}

const build = (
	overrides: {
		producer?: ExitingProducer | null
		health?: HealthPort
		listener?: ListenerFake
		extraTransports?: IngestionTransport[]
		traffic?: TrafficRecorder
	} = {},
) => {
	const listener = overrides.listener ?? new ListenerFake()
	const activity = new ActivityFake()
	const locks = new LockFake()
	const producer =
		overrides.producer === undefined ? new ProducerFake() : overrides.producer
	const healthServer = overrides.health ?? new HealthFake()
	const traffic = overrides.traffic
	const service = new IngestionService({
		config: {
			...loadConfig({ KINESIS_STREAM_PREFIX: 'test-video' }),
			kinesisMinBytesBeforeStart: 0,
		},
		instanceId: 'i-test',
		locks,
		activity,
		transports: [
			{ name: 'unencrypted', portRange: PORTS, listener, producer },
			...(overrides.extraTransports ?? []),
		],
		healthServer,
		traffic,
	})
	return { service, listener, activity, locks, producer, healthServer, traffic }
}

const settle = async (): Promise<void> => {
	for (let i = 0; i < 20; i++) {
		await new Promise((resolve) => setImmediate(resolve))
	}
}

void describe('IngestionService', () => {
	void it('creates one machine per configured port', () => {
		const { service } = build()
		assert.strictEqual(service.stateFor(PORTS.start), 'Idle')
		assert.strictEqual(service.stateFor(PORTS.end), 'Idle')
		assert.strictEqual(service.stateFor(PORTS.end + 1), undefined)
	})

	void it('routes a datagram to its own port', async () => {
		const { service, listener, producer } = build()
		listener.deliver(5003, Buffer.alloc(10), new Date())
		await settle()
		assert.strictEqual(service.stateFor(5003), 'Running')
		assert.strictEqual(service.stateFor(5004), 'Idle')
		assert.deepStrictEqual((producer as ProducerFake).startedPorts, [5003])
	})

	void it('ignores a datagram for a port it does not serve', async () => {
		const { listener, producer } = build()
		listener.deliver(6000, Buffer.alloc(10), new Date())
		await settle()
		assert.deepStrictEqual((producer as ProducerFake).startedPorts, [])
	})

	void it('releases a port when the activity tracker reports it stopped', async () => {
		const { service, listener, activity, locks } = build()
		listener.deliver(5000, Buffer.alloc(10), new Date())
		await settle()
		activity.fireStop(5000)
		await settle()
		assert.strictEqual(service.stateFor(5000), 'Idle')
		assert.deepStrictEqual(locks.order, ['acquire:5000', 'release:5000'])
	})

	// The exit has to be attributed to the lifetime that started the producer, or a
	// late event would tear down whatever owns the port by the time it arrives.
	void it('attributes a producer exit to the epoch it was started with', async () => {
		const { service, listener, producer } = build()
		listener.deliver(5000, Buffer.alloc(10), new Date())
		await settle()
		;(producer as ProducerFake).simulateExit(5000)
		await settle()
		assert.strictEqual(service.stateFor(5000), 'Restarting')
	})

	void it('ignores an exit for a producer that was never started', async () => {
		const { service, producer } = build()
		;(producer as ProducerFake).simulateExit(5000)
		await settle()
		assert.strictEqual(service.stateFor(5000), 'Idle')
	})

	void it('tracks state without a producer when Kinesis is disabled', async () => {
		const { service, listener, locks } = build({ producer: null })
		listener.deliver(5000, Buffer.alloc(10), new Date())
		await settle()
		// No producer means nothing can become active, so the port never claims the slot.
		assert.strictEqual(service.stateFor(5000), 'Cooldown')
		assert.deepStrictEqual(locks.order, ['acquire:5000', 'release:5000'])
	})

	void describe('startup', () => {
		void it('opens the health port only after the listener is bound', async () => {
			const order: string[] = []
			const listener = new ListenerFake()
			const originalStart = listener.start.bind(listener)
			listener.start = async () => {
				order.push('listener')
				await originalStart()
			}
			const health = new HealthFake()
			health.start = async () => {
				order.push('health')
				health.started = true
			}

			const { service } = build({ listener, health })
			await service.start()
			assert.deepStrictEqual(order, ['listener', 'health'])
		})

		// A real bind failure must surface as a rejection the caller can act on, not as
		// an uncaught exception - see HealthServer.
		void it('rejects when the health port is already taken', async () => {
			const blocker = net.createServer()
			const port = await new Promise<number>((resolve) => {
				blocker.listen(0, '::', () => {
					const address = blocker.address()
					resolve(
						address !== null && typeof address !== 'string' ? address.port : 0,
					)
				})
			})
			try {
				const { service } = build({ health: new HealthServer(port) })
				await assert.rejects(async () => service.start())
			} finally {
				await new Promise<void>((resolve) => blocker.close(() => resolve()))
			}
		})
	})

	void describe('shutdown', () => {
		void it('stops serving, releases every held port, then disposes the producer', async () => {
			const { service, listener, activity, locks, producer, healthServer } =
				build()
			listener.deliver(5000, Buffer.alloc(10), new Date())
			listener.deliver(5001, Buffer.alloc(10), new Date())
			await settle()

			await service.shutdown()

			assert.strictEqual((healthServer as HealthFake).stopped, true)
			assert.strictEqual(listener.stopped, true)
			assert.strictEqual(activity.stopped, true)
			assert.strictEqual(service.stateFor(5000), 'Terminated')
			assert.strictEqual(service.stateFor(5001), 'Terminated')
			assert.ok(locks.order.includes('release:5000'))
			assert.ok(locks.order.includes('release:5001'))
			assert.strictEqual((producer as ProducerFake).shutdownCalls, 1)
		})

		void it('leaves every port unable to accept more traffic', async () => {
			const { service, listener, producer } = build()
			await service.shutdown()
			listener.deliver(5000, Buffer.alloc(10), new Date())
			await settle()
			assert.deepStrictEqual((producer as ProducerFake).startedPorts, [])
		})

		void it('is safe with no ports ever active', async () => {
			const { service, locks } = build()
			await service.shutdown()
			assert.deepStrictEqual(locks.order, [])
		})
	})
})

void describe('IngestionService with an additive transport', () => {
	const srtpPorts = { start: 6000, end: 6009 }

	const buildWithSrtp = (
		srtpListener: ListenerFake,
		srtpProducer: ProducerFake | null = new ProducerFake(),
	) => {
		const built = build({
			extraTransports: [
				{
					name: 'srtp',
					portRange: srtpPorts,
					listener: srtpListener,
					producer: srtpProducer,
					provisionalTimeoutMs: 20_000,
					provisionalCooldownMs: 60_000,
				},
			],
		})
		return { ...built, srtpProducer }
	}

	void it('serves both port ranges from their own listeners', async () => {
		const srtpListener = new ListenerFake()
		const { service, listener, producer, srtpProducer } =
			buildWithSrtp(srtpListener)

		listener.deliver(5000, Buffer.alloc(10), new Date())
		srtpListener.deliver(6000, Buffer.alloc(10), new Date())
		await settle()

		assert.strictEqual(service.stateFor(5000), 'Running')
		// The additive transport waits for authentication before it counts as running.
		assert.strictEqual(service.stateFor(6000), 'Provisional')
		assert.deepStrictEqual((producer as ProducerFake).startedPorts, [5000])
		assert.deepStrictEqual(srtpProducer?.startedPorts, [6000])
	})

	// Each port has its own Kinesis stream and its own lock row, so the paired ports
	// never contend: both can own their slot at the same time.
	void it('lets paired ports own their own slots independently', async () => {
		const srtpListener = new ListenerFake()
		const { service, listener, locks } = buildWithSrtp(srtpListener)

		listener.deliver(5000, Buffer.alloc(10), new Date())
		srtpListener.deliver(6000, Buffer.alloc(10), new Date())
		await settle()

		assert.deepStrictEqual(locks.order, ['acquire:5000', 'acquire:6000'])
		assert.strictEqual(service.stateFor(5000), 'Running')
		assert.strictEqual(service.stateFor(6000), 'Provisional')
	})

	void it('promotes a provisional port when the transport reports authentication', async () => {
		const srtpListener = new ListenerFake()
		const { service } = buildWithSrtp(srtpListener)
		srtpListener.deliver(6000, Buffer.alloc(10), new Date())
		await settle()
		service.authenticated(6000)
		await settle()
		assert.strictEqual(service.stateFor(6000), 'Running')
	})

	// The health port says "this backend is up". An additive transport that cannot
	// bind must not close it and have the fleet replaced over a condition that is
	// identical on every instance.
	void it('keeps running when the additive transport cannot bind', async () => {
		const srtpListener = new ListenerFake()
		srtpListener.failOnStart = new Error('EADDRINUSE')
		const { service, listener, healthServer } = buildWithSrtp(srtpListener)

		await service.start()

		assert.strictEqual(listener.started, true)
		assert.strictEqual((healthServer as HealthFake).started, true)
		// And the half-bound listener was cleaned up.
		assert.strictEqual(srtpListener.stopped, true)
	})

	void it('fails startup when the primary transport cannot bind', async () => {
		const listener = new ListenerFake()
		listener.failOnStart = new Error('EADDRINUSE')
		const { service, healthServer } = build({ listener })
		await assert.rejects(async () => service.start())
		assert.strictEqual((healthServer as HealthFake).started, false)
	})

	void it('shuts every transport down', async () => {
		const srtpListener = new ListenerFake()
		const { service, listener, srtpProducer } = buildWithSrtp(srtpListener)
		await service.shutdown()
		assert.strictEqual(listener.stopped, true)
		assert.strictEqual(srtpListener.stopped, true)
		assert.strictEqual(srtpProducer?.shutdownCalls, 1)
		assert.strictEqual(service.stateFor(6000), 'Terminated')
	})
})

void describe('IngestionService transport preparation', () => {
	void it('prepares the primary transport before its listener binds', async () => {
		const order: string[] = []
		const listener = new ListenerFake()
		const originalStart = listener.start.bind(listener)
		listener.start = async () => {
			order.push('listen')
			await originalStart()
		}
		const { service } = build({ listener })
		// The primary transport's own preparation is fatal, like its listener.
		await service.start()
		assert.deepStrictEqual(order, ['listen'])
	})

	// An additive transport's setup can stall for minutes on an unreachable
	// dependency, so it must not run before the working path is already serving - and
	// the health port must not wait behind it either, or the instance stays unhealthy
	// in every target group for the length of that stall.
	void it('prepares an additive transport only after the health port is open', async () => {
		const order: string[] = []
		const primary = new ListenerFake()
		const primaryStart = primary.start.bind(primary)
		primary.start = async () => {
			order.push('primary-listen')
			await primaryStart()
		}
		const srtpListener = new ListenerFake()
		const srtpStart = srtpListener.start.bind(srtpListener)
		srtpListener.start = async () => {
			order.push('srtp-listen')
			await srtpStart()
		}
		const health = new HealthFake()
		health.start = async () => {
			order.push('health')
			health.started = true
		}

		const { service } = build({
			listener: primary,
			health,
			extraTransports: [
				{
					name: 'srtp',
					portRange: { start: 6000, end: 6009 },
					listener: srtpListener,
					producer: new ProducerFake(),
					prepare: async () => {
						order.push('srtp-prepare')
					},
				},
			],
		})
		await service.start()
		assert.deepStrictEqual(order, [
			'primary-listen',
			'health',
			'srtp-prepare',
			'srtp-listen',
		])
	})

	// The stall this exists for: an SRTP key lookup that never returns must not hold
	// the health port closed, so it is tested with preparation that does not finish.
	void it('opens the health port while an additive transport is still stalled', async () => {
		const srtpListener = new ListenerFake()
		const health = new HealthFake()
		const { service } = build({
			health,
			extraTransports: [
				{
					name: 'srtp',
					portRange: { start: 6000, end: 6009 },
					listener: srtpListener,
					producer: new ProducerFake(),
					prepare: async () => new Promise(() => undefined),
				},
			],
		})

		const starting = service.start()
		await settle()
		assert.strictEqual(health.started, true)
		assert.strictEqual(srtpListener.started, false)
		// start() is still pending on the stalled transport, which is why the health
		// port could not be left until the end.
		const outcome = await Promise.race([
			starting.then(() => 'returned'),
			new Promise((resolve) => setTimeout(() => resolve('pending'), 20)),
		])
		assert.strictEqual(outcome, 'pending')
	})

	/**
	 * The signal the zero-ingestion alarms pair with "did anything reach Kinesis".
	 *
	 * It has to be attributed to the transport the datagram arrived on: the load
	 * balancer's own byte count covers both transports at once, which is what made the
	 * restart composite compare two different things.
	 */
	void describe('per-transport traffic reporting', () => {
		const withSrtp = (
			traffic: TrafficRecorder,
		): ReturnType<typeof build> & { srtpListener: ListenerFake } => {
			const srtpListener = new ListenerFake()
			const built = build({
				traffic,
				extraTransports: [
					{
						name: 'srtp',
						portRange: { start: 6000, end: 6009 },
						listener: srtpListener,
						producer: new ProducerFake(),
					},
				],
			})
			return { ...built, srtpListener }
		}

		void it('attributes each datagram to the transport it arrived on', async () => {
			const traffic = new TrafficFake()
			const { listener, srtpListener } = withSrtp(traffic)

			listener.deliver(5000, Buffer.alloc(700), new Date())
			listener.deliver(5001, Buffer.alloc(300), new Date())
			srtpListener.deliver(6000, Buffer.alloc(40), new Date())
			await settle()

			assert.strictEqual(traffic.bytesFor('unencrypted'), 1000)
			assert.strictEqual(traffic.bytesFor('srtp'), 40)
		})

		// Otherwise the failure where a transport admits nothing looks like no traffic
		// at all, and the alarm gated on traffic can never fire for it.
		void it('counts a datagram no port accepts', async () => {
			const traffic = new TrafficFake()
			const { srtpListener } = withSrtp(traffic)

			// Outside every configured port range, so no machine takes it.
			srtpListener.deliver(6100, Buffer.alloc(80), new Date())
			await settle()

			assert.strictEqual(traffic.bytesFor('srtp'), 80)
		})

		// Its zeros are what tell the alarms this instance is reporting at all, so they
		// must not wait behind an additive transport that can stall for minutes.
		void it('starts reporting before any additive transport is prepared', async () => {
			const traffic = new TrafficFake()
			const srtpListener = new ListenerFake()
			const { service } = build({
				traffic,
				extraTransports: [
					{
						name: 'srtp',
						portRange: { start: 6000, end: 6009 },
						listener: srtpListener,
						producer: new ProducerFake(),
						prepare: async () => new Promise(() => undefined),
					},
				],
			})

			void service.start()
			await settle()

			assert.strictEqual(traffic.started, true)
			assert.strictEqual(srtpListener.started, false)
		})

		void it('reports the final period on shutdown, after stopping the interval', async () => {
			const traffic = new TrafficFake()
			const { service } = build({ traffic })
			await service.start()
			await service.shutdown()

			assert.strictEqual(traffic.stopped, true)
			assert.strictEqual(
				traffic.publishCalls,
				1,
				'the last period would otherwise be lost',
			)
		})

		void it('runs without a recorder at all', async () => {
			const { service, listener } = build()
			await service.start()
			listener.deliver(5000, Buffer.alloc(10), new Date())
			await settle()
			await assert.doesNotReject(service.shutdown())
		})
	})

	void it('keeps serving when an additive transport cannot prepare', async () => {
		const srtpListener = new ListenerFake()
		const { service, listener, healthServer } = build({
			extraTransports: [
				{
					name: 'srtp',
					portRange: { start: 6000, end: 6009 },
					listener: srtpListener,
					producer: new ProducerFake(),
					prepare: async () => {
						throw new Error('parameter store unreachable')
					},
				},
			],
		})

		await service.start()

		assert.strictEqual(listener.started, true)
		assert.strictEqual((healthServer as HealthFake).started, true)
		// Its listener never bound, so the transport is simply absent.
		assert.strictEqual(srtpListener.started, false)
	})
})
