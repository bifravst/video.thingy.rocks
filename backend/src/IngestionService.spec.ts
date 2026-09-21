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
	type PacketSource,
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

const build = (
	overrides: {
		producer?: ExitingProducer | null
		health?: HealthPort
		listener?: ListenerFake
	} = {},
) => {
	const listener = overrides.listener ?? new ListenerFake()
	const activity = new ActivityFake()
	const locks = new LockFake()
	const producer =
		overrides.producer === undefined ? new ProducerFake() : overrides.producer
	const healthServer = overrides.health ?? new HealthFake()
	const service = new IngestionService({
		config: {
			...loadConfig({ KINESIS_STREAM_PREFIX: 'test-video' }),
			kinesisMinBytesBeforeStart: 0,
		},
		instanceId: 'i-test',
		locks,
		activity,
		producer,
		listener,
		healthServer,
	})
	return { service, listener, activity, locks, producer, healthServer }
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
