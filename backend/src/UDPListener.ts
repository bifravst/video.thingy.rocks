import dgram from 'node:dgram'
import { EventEmitter } from 'node:events'
import { Logger } from './Logger.ts'

export type UDPListenerConfig = {
	portRange: { start: number; end: number }
	bufferSize: number
	flushInterval: number // milliseconds
	outputDirectory: string
}

export type PacketHandler = {
	/**
	 * Called for every valid datagram, synchronously on the socket's 'message' event.
	 *
	 * Returns void, not a promise: the listener cannot apply backpressure to a UDP socket,
	 * so a handler that awaited anything here (a DynamoDB write, credential resolution, a
	 * pipeline start) would let datagrams accumulate in an unbounded promise chain outside
	 * any buffer that bounds them. Handlers must record the packet and return; anything
	 * slower belongs behind their own bounded queue.
	 */
	onPacket(port: number, data: Buffer, timestamp: Date): void
	onStreamStart(port: number): Promise<void>
	onStreamStop(port: number, inactivityDuration: number): Promise<void>
}

export class UDPListener extends EventEmitter {
	private readonly config: UDPListenerConfig
	private readonly sockets: Map<number, dgram.Socket> = new Map()
	private packetHandler?: PacketHandler
	private readonly logger: Logger

	constructor(config: UDPListenerConfig) {
		super()
		this.config = config
		this.logger = new Logger('UDPListener')
	}

	setPacketHandler(handler: PacketHandler): void {
		this.packetHandler = handler
	}

	async start(): Promise<void> {
		const ports = this.getPortRange()

		for (const port of ports) {
			await this.bindPort(port)
		}
	}

	async stop(): Promise<void> {
		const closePromises: Promise<void>[] = []

		for (const [port, socket] of this.sockets.entries()) {
			closePromises.push(
				new Promise<void>((resolve) => {
					socket.close(() => {
						this.logger.info('Closed socket', { port })
						resolve()
					})
				}),
			)
		}

		await Promise.all(closePromises)
		this.sockets.clear()
	}

	getActiveStreams(): number[] {
		return Array.from(this.sockets.keys())
	}

	private isValidPacket(data: Buffer): boolean {
		// Basic validation: check if packet has minimum size
		// MPEG-TS packets are typically 188 bytes, but we'll accept any non-empty packet
		if (data.length === 0) {
			return false
		}

		// Additional validation could be added here (e.g., check for MPEG-TS sync byte 0x47)
		// For now, we accept any non-empty packet
		return true
	}

	private getPortRange(): number[] {
		const ports: number[] = []
		for (
			let port = this.config.portRange.start;
			port <= this.config.portRange.end;
			port++
		) {
			ports.push(port)
		}
		return ports
	}

	private async bindPort(port: number, retryCount = 0): Promise<void> {
		const maxRetries = 3
		const backoffMs = Math.pow(2, retryCount) * 1000 // Exponential backoff

		return new Promise((resolve, reject) => {
			// udp6 with ipv6Only:false is dual-stack on Linux: IPv4 packets arrive
			// as ::ffff:<v4-addr>. The NLB target group is IPv6, so the NLB forwards
			// every packet over IPv6 to the instance (IPv4 clients are translated via
			// the source-NAT prefix).
			const socket = dgram.createSocket({ type: 'udp6', ipv6Only: false })

			socket.on('error', (err) => {
				this.logger.warn('Error on port', {
					port,
					error: err.message,
					retryCount,
				})

				if (retryCount < maxRetries) {
					this.logger.info('Retrying port binding', {
						port,
						backoffMs,
						attempt: retryCount + 1,
						maxRetries,
					})
					// Use void to explicitly ignore the promise
					void (async () => {
						try {
							await new Promise((r) => setTimeout(r, backoffMs))
							await this.bindPort(port, retryCount + 1)
							resolve()
						} catch (retryErr) {
							reject(
								retryErr instanceof Error
									? retryErr
									: new Error(String(retryErr)),
							)
						}
					})()
				} else {
					this.logger.error(
						'Failed to bind port after retries',
						err instanceof Error ? err : new Error(String(err)),
						{ port, maxRetries },
					)
					reject(err)
				}
			})

			socket.on('message', (msg, rinfo) => {
				const timestamp = new Date()

				// Validate packet (basic validation)
				if (!this.isValidPacket(msg)) {
					this.logger.warn('Malformed packet received, discarding', {
						port,
						packetSize: msg.length,
						source: `${rinfo.address}:${rinfo.port}`,
					})
					return
				}

				if (this.packetHandler) {
					// A throw here would otherwise escape the 'message' listener as an
					// uncaught exception and take the process down, losing every port.
					try {
						this.packetHandler.onPacket(port, msg, timestamp)
					} catch (err) {
						this.logger.error(
							'Packet handler threw',
							err instanceof Error ? err : new Error(String(err)),
							{ port },
						)
					}
				}

				this.emit('packet', { port, data: msg, timestamp })
			})

			socket.on('listening', () => {
				const address = socket.address()
				this.logger.info('Listening on port', { port: address.port })
				this.sockets.set(port, socket)
				resolve()
			})

			socket.bind(port)
		})
	}
}
