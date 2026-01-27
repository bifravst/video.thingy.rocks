import dgram from 'node:dgram'
import { EventEmitter } from 'node:events'

export type UDPListenerConfig = {
	portRange: { start: number; end: number }
	bufferSize: number
	flushInterval: number // milliseconds
	outputDirectory: string
}

export type PacketHandler = {
	onPacket(port: number, data: Buffer, timestamp: Date): Promise<void>
	onStreamStart(port: number): Promise<void>
	onStreamStop(port: number, inactivityDuration: number): Promise<void>
}

export class UDPListener extends EventEmitter {
	private readonly config: UDPListenerConfig
	private readonly sockets: Map<number, dgram.Socket> = new Map()
	private packetHandler?: PacketHandler

	constructor(config: UDPListenerConfig) {
		super()
		this.config = config
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
						console.log(`[UDPListener] Closed socket on port ${port}`)
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
			const socket = dgram.createSocket('udp4')

			socket.on('error', (err) => {
				console.warn(`[UDPListener] Error on port ${port}: ${err.message}`)

				if (retryCount < maxRetries) {
					console.log(
						`[UDPListener] Retrying port ${port} in ${backoffMs}ms (attempt ${retryCount + 1}/${maxRetries})`,
					)
					// Use void to explicitly ignore the promise
					void (async () => {
						try {
							await new Promise((r) => setTimeout(r, backoffMs))
							await this.bindPort(port, retryCount + 1)
							resolve()
						} catch (retryErr) {
							reject(retryErr)
						}
					})()
				} else {
					console.error(
						`[UDPListener] Failed to bind port ${port} after ${maxRetries} retries`,
					)
					reject(err)
				}
			})

			socket.on('message', (msg, rinfo) => {
				const timestamp = new Date()

				// Validate packet (basic validation)
				if (!this.isValidPacket(msg)) {
					console.warn(
						`[UDPListener] Malformed packet received on port ${port}, discarding`,
					)
					return
				}

				console.log(
					`[UDPListener] Received ${msg.length} bytes on port ${port} from ${rinfo.address}:${rinfo.port}`,
				)

				if (this.packetHandler) {
					void this.packetHandler.onPacket(port, msg, timestamp)
				}

				this.emit('packet', { port, data: msg, timestamp })
			})

			socket.on('listening', () => {
				const address = socket.address()
				console.log(`[UDPListener] Listening on port ${address.port}`)
				this.sockets.set(port, socket)
				resolve()
			})

			socket.bind(port)
		})
	}
}
