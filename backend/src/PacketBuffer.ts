import fs from 'node:fs/promises'
import path from 'node:path'
import { Logger } from './Logger.ts'

export type PacketBufferConfig = {
	bufferSize: number // bytes
	flushInterval: number // milliseconds
	outputDirectory: string
}

type BufferedPacket = {
	data: Buffer
	timestamp: Date
}

export class PacketBuffer {
	private readonly config: PacketBufferConfig
	private readonly buffers: Map<number, BufferedPacket[]> = new Map()
	private readonly bufferSizes: Map<number, number> = new Map()
	private readonly flushTimers: Map<number, NodeJS.Timeout> = new Map()
	private readonly logger: Logger

	constructor(config: PacketBufferConfig) {
		this.config = config
		this.logger = new Logger('PacketBuffer')
	}

	async addPacket(port: number, data: Buffer, timestamp: Date): Promise<void> {
		// Initialize buffer for this port if it doesn't exist
		if (!this.buffers.has(port)) {
			this.buffers.set(port, [])
			this.bufferSizes.set(port, 0)
			this.startFlushTimer(port)
		}

		const buffer = this.buffers.get(port)!
		const currentSize = this.bufferSizes.get(port)!

		// Check for buffer overflow
		if (currentSize + data.length > this.config.bufferSize) {
			this.logger.warn('Buffer overflow, dropping oldest packets', {
				port,
				currentSize,
				incomingSize: data.length,
				bufferSize: this.config.bufferSize,
			})
			await this.handleBufferOverflow(port, data.length)
		}

		// Add packet to buffer
		buffer.push({ data, timestamp })
		this.bufferSizes.set(port, currentSize + data.length)

		// Flush if buffer size threshold reached
		if (this.bufferSizes.get(port)! >= this.config.bufferSize) {
			await this.flush(port)
		}
	}

	async flush(port: number): Promise<void> {
		const buffer = this.buffers.get(port)
		const bufferSize = this.bufferSizes.get(port)
		if (!buffer || buffer.length === 0 || bufferSize === undefined) {
			return
		}

		// Combine all buffered packets
		const totalSize = bufferSize
		const combined = Buffer.allocUnsafe(totalSize)
		let offset = 0

		for (const packet of buffer) {
			packet.data.copy(combined, offset)
			offset += packet.data.length
		}

		// Generate filename with timestamp
		const firstPacket = buffer[0]
		const firstTimestamp = firstPacket?.timestamp ?? new Date()
		const filename = this.generateFilename(port, firstTimestamp)
		const filepath = path.join(this.config.outputDirectory, filename)

		// Ensure output directory exists
		await fs.mkdir(this.config.outputDirectory, { recursive: true })

		// Write to disk
		await fs.writeFile(filepath, combined)
		this.logger.info('Flushed buffer to disk', {
			port,
			packetCount: buffer.length,
			totalSize,
			filepath,
		})

		// Clear buffer
		this.buffers.set(port, [])
		this.bufferSizes.set(port, 0)

		// Restart flush timer
		this.resetFlushTimer(port)
	}

	async flushAll(): Promise<void> {
		const ports = Array.from(this.buffers.keys())
		await Promise.all(ports.map(async (port) => this.flush(port)))
	}

	async stop(): Promise<void> {
		// Clear all timers
		for (const timer of this.flushTimers.values()) {
			clearTimeout(timer)
		}
		this.flushTimers.clear()

		// Flush all remaining buffers
		await this.flushAll()
	}

	private async handleBufferOverflow(
		port: number,
		incomingSize: number,
	): Promise<void> {
		const buffer = this.buffers.get(port)!
		let currentSize = this.bufferSizes.get(port)!

		// Drop oldest packets (FIFO) until there's room
		while (
			buffer.length > 0 &&
			currentSize + incomingSize > this.config.bufferSize
		) {
			const oldest = buffer.shift()
			if (oldest) {
				currentSize -= oldest.data.length
			}
		}

		this.bufferSizes.set(port, currentSize)
	}

	private startFlushTimer(port: number): void {
		const timer = setTimeout(() => {
			void this.flush(port).catch((err) => {
				this.logger.error(
					'Error flushing buffer',
					err instanceof Error ? err : new Error(String(err)),
					{ port },
				)
			})
		}, this.config.flushInterval)

		this.flushTimers.set(port, timer)
	}

	private resetFlushTimer(port: number): void {
		const existingTimer = this.flushTimers.get(port)
		if (existingTimer) {
			clearTimeout(existingTimer)
		}
		this.startFlushTimer(port)
	}

	private generateFilename(port: number, timestamp: Date): string {
		const year = timestamp.getFullYear()
		const month = String(timestamp.getMonth() + 1).padStart(2, '0')
		const day = String(timestamp.getDate()).padStart(2, '0')
		const hour = String(timestamp.getHours()).padStart(2, '0')
		const minute = String(timestamp.getMinutes()).padStart(2, '0')
		const second = String(timestamp.getSeconds()).padStart(2, '0')

		return `port_${port}_${year}${month}${day}_${hour}${minute}${second}.ts`
	}
}
