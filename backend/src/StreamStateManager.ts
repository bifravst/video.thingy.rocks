import { EventEmitter } from 'node:events'
import { Logger } from './Logger.ts'

export type StreamStatus = 'active' | 'inactive'

export type StreamState = {
	port: number
	status: StreamStatus
	firstPacketTime?: Date
	lastPacketTime?: Date
	packetCount: number
}

export type StreamStateManagerConfig = {
	inactivityTimeout: number // milliseconds (default: 60000 = 1 minute)
}

export class StreamStateManager extends EventEmitter {
	private readonly config: StreamStateManagerConfig
	private readonly streams: Map<number, StreamState> = new Map()
	private readonly inactivityTimers: Map<number, NodeJS.Timeout> = new Map()
	private readonly logger: Logger

	constructor(config: StreamStateManagerConfig = { inactivityTimeout: 60000 }) {
		super()
		this.config = config
		this.logger = new Logger('StreamStateManager')
	}

	onPacketReceived(port: number, timestamp: Date): void {
		const existingStream = this.streams.get(port)

		if (!existingStream) {
			// Stream start - first packet on this port
			const newStream: StreamState = {
				port,
				status: 'active',
				firstPacketTime: timestamp,
				lastPacketTime: timestamp,
				packetCount: 1,
			}
			this.streams.set(port, newStream)
			this.logger.info('Stream started', { port, timestamp })
			this.emit('streamStart', port)
		} else {
			// Update existing stream
			existingStream.lastPacketTime = timestamp
			existingStream.packetCount++

			// If stream was inactive, mark as active again
			if (existingStream.status === 'inactive') {
				existingStream.status = 'active'
				this.logger.info('Stream resumed', { port, timestamp })
				this.emit('streamResume', port)
			}
		}

		// Reset inactivity timer
		this.resetInactivityTimer(port)
	}

	getStreamState(port: number): StreamState | undefined {
		return this.streams.get(port)
	}

	getActiveStreams(): number[] {
		const activeStreams: number[] = []
		for (const [port, state] of this.streams.entries()) {
			if (state.status === 'active') {
				activeStreams.push(port)
			}
		}
		return activeStreams
	}

	getActiveStreamCount(): number {
		return this.getActiveStreams().length
	}

	getAllStreams(): StreamState[] {
		return Array.from(this.streams.values())
	}

	stop(): void {
		// Clear all inactivity timers
		for (const timer of this.inactivityTimers.values()) {
			clearTimeout(timer)
		}
		this.inactivityTimers.clear()
	}

	private resetInactivityTimer(port: number): void {
		// Clear existing timer
		const existingTimer = this.inactivityTimers.get(port)
		if (existingTimer) {
			clearTimeout(existingTimer)
		}

		// Set new timer
		const timer = setTimeout(() => {
			this.handleStreamInactivity(port)
		}, this.config.inactivityTimeout)

		this.inactivityTimers.set(port, timer)
	}

	private handleStreamInactivity(port: number): void {
		const stream = this.streams.get(port)
		if (!stream) {
			return
		}

		// Calculate inactivity duration
		const now = new Date()
		const lastPacketTime = stream.lastPacketTime ?? now
		const inactivityDuration = now.getTime() - lastPacketTime.getTime()

		// Mark stream as inactive
		stream.status = 'inactive'
		this.logger.info('Stream stopped', {
			port,
			inactivityDuration,
			timestamp: now,
		})

		this.emit('streamStop', port, inactivityDuration)
	}
}
