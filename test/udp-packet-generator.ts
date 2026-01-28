#!/usr/bin/env node
/**
 * UDP Packet Generator for Testing
 *
 * Generates UDP video packets for testing the Video Streaming system.
 * Simulates an Cat1bisCam device sending MPEG-TS video data.
 *
 * Usage:
 *   node --experimental-transform-types test/udp-packet-generator.ts [options]
 *
 * Options:
 *   --port <number>        Target port (5000-5009, default: 5000)
 *   --host <string>        Target host (default: localhost)
 *   --duration <number>    Duration in seconds (default: 60)
 *   --bitrate <number>     Bitrate in kbps (default: 1000)
 *   --packet-size <number> Packet size in bytes (default: 1316)
 *   --interval <number>    Packet interval in ms (default: calculated from bitrate)
 *   --pattern <string>     Test pattern (color-bars, test-card, random, default: color-bars)
 */

import dgram from 'node:dgram'
import { parseArgs } from 'node:util'

// Parse command line arguments
const { values } = parseArgs({
	options: {
		port: { type: 'string', default: '5000' },
		host: { type: 'string', default: 'localhost' },
		duration: { type: 'string', default: '60' },
		bitrate: { type: 'string', default: '1000' },
		'packet-size': { type: 'string', default: '1316' },
		interval: { type: 'string' },
		pattern: { type: 'string', default: 'color-bars' },
	},
})

const port = Number.parseInt(values.port ?? '5000', 10)
const host = values.host ?? 'localhost'
const duration = Number.parseInt(values.duration ?? '60', 10)
const bitrate = Number.parseInt(values.bitrate ?? '1000', 10)
const packetSize = Number.parseInt(values['packet-size'] ?? '1316', 10)
const pattern = values.pattern ?? 'color-bars'

// Calculate packet interval from bitrate if not specified
// bitrate (kbps) = (packetSize * 8 * 1000) / (interval * 1000)
// interval (ms) = (packetSize * 8) / bitrate
const interval =
	values.interval !== undefined && values.interval !== null
		? Number.parseInt(values.interval, 10)
		: Math.floor((packetSize * 8) / bitrate)

// Validate inputs
if (port < 5000 || port > 5009) {
	console.error('Error: Port must be between 5000 and 5009')
	process.exit(1)
}

if (duration <= 0) {
	console.error('Error: Duration must be positive')
	process.exit(1)
}

if (bitrate <= 0) {
	console.error('Error: Bitrate must be positive')
	process.exit(1)
}

if (packetSize <= 0 || packetSize > 65507) {
	console.error('Error: Packet size must be between 1 and 65507 bytes')
	process.exit(1)
}

console.log('UDP Packet Generator')
console.log('===================')
console.log(`Target: ${host}:${port}`)
console.log(`Duration: ${duration} seconds`)
console.log(`Bitrate: ${bitrate} kbps`)
console.log(`Packet size: ${packetSize} bytes`)
console.log(`Interval: ${interval} ms`)
console.log(`Pattern: ${pattern}`)
console.log()

// Create UDP socket
const socket = dgram.createSocket('udp4')

// Statistics
let packetsSent = 0
let bytesSent = 0
let errors = 0
const startTime = Date.now()

/**
 * Generate MPEG-TS packet data
 *
 * MPEG-TS packet structure:
 * - Sync byte: 0x47
 * - Transport Error Indicator: 1 bit
 * - Payload Unit Start Indicator: 1 bit
 * - Transport Priority: 1 bit
 * - PID: 13 bits
 * - Scrambling control: 2 bits
 * - Adaptation field control: 2 bits
 * - Continuity counter: 4 bits
 * - Payload: remaining bytes
 */
const generateMpegTsPacket = (
	sequenceNumber: number,
	pattern: string,
): Buffer => {
	const packet = Buffer.alloc(packetSize)

	// MPEG-TS sync byte
	packet[0] = 0x47

	// Transport Error Indicator (0), Payload Unit Start (1 for first packet), Priority (0)
	// PID (0x0100 for video)
	const payloadStart = sequenceNumber % 7 === 0 ? 1 : 0
	packet[1] = (payloadStart << 6) | 0x01
	packet[2] = 0x00

	// Scrambling (00), Adaptation field (01 = payload only), Continuity counter
	const continuityCounter = sequenceNumber % 16
	packet[3] = 0x10 | continuityCounter

	// Fill payload based on pattern
	switch (pattern) {
		case 'color-bars':
			// Simulate color bars with repeating pattern
			for (let i = 4; i < packetSize; i++) {
				packet[i] = ((i - 4) % 256) & 0xff
			}
			break

		case 'test-card':
			// Simulate test card with structured pattern
			for (let i = 4; i < packetSize; i++) {
				const position = (i - 4) % 188
				if (position < 47)
					packet[i] = 0xff // White
				else if (position < 94)
					packet[i] = 0xc0 // Light gray
				else if (position < 141)
					packet[i] = 0x80 // Dark gray
				else packet[i] = 0x00 // Black
			}
			break

		case 'random':
			// Random data
			for (let i = 4; i < packetSize; i++) {
				packet[i] = Math.floor(Math.random() * 256)
			}
			break

		default:
			// Default to color bars
			for (let i = 4; i < packetSize; i++) {
				packet[i] = ((i - 4) % 256) & 0xff
			}
	}

	// Add timestamp metadata in last 8 bytes
	const timestamp = Date.now()
	packet.writeBigUInt64BE(BigInt(timestamp), packetSize - 8)

	return packet
}

/**
 * Send a single UDP packet
 */
const sendPacket = async (sequenceNumber: number): Promise<void> => {
	return new Promise((resolve, reject) => {
		const packet = generateMpegTsPacket(sequenceNumber, pattern)

		socket.send(packet, port, host, (error) => {
			if (error) {
				errors++
				console.error(`Error sending packet ${sequenceNumber}:`, error.message)
				reject(error)
			} else {
				packetsSent++
				bytesSent += packet.length
				resolve()
			}
		})
	})
}

/**
 * Print statistics
 */
const printStats = (): void => {
	const elapsed = (Date.now() - startTime) / 1000
	const actualBitrate = (bytesSent * 8) / elapsed / 1000
	const packetLoss = errors / (packetsSent + errors)

	console.log('\nStatistics:')
	console.log(`  Packets sent: ${packetsSent}`)
	console.log(`  Bytes sent: ${bytesSent.toLocaleString()}`)
	console.log(`  Errors: ${errors}`)
	console.log(`  Elapsed time: ${elapsed.toFixed(2)} seconds`)
	console.log(`  Actual bitrate: ${actualBitrate.toFixed(2)} kbps`)
	console.log(`  Packet loss: ${(packetLoss * 100).toFixed(2)}%`)
}

/**
 * Main loop
 */
const main = async (): Promise<void> => {
	console.log('Starting packet generation...\n')

	let sequenceNumber = 0
	const endTime = Date.now() + duration * 1000

	// Send packets at specified interval
	const intervalId = setInterval(async () => {
		if (Date.now() >= endTime) {
			clearInterval(intervalId)
			socket.close()
			printStats()
			console.log('\nPacket generation complete!')
			return
		}

		try {
			await sendPacket(sequenceNumber++)

			// Print progress every 100 packets
			if (sequenceNumber % 100 === 0) {
				const elapsed = (Date.now() - startTime) / 1000
				const rate = packetsSent / elapsed
				process.stdout.write(
					`\rPackets: ${packetsSent} | Rate: ${rate.toFixed(1)} pkt/s | Errors: ${errors}`,
				)
			}
		} catch (error) {
			// Error already logged in sendPacket
		}
	}, interval)

	// Handle Ctrl+C
	process.on('SIGINT', () => {
		console.log('\n\nInterrupted by user')
		clearInterval(intervalId)
		socket.close()
		printStats()
		process.exit(0)
	})
}

// Run the generator
main().catch((error) => {
	console.error('Fatal error:', error)
	process.exit(1)
})
