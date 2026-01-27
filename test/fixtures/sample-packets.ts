/**
 * Sample MPEG-TS packets for testing
 *
 * These pre-generated packets are used in unit tests to validate
 * packet handling, buffering, and processing logic.
 */

/**
 * Valid MPEG-TS packet (188 bytes)
 *
 * Structure:
 * - Sync byte: 0x47
 * - PID: 0x0100 (video)
 * - Continuity counter: 0
 * - Payload: test pattern
 */
export const validPacket = Buffer.from([
	0x47, // Sync byte
	0x41,
	0x00, // Payload start, PID 0x0100
	0x10, // No adaptation field, continuity counter 0
	// Payload (184 bytes)
	...Array.from({ length: 184 }, (_, i) => i % 256),
])

/**
 * Valid MPEG-TS packet with sequence number 1
 */
export const validPacket2 = Buffer.from([
	0x47, // Sync byte
	0x01,
	0x00, // No payload start, PID 0x0100
	0x11, // No adaptation field, continuity counter 1
	// Payload (184 bytes)
	...Array.from({ length: 184 }, (_, i) => (i + 100) % 256),
])

/**
 * Valid MPEG-TS packet with sequence number 2
 */
export const validPacket3 = Buffer.from([
	0x47, // Sync byte
	0x01,
	0x00, // No payload start, PID 0x0100
	0x12, // No adaptation field, continuity counter 2
	// Payload (184 bytes)
	...Array.from({ length: 184 }, (_, i) => (i + 200) % 256),
])

/**
 * Malformed packet - invalid sync byte
 */
export const malformedPacket = Buffer.from([
	0x48, // Invalid sync byte (should be 0x47)
	0x41,
	0x00,
	0x10,
	...Array.from({ length: 184 }, () => 0),
])

/**
 * Malformed packet - too short
 */
export const shortPacket = Buffer.from([
	0x47, // Sync byte
	0x41,
	0x00,
	0x10,
	// Only 10 bytes of payload (should be 184)
	...Array.from({ length: 10 }, (_, i) => i),
])

/**
 * Large packet (1316 bytes - typical UDP payload size)
 * Contains 7 MPEG-TS packets
 */
export const largePacket = Buffer.concat([
	validPacket,
	validPacket2,
	validPacket3,
	validPacket,
	validPacket2,
	validPacket3,
	validPacket,
])

/**
 * Packet with timestamp metadata
 * Last 8 bytes contain timestamp
 */
export const createPacketWithTimestamp = (timestamp: number): Buffer => {
	const packet = Buffer.alloc(1316)

	// Fill with valid MPEG-TS packets
	for (let i = 0; i < 7; i++) {
		const offset = i * 188
		packet[offset] = 0x47 // Sync byte
		packet[offset + 1] = i === 0 ? 0x41 : 0x01 // Payload start for first packet
		packet[offset + 2] = 0x00
		packet[offset + 3] = 0x10 | (i % 16) // Continuity counter

		// Fill payload
		for (let j = 4; j < 188; j++) {
			packet[offset + j] = (i * 188 + j) % 256
		}
	}

	// Add timestamp in last 8 bytes
	packet.writeBigUInt64BE(BigInt(timestamp), packet.length - 8)

	return packet
}

/**
 * Generate a sequence of packets
 */
export const generatePacketSequence = (count: number): Buffer[] => {
	const packets: Buffer[] = []

	for (let i = 0; i < count; i++) {
		const packet = Buffer.alloc(188)
		packet[0] = 0x47 // Sync byte
		packet[1] = i === 0 ? 0x41 : 0x01 // Payload start for first packet
		packet[2] = 0x00
		packet[3] = 0x10 | (i % 16) // Continuity counter

		// Fill payload with sequence-specific data
		for (let j = 4; j < 188; j++) {
			packet[j] = (i * 188 + j) % 256
		}

		packets.push(packet)
	}

	return packets
}

/**
 * Create a packet for a specific port
 */
export const createPacketForPort = (port: number): Buffer => {
	const packet = Buffer.alloc(188)
	packet[0] = 0x47 // Sync byte
	packet[1] = 0x41 // Payload start
	packet[2] = 0x00
	packet[3] = 0x10 // Continuity counter 0

	// Encode port number in payload
	packet.writeUInt16BE(port, 4)

	// Fill rest of payload
	for (let i = 6; i < 188; i++) {
		packet[i] = i % 256
	}

	return packet
}

/**
 * Validate MPEG-TS packet structure
 */
export const isValidMpegTsPacket = (packet: Buffer): boolean => {
	if (packet.length !== 188) return false
	if (packet[0] !== 0x47) return false
	return true
}

/**
 * Extract continuity counter from packet
 */
export const getContinuityCounter = (packet: Buffer): number => {
	if (!isValidMpegTsPacket(packet)) return -1
	return packet[3]! & 0x0f
}

/**
 * Extract PID from packet
 */
export const getPID = (packet: Buffer): number => {
	if (!isValidMpegTsPacket(packet)) return -1
	return ((packet[1]! & 0x1f) << 8) | packet[2]!
}

/**
 * Check if packet has payload unit start indicator
 */
export const hasPayloadStart = (packet: Buffer): boolean => {
	if (!isValidMpegTsPacket(packet)) return false
	return (packet[1]! & 0x40) !== 0
}
