/**
 * Syntactic filter for datagrams arriving on an SRTP ingest port.
 *
 * This decides one thing: whether a datagram is worth *buffering*. It is not
 * authentication and must never be treated as any: the RTP version and the SSRC are
 * public header fields that SRTP leaves in the clear, so anyone able to reach the port
 * can produce a datagram that passes. What a datagram that passes earns is a place in
 * a bounded buffer, and nothing else - no lease refresh, no rollover-counter update,
 * and no sustained claim on the stream, all of which wait for the pipeline to report
 * that libsrtp authenticated something.
 *
 * A port with no key admits nothing, because nothing sent to it could ever be
 * decrypted.
 */

const RTP_HEADER_BYTES = 12
const RTP_VERSION_2 = 2

export type SrtpPortIdentity = {
	/** The SSRC the port's key was provisioned for, or undefined when unkeyed. */
	ssrcForPort(port: number): number | undefined
}

/** Reads the sequence number if the header is plausibly this port's RTP, else undefined. */
export const plausibleRtpSequenceNumber = (
	datagram: Buffer,
	expectedSsrc: number,
): number | undefined => {
	if (datagram.length < RTP_HEADER_BYTES) return undefined
	if ((datagram[0] ?? 0) >>> 6 !== RTP_VERSION_2) return undefined
	if (datagram.readUInt32BE(8) !== expectedSsrc) return undefined
	return datagram.readUInt16BE(2)
}

/**
 * Builds the per-port filter PortIngestion applies while a port is unowned.
 *
 * Note that the machine bypasses this once it owns the port: a filter that misjudges
 * live traffic must not be able to starve a stream that is already running.
 */
export const createSrtpAdmissionFilter = (
	identity: SrtpPortIdentity,
	port: number,
): ((datagram: Buffer) => boolean) => {
	return (datagram: Buffer): boolean => {
		const ssrc = identity.ssrcForPort(port)
		if (ssrc === undefined) return false
		return plausibleRtpSequenceNumber(datagram, ssrc) !== undefined
	}
}
