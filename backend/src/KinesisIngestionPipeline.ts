import { spawn, spawnSync } from 'node:child_process'
import dgram from 'node:dgram'
import { EventEmitter } from 'node:events'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import type { Writable } from 'node:stream'

import { fromNodeProviderChain } from '@aws-sdk/credential-providers'
import { Logger } from './Logger.ts'
import type { SrtpKeyStore, SrtpPortKey } from './SrtpKeyStore.ts'

export type KinesisIngestionPipelineConfig = {
	region: string
	portRange: { start: number; end: number }
	/**
	 * Max packets to buffer before writing to GStreamer stdin (0 = write immediately).
	 * Packets are emitted in receive order; does not fix network reordering.
	 * Default 128. Helps smooth bursts and applies backpressure.
	 * Only applies to the FIFO/MPEG-TS transport (ports in `portRange`).
	 */
	reorderBufferSize?: number
	/**
	 * When true, log GStreamer/KVS stdout and stderr. Disabled by default to avoid noisy logs.
	 */
	logGstreamerOutput?: boolean
	/**
	 * Optional SRTP ingestion: a separate port range whose packets are RTP/H.264 wrapped in
	 * SRTP (RFC 6184 + static pre-shared key), relayed to a loopback udpsrc, decrypted by
	 * srtpdec, depayloaded, and sent to Kinesis the same way as the FIFO/MPEG-TS path.
	 *
	 * `portRange` must have the same number of ports as the top-level `portRange`: port
	 * `portRange.start + i` (unencrypted) and `srtp.portRange.start + i` (SRTP) both target
	 * the same Kinesis Video Stream (see streamNameForPort). A device is assigned one port
	 * range or the other, never both, so nothing arbitrates between them beyond the existing
	 * per-port Kinesis lock (StreamMetadataService).
	 */
	srtp?: {
		portRange: { start: number; end: number }
		/** Local relay port = publicPort + relayPortOffset. Default 10000. */
		relayPortOffset?: number
		/** rtpjitterbuffer latency in ms. Default 200. */
		jitterBufferLatencyMs?: number
		/**
		 * Max bytes to hold in a starting pipeline's `pendingQueue` (live datagrams that
		 * arrive while startup replay is still in flight - see writePacket/runStartForSrtpPort).
		 * Default 10MB (DEFAULT_SRTP_PENDING_QUEUE_MAX_BYTES). Oldest datagrams are dropped
		 * once exceeded, same pattern as index.ts's other per-port buffers/queues.
		 */
		pendingQueueMaxBytes?: number
		keyStore: SrtpKeyStore
	}
}

/** Must match cdk/StreamingStack.ts's KINESIS_STREAM_NAME_PREFIX (duplicated, not shared,
 * since backend/ and cdk/ are deployed separately). */
const KINESIS_STREAM_NAME_PREFIX = 'video-streaming-2026-09-video'

const DEFAULT_REORDER_BUFFER_SIZE = 128
const DEFAULT_SRTP_RELAY_PORT_OFFSET = 10_000
const DEFAULT_SRTP_JITTER_BUFFER_LATENCY_MS = 200
const DEFAULT_SRTP_PENDING_QUEUE_MAX_BYTES = 10 * 1024 * 1024 // 10MB
/** Grace period after spawning gst-launch-1.0 before replaying pre-buffered SRTP datagrams;
 * unlike the FIFO path (whose fs.open() blocks until filesrc opens for read), there is no
 * OS-level handshake for udpsrc binding a port, so we just wait a bit. A few early datagrams
 * lost here self-heal at the next H.264 keyframe, the same tolerance the system already has
 * for ordinary network loss. */
const SRTP_STARTUP_GRACE_MS = 300
/** Pace the pre-start buffer replay: await this many sends, then pause, instead of firing
 * the whole (potentially thousands-of-datagrams) buffer at once with no backpressure. */
const SRTP_REPLAY_BATCH_SIZE = 50
const SRTP_REPLAY_BATCH_PAUSE_MS = 10
/** Bounds how long runStartForFifoPort waits for GStreamer's filesrc to open the FIFO for
 * reading. Opening a FIFO for writing blocks at the OS level until *some* reader opens it -
 * if GStreamer fails to spawn or exits before ever opening it (missing binary/plugin), that
 * wait would otherwise hang forever, holding the Kinesis lock indefinitely with nothing to
 * release/retry it. Generous (matches stop()'s own exit-wait budget) since a slow-but-normal
 * startup shouldn't be mistaken for a hang. */
const FIFO_OPEN_TIMEOUT_MS = 15_000

type ReorderState = {
	nextSeq: number
	nextToEmit: number
	buffer: Map<number, Buffer>
}

/**
 * Parses and lightly validates a plain RTP/SRTP header: the RTP version (top 2 bits of byte
 * 0) must be 2, and the SSRC (bytes 8-11) must match `expectedSsrc`. Returns the sequence
 * number (bytes 2-3) if so, else undefined. SRTP (RFC 3711) never encrypts the header, only
 * the payload, so reading these fields needs no decryption - but that's also exactly why
 * this is NOT a substitute for real SRTP authentication: anyone who can reach the public UDP
 * port can forge a datagram with the right version/SSRC (neither is secret) and an arbitrary
 * sequence number, without knowing the key. This filters out obvious noise/unrelated traffic
 * before it can influence ROC tracking (see advanceSrtpRoc), not a determined spoofer - a
 * fully watertight fix would need per-packet authentication feedback from srtpdec, which
 * isn't exposed by this gst-launch-1.0-based architecture. Exported for unit testing.
 *
 * Known, accepted limitation (see index.ts's looksPlausibleRtp, which reuses this same
 * check to gate lock acquisition/heartbeat/ROC-update eligibility): because this can't
 * authenticate, a party who has learned a port's SSRC (not secret) can forge packets that
 * pass it, and could use that to repeatedly acquire/refresh the shared stream lock (denying
 * the legitimate device) or feed bogus sequence numbers into the persisted ROC estimate -
 * srtpdec would still reject the forged payload's actual auth tag downstream, but only after
 * this layer has already granted it lock/heartbeat/ROC eligibility. Closing this fully needs
 * either real per-packet SRTP authentication computed here in Node (implementing RFC 3711's
 * key derivation and HMAC verification independently of srtpdec) or a trusted-source
 * admission control in front of these ports; both are out of scope for this static-caps
 * gst-launch-1.0 architecture and deliberately not attempted rather than risk a subtly wrong
 * from-scratch crypto implementation.
 */
export const parseAuthenticRtpSequenceNumber = (
	datagram: Buffer,
	expectedSsrc: number,
): number | undefined => {
	if (datagram.length < 12) return undefined
	const version = datagram[0]! >>> 6
	if (version !== 2) return undefined
	if (datagram.readUInt32BE(8) !== expectedSsrc) return undefined
	return datagram.readUInt16BE(2)
}

export type SrtpRocState = { highestSeq: number; roc: number }

/**
 * Advances an SRTP rollover-counter tracking state by one observed sequence number.
 * `state` undefined means "first packet ever observed for this port" - starts at ROC 0,
 * matching srtpdec's own default for a truly fresh session.
 *
 * Tracks the *highest* extended sequence index seen (roc * 65536 + seq), not just the most
 * recently arrived one: for each new seq, it guesses which of roc-1/roc/roc+1 puts the
 * extended index closest to the current highest (the standard RFC 3711 Appendix A
 * approach), and only advances the tracked state if that guess is actually higher than the
 * highest seen so far. A naive "compare only to the last-seen seq" version is vulnerable to
 * a single reordered packet right at a wrap boundary corrupting the estimate: e.g. seq
 * 65535 -> 0 correctly sets ROC 1, but if a late/reordered duplicate of 65535 then arrives
 * and is compared only against 0, it looks unwrapped and overwrites the tracked "last seq"
 * to 65535 again - so the next genuinely-forward packet (seq 1) gets misdetected as a
 * second wrap. Comparing against the highest-seen extended index instead means a
 * lower/older packet is simply ignored for tracking purposes (it still gets relayed to
 * GStreamer as normal - this only affects our own seed-value bookkeeping).
 * Exported for unit testing.
 */
export const advanceSrtpRoc = (
	state: SrtpRocState | undefined,
	seq: number,
): SrtpRocState => {
	if (!state) return { highestSeq: seq, roc: 0 }

	const highestExtended = state.roc * 0x10000 + state.highestSeq
	const candidateRocs =
		state.roc > 0
			? [state.roc - 1, state.roc, state.roc + 1]
			: [state.roc, state.roc + 1]

	let bestRoc = state.roc
	let bestExtended = state.roc * 0x10000 + seq
	let bestDistance = Math.abs(bestExtended - highestExtended)
	for (const candidateRoc of candidateRocs) {
		const extended = candidateRoc * 0x10000 + seq
		const distance = Math.abs(extended - highestExtended)
		if (distance < bestDistance) {
			bestRoc = candidateRoc
			bestExtended = extended
			bestDistance = distance
		}
	}

	if (bestExtended <= highestExtended) return state
	return { highestSeq: seq, roc: bestRoc }
}

/**
 * Computes the "roc" value to seed a fresh srtpdec instance with, given our best current
 * tracking state, so that libsrtp's own internal rollover estimate for the *next* packet it
 * actually receives resolves to the correct ROC.
 *
 * GStreamer's srtpdec only exposes a caps "roc" field (srtp_set_stream_roc), which sets the
 * ROC half of its internal 48-bit tracked index but leaves the low 16 bits (the "highest
 * sequence number seen") at 0 - there is no caps field for that. So if we seed roc=5 while
 * the sender's actual current sequence number is, say, 40000, libsrtp's own RFC3711-style
 * guess for the first real packet compares candidate extended indices (4,5,6)*65536+40000
 * against its baseline of 5*65536+0, and 4*65536+40000 is the closest - i.e. it silently
 * decides to use ROC 4, not the 5 we asked for, and decryption/auth fails.
 *
 * Working through that same fixed arithmetic (candidate distance vs a baseline pinned at
 * seededRoc*65536+0) shows the guess resolves to our seeded value exactly when the real
 * sequence number is below 32768, and to `seededRoc - 1` when it's 32768 or above. We exploit
 * that in reverse: when our own tracked highestSeq (the best available proxy for whatever
 * sequence number is about to arrive next) is in the upper half, seed `roc + 1` instead of
 * `roc` - libsrtp's own guess then resolves that back down to the correct roc.
 */
export const srtpdecSeedRoc = (state: SrtpRocState): number =>
	state.highestSeq >= 0x8000 ? state.roc + 1 : state.roc

/** Per-port throttle for GStreamer stderr warning categories. */
type GstStderrThrottle = { lastLog: Record<string, number> }

/** Per-port throttle for noisy GStreamer/KVS stdout lines (CONTINUITY, 0x30000005, etc.). */
type GstStdoutThrottle = { lastLog: Record<string, number> }

type FifoPortPipeline = {
	transport: 'fifo'
	gst: ReturnType<typeof spawn>
	/** Stream we write TS to (stdin pipe or FIFO). */
	inputStream: Writable
	reorder: ReorderState
	gstStderrThrottle: GstStderrThrottle
	gstStdoutThrottle: GstStdoutThrottle
	/** FIFO path when using filesrc; unlink on stop. */
	fifoPath: string
}

type SrtpRelayPortPipeline = {
	transport: 'srtp-relay'
	gst: ReturnType<typeof spawn>
	/** Node-side socket used to relay each UDP datagram, unmodified, to GStreamer's udpsrc. */
	relaySocket: dgram.Socket
	relayPort: number
	/**
	 * Closes relaySocket exactly once. Closing an already-closed dgram socket throws
	 * ERR_SOCKET_DGRAM_NOT_RUNNING, and this can be reached from three independent places
	 * (the gst 'error' handler, the gst 'exit' handler, and stop()) - always go through this
	 * instead of calling relaySocket.close() directly.
	 */
	closeRelaySocket: () => void
	gstStderrThrottle: GstStderrThrottle
	gstStdoutThrottle: GstStdoutThrottle
	/**
	 * True once startup replay (if any) has finished. While false, writePacket() queues live
	 * datagrams in pendingQueue instead of sending them immediately, so they can't overtake
	 * the still-in-flight replay of pre-buffered datagrams (which would let SRTP's
	 * anti-replay/jitter-buffer handling discard the older, buffered packets - including the
	 * initial keyframe - as apparent duplicates/out-of-order).
	 */
	ready: boolean
	pendingQueue: Buffer[]
	/** Running total of pendingQueue's byte size - see pendingQueueMaxBytes on the srtp
	 * config; tracked alongside the array so writePacket doesn't need to re-sum it per push. */
	pendingQueueBytes: number
}

type PortPipeline = FifoPortPipeline | SrtpRelayPortPipeline

/** Throttle repeated GStreamer stderr warnings (same category) per port. */
const GST_STDERR_THROTTLE_MS = 60_000
/** Throttle noisy stdout (CONTINUITY, KVS 0x30000005, "Could not write to resource") per port. */
const GST_STDOUT_THROTTLE_MS = 60_000

type ResolvedCredentials = {
	accessKeyId: string
	secretAccessKey: string
	sessionToken?: string
}

/**
 * Per-port pipeline: UDP packets -> GStreamer (TS -> H.264, or SRTP -> RTP -> H.264) -> kvssink
 * -> Kinesis Video. One GStreamer process per active port; kvssink sends directly to Kinesis
 * (no Node PutMedia).
 */
export class KinesisIngestionPipeline extends EventEmitter {
	private readonly config: KinesisIngestionPipelineConfig
	private readonly logger: Logger
	private readonly activePipelines: Map<number, PortPipeline> = new Map()
	/** Dedupe concurrent start(port) so only one credential fetch + spawn runs per port. */
	private readonly pendingStarts: Map<number, Promise<void>> = new Map()
	/**
	 * Best-known SRTP rollover counter (ROC) per port, tracked by observing the plaintext RTP
	 * sequence number of every SRTP datagram (SRTP never encrypts the RTP header - only the
	 * payload - so this needs no decryption). A fresh srtpdec instance always starts at ROC
	 * 0; without this, restarting GStreamer (crash, redeploy) or the whole process (EC2
	 * reboot) after the sender's 16-bit sequence number has ever wrapped would seed the new
	 * instance with the wrong ROC and it would never correctly decrypt again. See
	 * trackSrtpRoc/seedSrtpRoc/getSrtpRoc.
	 */
	private readonly srtpRocByPort: Map<number, SrtpRocState> = new Map()

	constructor(config: KinesisIngestionPipelineConfig) {
		super()
		this.config = config
		this.logger = new Logger('KinesisIngestionPipeline')
	}

	isSrtpPort(port: number): boolean {
		const srtp = this.config.srtp
		if (!srtp) return false
		return port >= srtp.portRange.start && port <= srtp.portRange.end
	}

	/**
	 * Canonical stream-slot number (1-based): a port's offset within its own range, plus 1.
	 * Ports at the same offset in the unencrypted and SRTP ranges resolve to the same slot,
	 * e.g. portRange.start and srtp.portRange.start both map to slot 1. This is the identity
	 * that should be used for anything shared between the two transports for the same
	 * device - the Kinesis stream name (below) and the DynamoDB lock/heartbeat row (see
	 * index.ts, which must key StreamMetadataService calls by this, not by the raw port -
	 * otherwise port 5000 and port 6000 can independently "acquire" what look like separate
	 * locks while actually contending for the same underlying stream).
	 */
	streamSlotForPort(port: number): number {
		const rangeStart = this.isSrtpPort(port)
			? this.config.srtp!.portRange.start
			: this.config.portRange.start
		return port - rangeStart + 1
	}

	/** Kinesis Video Stream name for a port: KINESIS_STREAM_NAME_PREFIX + streamSlotForPort. */
	streamNameForPort(port: number): string {
		return `${KINESIS_STREAM_NAME_PREFIX}-${this.streamSlotForPort(port)}`
	}

	/**
	 * The other transport's port for the same stream slot as `port` (port 5000+n's pair is
	 * 6000+n and vice versa), if SRTP is configured with a matching-length port range;
	 * undefined otherwise. Used to refuse starting a second producer for a slot that
	 * already has one active via the other transport.
	 */
	pairedPortFor(port: number): number | undefined {
		const srtp = this.config.srtp
		if (!srtp) return undefined
		const slot = this.streamSlotForPort(port)
		return this.isSrtpPort(port)
			? this.config.portRange.start + slot - 1
			: srtp.portRange.start + slot - 1
	}

	isPortInRange(port: number): boolean {
		const inMainRange =
			port >= this.config.portRange.start && port <= this.config.portRange.end
		return inMainRange || this.isSrtpPort(port)
	}

	/**
	 * Whether a pipeline is actually running for this port. start() can return without
	 * throwing but without registering a pipeline either (e.g. missing SRTP key, credential
	 * resolution failure) - callers must check this before treating start() as having
	 * succeeded, so they don't hold a Kinesis lock for a port nothing is ingesting.
	 */
	isActive(port: number): boolean {
		return this.activePipelines.has(port)
	}

	/** The configured SRTP SSRC for a port (undefined if SRTP isn't configured or no key is
	 * loaded for this port yet). Used both to validate inbound datagrams before they can
	 * influence ROC tracking (see trackSrtpRoc) and to tag persisted ROC state with the
	 * session it belongs to (see index.ts, which must reset the ROC on a key/SSRC change -
	 * a stale ROC from a previous session/device would misseed a genuinely fresh one). */
	getConfiguredSrtpSsrc(port: number): number | undefined {
		return this.config.srtp?.keyStore.getKeyForPort(port)?.ssrc
	}

	/** Non-secret fingerprint of the currently-configured SRTP key for a port (undefined if
	 * SRTP isn't configured or no key is loaded yet) - see SrtpKeyStore.keyFingerprint. A
	 * second identity check alongside getConfiguredSrtpSsrc for persisted ROC state, since a
	 * key can be rotated in SSM while keeping the same SSRC, which SSRC alone wouldn't catch. */
	getConfiguredSrtpKeyFingerprint(port: number): string | undefined {
		return this.config.srtp?.keyStore.getKeyForPort(port)?.keyFingerprint
	}

	/**
	 * Updates this port's best-known SRTP ROC by observing one datagram's plaintext RTP
	 * sequence number (SRTP/RFC 3711 never encrypts the header, only the payload, so this
	 * needs no decryption) - but only for datagrams that pass parseAuthenticRtpSequenceNumber's
	 * version/SSRC check, so unrelated noise on the public UDP port can't influence it. Uses
	 * advanceSrtpRoc's rollover heuristic. This is a best-effort estimate for seeding a
	 * *future* restart, not a replacement for srtpdec/libsrtp's own real-time sliding-window
	 * tracking during normal operation. Call for every SRTP datagram, in arrival order,
	 * whether or not it's been sent on yet (including ones still sitting in the pre-start
	 * buffer).
	 */
	private trackSrtpRoc(port: number, datagram: Buffer): void {
		const expectedSsrc = this.getConfiguredSrtpSsrc(port)
		if (expectedSsrc === undefined) return
		const seq = parseAuthenticRtpSequenceNumber(datagram, expectedSsrc)
		if (seq === undefined) return
		this.srtpRocByPort.set(
			port,
			advanceSrtpRoc(this.srtpRocByPort.get(port), seq),
		)
	}

	/** Current best-known SRTP ROC for a port (0 if never observed/seeded). */
	getSrtpRoc(port: number): number {
		return this.getSrtpRocState(port).roc
	}

	/**
	 * Current best-known full SRTP rollover-tracking state for a port ({highestSeq: 0, roc: 0}
	 * if never observed/seeded) - unlike getSrtpRoc, this includes highestSeq, which is
	 * required to persist/restore tracking correctly (see seedSrtpRoc: restoring only the ROC
	 * and inventing highestSeq: 0 misclassifies the next real packet whenever the sender's
	 * current sequence number isn't actually near 0).
	 */
	getSrtpRocState(port: number): SrtpRocState {
		return this.srtpRocByPort.get(port) ?? { highestSeq: 0, roc: 0 }
	}

	/**
	 * Seeds this port's ROC-tracking state from persisted state (see index.ts, which loads it
	 * from DynamoDB before starting an SRTP port's pipeline, after checking the persisted
	 * value still belongs to the currently-configured SSRC). Takes the full extended-index
	 * state (roc AND highestSeq), not just the ROC - seeding with an invented highestSeq: 0
	 * would make advanceSrtpRoc misjudge the next real packet's candidate ROC whenever the
	 * sender's actual sequence number at restart time isn't near 0 (e.g. restoring ROC 5 while
	 * the sender is currently above 32768 would classify those packets as belonging to ROC 4
	 * and ignore them, then misdetect the next real wrap as ROC 5 instead of 6). Only raises
	 * the tracked extended index - never regresses a live, more-current in-process estimate to
	 * an older persisted one (e.g. on a same-process GStreamer-only restart, where we already
	 * have better information than whatever was last flushed to DynamoDB).
	 */
	seedSrtpRoc(port: number, seeded: SrtpRocState): void {
		const state = this.srtpRocByPort.get(port)
		if (!state) {
			this.srtpRocByPort.set(port, seeded)
			return
		}
		const seededExtended = seeded.roc * 0x10000 + seeded.highestSeq
		const currentExtended = state.roc * 0x10000 + state.highestSeq
		if (seededExtended > currentExtended) {
			this.srtpRocByPort.set(port, seeded)
		}
	}

	/**
	 * Logs GStreamer stderr; throttles repeated warnings per category.
	 */
	private logGstStderr(
		port: number,
		streamName: string,
		text: string,
		throttle: GstStderrThrottle,
	): void {
		if (this.config.logGstreamerOutput !== true) return
		const now = Date.now()
		const lines = text.split(/\r?\n/)
		for (const raw of lines) {
			const line = raw.trim()
			if (line.length === 0) continue

			let category: string | null = null
			if (/authentication failure|auth.*fail/i.test(line))
				category = 'srtp_auth'
			else if (/unknown ssrc|no matching key/i.test(line))
				category = 'srtp_ssrc_mismatch'
			else if (/error|ERROR/i.test(line)) category = 'error'
			else if (/warning|WARN/i.test(line)) category = 'warning'

			if (category !== null) {
				const last = throttle.lastLog[category] ?? 0
				if (now - last < GST_STDERR_THROTTLE_MS) continue
				throttle.lastLog[category] = now
			}

			this.logger.warn('GStreamer stderr', {
				port,
				streamName,
				message: line.slice(0, 500),
			})
		}
	}

	/**
	 * Logs GStreamer stdout; throttles noisy lines (CONTINUITY, KVS 0x30000005, "Could not write to resource").
	 */
	private logGstStdout(
		port: number,
		streamName: string,
		text: string,
		throttle: GstStdoutThrottle,
	): void {
		if (this.config.logGstreamerOutput !== true) return
		const now = Date.now()
		const lines = text.split(/\r?\n/)
		for (const raw of lines) {
			const line = raw.trim()
			if (line.length === 0) continue
			if (/streamLatencyPressure/i.test(line)) continue
			if (/droppedFrame callback/i.test(line)) continue
			if (/viewItemRemoved.*Reporting a dropped frame\/fragment/i.test(line))
				continue
			if (/Failed to submit ACK|0x52000047|status code: 0x52/i.test(line))
				continue

			let category: string | null = null
			if (/CONTINUITY:\s*Mismatch/i.test(line)) category = 'continuity'
			else if (
				/0x30000005|putKinesisVideoFrame.*Failed|Put frame.*failed/i.test(line)
			)
				category = 'kvs_putframe'
			else if (/Could not write to resource/i.test(line))
				category = 'write_resource'

			if (category !== null) {
				const last = throttle.lastLog[category] ?? 0
				if (now - last < GST_STDOUT_THROTTLE_MS) continue
				throttle.lastLog[category] = now
			}

			this.logger.info('GStreamer stdout', {
				port,
				streamName,
				message: line.slice(0, 1000),
			})
		}
	}

	/**
	 * Resolves AWS credentials for kvssink (which does not share Node's credential chain)
	 * and returns an env object with them injected, or undefined on failure (logged).
	 */
	private async resolveGstEnv(
		port: number,
		streamName: string,
	): Promise<NodeJS.ProcessEnv | undefined> {
		const credentialProvider = fromNodeProviderChain({
			timeout: 10_000,
			maxRetries: 5,
		})
		const maxResolutionAttempts = 3
		const resolutionDelayMs = 2000
		let credentials: ResolvedCredentials | undefined
		try {
			for (let attempt = 1; attempt <= maxResolutionAttempts; attempt++) {
				try {
					credentials = await credentialProvider()
					break
				} catch (e) {
					if (attempt === maxResolutionAttempts) throw e
					this.logger.warn('Credentials not yet available, retrying', {
						port,
						streamName,
						attempt,
						nextAttemptInMs: resolutionDelayMs,
					})
					await new Promise((r) => setTimeout(r, resolutionDelayMs))
				}
			}
		} catch (err) {
			this.logger.error(
				'Failed to resolve AWS credentials for kvssink',
				err instanceof Error ? err : new Error(String(err)),
				{ port, streamName },
			)
			return undefined
		}

		const env = { ...process.env }
		env.AWS_ACCESS_KEY_ID = credentials!.accessKeyId
		env.AWS_SECRET_ACCESS_KEY = credentials!.secretAccessKey
		if (
			credentials!.sessionToken !== undefined &&
			credentials!.sessionToken !== ''
		) {
			env.AWS_SESSION_TOKEN = credentials!.sessionToken
		}
		env.AWS_REGION = this.config.region
		if (env.KINESIS_GST_PLUGIN_PATH !== undefined) {
			env.GST_PLUGIN_PATH = env.KINESIS_GST_PLUGIN_PATH
		}
		if (env.KINESIS_LD_LIBRARY_PATH !== undefined) {
			env.LD_LIBRARY_PATH = env.KINESIS_LD_LIBRARY_PATH
		}
		return env
	}

	private logConfigPath(): string {
		return (
			process.env.KVS_LOG_CONFIG_PATH ??
			'/opt/video-streaming/kvs_log_configuration'
		)
	}

	/**
	 * Starts the pipeline for a port: spawns GStreamer with kvssink.
	 * Idempotent: no-op if already running for this port.
	 * Concurrent calls for the same port are deduped (single credential fetch + spawn).
	 *
	 * For FIFO/MPEG-TS ports, `initialData` (if provided) is a single Buffer written to stdin
	 * immediately after spawn. For SRTP ports, pass an array of individually-received
	 * datagrams (never Buffer.concat them - that would destroy the per-packet framing SRTP
	 * decryption depends on); each is relayed to GStreamer as its own UDP datagram.
	 */
	async start(port: number, initialData?: Buffer | Buffer[]): Promise<void> {
		if (!this.isPortInRange(port)) return
		if (this.activePipelines.has(port)) return

		const existing = this.pendingStarts.get(port)
		if (existing !== undefined) {
			await existing
			return
		}

		const promise = this.runStartForPort(port, initialData).finally(() => {
			this.pendingStarts.delete(port)
		})
		this.pendingStarts.set(port, promise)
		await promise
	}

	private async runStartForPort(
		port: number,
		initialData?: Buffer | Buffer[],
	): Promise<void> {
		if (this.activePipelines.has(port)) return

		// Refuse to start a second producer for a stream slot that already has one active via
		// the other transport (e.g. port 5000 and port 6000 both map to the same Kinesis
		// stream) - callers are expected to only ever use one transport per device, but this
		// is the in-process backstop if that's ever violated (misconfiguration, migration,
		// two clients hitting the same instance). The cross-instance case is covered
		// separately by index.ts keying the DynamoDB lock by streamSlotForPort(), not by port.
		const pairedPort = this.pairedPortFor(port)
		if (pairedPort !== undefined && this.activePipelines.has(pairedPort)) {
			this.logger.error(
				'Refusing to start ingestion: paired port for this stream slot is already active',
				new Error('Paired-port conflict'),
				{
					port,
					pairedPort,
					streamName: this.streamNameForPort(port),
				},
			)
			return
		}

		if (this.isSrtpPort(port)) {
			const initialDatagrams = Array.isArray(initialData)
				? initialData
				: initialData !== undefined
					? [initialData]
					: []
			return this.runStartForSrtpPort(port, initialDatagrams)
		}

		const initialBuffer = Array.isArray(initialData)
			? Buffer.concat(initialData)
			: initialData
		return this.runStartForFifoPort(port, initialBuffer)
	}

	/**
	 * Single-run start logic for a FIFO/MPEG-TS port (credentials + spawn). Call only via
	 * start() so dedupe applies. If initialData is provided, it is written to stdin
	 * immediately after spawn so fdsrc has data when it first reads.
	 */
	private async runStartForFifoPort(
		port: number,
		initialData?: Buffer,
	): Promise<void> {
		if (this.activePipelines.has(port)) return

		const streamName = this.streamNameForPort(port)
		const region = this.config.region

		const env = await this.resolveGstEnv(port, streamName)
		if (env === undefined) return

		const logConfigPath = this.logConfigPath()

		// Use a FIFO so GStreamer reads via filesrc (real path). filesrc blocks until we open for write, so data is ready when it reads; avoids fdsrc "not-linked" / stream error with pipes.
		const fifoPath = path.join(
			os.tmpdir(),
			`kinesis-${port}-${process.pid}-${Date.now()}.fifo`,
		)
		const mkfifo = spawnSync('mkfifo', [fifoPath], {
			encoding: 'utf8',
			timeout: 5000,
		})
		const failed =
			mkfifo.error != null || (mkfifo.status != null && mkfifo.status !== 0)
		if (failed) {
			const stderrStr = mkfifo.stderr != null ? String(mkfifo.stderr) : ''
			const msg =
				mkfifo.error?.message ??
				(stderrStr.trim() || `mkfifo exit code ${mkfifo.status ?? 'unknown'}`)
			this.logger.error(
				'Failed to create FIFO for GStreamer',
				mkfifo.error ?? new Error(msg),
				{
					port,
					streamName,
					fifoPath,
					exitCode: mkfifo.status ?? undefined,
					stderr: stderrStr.slice(0, 500) || undefined,
				},
			)
			return
		}

		// tsdemux creates pads like video_0_0c00 (template video_%01x_%05x), not "video_0". Use "d." to link to any pad so delayed linking succeeds regardless of PID.
		const pipelineStr = `filesrc location="${fifoPath}" ! capsfilter caps="video/mpegts,systemstream=(boolean)true" ! queue ! tsparse set-timestamps=true ! tsdemux name=d d. ! queue ! h264parse ! capsfilter caps="video/x-h264,stream-format=avc,alignment=au" ! kvssink stream-name="${streamName}" aws-region="${region}" storage-size=128 log-config="${logConfigPath}"`
		const shellCmd = `gst-launch-1.0 ${pipelineStr}`
		this.logger.info('GStreamer command', {
			port,
			streamName,
			pipelineStr,
			shellCmd,
		})
		const gst = spawn('sh', ['-c', shellCmd], {
			stdio: ['ignore', 'pipe', 'pipe'],
			env,
		})

		// Open FIFO for writing (blocks until GStreamer filesrc opens for read); then write
		// initial data so pipeline has data when it starts. Raced against the child exiting
		// or erroring first, and against a bounded timeout - opening a FIFO for writing
		// blocks at the OS level until *some* reader appears, so without this, a GStreamer
		// spawn failure or early exit (missing binary/plugin) would hang this wait forever,
		// holding the Kinesis lock indefinitely with nothing to release/retry it. Temporary
		// listeners here (not the permanent ones below) since gst's 'error' can fire before
		// this function would otherwise start listening for it at all.
		let inputStream: Writable
		try {
			inputStream = await new Promise<Writable>((resolve, reject) => {
				let settled = false
				const settleResolve = (w: Writable): void => {
					if (settled) return
					settled = true
					clearTimeout(timer)
					gst.off('exit', onEarlyExit)
					gst.off('error', onEarlyError)
					resolve(w)
				}
				const settleReject = (err: Error): void => {
					if (settled) return
					settled = true
					clearTimeout(timer)
					gst.off('exit', onEarlyExit)
					gst.off('error', onEarlyError)
					reject(err)
				}

				const onEarlyExit = (
					code: number | null,
					signal: string | null,
				): void => {
					settleReject(
						new Error(
							`GStreamer exited before opening the FIFO for reading (code=${code ?? 'null'}, signal=${signal ?? 'null'})`,
						),
					)
				}
				const onEarlyError = (err: Error): void => settleReject(err)
				gst.once('exit', onEarlyExit)
				gst.once('error', onEarlyError)

				const timer = setTimeout(() => {
					settleReject(
						new Error(
							`Timed out after ${FIFO_OPEN_TIMEOUT_MS}ms waiting for GStreamer to open the FIFO for reading`,
						),
					)
				}, FIFO_OPEN_TIMEOUT_MS)

				fs.open(fifoPath, 'w', (err, fd) => {
					if (err) {
						settleReject(err)
						return
					}
					const w = fs.createWriteStream('', { fd, autoClose: true })
					const data = initialData ?? Buffer.alloc(0)
					if (data.length > 0) {
						w.write(data, (e) => (e ? settleReject(e) : settleResolve(w)))
					} else {
						settleResolve(w)
					}
				})
			})
		} catch (err) {
			// Nothing was registered in activePipelines yet - clean up the child/FIFO and
			// rethrow so start()'s caller (index.ts's startPipelineOrReleaseLock) treats this
			// as a failed start and releases the Kinesis lock instead of holding it forever.
			this.logger.error(
				'Failed to open FIFO for GStreamer within the startup window',
				err instanceof Error ? err : new Error(String(err)),
				{ port, streamName, fifoPath },
			)
			gst.kill('SIGTERM')
			try {
				fs.unlinkSync(fifoPath)
			} catch {
				// best-effort cleanup; nothing more to do if this fails
			}
			throw err instanceof Error ? err : new Error(String(err))
		}
		inputStream.on('error', (err: NodeJS.ErrnoException) => {
			if (err.code !== 'EPIPE') {
				this.logger.warn('GStreamer FIFO write error', {
					port,
					streamName,
					code: err.code,
					message: err.message,
				})
			}
		})

		const gstStderrThrottle: GstStderrThrottle = { lastLog: {} }
		const gstStdoutThrottle: GstStdoutThrottle = { lastLog: {} }
		gst.stdout?.on('data', (data: Buffer) => {
			const text = data.toString()
			if (text.trim().length > 0) {
				this.logGstStdout(port, streamName, text, gstStdoutThrottle)
			}
		})
		gst.stderr?.on('data', (data: Buffer) => {
			this.logGstStderr(port, streamName, data.toString(), gstStderrThrottle)
		})
		gst.on('error', (err) => {
			this.logger.error('GStreamer error', err, { port, streamName })
			// Only mutate/notify if the map still points to *this* pipeline instance - see
			// the identical check (and rationale) on the SRTP path's handlers.
			if (this.activePipelines.get(port) !== pipeline) return
			this.activePipelines.delete(port)
			this.emit('pipelineExited', { port, code: null, signal: null })
		})
		gst.on('exit', (code, signal) => {
			const wasUnexpected = this.activePipelines.get(port) === pipeline
			this.logger.info('GStreamer exited', {
				port,
				streamName,
				code: code ?? undefined,
				signal: signal ?? undefined,
				unexpected: wasUnexpected,
			})
			if (!wasUnexpected) return
			this.activePipelines.delete(port)
			this.emit('pipelineExited', { port, code, signal })
		})

		const reorderBufferSize =
			this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		const reorder: ReorderState = {
			nextSeq: 0,
			nextToEmit: 1,
			buffer: new Map(),
		}
		const pipeline: FifoPortPipeline = {
			transport: 'fifo',
			gst,
			inputStream,
			reorder,
			gstStderrThrottle,
			gstStdoutThrottle,
			fifoPath,
		}
		this.activePipelines.set(port, pipeline)
		this.logger.info('Kinesis ingestion started', {
			port,
			streamName,
			reorderBufferSize,
		})
	}

	/**
	 * Builds the argv (not a shell string) for the SRTP -> RTP -> H264 -> kvssink pipeline.
	 * Spawned directly (no `sh -c`) because, unlike the FIFO path's interpolated values
	 * (stream name, region, paths), this pipeline interpolates key material - avoiding a
	 * shell entirely avoids any shell-metacharacter injection risk.
	 */
	private buildSrtpPipelineArgs(params: {
		relayPort: number
		streamName: string
		region: string
		logConfigPath: string
		key: SrtpPortKey
		jitterBufferLatencyMs: number
		roc: number
	}): string[] {
		const {
			relayPort,
			streamName,
			region,
			logConfigPath,
			key,
			jitterBufferLatencyMs,
			roc,
		} = params

		// Verify caps field names/values (especially srtp-cipher/srtp-auth enum literals) against
		// the actual installed GStreamer version via `gst-inspect-1.0 srtpdec` - see docs/TESTING-SRTP-INGESTION.md.
		// "roc" seeds srtpdec's rollover counter (see trackSrtpRoc/getSrtpRoc) so a restart
		// after the sender's sequence number has wrapped doesn't start back at ROC 0. Callers
		// must pass it through srtpdecSeedRoc, not a raw tracked/persisted roc value - see
		// that function's doc comment for why the raw value alone can seed the wrong ROC.
		const caps = [
			'application/x-srtp',
			'media=(string)video',
			'payload=(int)96',
			'clock-rate=(int)90000',
			'encoding-name=(string)H264',
			`ssrc=(uint)${key.ssrc}`,
			`srtp-key=(buffer)${key.keyHex}`,
			`srtp-cipher=(string)${key.cipher}`,
			`srtp-auth=(string)${key.auth}`,
			`srtcp-cipher=(string)${key.cipher}`,
			`srtcp-auth=(string)${key.auth}`,
			`roc=(uint)${roc}`,
		].join(',')

		return [
			'udpsrc',
			`port=${relayPort}`,
			'address=127.0.0.1',
			`caps=${caps}`,
			'!',
			'srtpdec',
			'!',
			'rtpjitterbuffer',
			`latency=${jitterBufferLatencyMs}`,
			'!',
			'rtph264depay',
			'!',
			'h264parse',
			'config-interval=-1',
			'!',
			'capsfilter',
			'caps=video/x-h264,stream-format=avc,alignment=au',
			'!',
			'kvssink',
			`stream-name=${streamName}`,
			`aws-region=${region}`,
			'storage-size=128',
			`log-config=${logConfigPath}`,
		]
	}

	/** Redacts key material from an argv array before logging it. */
	private redactSrtpArgv(argv: string[]): string[] {
		return argv.map((token) =>
			/^caps=.*srtp-key=\(buffer\)/.test(token)
				? token.replace(
						/srtp-key=\(buffer\)[0-9a-fA-F]+/,
						'srtp-key=(buffer)***REDACTED***',
					)
				: token,
		)
	}

	/**
	 * Resolves once GStreamer's stdout reports the pipeline reached PLAYING - gst-launch-1.0
	 * prints "Setting pipeline to PLAYING ..." by default once udpsrc and the rest of the
	 * pipeline have been set up, so this is a real (if best-effort) readiness signal instead
	 * of a blind fixed sleep. Not a byte-for-byte guarantee the udpsrc bind() syscall itself
	 * succeeded - a bulletproof check would need programmatic GStreamer bindings rather than
	 * a spawned gst-launch-1.0 process - but it's a meaningful improvement, and
	 * SRTP_STARTUP_GRACE_MS still bounds the wait as a fallback in case the message never
	 * appears (e.g. output format differs across GStreamer versions, or gets swallowed by
	 * stdout buffering).
	 */
	private async waitForSrtpPipelineReady(
		gst: ReturnType<typeof spawn>,
	): Promise<void> {
		return new Promise((resolve) => {
			let settled = false
			const finish = (): void => {
				if (settled) return
				settled = true
				gst.stdout?.off('data', onStdout)
				clearTimeout(timer)
				resolve()
			}
			const onStdout = (data: Buffer): void => {
				if (/Setting pipeline to PLAYING/i.test(data.toString())) finish()
			}
			gst.stdout?.on('data', onStdout)
			const timer = setTimeout(finish, SRTP_STARTUP_GRACE_MS)
		})
	}

	/**
	 * Single-run start logic for an SRTP port (credentials + spawn). Call only via start() so
	 * dedupe applies. `initialDatagrams` are individually-received UDP datagrams buffered
	 * before this port's Kinesis lock was acquired; they are replayed (each as its own
	 * datagram, never concatenated) once GStreamer's udpsrc has had a moment to bind.
	 */
	private async runStartForSrtpPort(
		port: number,
		initialDatagrams: Buffer[],
	): Promise<void> {
		if (this.activePipelines.has(port)) return
		const srtpConfig = this.config.srtp
		if (!srtpConfig) return

		const streamName = this.streamNameForPort(port)
		const region = this.config.region

		const key = srtpConfig.keyStore.getKeyForPort(port)
		if (!key) {
			this.logger.error(
				'No SRTP key configured for port; refusing to start ingestion',
				new Error('Missing SRTP key'),
				{ port, streamName },
			)
			return
		}

		const env = await this.resolveGstEnv(port, streamName)
		if (env === undefined) return

		const relayPortOffset =
			srtpConfig.relayPortOffset ?? DEFAULT_SRTP_RELAY_PORT_OFFSET
		const relayPort = port + relayPortOffset
		const jitterBufferLatencyMs =
			srtpConfig.jitterBufferLatencyMs ?? DEFAULT_SRTP_JITTER_BUFFER_LATENCY_MS

		// Capture the state applicable to the very first datagram we're about to actually
		// feed this fresh srtpdec instance - NOT the state after folding in the whole
		// buffered batch below, which could itself span a rollover and would then misseed
		// the earliest replayed packets with a ROC that only applies to the latest ones.
		const rocStateBeforeReplay = this.getSrtpRocState(port)

		// Still track every buffered datagram (including via any rollover within the batch
		// itself) for our own bookkeeping, independently of what gets seeded above - this is
		// what the next heartbeat/persist call (see index.ts) reports as current.
		for (const datagram of initialDatagrams) {
			this.trackSrtpRoc(port, datagram)
		}

		const argv = this.buildSrtpPipelineArgs({
			relayPort,
			streamName,
			region,
			logConfigPath: this.logConfigPath(),
			key,
			jitterBufferLatencyMs,
			roc: srtpdecSeedRoc(rocStateBeforeReplay),
		})

		this.logger.info('GStreamer command (SRTP)', {
			port,
			streamName,
			relayPort,
			argv: this.redactSrtpArgv(argv),
		})

		const gst = spawn('gst-launch-1.0', argv, {
			stdio: ['ignore', 'pipe', 'pipe'],
			env,
		})

		// Best-effort readiness signal instead of a blind fixed sleep: start listening now
		// (before any awaits) so an early message isn't missed. See waitForSrtpPipelineReady.
		const pipelineReady = this.waitForSrtpPipelineReady(gst)

		const relaySocket = dgram.createSocket('udp4')
		relaySocket.on('error', (err) => {
			this.logger.warn('SRTP relay socket error', {
				port,
				relayPort,
				message: err.message,
			})
		})

		let relaySocketClosed = false
		const closeRelaySocket = (): void => {
			if (relaySocketClosed) return
			relaySocketClosed = true
			try {
				relaySocket.close()
			} catch (err) {
				this.logger.warn('Error closing SRTP relay socket', {
					port,
					relayPort,
					message: err instanceof Error ? err.message : String(err),
				})
			}
		}

		const gstStderrThrottle: GstStderrThrottle = { lastLog: {} }
		const gstStdoutThrottle: GstStdoutThrottle = { lastLog: {} }
		gst.stdout?.on('data', (data: Buffer) => {
			const text = data.toString()
			if (text.trim().length > 0) {
				this.logGstStdout(port, streamName, text, gstStdoutThrottle)
			}
		})
		gst.stderr?.on('data', (data: Buffer) => {
			this.logGstStderr(port, streamName, data.toString(), gstStderrThrottle)
		})
		gst.on('error', (err) => {
			this.logger.error('GStreamer error', err, { port, streamName })
			// Only mutate/notify if the map still points to *this* pipeline instance - a
			// stale event from an already-replaced (newer) pipeline for the same port must
			// not delete that replacement's entry or trigger a spurious restart for it.
			// (`pipeline` is assigned synchronously below with no intervening `await`, so by
			// the time this handler can actually fire - always deferred by Node - it's set.)
			if (this.activePipelines.get(port) !== pipeline) return
			this.activePipelines.delete(port)
			closeRelaySocket()
			// Unlike 'exit', a spawn failure (missing binary/plugin) can fire 'error' without
			// ever firing 'exit' - previously only 'exit' emitted 'pipelineExited', so this
			// case silently left the Kinesis lock held with nothing telling index.ts to
			// release/retry it.
			this.emit('pipelineExited', { port, code: null, signal: null })
		})
		gst.on('exit', (code, signal) => {
			// Same identity check as 'error' above: only the pipeline currently registered
			// for this port may treat its own exit as unexpected. An intentional stop()
			// deletes the map entry before killing the process, so its own exit correctly
			// sees `!== pipeline` (or a newer pipeline's entry) and does nothing further here.
			const wasUnexpected = this.activePipelines.get(port) === pipeline
			this.logger.info('GStreamer exited', {
				port,
				streamName,
				code: code ?? undefined,
				signal: signal ?? undefined,
				unexpected: wasUnexpected,
			})
			if (!wasUnexpected) return
			this.activePipelines.delete(port)
			closeRelaySocket()
			this.emit('pipelineExited', { port, code, signal })
		})

		const pipeline: SrtpRelayPortPipeline = {
			transport: 'srtp-relay',
			gst,
			relaySocket,
			relayPort,
			closeRelaySocket,
			gstStderrThrottle,
			gstStdoutThrottle,
			// Always false until the startup grace period below elapses - even a resume/
			// restart with no initialDatagrams needs udpsrc to have bound before it's safe to
			// send, so this can't be short-circuited to true just because there's nothing to
			// replay (that previously let restart traffic race udpsrc's bind and get dropped).
			ready: false,
			pendingQueue: [],
			pendingQueueBytes: 0,
		}
		this.activePipelines.set(port, pipeline)

		// Unlike the FIFO path (whose fs.open() blocks until filesrc opens for read), there is
		// no OS-level handshake for udpsrc binding a port - wait for GStreamer's own readiness
		// signal (or the fallback timeout) before sending anything. Live packets queue in
		// pendingQueue (via writePacket) during this window instead of racing udpsrc's bind.
		await pipelineReady

		// A stop()/crash during the wait above deletes or replaces this port's pipeline entry
		// (see stop() and the gst 'error'/'exit' handlers above) - if that happened, this
		// (now-stale) pipeline must not replay, flip ready, log "started", or touch the relay
		// socket again. Compared by reference, not just isActive(port), so a *newer* pipeline
		// that already took over this port isn't mistaken for this one.
		if (this.activePipelines.get(port) !== pipeline) return

		// Await each send and pause briefly every batch - the pre-start buffer can hold
		// thousands of packets (10MB / ~1300 bytes each), and firing them all at once with no
		// pacing can overrun the local UDP socket/receiving udpsrc, silently dropping
		// datagrams (including the first keyframe/SPS/PPS) before GStreamer can consume them.
		for (const [index, datagram] of initialDatagrams.entries()) {
			await this.sendSrtpDatagram(port, pipeline, datagram, 'initial')
			if (this.activePipelines.get(port) !== pipeline) return
			if ((index + 1) % SRTP_REPLAY_BATCH_SIZE === 0) {
				await new Promise((resolve) =>
					setTimeout(resolve, SRTP_REPLAY_BATCH_PAUSE_MS),
				)
				if (this.activePipelines.get(port) !== pipeline) return
			}
		}

		// Flush whatever writePacket() queued while replay was in flight, in the order it
		// arrived, before accepting further direct sends - this is what stops live packets
		// from overtaking (and causing anti-replay/jitter-buffer discard of) the replay. This
		// queue is bounded by pendingQueueMaxBytes (see writePacket), not the pre-start
		// buffer's 10MB cap, but is normally much smaller in practice (just the grace
		// period's real-time packet arrival), so it doesn't need the same batching/pacing.
		pipeline.ready = true
		const queued = pipeline.pendingQueue
		pipeline.pendingQueue = []
		pipeline.pendingQueueBytes = 0
		for (const datagram of queued) {
			void this.sendSrtpDatagram(port, pipeline, datagram, 'queued')
		}

		this.logger.info('SRTP Kinesis ingestion started', {
			port,
			streamName,
			relayPort,
		})
	}

	/**
	 * Sends one datagram to the SRTP relay port, logging (not throwing) on failure. Resolves
	 * once the send completes (success or failure) - callers that fire many of these in a
	 * row (the startup replay) must await each one rather than firing them all at once, or
	 * nothing paces the burst against what the receiving udpsrc/srtpdec can actually consume.
	 * Live per-packet sends (already paced by real network arrival) may call this without
	 * awaiting it.
	 */
	private async sendSrtpDatagram(
		port: number,
		pipeline: SrtpRelayPortPipeline,
		datagram: Buffer,
		kind: 'initial' | 'queued' | 'live',
	): Promise<void> {
		// Never throws/rejects, even on a synchronous error (e.g. the relay socket was
		// already closed by a concurrent stop()/crash) - callers that await a run of these in
		// a loop (the startup replay) must not have that loop aborted by a single failed send,
		// or a caller further up (start()) could reject and leave an already-acquired Kinesis
		// lock unreleased.
		try {
			await new Promise<void>((resolve) => {
				pipeline.relaySocket.send(
					datagram,
					pipeline.relayPort,
					'127.0.0.1',
					(err) => {
						if (err) {
							this.logger.warn('Failed to relay SRTP datagram', {
								port,
								kind,
								message: err.message,
							})
						}
						resolve()
					},
				)
			})
		} catch (err) {
			this.logger.warn(
				'Failed to relay SRTP datagram (synchronous send error)',
				{
					port,
					kind,
					message: err instanceof Error ? err.message : String(err),
				},
			)
		}
	}

	private drainFifo(port: number, stdin: Writable): void {
		const pipeline = this.activePipelines.get(port)
		if (pipeline?.transport !== 'fifo') return
		const { reorder } = pipeline
		while (reorder.buffer.has(reorder.nextToEmit)) {
			const data = reorder.buffer.get(reorder.nextToEmit)
			reorder.buffer.delete(reorder.nextToEmit)
			reorder.nextToEmit += 1
			const ok = stdin.write(data)
			if (!ok) {
				stdin.once('drain', () => this.drainFifo(port, stdin))
				return
			}
		}
		// If buffer overfull, skip missing packets so we don't stall on loss
		const maxBuf = this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		if (maxBuf > 0 && reorder.buffer.size >= maxBuf) {
			while (
				!reorder.buffer.has(reorder.nextToEmit) &&
				reorder.buffer.size > 0
			) {
				reorder.nextToEmit += 1
			}
			this.drainFifo(port, stdin)
		}
	}

	/**
	 * Writes a UDP packet into the pipeline for that port. FIFO/MPEG-TS ports buffer and
	 * reorder by receive-sequence (see `reorderBufferSize`) before writing to GStreamer's
	 * stdin/FIFO. SRTP ports relay each datagram immediately and unbuffered - GStreamer's
	 * rtpjitterbuffer does real RTP-sequence-aware reordering downstream, so Node must not
	 * reorder or coalesce these datagrams itself.
	 */
	writePacket(port: number, data: Buffer): void {
		// Track ROC unconditionally for SRTP ports, even if no pipeline is currently
		// registered (e.g. the lock-held window between an unexpected GStreamer exit and its
		// throttled restart in index.ts - see the 'pipelineExited' handler there). Without
		// this, sequence numbers observed only during that window are invisible to ROC
		// tracking, so a rollover happening while the pipeline is down is missed entirely and
		// the eventual restart is seeded with a stale ROC.
		if (this.isSrtpPort(port)) {
			this.trackSrtpRoc(port, data)
		}

		const pipeline = this.activePipelines.get(port)
		if (!pipeline) return

		if (pipeline.transport === 'srtp-relay') {
			if (!pipeline.ready) {
				// Startup replay is still in flight - queue rather than send now, so this
				// datagram can't overtake the older, still-buffered ones (see `ready` above).
				// Bounded: a restart/resume can hold the pipeline in this not-yet-ready state
				// for a while (waiting on the paced 10MB replay above), and a high-rate source
				// pushing here the whole time would otherwise grow this array unboundedly -
				// the 2x-threshold-style cap other per-port buffers/queues in index.ts use
				// doesn't apply here since this array lives inside KinesisIngestionPipeline,
				// not index.ts.
				pipeline.pendingQueue.push(data)
				pipeline.pendingQueueBytes += data.length
				const maxPendingQueueBytes =
					this.config.srtp?.pendingQueueMaxBytes ??
					DEFAULT_SRTP_PENDING_QUEUE_MAX_BYTES
				while (
					pipeline.pendingQueueBytes > maxPendingQueueBytes &&
					pipeline.pendingQueue.length > 1
				) {
					const dropped = pipeline.pendingQueue.shift()
					if (dropped !== undefined)
						pipeline.pendingQueueBytes -= dropped.length
				}
				return
			}
			void this.sendSrtpDatagram(port, pipeline, data, 'live')
			return
		}

		const inputStream = pipeline.inputStream
		if (inputStream.writable !== true) return

		const maxBuf = this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		if (maxBuf <= 0) {
			const ok = inputStream.write(data)
			if (!ok) inputStream.once('drain', () => {})
			return
		}

		const { reorder } = pipeline
		reorder.nextSeq += 1
		reorder.buffer.set(reorder.nextSeq, data)
		this.drainFifo(port, inputStream)
	}

	/**
	 * Stops the pipeline for a port: flushes reorder buffer (FIFO) or closes the relay
	 * socket (SRTP), then waits for process exit.
	 */
	async stop(port: number): Promise<void> {
		const pipeline = this.activePipelines.get(port)
		if (!pipeline) return

		this.activePipelines.delete(port)
		const { gst } = pipeline

		if (pipeline.transport === 'fifo') {
			const { inputStream, reorder, fifoPath } = pipeline
			if (inputStream.writable) {
				while (reorder.buffer.has(reorder.nextToEmit)) {
					const buf = reorder.buffer.get(reorder.nextToEmit)
					reorder.buffer.delete(reorder.nextToEmit)
					reorder.nextToEmit += 1
					if (buf !== undefined) inputStream.write(buf)
				}
				inputStream.end()
			}
			try {
				fs.unlinkSync(fifoPath)
			} catch (e) {
				this.logger.warn('Failed to unlink FIFO', {
					port,
					fifoPath,
					message: e instanceof Error ? e.message : String(e),
				})
			}
		} else {
			// Unlike the FIFO path (where ending the input stream gives filesrc a natural EOS
			// that makes GStreamer exit on its own), closing our relay socket has no effect on
			// GStreamer's own, independently bound udpsrc socket - nothing will make this
			// process exit by itself. Signal it immediately rather than idling through the
			// exit-wait below for the full 15 seconds for nothing.
			pipeline.closeRelaySocket()
			gst.kill('SIGTERM')
		}

		try {
			await new Promise<void>((resolve) => {
				const t = setTimeout(resolve, 15_000)
				gst.once('exit', () => {
					clearTimeout(t)
					resolve()
				})
			})
		} catch (err) {
			this.logger.warn('Error waiting for pipeline stop', {
				port,
				error: err instanceof Error ? err.message : String(err),
			})
		}
		gst.kill('SIGTERM')
		this.logger.info('Kinesis ingestion stopped', { port })
	}

	/**
	 * Stops all active pipelines (e.g. on shutdown).
	 */
	async stopAll(): Promise<void> {
		const ports = Array.from(this.activePipelines.keys())
		await Promise.all(ports.map(async (port) => this.stop(port)))
	}
}
