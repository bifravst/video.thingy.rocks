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

/**
 * Injectable process dependencies, so the SRTP lifecycle (spawn argv/caps construction,
 * readiness gating, ordered replay, pending-queue bounds, stop/shutdown cleanup) can be
 * exercised by the spec without a real gst-launch-1.0 or AWS credentials. Both are
 * optional and default to the real things; production code never passes them.
 */
export type KinesisIngestionPipelineDependencies = {
	/** Replaces the gst-launch-1.0 / sh -c spawn in both transports. */
	spawn?: typeof spawn
	/**
	 * Replaces AWS credential resolution for kvssink's env (see resolveGstEnv) - must
	 * return the env for the child, or undefined to simulate a failed resolution.
	 */
	gstEnv?: (
		port: number,
		streamName: string,
	) => Promise<NodeJS.ProcessEnv | undefined>
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
/** How often runStartForFifoPort re-probes whether GStreamer's filesrc has opened the FIFO
 * for reading yet (see openFifoWriterFd's nonblocking probe). */
const FIFO_OPEN_PROBE_INTERVAL_MS = 50
/** Bounds how long killAndWait waits for a still-running GStreamer child to exit after
 * SIGTERM before escalating to SIGKILL - matches stop()'s EOS grace-wait budget, so no
 * caller can hang indefinitely on a child that refuses to die. */
const CHILD_EXIT_TIMEOUT_MS = 15_000
/** Grace period for a child's exit event to arrive after SIGKILL: SIGKILL cannot be
 * caught or ignored, but reaping the child (and delivering its 'exit' event) is still
 * asynchronous, so this bounds the final wait in killAndWait. */
const CHILD_SIGKILL_GRACE_MS = 1_000

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
 * tracking state: the raw tracked ROC, unmodified, regardless of which half of the 16-bit
 * sequence space the sender's current sequence number is in.
 *
 * libsrtp's srtp_set_stream_roc - which GStreamer's srtpdec calls with the caps "roc" field -
 * does NOT pin the decoder's internal index to roc*65536+0 and then re-guess. In every
 * libsrtp version that has the API (>= 2.3.0, which any srtpdec with caps "roc" support
 * requires), it sets stream->pending_roc, and the first packet's extended index is computed
 * DIRECTLY as (seededRoc << 16) | seq - using the packet's real sequence number - after
 * which srtp_rdbx_set_roc_seq pins both the ROC and the sequence halves from that estimate
 * (see srtp.c's srtp_estimate_index/srtp_get_est_pkt_index and the srtp_unprotect caller).
 * So the correct seed is simply the ROC the sender is currently in, in both halves of the
 * range; no compensation is needed or correct.
 *
 * (An earlier version of this seeded `roc + 1` when the tracked highestSeq was >= 0x8000,
 * based on a model of libsrtp that re-guesses the first packet's ROC against a
 * seededRoc*65536+0 baseline - verified wrong both against the libsrtp source and
 * empirically, GStreamer 1.28 + libsrtp, sender pinned to ROC 0 with sequence numbers
 * starting at 40000: seeding the raw roc=0 decrypts 151/151 packets, seeding roc=1 decrypts
 * 0/151. Seeding one ROC too high makes the first packet fail authentication AND pins the
 * replay/rollover state one ROC forward, dropping every packet until the sender genuinely
 * wraps - up to ~32768 sequence numbers (~18 minutes at 30 fps) of lost video, on roughly
 * half of all restarts.) Exported for unit testing.
 */
export const srtpdecSeedRoc = (state: SrtpRocState): number => state.roc

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
	 * SRTP datagrams received while no pipeline is active for the port (e.g. the window
	 * between an unexpected GStreamer exit and its throttled restart in index.ts, or while
	 * any start is still resolving credentials/spawning): writePacket would otherwise drop
	 * the payload entirely - only its ROC contribution would survive - losing the first
	 * post-crash keyframe/SPS/PPS and delaying recovery until the sender's next keyframe.
	 * Seeded into the replacement pipeline's pendingQueue at registration (see
	 * runStartForSrtpPort), so it is sent in order after the startup replay and before any
	 * live traffic. Bounded by the same pendingQueueMaxBytes cap; cleared by stop() - an
	 * intentional stop has no replacement pipeline to replay into.
	 */
	private readonly srtpHoldByPort = new Map<
		number,
		{ chunks: Buffer[]; totalBytes: number }
	>()
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
	/**
	 * Every GStreamer child this instance has spawned that hasn't exited yet. shutdown()
	 * sweeps this set so no child - including one registered by a start() that was still in
	 * flight when shutdown began, after stopAll() had already snapshotted the active
	 * pipelines - can outlive the process as an orphan: process.exit() does not signal
	 * children, and an orphaned kvssink producer would keep writing to Kinesis with no
	 * owner.
	 */
	private readonly spawnedGst: Set<ReturnType<typeof spawn>> = new Set()
	/** True once shutdown() has run; further starts are refused (see shutdown()). */
	private closed = false

	/**
	 * Invalidation counter for in-flight starts, bumped by stop() (and effectively by
	 * shutdown() via `closed`): stop() is a no-op until a pipeline is *registered*, so a
	 * start() that is still awaiting credential resolution or the FIFO open would
	 * otherwise happily register its GStreamer child *after* the teardown already
	 * returned - leaving a producer that no stop() ever saw, which the ownership
	 * generation guards in index.ts cannot reliably catch either (the generation
	 * capture there predates the child's appearance, and clearLockHeld can run before
	 * it). Starts capture the counter at entry and re-check it after every await
	 * before registering; a start that observes an invalidation cleans up its own child
	 * and abandons, so nothing registers behind a completed teardown.
	 */
	private readonly startGenerationByPort = new Map<number, number>()

	constructor(
		config: KinesisIngestionPipelineConfig,
		deps?: KinesisIngestionPipelineDependencies,
	) {
		super()
		// Fail fast on a config that would silently map ports to nonexistent Kinesis
		// VideoStreams: streamSlotForPort derives the slot from the port's offset within its
		// own range, so an SRTP range covering more ports than the main range yields slots
		// beyond the streams the CDK stack creates (and fewer ports would strand SRTP ports
		// at the end of the range with no slot at all).
		if (config.portRange.start > config.portRange.end) {
			throw new Error(
				`Invalid config: portRange must be non-empty (${config.portRange.start} > ${config.portRange.end})`,
			)
		}
		if (config.srtp !== undefined) {
			const mainPorts = config.portRange.end - config.portRange.start + 1
			const srtpPorts =
				config.srtp.portRange.end - config.srtp.portRange.start + 1
			if (srtpPorts !== mainPorts) {
				throw new Error(
					`Invalid config: srtp.portRange (${config.srtp.portRange.start}-${config.srtp.portRange.end}, ${srtpPorts} ports) must cover exactly as many ports as portRange (${config.portRange.start}-${config.portRange.end}, ${mainPorts} ports): unencrypted port start+N and SRTP port start+N must stay paired (see streamSlotForPort/streamNameForPort)`,
				)
			}
			// Overlapping ranges would leave isSrtpPort() claiming ports the main
			// (unencrypted) listener is bound to - routing their packets to the SRTP
			// handler/pipeline - while the SRTP listener cannot bind them at all
			// (EADDRINUSE). The ranges must be fully disjoint.
			const overlaps =
				config.srtp.portRange.start <= config.portRange.end &&
				config.portRange.start <= config.srtp.portRange.end
			if (overlaps) {
				throw new Error(
					`Invalid config: srtp.portRange (${config.srtp.portRange.start}-${config.srtp.portRange.end}) must not overlap portRange (${config.portRange.start}-${config.portRange.end}): overlapping ports would be received by the unencrypted listener but routed to the SRTP pipeline, and the SRTP listener could never bind them`,
				)
			}
		}
		this.config = config
		this.logger = new Logger('KinesisIngestionPipeline')
		// Test-injectable process dependencies (see KinesisIngestionPipelineDependencies):
		// defaults are the real implementations, so production behavior is unchanged.
		this.spawnProcess = deps?.spawn ?? spawn
		this.gstEnvResolver = deps?.gstEnv
	}

	/** Spawn to use for GStreamer children (injectable for tests). */
	private readonly spawnProcess: typeof spawn
	/** Optional replacement for resolveGstEnv's credential resolution (tests). */
	private readonly gstEnvResolver:
		| ((
				port: number,
				streamName: string,
		  ) => Promise<NodeJS.ProcessEnv | undefined>)
		| undefined

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
	 *
	 * Idempotent for the same datagram: advanceSrtpRoc only raises the tracked highest
	 * extended index, so tracking the same (or an older) datagram a second time is a no-op.
	 * This lets index.ts call it for a datagram *before* taking the ROC snapshot it persists
	 * in the same heartbeat (see processPacket) without writePacket's own tracking of that
	 * same datagram double-counting anything.
	 *
	 * Public rather than private for that pre-heartbeat caller.
	 */
	trackSrtpRoc(port: number, datagram: Buffer): void {
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
	 *
	 * The in-memory estimate is only ever cleared by a process restart: a stream stopping
	 * does not reset it, and clearing the persisted DynamoDB fields alone does not either
	 * (a cleared slot reads back as {roc: 0, highestSeq: 0}, which this deliberately ignores
	 * in favor of the live estimate). The documented fresh-session recovery of clearing
	 * srtpRoc/srtpHighestSeq/srtpRocSsrc/srtpRocKeyFingerprint from the slot's
	 * StreamMetadata item therefore also requires a backend restart, or the sender
	 * restarted against the same still-running process is still seeded with the old ROC
	 * and cannot authenticate - see docs/TESTING-SRTP-INGESTION.md ("Session continuity")
	 * and backend/README.md.
	 */
	seedSrtpRoc(port: number, seeded: SrtpRocState): void {
		const state = this.srtpRocByPort.get(port)
		if (!state) {
			// {roc: 0, highestSeq: 0} here is not a real observed state - it is what
			// StreamMetadataService.getSrtpRocState returns for a missing or
			// identity-mismatched (e.g. cleared, or key/SSRC-rotated) persisted item: a
			// fresh-session *sentinel*. Installing it as an initialized baseline would
			// make the first observed datagram bypass advanceSrtpRoc's genuine
			// first-packet branch and be classified against that baseline instead. The
			// current nearest-index heuristic happens to produce ROC 0 from a {0,0}
			// baseline for any first sequence number (locked in by the spec), so the two
			// are equivalent today - but conflating "initialized" with "fresh" like that
			// is exactly the kind of implicit invariant a future heuristic change could
			// silently break into an off-by-one-ROC misclassification, which would then
			// be persisted and seed the *next* restart one rollover too high (undecryptable
			// until the sender wraps again). Leave tracking uninitialized instead: a
			// fresh session's first observed datagram is advanceSrtpRoc's job to start.
			if (seeded.roc === 0 && seeded.highestSeq === 0) return
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
		// Test injection point (see KinesisIngestionPipelineDependencies) - skips the real
		// AWS credential chain, which must not run in unit tests.
		if (this.gstEnvResolver !== undefined) {
			return this.gstEnvResolver(port, streamName)
		}
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
		if (this.closed) return
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
	 * Opens a FIFO for writing without ever parking a libuv threadpool worker on the open
	 * itself. A plain blocking fs.open('w') on a FIFO parks its worker until a reader
	 * (GStreamer's filesrc) appears - if the child never opens its end (missing
	 * binary/plugin, early exit), that worker stays blocked forever (unlinking the FIFO
	 * does not wake it), and repeated failed starts exhaust the small (default 4) worker
	 * pool, stalling every fs operation in the process. Instead: probe with
	 * O_WRONLY|O_NONBLOCK, which the kernel completes immediately - with ENXIO while no
	 * reader exists, successfully once one does - and open the real (blocking-mode) writer
	 * only once a reader is confirmed. `shouldAbort` is polled between probes so the
	 * caller's timeout / child-exit guards cancel the wait.
	 *
	 * The probe descriptor stays open *until the blocking writer has opened*, and is only
	 * then closed: filesrc unblocks the moment the probe opens, and if the probe were
	 * closed before the real writer takes over, the FIFO would briefly have no writer at
	 * all - filesrc reads EOF, GStreamer exits, and the now-readerless blocking open can
	 * park a threadpool worker indefinitely (fs.open cannot be cancelled). With the probe
	 * held open across the handoff there is always a writer, so filesrc never sees a
	 * spurious EOF. The only way the blocking open can still park a worker is GStreamer
	 * organically exiting in the microseconds between the probe succeeding and the open
	 * completing - bounded by the caller's guards, and vanishingly rare by comparison
	 * with parking a worker on every failed start.
	 */
	private async openFifoWriterFd(
		fifoPath: string,
		shouldAbort: () => boolean,
	): Promise<number> {
		for (;;) {
			if (shouldAbort()) {
				throw new Error(
					'Aborted waiting for GStreamer to open the FIFO for reading',
				)
			}
			const probed = await new Promise<number | undefined>(
				(resolve, reject) => {
					fs.open(
						fifoPath,
						fs.constants.O_WRONLY | fs.constants.O_NONBLOCK,
						(err, fd) => {
							if (err !== null) {
								// ENXIO is the expected "no reader yet" signal; anything
								// else (e.g. ENOENT if the FIFO vanished) is a real error.
								if (err.code === 'ENXIO') resolve(undefined)
								else reject(err)
								return
							}
							resolve(fd)
						},
					)
				},
			)
			if (probed !== undefined) {
				// Reader confirmed: open the real (blocking-mode) writer for the write
				// stream (blocking write semantics are what the reorder-buffered stream
				// and the initial-data write rely on) - and close the probe only after
				// that succeeds, so the write end is never momentarily writerless (see
				// the doc comment above).
				return await new Promise<number>((resolve, reject) => {
					fs.open(fifoPath, 'w', (err, fd) => {
						fs.close(probed, () => {})
						if (err !== null) reject(err)
						else resolve(fd)
					})
				})
			}
			await new Promise((resolve) =>
				setTimeout(resolve, FIFO_OPEN_PROBE_INTERVAL_MS),
			)
		}
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

		// Start-invalidation token (see startGenerationByPort): captured before the
		// first await below, re-checked after each await before anything is registered.
		const startGeneration = this.startGenerationByPort.get(port) ?? 0
		const startInvalidated = (): boolean =>
			this.closed ||
			(this.startGenerationByPort.get(port) ?? 0) !== startGeneration

		const streamName = this.streamNameForPort(port)
		const region = this.config.region

		const env = await this.resolveGstEnv(port, streamName)
		if (env === undefined) return
		// A stop()/shutdown() during credential resolution invalidated this start:
		// nothing has been spawned yet, so there is nothing to clean up.
		if (startInvalidated()) return

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
		// Checked in the same synchronous block as the spawn below, so a concurrent
		// shutdown() can never interleave between the check and the child appearing.
		if (this.closed) {
			this.logger.warn(
				'Refusing to spawn GStreamer: pipeline manager is shut down',
				{ port, streamName },
			)
			// mkfifo above already created the FIFO - a shutdown that raced this start
			// (e.g. during its credential resolution) must not leave a stale one behind.
			// Best-effort, same as the failure path below.
			try {
				fs.unlinkSync(fifoPath)
			} catch {
				// best-effort cleanup; nothing more to do if this fails
			}
			return
		}
		const gst = this.spawnProcess('sh', ['-c', shellCmd], {
			stdio: ['ignore', 'pipe', 'pipe'],
			env,
		})
		this.spawnedGst.add(gst)

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

				// The open itself never parks a threadpool worker (see openFifoWriterFd);
				// `settled` doubles as its abort flag so the timeout above and the child's
				// early exit/error both cancel it.
				void (async () => {
					try {
						const fd = await this.openFifoWriterFd(fifoPath, () => settled)
						const w = fs.createWriteStream('', { fd, autoClose: true })
						const data = initialData ?? Buffer.alloc(0)
						if (data.length > 0) {
							w.write(data, (e) => (e ? settleReject(e) : settleResolve(w)))
						} else {
							settleResolve(w)
						}
					} catch (err) {
						settleReject(err instanceof Error ? err : new Error(String(err)))
					}
				})()
			})
		} catch (err) {
			// Nothing was registered in activePipelines yet - clean up the child/FIFO and
			// rethrow so start()'s caller (index.ts's startPipelineOrReleaseLock) treats this
			// as a failed start and releases the Kinesis lock instead of holding it forever.
			// The child is killed and its actual exit *awaited* (escalating to SIGKILL if
			// needed - see killAndWait) *before* it is removed from spawnedGst: untracking a
			// still-live child here would leave it running with no owner AND invisible to
			// shutdown()'s orphan sweep, free to keep writing to the Kinesis stream after
			// this instance has already released its lock.
			this.logger.error(
				'Failed to open FIFO for GStreamer within the startup window',
				err instanceof Error ? err : new Error(String(err)),
				{ port, streamName, fifoPath },
			)
			await this.killAndWait(gst, { port, pid: gst.pid, fifoPath })
			this.spawnedGst.delete(gst)
			try {
				fs.unlinkSync(fifoPath)
			} catch {
				// best-effort cleanup; nothing more to do if this fails
			}
			throw err instanceof Error ? err : new Error(String(err))
		}
		// A stop()/shutdown() invalidated this start while it waited for GStreamer to
		// open the FIFO - nothing is registered yet, but a child was already spawned:
		// stop it (and its actual exit awaited) ourselves, the same way the failure path
		// above cleans up, instead of registering a producer behind the completed
		// teardown.
		if (startInvalidated()) {
			this.logger.warn(
				'Start invalidated while waiting for the FIFO; aborting startup',
				{ port, streamName, fifoPath },
			)
			await this.killAndWait(gst, { port, pid: gst.pid, fifoPath })
			this.spawnedGst.delete(gst)
			try {
				fs.unlinkSync(fifoPath)
			} catch {
				// best-effort cleanup; nothing more to do if this fails
			}
			return
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
			this.spawnedGst.delete(gst)
			// Only mutate/notify if the map still points to *this* pipeline instance - see
			// the identical check (and rationale) on the SRTP path's handlers.
			if (this.activePipelines.get(port) !== pipeline) return
			this.activePipelines.delete(port)
			this.emit('pipelineExited', { port, code: null, signal: null })
		})
		gst.on('exit', (code, signal) => {
			this.spawnedGst.delete(gst)
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
		// must pass it through srtpdecSeedRoc, which returns the raw tracked/persisted ROC -
		// see that function's doc comment for why no compensation is applied (and why the
		// roc+1 compensation that used to live there was wrong).
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
	 * stdout buffering). Matches against accumulated stdout rather than each 'data' chunk
	 * individually, since a pipe-buffered child can split the line across two chunks.
	 */
	private async waitForSrtpPipelineReady(
		gst: ReturnType<typeof spawn>,
	): Promise<void> {
		return new Promise((resolve) => {
			let settled = false
			// Bounded rolling tail of everything seen so far - only needs to be long enough
			// to hold one "Setting pipeline to PLAYING ..." line (~29 chars) plus a
			// chunk-sized margin, in case a chunk boundary falls inside it.
			let text = ''
			const finish = (): void => {
				if (settled) return
				settled = true
				gst.stdout?.off('data', onStdout)
				clearTimeout(timer)
				resolve()
			}
			const onStdout = (data: Buffer): void => {
				text = (text + data.toString()).slice(-1024)
				if (/Setting pipeline to PLAYING/i.test(text)) finish()
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

		// Start-invalidation token (see startGenerationByPort): captured before the
		// first await below, re-checked after each await before anything is registered.
		// (The SRTP path's spawn and registration are one synchronous block, so unlike
		// the FIFO path there is no post-spawn pre-registration window to guard - the
		// only await before registering is the credential resolution.)
		const startGeneration = this.startGenerationByPort.get(port) ?? 0
		const startInvalidated = (): boolean =>
			this.closed ||
			(this.startGenerationByPort.get(port) ?? 0) !== startGeneration

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
		// A stop()/shutdown() during credential resolution invalidated this start:
		// nothing has been spawned yet, so there is nothing to clean up - abandoning
		// here is what keeps a stop() from being a silent no-op against this start.
		if (startInvalidated()) return

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

		// Checked in the same synchronous block as the spawn below, so a concurrent
		// shutdown() can never interleave between the check and the child appearing.
		if (this.closed) {
			this.logger.warn(
				'Refusing to spawn GStreamer: pipeline manager is shut down',
				{ port, streamName, relayPort },
			)
			return
		}

		const gst = this.spawnProcess('gst-launch-1.0', argv, {
			stdio: ['ignore', 'pipe', 'pipe'],
			env,
		})
		this.spawnedGst.add(gst)

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
			this.spawnedGst.delete(gst)
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
			this.spawnedGst.delete(gst)
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

		// Seed the pending queue with the datagrams held while no pipeline was active (see
		// writePacket's hold): they predate everything that arrives from here on, and the
		// queued-drain below sends them in order after the startup replay and before any
		// live traffic. Doing this in the same synchronous block as the registration above
		// closes the window atomically - writePacket stops holding (and starts queueing/
		// relaying into this pipeline) exactly when the hold is picked up, so no datagram is
		// both held and queued, and none is lost between the two.
		const held = this.srtpHoldByPort.get(port)
		if (held !== undefined) {
			this.srtpHoldByPort.delete(port)
			pipeline.pendingQueue.push(...held.chunks)
			pipeline.pendingQueueBytes += held.totalBytes
		}

		// Returns the unsent remainder of this pipeline's pending queue to the hold (see
		// srtpHoldByPort) on every stale-startup exit below: the queued datagrams were
		// already *taken out* of the hold (seeded above) and out of writePacket's live
		// path, so a start that dies mid-readiness/mid-drain must hand them back - the
		// failed-start path in index.ts and the port's next start both come looking for
		// them via takeHeldDatagrams, and without this they would be permanently dropped
		// (exactly the post-crash recovery packets this hold exists to preserve).
		// Our remainder is older than anything writePacket held after this pipeline's
		// entry was removed, so it goes first; the byte cap keeps the hold bounded.
		const restoreUnsentToHold = (): void => {
			if (pipeline.pendingQueue.length === 0) return
			const remainder = pipeline.pendingQueue.splice(
				0,
				pipeline.pendingQueue.length,
			)
			pipeline.pendingQueueBytes = 0
			const existing = this.srtpHoldByPort.get(port)?.chunks ?? []
			const chunks = [...remainder, ...existing]
			let totalBytes = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
			const maxHoldBytes =
				this.config.srtp?.pendingQueueMaxBytes ??
				DEFAULT_SRTP_PENDING_QUEUE_MAX_BYTES
			while (totalBytes > maxHoldBytes && chunks.length > 1) {
				const dropped = chunks.shift()
				if (dropped !== undefined) totalBytes -= dropped.length
			}
			this.srtpHoldByPort.set(port, { chunks, totalBytes })
		}

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
		if (this.activePipelines.get(port) !== pipeline) {
			restoreUnsentToHold()
			return
		}

		// Await each send and pause briefly every batch - the pre-start buffer can hold
		// thousands of packets (10MB / ~1300 bytes each), and firing them all at once with no
		// pacing can overrun the local UDP socket/receiving udpsrc, silently dropping
		// datagrams (including the first keyframe/SPS/PPS) before GStreamer can consume them.
		for (const [index, datagram] of initialDatagrams.entries()) {
			await this.sendSrtpDatagram(port, pipeline, datagram, 'initial')
			if (this.activePipelines.get(port) !== pipeline) {
				restoreUnsentToHold()
				return
			}
			if ((index + 1) % SRTP_REPLAY_BATCH_SIZE === 0) {
				await new Promise((resolve) =>
					setTimeout(resolve, SRTP_REPLAY_BATCH_PAUSE_MS),
				)
				if (this.activePipelines.get(port) !== pipeline) {
					restoreUnsentToHold()
					return
				}
			}
		}

		// Drain whatever writePacket() queued while the replay was in flight, in arrival
		// order, before accepting direct sends - this is what stops live packets from
		// overtaking (and causing anti-replay/jitter-buffer discard of) the replay. The
		// queue is drained with the same awaited, batch-paced sends as the initial replay:
		// during a slow restart it can hold up to pendingQueueMaxBytes (10MB by default,
		// thousands of datagrams), and firing that as one unawaited send() burst would
		// overrun the loopback socket's/udpsrc's buffers and silently drop SRTP packets
		// (including the first post-restart keyframe). New datagrams keep appending to the
		// queue while `ready` is still false, so this loops until it has caught up with
		// the live arrival rate - a normal stream's rate is far below the paced drain rate
		// (~5000 datagrams/sec), so it converges quickly; the queue's byte cap bounds
		// memory in the meantime, and a stop()/replacement during the drain exits via the
		// staleness checks below.
		let drainedFromQueue = 0
		while (pipeline.pendingQueue.length > 0) {
			if (this.activePipelines.get(port) !== pipeline) {
				restoreUnsentToHold()
				return
			}
			const datagram = pipeline.pendingQueue.shift()
			if (datagram === undefined) break
			pipeline.pendingQueueBytes -= datagram.length
			await this.sendSrtpDatagram(port, pipeline, datagram, 'queued')
			drainedFromQueue++
			if (drainedFromQueue % SRTP_REPLAY_BATCH_SIZE === 0) {
				await new Promise((resolve) =>
					setTimeout(resolve, SRTP_REPLAY_BATCH_PAUSE_MS),
				)
				if (this.activePipelines.get(port) !== pipeline) {
					restoreUnsentToHold()
					return
				}
			}
		}

		// Caught up - accept direct (unawaited) sends from here on; at normal line rate
		// these are one-at-a-time, nowhere near a burst.
		pipeline.ready = true

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
	 * Returns (and clears) the datagrams held for this port while no pipeline was active
	 * (see writePacket's hold). index.ts's failed-start re-arm merges them into the
	 * re-armed pre-start buffer: the hold predates everything buffered from that point on,
	 * so merging keeps the next start's replay a single oldest-first sequence instead of
	 * two competing sources (the replayed initialData vs. the hold seeded into the new
	 * pipeline's pendingQueue), where newer sequence numbers would overtake the older
	 * held keyframe/SPS/PPS and have it discarded by the jitter buffer or anti-replay
	 * window.
	 */
	takeHeldDatagrams(port: number): Buffer[] {
		const held = this.srtpHoldByPort.get(port)
		this.srtpHoldByPort.delete(port)
		return held?.chunks ?? []
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
		if (!pipeline) {
			// SRTP: hold the datagram for the replacement pipeline (see srtpHoldByPort)
			// instead of dropping its payload; FIFO/MPEG-TS stays a plain drop, as before -
			// that path's payload is self-synchronizing and recovers at the next sync
			// point, so replaying stale bytes buys nothing.
			if (!this.isSrtpPort(port)) return
			let hold = this.srtpHoldByPort.get(port)
			if (hold === undefined) {
				hold = { chunks: [], totalBytes: 0 }
				this.srtpHoldByPort.set(port, hold)
			}
			hold.chunks.push(data)
			hold.totalBytes += data.length
			const maxHoldBytes =
				this.config.srtp?.pendingQueueMaxBytes ??
				DEFAULT_SRTP_PENDING_QUEUE_MAX_BYTES
			while (hold.totalBytes > maxHoldBytes && hold.chunks.length > 1) {
				const dropped = hold.chunks.shift()
				if (dropped !== undefined) hold.totalBytes -= dropped.length
			}
			return
		}

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
	 * Waits (bounded) for a child's 'exit' event. Returns immediately if the child has
	 * already exited.
	 */
	private async waitForChildExit(
		gst: ReturnType<typeof spawn>,
		timeoutMs: number,
	): Promise<void> {
		if (gst.exitCode !== null || gst.signalCode !== null) return
		await new Promise<void>((resolve) => {
			const t = setTimeout(resolve, timeoutMs)
			gst.once('exit', () => {
				clearTimeout(t)
				resolve()
			})
		})
	}

	/**
	 * Ensures a GStreamer child is *actually gone* before returning: SIGTERM (idempotent,
	 * and the first real signal for paths that never sent one - e.g. stop()'s FIFO arm,
	 * which first gives filesrc a natural EOS), a bounded wait for the exit event, then
	 * SIGKILL, with a short bounded wait for that too.
	 *
	 * kill() only *signals* the process; every path that releases stream ownership (stop(),
	 * a failed FIFO startup, shutdown()'s orphan sweep) must wait for the real exit, or a
	 * still-draining kvssink producer can outlive its owner and keep writing while another
	 * instance acquires the same Kinesis stream - exactly the competing-producer problem
	 * the lock exists to prevent. A child that failed to spawn (missing binary/plugin)
	 * never had a process to signal - its 'error' event has already fired and been
	 * consumed by the start path's own handler - so there is nothing to wait for.
	 */
	private async killAndWait(
		gst: ReturnType<typeof spawn>,
		logContext: Record<string, unknown>,
	): Promise<void> {
		if (gst.pid === undefined) return
		gst.kill('SIGTERM')
		await this.waitForChildExit(gst, CHILD_EXIT_TIMEOUT_MS)
		if (gst.exitCode !== null || gst.signalCode !== null) return
		this.logger.warn(
			'GStreamer child did not exit after SIGTERM; sending SIGKILL',
			logContext,
		)
		gst.kill('SIGKILL')
		await this.waitForChildExit(gst, CHILD_SIGKILL_GRACE_MS)
	}

	/**
	 * Stops the pipeline for a port: flushes reorder buffer (FIFO) or closes the relay
	 * socket (SRTP), then waits for process exit, escalating to SIGKILL (bounded) if the
	 * child does not exit - so callers only release the Kinesis lock / hand the slot over
	 * once the producer is really gone.
	 */
	async stop(port: number): Promise<void> {
		// An intentional stop has no replacement pipeline to replay into - drop any
		// datagrams held for one (see writePacket's hold) instead of replaying stale traffic
		// into whatever starts next. Before the pipeline lookup: the hold exists exactly
		// when no pipeline is active, which is also when the lookup below returns early.
		this.srtpHoldByPort.delete(port)

		// Invalidate any start still in flight for this port (see startGenerationByPort):
		// the lookup below only stops a *registered* pipeline - a start that has spawned
		// but not yet registered (still resolving credentials, or waiting for the FIFO) is
		// invisible to it and would register a fresh producer after this teardown
		// returned. The start re-checks its token after each await and aborts.
		this.startGenerationByPort.set(
			port,
			(this.startGenerationByPort.get(port) ?? 0) + 1,
		)

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
		// The wait above was the EOS/SIGTERM grace period; if it timed out without the
		// child exiting, escalate (SIGTERM again - for the FIFO arm it is the first real
		// signal, since its only prior prompt to exit was the natural EOS - then SIGKILL)
		// and wait for the *actual* exit before returning: callers release the Kinesis
		// lock / hand the slot over next, and a resistant kvssink producer that outlives
		// this owner is exactly the competing-producer problem the lock exists to prevent.
		await this.killAndWait(gst, { port, pid: gst.pid })
		this.logger.info('Kinesis ingestion stopped', { port })
	}

	/**
	 * Stops all active pipelines (e.g. on shutdown).
	 */
	async stopAll(): Promise<void> {
		const ports = Array.from(this.activePipelines.keys())
		await Promise.all(ports.map(async (port) => this.stop(port)))
	}

	/**
	 * Permanently stops this pipeline manager and refuses further starts. index.ts stops
	 * initiating restarts/resumes first (see its `shuttingDown` guards) and drains its
	 * packet queues before calling this, so by the time it runs only starts that were
	 * already in flight can still be pending: those refuse to spawn (see the closed checks
	 * in runStartForFifoPort/runStartForSrtpPort), and any child that slipped past the
	 * checks before `closed` was set - and would otherwise outlive the process, since
	 * process.exit() does not signal children - is swept from spawnedGst so no orphaned
	 * kvssink producer keeps writing to Kinesis with no owner.
	 *
	 * The sweep *waits* for each swept child to actually exit (via killAndWait), with a
	 * bounded timeout and forced termination: kill() only signals the process
	 * (asynchronously), so returning while a child is still draining would let index.ts
	 * release its Kinesis lock and process.exit(0) underneath it - the still-running
	 * producer would keep writing to the stream while another instance acquires it,
	 * exactly the competing-producer problem the lock exists to prevent. The exit/error
	 * handlers registered at spawn remove each child from spawnedGst as soon as it is
	 * gone, so the snapshot below only ever contains genuinely still-running children.
	 */
	async shutdown(): Promise<void> {
		this.closed = true
		await this.stopAll()
		this.srtpHoldByPort.clear()
		const stillRunning = Array.from(this.spawnedGst)
		if (stillRunning.length > 0) {
			this.logger.warn(
				'Waiting for still-running GStreamer children to exit on shutdown',
				{ children: stillRunning.length },
			)
		}
		await Promise.all(
			stillRunning.map(async (gst) => {
				await this.killAndWait(gst, { pid: gst.pid })
			}),
		)
	}
}
