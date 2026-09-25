import dgram from 'node:dgram'
import { EventEmitter } from 'node:events'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { srtpPacket } from '../src/testing/srtpSender.ts'

/**
 * An SRTP H.264 sender for the e2e suite, in Node, with first-class control over
 * the rollover counter and the starting sequence number.
 *
 * A real encoder always starts a session at ROC 0 and a semi-random sequence
 * number, which makes the behaviors this system exists to get right - a stream
 * above its replay floor, a wrap, a rewind under the same key - untestable end
 * to end. This sender can start anywhere in the 48-bit packet-index space, and
 * it carries real H.264 (a committed x264 fixture with a one-second GOP, so a
 * recovery gap is measured in seconds, not minutes): what it sends is decodable
 * video, so "the stream reached Kinesis" can mean actual fragments came back,
 * not just that a byte counter moved.
 *
 * The cryptography is the same srtpPacket() the unit tests use, and it cannot
 * make a test pass that should fail: if any of it is wrong, srtpdec does not
 * authenticate and every assertion fails loudly.
 */

const SEQ_MODULO = 2 ** 16
const ROC_MODULO = 2 ** 32
const FU_MAX = 1200
const CLOCK_RATE = 90_000

export type AnnexB = {
	/** NAL units in file order, with their type nibble. */
	nals: { type: number; data: Buffer }[]
	/** Byte offsets where a new access unit starts (index into nals). */
	accessUnitStarts: number[]
}

/**
 * Parses an Annex B byte stream into NAL units and access-unit boundaries.
 *
 * An access unit starts at an SPS or an IDR slice: x264 emits SPS, PPS, IDR for
 * every keyframe, so this is stable for the fixture, and h264parse re-derives the
 * same boundaries downstream anyway. A wrong boundary would cost a marker bit in
 * the wrong place, which the depayloader tolerates - the timestamp is what
 * matters, and it is assigned per access unit here.
 */
export const parseAnnexB = (bytes: Buffer): AnnexB => {
	const nals: { type: number; data: Buffer }[] = []
	// Find start codes (00 00 01) and note where each NAL begins. The 00 00 00 01
	// form is handled by the trimming below, not by matching four bytes here.
	const starts: number[] = []
	let i = 0
	while (i < bytes.length - 3) {
		if (bytes[i] === 0 && bytes[i + 1] === 0 && bytes[i + 2] === 1) {
			starts.push(i + 3)
			i += 3
			continue
		}
		i++
	}
	for (const from of starts) {
		// The NAL runs to the next start code, skipping 00 00 00 01 and 00 00 01.
		let to = bytes.length
		for (let k = from + 1; k < bytes.length - 2; k++) {
			if (
				bytes[k] === 0 &&
				bytes[k + 1] === 0 &&
				(bytes[k + 2] === 1 || (bytes[k + 2] === 0 && bytes[k + 3] === 1))
			) {
				// Trim the 00 padding before the start code.
				to = k
				while (to > from && bytes[to - 1] === 0) to--
				break
			}
		}
		const data = bytes.subarray(from, to)
		if (data.length === 0) continue
		nals.push({ type: (data[0] as number) & 0x1f, data })
	}
	const accessUnitStarts: number[] = []
	for (let j = 0; j < nals.length; j++) {
		const type = nals[j]?.type
		// 7 = SPS, 5 = IDR: a keyframe access unit starts here. P-slice NALs
		// after it belong to it until the next SPS.
		if (type === 7 || type === 5) accessUnitStarts.push(j)
	}
	if (accessUnitStarts.length === 0 || accessUnitStarts[0] !== 0) {
		accessUnitStarts.unshift(0)
	}
	return { nals, accessUnitStarts }
}

const fixturePath = (): string =>
	fileURLToPath(new URL('./fixtures/testsrc.h264', import.meta.url))

/** One RTP packet's payload: either the whole NAL, or one FU-A chunk of it. */
const rtpPayloadsForNal = (
	nal: Buffer,
): { payload: Buffer; lastOfNal: boolean }[] => {
	if (nal.length <= FU_MAX) {
		return [{ payload: nal, lastOfNal: true }]
	}
	// FU-A (RFC 6184 section 5.8): the NAL header's type is replaced by 28, and
	// each chunk carries a FU header with the start/end bits and the original type.
	const chunks: { payload: Buffer; lastOfNal: boolean }[] = []
	const indicator = ((nal[0] as number) & 0xe0) | 28
	const nriType = (nal[0] as number) & 0x1f
	const body = nal.subarray(1)
	for (let offset = 0; offset < body.length; offset += FU_MAX) {
		const chunk = body.subarray(offset, offset + FU_MAX)
		const fuHeader =
			(offset === 0 ? 0x80 : 0) |
			(offset + chunk.length >= body.length ? 0x40 : 0) |
			nriType
		chunks.push({
			payload: Buffer.concat([Buffer.from([indicator, fuHeader]), chunk]),
			lastOfNal: offset + chunk.length >= body.length,
		})
	}
	return chunks
}

export type E2eSenderOptions = {
	host: string
	port: number
	keyHex: string
	ssrc: number
	/** The rollover counter to start at; the real thing a camera cannot do. */
	roc?: number
	/** The RTP sequence number to start at. */
	seq?: number
	/** RTP timestamp to start at, in 90 kHz units. */
	timestamp?: number
	fps?: number
	/** How many seconds to run before stopping; default until stop() is called. */
	durationS?: number
	/** Bind the sending socket to a fixed local port, keeping the NLB flow stable. */
	localPort?: number
	/**
	 * Corrupts every authentication tag. What is sent is exactly what an attacker
	 * who has read an RTP header can produce: version 2, the right SSRC, a
	 * plausible payload - everything but the key. Cannot authenticate anywhere.
	 */
	forged?: boolean
	/** Log every access unit, for the runner's output. */
	onAccessUnit?: (frame: number, seq: number, roc: number) => void
}

export class E2eSender extends EventEmitter {
	readonly host: string
	readonly port: number
	readonly keyHex: string
	readonly ssrc: number
	private roc: number
	private seq: number
	private timestamp: number
	private readonly fps: number
	private readonly durationS: number | undefined
	private readonly forged: boolean
	private readonly fixture: AnnexB
	private socket: dgram.Socket | undefined
	private stopped = false
	private readonly onAccessUnit?: (
		frame: number,
		seq: number,
		roc: number,
	) => void

	constructor(options: E2eSenderOptions) {
		super()
		this.host = options.host
		this.port = options.port
		this.keyHex = options.keyHex
		this.ssrc = options.ssrc
		this.roc = options.roc ?? 0
		this.seq = options.seq ?? Math.floor(Math.random() * 65536)
		this.timestamp = options.timestamp ?? 0
		this.fps = options.fps ?? 15
		this.durationS = options.durationS
		this.forged = options.forged ?? false
		this.onAccessUnit = options.onAccessUnit
		this.fixture = parseAnnexB(readFileSync(fixturePath()))
		if (this.fixture.accessUnitStarts.length < 10) {
			throw new Error('the H.264 fixture has too few access units to stream')
		}
	}

	/** Streams the fixture in a loop, in real time, until stopped or duration ends. */
	async run(): Promise<void> {
		this.socket = dgram.createSocket('udp4')
		// Connect once, before any packet: resolving the host is the only async
		// step ahead of streaming, and it is deliberately done here where a
		// failure rejects loudly rather than disappearing into a send callback.
		// A connected socket also means each send carries no destination - no
		// per-packet DNS lookup on the send path at all.
		await new Promise<void>((resolve, reject) => {
			const socket = this.socket
			if (socket === undefined) {
				resolve()
				return
			}
			// DNS failure surfaces as an 'error' event, not a connect callback
			// argument - so it is handled here, before it could crash the
			// process as an unhandled event.
			const onError = (err: Error): void => {
				socket.removeListener('error', onError)
				reject(err)
			}
			socket.once('error', onError)
			socket.connect(this.port, this.host, () => {
				socket.removeListener('error', onError)
				resolve()
			})
		})
		if (this.stopped) {
			this.socket?.close()
			this.socket = undefined
			return
		}
		const frameIntervalMs = 1000 / this.fps
		const { nals, accessUnitStarts } = this.fixture
		const started = Date.now()
		let frame = 0
		for (;;) {
			if (this.stopped) break
			if (
				this.durationS !== undefined &&
				Date.now() - started >= this.durationS * 1000
			) {
				break
			}
			// The access unit at `frame`, looping the fixture; sequence numbers and
			// timestamps never rewind, whichever loop it is.
			const auIndex = frame % accessUnitStarts.length
			const from = accessUnitStarts[auIndex] as number
			const to =
				auIndex + 1 < accessUnitStarts.length
					? (accessUnitStarts[auIndex + 1] as number)
					: nals.length
			const timestamp = this.timestamp
			for (let n = from; n < to; n++) {
				if (this.stopped) break
				const nal = nals[n]?.data
				if (nal === undefined) continue
				const pieces = rtpPayloadsForNal(nal)
				for (let p = 0; p < pieces.length; p++) {
					if (this.stopped) break
					const piece = pieces[p]
					if (piece === undefined) continue
					const isLastPacket = n === to - 1 && p === pieces.length - 1
					const packet = srtpPacket({
						keyHex: this.keyHex,
						ssrc: this.ssrc,
						seq: this.seq,
						roc: this.roc,
						payload: piece.payload,
						timestamp,
						marker: isLastPacket,
					})
					if (this.forged) {
						// Overwrite the authentication tag with noise: the header,
						// the payload and the shape stay plausible, only the proof of
						// the key is gone.
						;(await import('node:crypto')).randomFillSync(
							packet.subarray(packet.length - 10),
						)
					}
					await this.send(packet)
					this.seq = (this.seq + 1) % SEQ_MODULO
					if (this.seq === 0) this.roc = (this.roc + 1) % ROC_MODULO
				}
			}
			this.onAccessUnit?.(frame, this.seq, this.roc)
			this.emit('frame', { frame, seq: this.seq, roc: this.roc })
			this.timestamp += CLOCK_RATE / this.fps
			frame++
			await sleep(frameIntervalMs)
		}
		// The socket is closed only here, at a point where no send is in flight.
		// run() owns the socket's lifecycle and stop() only signals: closing
		// from stop() raced with an in-flight send, whose callback could then
		// never fire - stranding every await upstream of it, and when the event
		// loop drained, Node exited 0 in the middle of the suite, silently, with
		// cases still unrun.
		this.socket?.close()
		this.socket = undefined
	}

	private async send(packet: Buffer): Promise<void> {
		return new Promise((resolve) => {
			const socket = this.socket
			if (socket === undefined) {
				resolve()
				return
			}
			// The socket is connected, so no destination: no per-packet DNS
			// lookup, and the send cannot outlive the lifecycle run() owns.
			socket.send(packet, () => resolve())
		})
	}

	/**
	 * Signals the stream to stop. Only signals: the socket is closed by run()
	 * at a point where no send is in flight (see there for the race this
	 * avoids). run() then resolves within a frame interval.
	 */
	stop(): void {
		this.stopped = true
	}

	get currentState(): { roc: number; seq: number; timestamp: number } {
		return { roc: this.roc, seq: this.seq, timestamp: this.timestamp }
	}
}

const sleep = async (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)))
