/**
 * Parser for the SRTP pipeline helper's stdout (see srtp_pipeline.py).
 *
 * The helper writes one JSON object per line. This turns a byte stream into those
 * objects and never throws: the helper is a child process whose output can be
 * truncated, interleaved with a partial write, or - if something else ends up on its
 * stdout - not JSON at all, and none of that may take the parent down.
 *
 * Only `auth` with status 'ok' grants anything. Every other message is advisory.
 */

export const SRTP_HELPER_PROTOCOL_VERSION = 2

/**
 * Lines longer than this are discarded - neither buffered indefinitely nor parsed.
 *
 * Both halves matter. A line that never ends is cut off once the buffer passes this,
 * and a line that does end is measured before it reaches JSON.parse, however it was
 * chunked. Measured in characters, which for this protocol are bytes: everything the
 * helper writes to stdout goes through json.dumps, which escapes anything non-ASCII.
 */
export const MAX_LINE_BYTES = 64 * 1024

export type SrtpHelperMessage =
	/** The pipeline is playing and `relayPort` is the port udpsrc actually bound. */
	| { t: 'ready'; v: number; relayPort: number; pid?: number }
	/** A rollover-counter candidate is being tried. */
	| { t: 'searching'; candidate: number; trial: number }
	/**
	 * `ok` means libsrtp authenticated traffic with `roc`, so the value is safe to
	 * trust and to persist. `first` distinguishes the initial confirmation from a
	 * later rollover. `fail`/`exhausted` are progress reports; `lost` means the
	 * authenticated stream stopped authenticating.
	 */
	| {
			t: 'auth'
			status: 'ok'
			first: boolean
			roc: number
			seq?: number
			candidate?: number
			trials?: number
			authenticated?: number
	  }
	| {
			t: 'auth'
			status: 'fail'
			candidate: number
			inputs?: number
			drops?: number
			/** Where the search's far climb has got to; see Search in the helper. */
			searchFrom?: number
	  }
	| { t: 'auth'; status: 'exhausted'; trials: number }
	/**
	 * The highest packet index accepted so far - authenticated, and above the floor
	 * the helper was started with. Safe to persist as the next floor.
	 */
	| { t: 'index'; index: number }
	| { t: 'auth'; status: 'lost'; sinceMs?: number; drops?: number }
	| {
			t: 'stats'
			inputs: number
			authenticated: number
			aus: number
			roc: number | null
			drops: number
	  }
	| { t: 'warning'; element?: string; message: string }
	/**
	 * No GStreamer debug string, deliberately: its text is unconstrained and can
	 * describe caps, and the helper's caps carry the key. Leaving the field out of the
	 * type means nothing downstream can start logging it.
	 */
	| { t: 'error'; element?: string; message: string }
	| { t: 'fatal'; reason: SrtpHelperFatalReason; message: string }
	| { t: 'eos' }
	/** Anything that was not a message: kept so the caller can log it once. */
	| { t: 'unparsed'; raw: string }

export type SrtpHelperFatalReason =
	| 'bind-failed'
	| 'state-change'
	| 'bad-init'
	| 'key-in-argv'
	| 'unsafe-debug-env'
	| 'missing-element'

/**
 * Reasons that will not be fixed by starting the helper again.
 *
 * A retryable failure gets the normal restart path; these would spin, so the caller
 * must back off instead of restarting immediately.
 */
const NON_RETRYABLE: ReadonlySet<string> = new Set([
	'bad-init',
	'key-in-argv',
	'unsafe-debug-env',
	'missing-element',
])

export const isRetryableFatal = (reason: SrtpHelperFatalReason): boolean =>
	!NON_RETRYABLE.has(reason)

const isRecord = (value: unknown): value is Record<string, unknown> =>
	typeof value === 'object' && value !== null && !Array.isArray(value)

const UINT16_MAX = 0xffff
const UINT32_MAX = 0xffffffff
/** A packet index is a 32-bit rollover counter and a 16-bit sequence number. */
export const SRTP_INDEX_MAX = 2 ** 48 - 1

/**
 * An integer within [min, max], or undefined.
 *
 * Being finite is not enough for what these numbers are used as. relayPort goes
 * straight to dgram.connect, which throws synchronously for 0, 1.5 or 70000 - and it
 * is called from the stdout handler, where a throw ends the process. index is persisted
 * and handed back to the next helper as its floor. So a value outside its field's range
 * is treated as if it had not been sent: a required field makes the line unparsed, an
 * optional one is dropped.
 */
const int = (value: unknown, min: number, max: number): number | undefined =>
	typeof value === 'number' &&
	Number.isInteger(value) &&
	value >= min &&
	value <= max
		? value
		: undefined

const port = (value: unknown): number | undefined => int(value, 1, 65535)
const uint16 = (value: unknown): number | undefined => int(value, 0, UINT16_MAX)
const uint32 = (value: unknown): number | undefined => int(value, 0, UINT32_MAX)
const count = (value: unknown): number | undefined =>
	int(value, 0, Number.MAX_SAFE_INTEGER)

const str = (value: unknown): string | undefined =>
	typeof value === 'string' ? value : undefined

const parseMessage = (line: string): SrtpHelperMessage => {
	const unparsed: SrtpHelperMessage = { t: 'unparsed', raw: line.slice(0, 200) }
	let value: unknown
	try {
		value = JSON.parse(line)
	} catch {
		return unparsed
	}
	if (!isRecord(value)) return unparsed

	switch (value.t) {
		case 'ready': {
			const relayPort = port(value.relayPort)
			const v = count(value.v)
			// A version this parser does not understand is not usable: the caller must
			// refuse the child rather than guess at the meaning of later lines. A port
			// that cannot be connected to is not usable either, and is refused here so
			// that the caller never has to find that out by throwing.
			if (relayPort === undefined || v === undefined) return unparsed
			return { t: 'ready', v, relayPort, pid: count(value.pid) }
		}
		case 'searching': {
			const candidate = uint32(value.candidate)
			const trial = count(value.trial)
			if (candidate === undefined || trial === undefined) return unparsed
			return { t: 'searching', candidate, trial }
		}
		case 'auth': {
			switch (value.status) {
				case 'ok': {
					const roc = uint32(value.roc)
					if (roc === undefined) return unparsed
					return {
						t: 'auth',
						status: 'ok',
						first: value.first === true,
						roc,
						seq: uint16(value.seq),
						candidate: uint32(value.candidate),
						trials: count(value.trials),
						authenticated: count(value.authenticated),
					}
				}
				case 'fail': {
					const candidate = uint32(value.candidate)
					if (candidate === undefined) return unparsed
					return {
						t: 'auth',
						status: 'fail',
						candidate,
						inputs: count(value.inputs),
						drops: count(value.drops),
						searchFrom: uint32(value.searchFrom),
					}
				}
				case 'exhausted': {
					const trials = count(value.trials)
					if (trials === undefined) return unparsed
					return { t: 'auth', status: 'exhausted', trials }
				}
				case 'lost':
					return {
						t: 'auth',
						status: 'lost',
						sinceMs: count(value.sinceMs),
						drops: count(value.drops),
					}
				default:
					return unparsed
			}
		}
		case 'index': {
			const index = int(value.index, 0, SRTP_INDEX_MAX)
			if (index === undefined) return unparsed
			return { t: 'index', index }
		}
		case 'stats': {
			const inputs = count(value.inputs)
			const authenticated = count(value.authenticated)
			if (inputs === undefined || authenticated === undefined) return unparsed
			return {
				t: 'stats',
				inputs,
				authenticated,
				aus: count(value.aus) ?? 0,
				roc: uint32(value.roc) ?? null,
				drops: count(value.drops) ?? 0,
			}
		}
		case 'warning': {
			const message = str(value.message)
			if (message === undefined) return unparsed
			return { t: 'warning', element: str(value.element), message }
		}
		case 'error': {
			const message = str(value.message)
			if (message === undefined) return unparsed
			return { t: 'error', element: str(value.element), message }
		}
		case 'fatal': {
			const reason = str(value.reason)
			const message = str(value.message) ?? ''
			if (reason === undefined) return unparsed
			return {
				t: 'fatal',
				reason: reason as SrtpHelperFatalReason,
				message,
			}
		}
		case 'eos':
			return { t: 'eos' }
		default:
			return unparsed
	}
}

const overlongLine = (length: number): SrtpHelperMessage => ({
	t: 'unparsed',
	raw: `discarded an over-long line (${String(length)} bytes)`,
})

/**
 * Accumulates helper stdout and yields whole messages.
 *
 * One instance per child process; `push` may be called with any chunking, including
 * a chunk that splits a line mid-way through a JSON object.
 */
export class SrtpHelperProtocol {
	private buffer = ''
	private overlong = false

	push(chunk: string): SrtpHelperMessage[] {
		const messages: SrtpHelperMessage[] = []
		this.buffer += chunk

		for (;;) {
			const newline = this.buffer.indexOf('\n')
			if (newline === -1) break
			const raw = this.buffer.slice(0, newline)
			this.buffer = this.buffer.slice(newline + 1)
			if (this.overlong) {
				// The tail of a line already reported as too long.
				this.overlong = false
				continue
			}
			// Measured here, before parsing, and not only in the check below: that one
			// sees only what is left once every complete line has been taken out, so a
			// long line arriving with its newline in the same chunk passed it and went
			// to JSON.parse whole - up to about twice the limit in 64 KiB pipe reads,
			// and without bound in a single larger chunk.
			if (raw.length > MAX_LINE_BYTES) {
				messages.push(overlongLine(raw.length))
				continue
			}
			const line = raw.trim()
			if (line.length > 0) messages.push(parseMessage(line))
		}

		// A line that never ends must not grow the buffer without bound.
		if (this.buffer.length > MAX_LINE_BYTES) {
			messages.push(overlongLine(this.buffer.length))
			this.buffer = ''
			this.overlong = true
		}
		return messages
	}
}
