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

export const SRTP_HELPER_PROTOCOL_VERSION = 1

/** Lines longer than this are discarded rather than buffered indefinitely. */
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
	  }
	| { t: 'auth'; status: 'exhausted'; trials: number }
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
	| { t: 'error'; element?: string; message: string; debug?: string }
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

/**
 * An integer within [min, max], or undefined.
 *
 * Being finite is not enough for what these numbers are used as. relayPort goes
 * straight to dgram.connect, which throws synchronously for 0, 1.5 or 70000 - and it
 * is called from the stdout handler, where a throw ends the process. roc is persisted
 * and handed back to the next helper as its hint. So a value outside its field's range
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
			return {
				t: 'error',
				element: str(value.element),
				message,
				debug: str(value.debug),
			}
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
			const line = this.buffer.slice(0, newline).trim()
			this.buffer = this.buffer.slice(newline + 1)
			if (this.overlong) {
				// The tail of a line already reported as too long.
				this.overlong = false
				continue
			}
			if (line.length > 0) messages.push(parseMessage(line))
		}

		// A line that never ends must not grow the buffer without bound.
		if (this.buffer.length > MAX_LINE_BYTES) {
			messages.push({
				t: 'unparsed',
				raw: `discarded an over-long line (${String(this.buffer.length)} bytes)`,
			})
			this.buffer = ''
			this.overlong = true
		}
		return messages
	}
}
