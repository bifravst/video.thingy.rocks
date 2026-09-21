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

const num = (value: unknown): number | undefined =>
	typeof value === 'number' && Number.isFinite(value) ? value : undefined

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
			const relayPort = num(value.relayPort)
			const v = num(value.v)
			// A version this parser does not understand is not usable: the caller must
			// refuse the child rather than guess at the meaning of later lines.
			if (relayPort === undefined || v === undefined) return unparsed
			return { t: 'ready', v, relayPort, pid: num(value.pid) }
		}
		case 'searching': {
			const candidate = num(value.candidate)
			const trial = num(value.trial)
			if (candidate === undefined || trial === undefined) return unparsed
			return { t: 'searching', candidate, trial }
		}
		case 'auth': {
			switch (value.status) {
				case 'ok': {
					const roc = num(value.roc)
					if (roc === undefined) return unparsed
					return {
						t: 'auth',
						status: 'ok',
						first: value.first === true,
						roc,
						seq: num(value.seq),
						candidate: num(value.candidate),
						trials: num(value.trials),
						authenticated: num(value.authenticated),
					}
				}
				case 'fail': {
					const candidate = num(value.candidate)
					if (candidate === undefined) return unparsed
					return {
						t: 'auth',
						status: 'fail',
						candidate,
						inputs: num(value.inputs),
						drops: num(value.drops),
					}
				}
				case 'exhausted': {
					const trials = num(value.trials)
					if (trials === undefined) return unparsed
					return { t: 'auth', status: 'exhausted', trials }
				}
				case 'lost':
					return {
						t: 'auth',
						status: 'lost',
						sinceMs: num(value.sinceMs),
						drops: num(value.drops),
					}
				default:
					return unparsed
			}
		}
		case 'stats': {
			const inputs = num(value.inputs)
			const authenticated = num(value.authenticated)
			if (inputs === undefined || authenticated === undefined) return unparsed
			return {
				t: 'stats',
				inputs,
				authenticated,
				aus: num(value.aus) ?? 0,
				roc: num(value.roc) ?? null,
				drops: num(value.drops) ?? 0,
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
