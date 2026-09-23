import {
	spawn,
	spawnSync,
	type ChildProcessWithoutNullStreams,
} from 'node:child_process'
import dgram from 'node:dgram'

import {
	SrtpHelperProtocol,
	type SrtpHelperMessage,
} from '../SrtpHelperProtocol.ts'

/**
 * Drives the real receiver, backend/src/srtp_pipeline.py, for tests.
 *
 * Shared because two suites need the same receiver: its own, and the sender's, which
 * has to prove its stream authenticates against real libsrtp rather than against a
 * model of it. Paths are relative to the repository root, where the tests run.
 */

export const HELPER = 'backend/src/srtp_pipeline.py'

/**
 * Whether this machine has GStreamer Python bindings with every named element.
 *
 * Suites skip rather than fail where these are missing, so they can run in the same
 * command as the pure unit tests.
 */
export const hasGstElements = (elements: string[]): boolean =>
	spawnSync(
		'python3',
		[
			'-c',
			"import gi, sys; gi.require_version('Gst','1.0')\n" +
				'from gi.repository import Gst\n' +
				'Gst.init(None)\n' +
				'raise SystemExit(0 if all(Gst.ElementFactory.find(e) for e in sys.argv[1:]) else 1)',
			...elements,
		],
		{ timeout: 30_000 },
	).status === 0

/**
 * Fails if the helper wrote its key anywhere the parent reads.
 *
 * The helper's rule is that the key reaches no emitted line, and its stderr is logged
 * by the parent verbatim - so either stream carrying it would put the key in the
 * application log. Checked on every stop, which makes each test that drives the helper
 * through a failure path a leak test for that path too. Both cases, since caps print
 * buffers in lower case and nothing guarantees every writer does.
 */
const assertKeyNotWritten = (
	key: string,
	stdout: string,
	stderr: string,
): void => {
	for (const [stream, text] of [
		['stdout', stdout],
		['stderr', stderr],
	] as const) {
		const lower = text.toLowerCase()
		if (lower.includes(key.toLowerCase())) {
			throw new Error(`the SRTP helper wrote its key to ${stream}`)
		}
	}
}

export type Helper = {
	child: ChildProcessWithoutNullStreams
	messages: SrtpHelperMessage[]
	relayPort: number
	send: (packet: Buffer) => void
	waitFor: (
		predicate: (m: SrtpHelperMessage) => boolean,
		timeoutMs?: number,
	) => Promise<SrtpHelperMessage>
	stop: () => Promise<number | null>
	stderr: () => string
}

export const startHelper = async (options: {
	key: string
	ssrc: number
	rocHint?: number
	extraArgs?: string[]
}): Promise<Helper> => {
	const child = spawn(
		'python3',
		[
			HELPER,
			'--ssrc',
			String(options.ssrc),
			'--fake-sink',
			'--stats-interval-ms',
			'250',
			// Out of the way, so a test that pauses between bursts is not ended by it.
			// The window is a security bound, not just a timeout - it limits how long
			// unauthenticated traffic keeps a running port's lease fresh - so the test
			// for it overrides this through extraArgs rather than leaving it untested.
			'--auth-loss-ms',
			'100000',
			'--trial-drops',
			'4',
			'--trial-timeout-ms',
			'400',
			...(options.extraArgs ?? []),
		],
		{ stdio: ['pipe', 'pipe', 'pipe'] },
	)

	const protocol = new SrtpHelperProtocol()
	const messages: SrtpHelperMessage[] = []
	let stdout = ''
	child.stdout.setEncoding('utf8')
	child.stdout.on('data', (chunk: string) => {
		stdout += chunk
		messages.push(...protocol.push(chunk))
	})
	let stderr = ''
	child.stderr.setEncoding('utf8')
	child.stderr.on('data', (chunk: string) => {
		stderr += chunk
	})

	const waitFor = async (
		predicate: (m: SrtpHelperMessage) => boolean,
		timeoutMs = 10_000,
	): Promise<SrtpHelperMessage> => {
		const deadline = Date.now() + timeoutMs
		for (;;) {
			const found = messages.find(predicate)
			if (found !== undefined) return found
			if (Date.now() > deadline) {
				throw new Error(
					`timed out; messages so far: ${JSON.stringify(messages)}\nstderr: ${stderr}`,
				)
			}
			await new Promise((resolve) => setTimeout(resolve, 20))
		}
	}

	child.stdin.write(
		`${JSON.stringify({
			type: 'init',
			v: 1,
			key: options.key,
			ssrc: options.ssrc,
			cipher: 'aes-128-icm',
			auth: 'hmac-sha1-80',
			...(options.rocHint === undefined ? {} : { rocHint: options.rocHint }),
		})}\n`,
	)

	const ready = await waitFor((m) => m.t === 'ready')
	const relayPort = ready.t === 'ready' ? ready.relayPort : 0
	const socket = dgram.createSocket('udp4')

	return {
		child,
		messages,
		relayPort,
		send: (packet) => socket.send(packet, relayPort, '127.0.0.1'),
		waitFor,
		stop: async () => {
			socket.close()
			// Already gone: 'exit' has fired and will not fire again, so waiting for it
			// would just burn the timeout.
			const code =
				child.exitCode !== null || child.signalCode !== null
					? child.exitCode
					: await new Promise<number | null>((resolve) => {
							child.kill('SIGTERM')
							const timer = setTimeout(() => {
								child.kill('SIGKILL')
								resolve(child.exitCode)
							}, 8000)
							child.once('exit', (exitCode) => {
								clearTimeout(timer)
								resolve(exitCode)
							})
						})
			assertKeyNotWritten(options.key, stdout, stderr)
			return code
		},
		stderr: () => stderr,
	}
}
