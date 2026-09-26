import assert from 'node:assert/strict'
import { afterEach, describe, it } from 'node:test'

import {
	SRTP_HELPER_PROTOCOL_VERSION,
	type SrtpHelperMessage,
} from './SrtpHelperProtocol.ts'
import { keyFingerprint, type SrtpPortKey } from './SrtpKeyStore.ts'
import type {
	HelperProcess,
	SrtpSupervisorState,
} from './SrtpPortSupervisor.ts'
import { SrtpTransport, type SrtpKeyStoreLike } from './SrtpTransport.ts'

/**
 * The transport's supervisors are real, so they need a helper process shaped
 * like a child: one that answers nothing until the test emits a frame, stays
 * alive until killed, and records what was written to it.
 */
class FakeChild implements HelperProcess {
	exitCode: number | null = null
	signalCode: NodeJS.Signals | null = null
	written: string[] = []
	private exitListeners: (() => void)[] = []
	private readonly dataListeners: ((chunk: string) => void)[] = []
	on(event: 'exit' | 'error', listener: () => void): this {
		if (event === 'exit') this.exitListeners.push(listener)
		return this
	}

	removeListener(event: 'exit' | 'error', listener: () => void): this {
		if (event === 'exit') {
			this.exitListeners = this.exitListeners.filter((l) => l !== listener)
		}
		return this
	}

	kill(signal?: NodeJS.Signals): boolean {
		if (this.exitCode !== null || this.signalCode !== null) return false
		this.signalCode = signal ?? 'SIGTERM'
		for (const listener of [...this.exitListeners]) listener()
		return true
	}

	stdin = {
		write: (line: string): void => {
			this.written.push(line)
		},
		end: (): void => {},
		on: (): void => {},
	}

	stdout = {
		on: (_event: 'data', listener: (chunk: string) => void): void => {
			this.dataListeners.push(listener)
		},
	}

	stderr = {
		on: (): void => {},
	}

	emitFrame(message: SrtpHelperMessage): void {
		for (const listener of [...this.dataListeners]) {
			listener(`${JSON.stringify(message)}\n`)
		}
	}

	exit(code = 0): void {
		if (this.exitCode !== null || this.signalCode !== null) return
		this.exitCode = code
		for (const listener of [...this.exitListeners]) listener()
	}
}

const readyFrame = {
	t: 'ready',
	v: SRTP_HELPER_PROTOCOL_VERSION,
	port: 6000,
} as const

const key: SrtpPortKey = {
	keyHex: 'ab'.repeat(30),
	ssrc: 42,
	cipher: 'aes-128-icm',
	auth: 'hmac-sha1-80',
	keyFingerprint: keyFingerprint('ab'.repeat(30)),
	generation: 1,
}

const keyStore: SrtpKeyStoreLike = {
	loadPorts: async () => {},
	keyedPorts: () => [6000],
	getKeyForPort: () => key,
}

const locks = {
	tryAcquireKinesisLock: async () => true,
	releaseKinesisLock: async () => {},
	updateLastPacketTime: async () => 'ok' as const,
}

const floors = {
	getSrtpIndexFloor: async () => undefined,
	raiseSrtpIndexFloor: async () => {},
}

const waitFor = async (
	condition: () => boolean,
	what: string,
	timeoutMs = 30_000,
): Promise<void> => {
	const deadline = Date.now() + timeoutMs
	while (!condition()) {
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
		await new Promise((resolve) => setTimeout(resolve, 20))
	}
}

const active: SrtpTransport[] = []

afterEach(async () => {
	for (const transport of active.splice(0)) await transport.stop()
})

const makeTransport = (config: {
	credentialProvider: () => Promise<{
		accessKeyId: string
		secretAccessKey: string
		sessionToken?: string
		expiration?: Date
	}>
	spawnProcess: (
		port: number,
		credentials: { accessKeyId: string; sessionToken?: string },
	) => HelperProcess
	onTransition?: (from: SrtpSupervisorState, to: SrtpSupervisorState) => void
}): { transport: SrtpTransport; serving: boolean[]; children: FakeChild[] } => {
	const children: FakeChild[] = []
	const serving: boolean[] = []
	const transport = new SrtpTransport({
		keyParameterPrefix: '/test-stack/srtp/port',
		portRange: { start: 6000, end: 6000 },
		region: 'eu-central-1',
		instanceId: 'i-test',
		streamNameForPort: (port) => `test-stack-video-${String(port)}`,
		locks,
		floors,
		keyStore,
		metrics: {
			setServing: (_transport, isServing) => {
				serving.push(isServing)
			},
			recordReceived: () => {},
		},
		credentialProvider: config.credentialProvider,
		spawnProcess: (port, _init, _key, credentials) =>
			config.spawnProcess(port, credentials),
	})
	active.push(transport)
	return { transport, serving, children }
}

void describe('SrtpTransport', () => {
	void it('reports serving from helper readiness, not from startup having been queued', async () => {
		const { transport, serving, children } = makeTransport({
			credentialProvider: async () => ({
				accessKeyId: 'key-1',
				secretAccessKey: 'secret',
			}),
			spawnProcess: () => {
				const child = new FakeChild()
				children.push(child)
				return child
			},
		})
		assert.strictEqual(await transport.start(), true)
		// Supervisors are running, but no helper has answered ready: a transport
		// whose helpers never come up (a missing plugin, a bind failure) is not
		// serving, and that is the exact failure the zero-ingestion alarm leg
		// exists to see.
		assert.strictEqual(
			serving.at(-1),
			false,
			'the transport must not claim serving before any helper is ready',
		)
		await waitFor(() => children.length === 1, 'the first helper spawn')
		const first = children[0]
		assert.ok(first !== undefined)
		first.emitFrame(readyFrame)
		await waitFor(() => serving.at(-1) === true, 'serving once ready')
		// A port that gives up permanently takes the transport back out: the
		// metric must follow the aggregate, in both directions.
		first.emitFrame({
			t: 'fatal',
			reason: 'missing-element',
			message: 'srtpdec is not installed',
		})
		first.exit(2)
		await waitFor(
			() => serving.at(-1) === false,
			'not serving once every port gave up',
		)
	})

	void it('resolves fresh credentials for every respawn', async () => {
		const seen: string[] = []
		let resolutions = 0
		const { transport, children } = makeTransport({
			// The session token expires within the refresh margin, so every spawn
			// after the first must resolve a fresh one.
			credentialProvider: async () => {
				resolutions++
				return {
					accessKeyId: `key-${String(resolutions)}`,
					secretAccessKey: 'secret',
					sessionToken: `token-${String(resolutions)}`,
					expiration: new Date(Date.now() + 60_000),
				}
			},
			spawnProcess: (_port, credentials) => {
				seen.push(credentials.sessionToken as string)
				const child = new FakeChild()
				children.push(child)
				return child
			},
		})
		assert.strictEqual(await transport.start(), true)
		await waitFor(() => children.length === 1, 'the first helper spawn')
		const first = children[0]
		assert.ok(first !== undefined)
		first.emitFrame(readyFrame)
		await waitFor(() => seen.length >= 1, 'the first spawn')
		const firstSpawnToken = seen[0] as string
		// The helper dies and is respawned: the respawn must not hand kvssink
		// the session token the service started with, which by then is expired.
		// (A snapshot resolved once at startup gives both spawns the same one.)
		first.exit(1)
		await waitFor(() => children.length === 2, 'the respawn')
		const second = children[1] as FakeChild
		second.emitFrame(readyFrame)
		await waitFor(() => seen.length === 2, 'the second spawn')
		assert.notStrictEqual(
			seen[1],
			firstSpawnToken,
			'the respawn must get a fresh session token',
		)
	})
})
