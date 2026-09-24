import assert from 'node:assert/strict'
import { spawn as nodeSpawn } from 'node:child_process'
import { describe, it } from 'node:test'

import { Logger } from './Logger.ts'
import type { SrtpKeyStore } from './SrtpKeyStore.ts'
import { SrtpProducer } from './SrtpProducer.ts'
import { HELPER, hasGstElements } from './testing/srtpHelper.ts'
import { h264Payload, srtpPacket } from './testing/srtpSender.ts'

const KEY = '000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d'
const SSRC = 42
const PORT = 6000

const skip = hasGstElements(['srtpdec'])
	? false
	: 'requires python3 GStreamer bindings with the srtp plugin'

class QuietLogger extends Logger {
	constructor() {
		super('SrtpSearchAcrossSessionsSpec')
	}
	override info(): void {}
	override warn(): void {}
	override error(): void {}
}

const delay = async (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms))

/**
 * The rollover-counter search, as the service runs it: one short session at a time.
 *
 * PortIngestion ends a session that has not authenticated anything when its
 * provisional window closes, and starts a new one after a cooldown. Each new helper
 * used to begin the search from scratch, so a session only ever reached as far as one
 * window allowed, and every session after it tried the same candidates again: a
 * counter further above the floor than that was never found, however long the port
 * kept trying. The search is the real helper against real libsrtp, driven through the
 * real producer; only the sessions are shortened, from twenty seconds to a second and
 * a half, which is also what puts the counter out of one session's reach - the helper
 * steps at most once per 50 ms supervisor tick, so thirty candidates at most.
 */
void describe('the rollover-counter search across sessions', { skip }, () => {
	void it(
		'reaches a counter further above the floor than one session can search',
		{ timeout: 60_000 },
		async () => {
			const SESSION_MS = 1_500
			const ROC = 40
			const confirmedIn: number[] = []
			let session = 0

			const producer = new SrtpProducer({
				keyStore: {
					getKeyForPort: () => ({
						keyHex: KEY,
						ssrc: SSRC,
						cipher: 'aes-128-icm',
						auth: 'hmac-sha1-80',
						keyFingerprint: 'abcdef0123456789',
					}),
				} as unknown as SrtpKeyStore,
				floors: {
					getSrtpIndexFloor: async () => undefined,
					raiseSrtpIndexFloor: async () => undefined,
				},
				region: 'eu-central-1',
				streamNameForPort: (port) => `test-video-${String(port)}`,
				kvsLogConfigPath: '/dev/null',
				helperPath: HELPER,
				onAuthenticated: () => confirmedIn.push(session),
				spawn: ((command: string, args: string[], options: object) =>
					nodeSpawn(
						command,
						[...args, '--fake-sink'],
						options,
					)) as typeof nodeSpawn,
				logger: new QuietLogger(),
			})

			let seq = 0
			try {
				for (session = 1; session <= 8 && confirmedIn.length === 0; session++) {
					await producer.start(PORT, [], { epoch: session })
					const deadline = Date.now() + SESSION_MS
					while (Date.now() < deadline && confirmedIn.length === 0) {
						for (let i = 0; i < 10; i++) {
							producer.writePacket(
								PORT,
								srtpPacket({
									keyHex: KEY,
									ssrc: SSRC,
									seq: seq & 0xffff,
									roc: ROC,
									payload: h264Payload(),
								}),
							)
							seq += 1
						}
						await delay(50)
					}
					await producer.stop(PORT)
				}
			} finally {
				await producer.shutdown()
			}

			assert.ok(
				confirmedIn.length > 0,
				'eight sessions never reached the counter: each one searched the same candidates',
			)
			assert.ok(
				(confirmedIn[0] ?? 0) > 1,
				'found in the first session, so the counter was not out of one session’s reach',
			)
		},
	)
})
