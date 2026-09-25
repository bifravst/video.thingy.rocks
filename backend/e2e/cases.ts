import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { E2eSender } from './sender.ts'
import type { E2eConfig } from './stack.ts'
import {
	assertNoStreamIngestion,
	lockRow,
	provisionKey,
	readMedia,
	restartBackend,
	waitForLogLines,
	waitForStreamIngestion,
} from './stack.ts'

/**
 * The e2e cases.
 *
 * Every case gets its own SRTP port and its own freshly provisioned key, so
 * nothing one case does can disturb another's floor, and every case asserts on
 * the deployed system's own observables: the KVS metric, the application log,
 * the lock table, and - where it matters most - media read back out of Kinesis.
 *
 * Ports: 6000 happy path, 6001 wrong key, 6002 forged flood, 6003 wrap,
 * 6004 hint climb, 6005 rewind, 6006 restart recovery, 6007 fresh key,
 * 6008 isolation noise (plus unencrypted 5000).
 */

export type CaseContext = {
	config: E2eConfig
	instances: string[]
	/** The CDK code bucket, for the in-case backend deploy/restarts. */
	codeBucket: string
}

export type E2eCase = {
	name: string
	port: number
	ssrc: number
	/**
	 * Provisions the port's initial key. Called once, before the fleet is
	 * restarted, so the service loads the keys at its start the way production
	 * does. Cases that reprovision mid-run do so themselves and restart again.
	 */
	provision: (region: string, stackName: string) => Promise<{ keyHex: string }>
	run: (ctx: CaseContext, key: { keyHex: string }) => Promise<void>
}

const streamNameFor = (config: E2eConfig, port: number): string =>
	`${config.streamPrefix}-${String(port)}`

/**
 * One log line per phase of a case, with the runner's format. The cases wait
 * minutes at a time on CloudWatch metric propagation and log indexing with
 * nothing happening in the terminal; without these lines a working run is
 * indistinguishable from a hung one (which it was, twice, on 2026-09-25).
 */
const step = (message: string): void => {
	console.log(`[e2e ${new Date().toISOString()}]   ${message}`)
}

/** Runs one sender and stops it after `seconds`, resolving the run promise. */
const streamFor = async (sender: E2eSender, seconds: number): Promise<void> => {
	const finished = sender.run()
	await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
	sender.stop()
	await finished
}

const makeSender = (
	config: E2eConfig,
	port: number,
	ssrc: number,
	keyHex: string,
	options: {
		roc: number
		seq?: number
		timestamp?: number
		durationS?: number
	},
): E2eSender =>
	new E2eSender({
		host: config.nlbHost,
		port,
		keyHex,
		ssrc,
		...options,
		fps: 15,
	})

export const cases: E2eCase[] = [
	{
		name: 'happy path: real media reaches Kinesis and can be read back',
		port: 6000,
		ssrc: 1010,
		provision: async (
			region: string,
			stackName: string,
		): Promise<{ keyHex: string }> => {
			const keyHex = await provisionKey(region, stackName, 6000, { ssrc: 1010 })
			return { keyHex }
		},
		run: async (ctx: CaseContext, key: { keyHex: string }): Promise<void> => {
			const since = new Date()
			const sender = makeSender(ctx.config, 6000, 1010, key.keyHex, {
				roc: 0,
				durationS: 45,
			})
			await streamFor(sender, 45)
			step('streamed for 45s; now waiting on logs, metrics and media')

			// Authenticated, and the port took the lock while it produced.
			const lines = await waitForLogLines(
				ctx.config.region,
				ctx.config.logGroup,
				`SRTP traffic authenticated.*"port":6000`,
				since,
			)
			assert.ok(lines.length > 0, 'the authentication must be in the log')
			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6000),
				since,
			)

			// The strongest assertion there is: the media itself comes back.
			const mediaBytes = await readMedia(
				ctx.config.region,
				streamNameFor(ctx.config, 6000),
			)
			assert.ok(
				mediaBytes > 0,
				'GetMedia must return fragments for the ingested stream',
			)

			// The lock went back once the sender stopped (auth loss releases it).
			step('waiting 20s for the lock to be released after the sender stopped')
			await new Promise((resolve) => setTimeout(resolve, 20_000))
			const row = await lockRow(ctx.config.region, ctx.config.tableName, 6000)
			assert.equal(
				row?.kinesisOwnerInstanceId,
				undefined,
				'the lock must be released after the sender stops',
			)
		},
	},
	{
		name: 'traffic without the key earns nothing',
		port: 6001,
		ssrc: 1011,
		provision: async (
			region: string,
			stackName: string,
		): Promise<{ keyHex: string }> => {
			const keyHex = await provisionKey(region, stackName, 6001, { ssrc: 1011 })
			return { keyHex }
		},
		run: async (ctx: CaseContext, key: { keyHex: string }): Promise<void> => {
			const since = new Date()
			// The sender uses a different key: syntactically the same SRTP, but
			// every tag is wrong for the port.
			const wrongKey = key.keyHex.slice(0, 58) + 'ff'
			const sender = makeSender(ctx.config, 6001, 1011, wrongKey, {
				roc: 0,
				durationS: 20,
			})
			await streamFor(sender, 20)
			step('streamed for 20s')

			// Metrics need a minute to be sure; the lock is the immediate signal.
			await new Promise((resolve) => setTimeout(resolve, 30_000))
			const row = await lockRow(ctx.config.region, ctx.config.tableName, 6001)
			assert.equal(
				row,
				undefined,
				'unauthenticated traffic must never acquire the lock',
			)
			await assertNoStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6001),
				since,
			)
		},
	},
	{
		name: 'a forged flood with the public SSRC earns nothing and stays quiet',
		port: 6002,
		ssrc: 1012,
		provision: async (
			region: string,
			stackName: string,
		): Promise<{ keyHex: string }> => {
			const keyHex = await provisionKey(region, stackName, 6002, { ssrc: 1012 })
			return { keyHex }
		},
		run: async (ctx: CaseContext, key: { keyHex: string }): Promise<void> => {
			const since = new Date()
			// Everything an attacker who has read the RTP header can produce:
			// version 2, the right SSRC, a real payload shape - tags are noise.
			const sender = new E2eSender({
				host: ctx.config.nlbHost,
				port: 6002,
				keyHex: key.keyHex,
				ssrc: 1012,
				roc: 0,
				fps: 15,
				forged: true,
				durationS: 20,
			})
			await streamFor(sender, 20)
			step('streamed for 20s')

			await new Promise((resolve) => setTimeout(resolve, 30_000))
			const row = await lockRow(ctx.config.region, ctx.config.tableName, 6002)
			assert.equal(row, undefined, 'forged traffic must never acquire the lock')
			await assertNoStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6002),
				since,
			)
			// And the log never claimed an authentication for it.
			const lines = await waitForLogLines(
				ctx.config.region,
				ctx.config.logGroup,
				`"port":6002`,
				since,
				10_000,
			).catch(() => [] as string[])
			for (const line of lines) {
				assert.ok(
					!line.includes('SRTP traffic authenticated'),
					'forged traffic must never be reported authenticated',
				)
			}
		},
	},
	{
		name: 'a stream that wraps keeps ingesting and reports the rollover',
		port: 6003,
		ssrc: 1013,
		provision: async (
			region: string,
			stackName: string,
		): Promise<{ keyHex: string }> => {
			const keyHex = await provisionKey(region, stackName, 6003, { ssrc: 1013 })
			return { keyHex }
		},
		run: async (ctx: CaseContext, key: { keyHex: string }): Promise<void> => {
			const since = new Date()
			// Two rollovers up, crossing the third within the first second: only a
			// synthetic sender can sit here, and it is exactly the state a receiver
			// restart used to lose.
			const sender = makeSender(ctx.config, 6003, 1013, key.keyHex, {
				roc: 2,
				seq: 65400,
				durationS: 45,
			})
			await streamFor(sender, 45)
			step('streamed for 45s; now waiting on logs, metrics and media')

			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6003),
				since,
			)
			const rollovers = await waitForLogLines(
				ctx.config.region,
				ctx.config.logGroup,
				`SRTP rollover.*"port":6003.*"roc":3`,
				since,
			)
			assert.ok(
				rollovers.length > 0,
				'the wrap must be reported as a rollover to 3',
			)
		},
	},
	{
		name: 'a fresh key below the old floor is found (the too-high hint)',
		port: 6004,
		ssrc: 1014,
		provision: async (
			region: string,
			stackName: string,
		): Promise<{ keyHex: string }> => {
			const keyHex = await provisionKey(region, stackName, 6004, { ssrc: 1014 })
			return { keyHex }
		},
		run: async (ctx: CaseContext, key: { keyHex: string }): Promise<void> => {
			// First session: five rollovers up. The floor ends up at ROC 5.
			let since = new Date()
			let sender = makeSender(ctx.config, 6004, 1014, key.keyHex, {
				roc: 5,
				durationS: 20,
			})
			await streamFor(sender, 20)
			step('streamed for 20s')
			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6004),
				since,
			)

			// A fresh key starts a new floor from zero. The sender restarts at
			// ROC 1 - BELOW where the old floor was. An ascending-only search
			// would never find this; the near climb has to come back down.
			const freshKey = await provisionKey(
				ctx.config.region,
				ctx.config.stackName,
				6004,
				{ ssrc: 1014 },
			)
			await restartBackend(ctx.config.region, ctx.instances, ctx.codeBucket)
			since = new Date()
			sender = makeSender(ctx.config, 6004, 1014, freshKey, {
				roc: 1,
				durationS: 45,
			})
			await streamFor(sender, 45)
			step('streamed for 45s; now waiting on logs, metrics and media')
			const lines = await waitForLogLines(
				ctx.config.region,
				ctx.config.logGroup,
				`SRTP traffic authenticated.*"port":6004`,
				since,
			)
			assert.ok(lines.length > 0)
			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6004),
				since,
			)
		},
	},
	{
		name: 'a rewind under the same key is refused',
		port: 6005,
		ssrc: 1015,
		provision: async (
			region: string,
			stackName: string,
		): Promise<{ keyHex: string }> => {
			const keyHex = await provisionKey(region, stackName, 6005, { ssrc: 1015 })
			return { keyHex }
		},
		run: async (ctx: CaseContext, key: { keyHex: string }): Promise<void> => {
			let since = new Date()
			// A session two rollovers up, so the floor rises there.
			let sender = makeSender(ctx.config, 6005, 1015, key.keyHex, {
				roc: 2,
				durationS: 20,
			})
			await streamFor(sender, 20)
			step('streamed for 20s')
			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6005),
				since,
			)
			// Rewind: the same key, the index back at zero. This is either a
			// recording or a keystream reuse, and both are refused.
			since = new Date()
			sender = makeSender(ctx.config, 6005, 1015, key.keyHex, {
				roc: 0,
				seq: 100,
				durationS: 30,
			})
			await streamFor(sender, 30)
			step('streamed for 30s')

			step('waiting 60s for the rewind window to pass before asserting refusal')
			await new Promise((resolve) => setTimeout(resolve, 60_000))
			// This is the proof of refusal: no media on the stream in the full
			// minutes after the rewind began. A ROC-reset rewind cannot
			// authenticate at all - the auth tag covers the rollover counter,
			// and no candidate below the floor's rollover is ever offered - so
			// the stale-drop warning is not this case's signature (that warning
			// belongs to a replay that DOES authenticate under an offered
			// candidate; see the replay-floor integration tests). Counting on
			// it here made the case fail while the receiver was refusing
			// correctly.
			await assertNoStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6005),
				since,
			)
			step('refused: no media reached the stream after the rewind began')
		},
	},
	{
		name: 'the receiver restarts and the stream recovers',
		port: 6006,
		ssrc: 1016,
		provision: async (
			region: string,
			stackName: string,
		): Promise<{ keyHex: string }> => {
			const keyHex = await provisionKey(region, stackName, 6006, { ssrc: 1016 })
			return { keyHex }
		},
		run: async (ctx: CaseContext, key: { keyHex: string }): Promise<void> => {
			let since = new Date()
			// Two rollovers up: after the restart the floor points there, so the
			// fresh helper must find a counter that is not zero.
			let sender = makeSender(ctx.config, 6006, 1016, key.keyHex, {
				roc: 2,
				durationS: 20,
			})
			await streamFor(sender, 20)
			step('streamed for 20s')
			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6006),
				since,
			)

			// The sender keeps counting while the receiver restarts beneath it.
			since = new Date()
			sender = makeSender(ctx.config, 6006, 1016, key.keyHex, {
				roc: sender.currentState.roc,
				seq: sender.currentState.seq,
				timestamp: sender.currentState.timestamp,
				durationS: 90,
			})
			const streaming = sender.run()
			await new Promise((resolve) => setTimeout(resolve, 5_000))
			await restartBackend(ctx.config.region, ctx.instances, ctx.codeBucket)
			await new Promise((resolve) => setTimeout(resolve, 10_000))

			// Re-authenticated after the restart (a new first authentication for
			// the port, in this process's lifetime) and media resumed.
			const lines = await waitForLogLines(
				ctx.config.region,
				ctx.config.logGroup,
				`SRTP traffic authenticated.*"port":6006`,
				since,
				240_000,
			)
			assert.ok(lines.length > 0)
			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6006),
				since,
				240_000,
			)
			sender.stop()
			await streaming
		},
	},
	{
		name: 'a fresh key restarts the index space cleanly',
		port: 6007,
		ssrc: 1017,
		provision: async (
			region: string,
			stackName: string,
		): Promise<{ keyHex: string }> => {
			const keyHex = await provisionKey(region, stackName, 6007, { ssrc: 1017 })
			return { keyHex }
		},
		run: async (ctx: CaseContext, key: { keyHex: string }): Promise<void> => {
			// The trivial rotation case: same SSRC, new key, index back to zero,
			// confirmed on the first trial.
			let since = new Date()
			let sender = makeSender(ctx.config, 6007, 1017, key.keyHex, {
				roc: 0,
				durationS: 15,
			})
			await streamFor(sender, 15)
			step('streamed for 15s')
			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6007),
				since,
			)

			const freshKey = await provisionKey(
				ctx.config.region,
				ctx.config.stackName,
				6007,
				{ ssrc: 1017 },
			)
			await restartBackend(ctx.config.region, ctx.instances, ctx.codeBucket)
			since = new Date()
			sender = makeSender(ctx.config, 6007, 1017, freshKey, {
				roc: 0,
				durationS: 45,
			})
			await streamFor(sender, 45)
			step('streamed for 45s; now waiting on logs, metrics and media')
			const lines = await waitForLogLines(
				ctx.config.region,
				ctx.config.logGroup,
				`SRTP traffic authenticated.*"port":6007`,
				since,
			)
			assert.ok(lines.length > 0)
			const firstTrial = lines.some((line) => line.includes('"trials":1'))
			assert.ok(
				firstTrial,
				'a fresh key with a floor-less index must confirm on the first trial',
			)
			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6007),
				since,
			)
		},
	},
	{
		name: 'SRTP noise never disturbs the unencrypted path',
		port: 6008,
		ssrc: 1018,
		provision: async (
			region: string,
			stackName: string,
		): Promise<{ keyHex: string }> => {
			const keyHex = await provisionKey(region, stackName, 6008, { ssrc: 1018 })
			return { keyHex }
		},
		run: async (ctx: CaseContext, key: { keyHex: string }): Promise<void> => {
			const since = new Date()
			// SRTP noise with the wrong key on 6008.
			const wrongKey = key.keyHex.slice(0, 58) + 'ff'
			const noise = makeSender(ctx.config, 6008, 1018, wrongKey, {
				roc: 0,
				durationS: 45,
			})
			const noiseRun = noise.run()

			// A real unencrypted MPEG-TS stream on 5000 at the same time. ffmpeg
			// is what the operator manual uses for the unencrypted path.
			const ffmpeg = spawn(
				'ffmpeg',
				[
					'-hide_banner',
					'-loglevel',
					'error',
					// -re paces the encode to real time: a lavfi source encodes
					// faster than playback and the UDP muxer does not pace, so
					// without it 45s of video arrives as a ~2s, 16k-datagram
					// burst that overflows the receiver's socket buffer (the
					// default receive buffer is ~200 KB) - almost everything is
					// dropped, the 10 MB pre-start gate is never crossed, and
					// the case fails while the path itself is healthy. That is
					// how this case failed on 2026-09-25 even after the
					// bitrate was raised.
					'-re',
					'-f',
					'lavfi',
					'-i',
					// 1280x720, not 320x180: the unencrypted path buffers
					// KINESIS_MIN_BYTES_BEFORE_START (10 MB) before it starts GStreamer,
					// so the stream must cross that within its window. At 320x180
					// ultrafast the source produces ~60 KB/s - 2.7 MB over 45s, never
					// enough, which is exactly how this case failed on 2026-09-25 while
					// the path itself was healthy. At 1280x720 the same content produces
					// ~475 KB/s, crossing 10 MB at about 21s. Measured, not guessed:
					// ffmpeg -t 5 of each source into mpegts, wc -c.
					'testsrc2=size=1280x720:rate=15',
					'-t',
					'45',
					'-c:v',
					'libx264',
					'-preset',
					'ultrafast',
					'-f',
					'mpegts',
					`udp://${ctx.config.nlbHost}:5000`,
				],
				{ stdio: ['ignore', 'ignore', 'pipe'] },
			)
			let ffmpegErr = ''
			ffmpeg.stderr.on('data', (chunk: Buffer) => {
				ffmpegErr += chunk.toString()
			})

			try {
				await new Promise((resolve) => setTimeout(resolve, 45_000))
				noise.stop()
				await noiseRun
				// The unencrypted stream kept ingesting throughout the noise.
				// The wait covers the pre-start threshold (~21s into the stream),
				// the pipeline start, the first upload, and - on the aligned
				// metric windows - a full minute boundary after it.
				await waitForStreamIngestion(
					ctx.config.region,
					streamNameFor(ctx.config, 5000),
					since,
					240_000,
				)
				// And the noise port did not ingest.
				await new Promise((resolve) => setTimeout(resolve, 30_000))
				await assertNoStreamIngestion(
					ctx.config.region,
					streamNameFor(ctx.config, 6008),
					since,
				)
			} finally {
				if (ffmpeg.exitCode === null) ffmpeg.kill('SIGKILL')
				assert.equal(ffmpeg.exitCode, 0, ffmpegErr)
			}
		},
	},
]
