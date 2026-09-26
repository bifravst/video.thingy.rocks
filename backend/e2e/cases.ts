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
	seedSrtpIndexFloor,
	waitForLogEvents,
	waitForLogLines,
	waitForSrtpIndexFloorFor,
	waitForStreamIngestion,
	waitForTransportMetrics,
} from './stack.ts'

/**
 * The e2e cases.
 *
 * Every case gets its own SRTP port and its own freshly provisioned key, so
 * nothing one case does can disturb another's floor, and every case asserts on
 * the deployed system's own observables: the KVS metric, the per-transport
 * traffic metrics, the application log, the lock table, and - where it matters
 * most - media read back out of Kinesis.
 *
 * Ports: 6000 happy path, 6001 wrong key, 6002 forged flood, 6003 wrap,
 * 6004 key rotation, 6005 rewind, 6006 restart recovery, 6007 fresh key,
 * 6008 isolation noise (plus unencrypted 5000), 6009 walked-past search.
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
		/** Default 15; wrong-key noise runs faster to walk the counter search. */
		fps?: number
	},
): E2eSender =>
	new E2eSender({
		host: config.nlbHost,
		port,
		keyHex,
		ssrc,
		...options,
		fps: options.fps ?? 15,
	})

/**
 * A key that is guaranteed to differ from `keyHex`, for the negative cases.
 *
 * Appending a fixed suffix cannot work: a randomly generated key that already
 * ends in that byte stays itself, and about one run in 256 the "wrong key"
 * case would send the real key and authenticate. Flipping the last byte to
 * the other safe value always changes it.
 */
export const wrongKeyOf = (keyHex: string): string =>
	keyHex.endsWith('ff')
		? `${keyHex.slice(0, 58)}00`
		: `${keyHex.slice(0, 58)}ff`

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

			// The strongest assertion there is: the media itself comes back - from
			// this run, not from whatever the stream retained before it.
			const mediaBytes = await readMedia(
				ctx.config.region,
				streamNameFor(ctx.config, 6000),
				since,
			)
			assert.ok(
				mediaBytes > 0,
				'GetMedia must return fragments for the ingested stream',
			)

			// The per-transport metrics the zero-ingestion alarms read are the
			// stack's own observables too: the deployed publisher must be showing
			// the transport as serving with real traffic on it, in the namespace
			// the alarms query - a publisher that drifted from what the alarms
			// expect watches nothing, which reads as "no traffic" and never fires.
			step('waiting for the transport metrics the alarms read')
			await waitForTransportMetrics(ctx.config.region, ctx.config.stackName, {
				transport: 'srtp',
				since,
			})

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
			const wrongKey = wrongKeyOf(key.keyHex)
			const sender = makeSender(ctx.config, 6001, 1011, wrongKey, {
				roc: 0,
				durationS: 20,
			})
			await streamFor(sender, 20)
			step('streamed for 20s')

			// Metrics need a minute to be sure; the lock is the immediate signal.
			// Asserted on the lock field, not on the row being absent: the row can
			// legitimately pre-exist with floor or status metadata from an earlier
			// run on a reused stack - what this case owns is that no instance ever
			// held the lock.
			await new Promise((resolve) => setTimeout(resolve, 30_000))
			const row = await lockRow(ctx.config.region, ctx.config.tableName, 6001)
			assert.equal(
				row?.kinesisOwnerInstanceId,
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
			// The lock field, not the whole row: see the wrong-key case for why.
			assert.equal(
				row?.kinesisOwnerInstanceId,
				undefined,
				'forged traffic must never acquire the lock',
			)
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
			// One rollover below 65536, crossing it - the upper end of the 48-bit
			// packet index space - within the first seconds: the first sequence
			// wrap of a 15 fps stream is ~9 s away, and the one after that a full
			// 65536 packets (~72 min) later, so the case has to sit at 65535 to
			// see the crossing at all. The floor row it seeds carries an index at
			// 65535 * 65536, which no 32-bit arithmetic in the receiver can afford
			// to misread. Only a synthetic sender can sit here at all.
			await seedSrtpIndexFloor(ctx.config.region, ctx.config.tableName, 6003, {
				keyHex: key.keyHex,
				ssrc: 1013,
				roc: 65535,
			})
			await restartBackend(ctx.config.region, ctx.instances, ctx.codeBucket)

			const since = new Date()
			const sender = makeSender(ctx.config, 6003, 1013, key.keyHex, {
				roc: 65535,
				seq: 65400,
				durationS: 45,
			})
			await streamFor(sender, 45)
			step('streamed for 45s; now waiting on logs, metrics and media')

			// Confirmed straight at the seeded floor's rollover - a 48-bit floor
			// must not cost the search its first trial - and ingesting.
			const lines = await waitForLogLines(
				ctx.config.region,
				ctx.config.logGroup,
				`SRTP traffic authenticated.*"port":6003`,
				since,
			)
			assert.ok(lines.length > 0, 'the authentication must be in the log')
			assert.ok(
				lines.some((line) => line.includes('"trials":1')),
				"a stream at the floor's own rollover must confirm on the first trial",
			)
			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6003),
				since,
			)
			const rollovers = await waitForLogLines(
				ctx.config.region,
				ctx.config.logGroup,
				`SRTP rollover.*"port":6003.*"roc":65536`,
				since,
			)
			assert.ok(
				rollovers.length > 0,
				'the wrap to rollover 65536 - the point where the 48-bit packet index leaves the 32-bit range - must be reported',
			)
		},
	},
	{
		name: 'a rotated key starts a new index space below the old floor',
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

			// What this proves: the floor is scoped to the key's identity, not to
			// the port. A fresh key has a fresh fingerprint, so the old floor is
			// not its floor - the search starts at zero and finds a sender below
			// where the old floor was. An implementation that keyed the floor by
			// port alone would hold the sender at ROC 5 and refuse everything
			// below it as a rewind. (The search returning to a walked-past
			// counter is a different behavior, proved by the 6009 case.)
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
			// The rotation fence's own proof: the rotated key's floor must
			// actually persist. A key that cannot write its floor still ingests
			// (no floor means searching from zero) and confirms on trial 1, so
			// the row is the only observable of the fence working.
			step("waiting for the rotated key's floor to persist")
			await waitForSrtpIndexFloorFor(
				ctx.config.region,
				ctx.config.tableName,
				6004,
				freshKey,
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
			const since = new Date()
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
			sender = makeSender(ctx.config, 6006, 1016, key.keyHex, {
				roc: sender.currentState.roc,
				seq: sender.currentState.seq,
				timestamp: sender.currentState.timestamp,
				durationS: 150,
			})
			const streaming = sender.run()
			await new Promise((resolve) => setTimeout(resolve, 5_000))
			const restartSince = new Date()
			await restartBackend(ctx.config.region, ctx.instances, ctx.codeBucket)

			// Anchor both assertions to the restarted process, not to the clock:
			// the old process authenticated this very sender during the five
			// seconds before the restart, so a window taken from wall-clock
			// `since` can match the OLD process's lines and the pre-restart
			// media, and the case can pass without proving any recovery at all.
			// The boot line is the one line only the new process writes.
			step('waiting for the restarted service to come up (its own boot line)')
			const boots = await waitForLogEvents(
				ctx.config.region,
				ctx.config.logGroup,
				'SRTP transport started',
				restartSince,
				300_000,
			)
			const postRestart = boots[boots.length - 1]?.timestamp as Date

			// Re-authenticated after the restart (a new first authentication for
			// the port, in this process's lifetime) and media resumed. The
			// ingestion window is ceiling-aligned: only bytes in minutes after
			// the boot count, never the pre-restart minute's earlier upload.
			const lines = await waitForLogLines(
				ctx.config.region,
				ctx.config.logGroup,
				`SRTP traffic authenticated.*"port":6006`,
				postRestart,
				240_000,
			)
			assert.ok(lines.length > 0)
			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6006),
				postRestart,
				240_000,
				'next',
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
			// And the rotated key's floor persists - see the 6004 case for why
			// the row is the only observable of the rotation fence.
			step("waiting for the rotated key's floor to persist")
			await waitForSrtpIndexFloorFor(
				ctx.config.region,
				ctx.config.tableName,
				6007,
				freshKey,
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
			const wrongKey = wrongKeyOf(key.keyHex)
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
	{
		name: 'the search recovers after traffic walks it past the answer',
		port: 6009,
		ssrc: 1019,
		provision: async (
			region: string,
			stackName: string,
		): Promise<{ keyHex: string }> => {
			const keyHex = await provisionKey(region, stackName, 6009, { ssrc: 1019 })
			return { keyHex }
		},
		run: async (ctx: CaseContext, key: { keyHex: string }): Promise<void> => {
			// The scenario the search's re-sweep exists for: traffic that cannot
			// authenticate - a sender holding a stale key, an attacker, another
			// test run against the same port - advances the climbing search one
			// candidate per few drops, and once the climb has passed the real
			// counter, only the periodic re-sweep of the counters just above the
			// floor reaches it again. A search that only ever climbed would never
			// come back down, and the port would stay bound but unable to ingest
			// for as long as the wrong-key traffic kept arriving.
			//
			// Phase one establishes the floor the search anchors to.
			let since = new Date()
			const establish = makeSender(ctx.config, 6009, 1019, key.keyHex, {
				roc: 7,
				durationS: 20,
			})
			await streamFor(establish, 20)
			step('streamed for 20s to establish the floor at rollover 7')
			await waitForStreamIngestion(
				ctx.config.region,
				streamNameFor(ctx.config, 6009),
				since,
			)

			// Phase two: wrong-key noise, fast enough to walk the search through
			// more than a full candidate cycle. The search re-offers the floor and
			// re-sweeps the counters just above it once every `floor-every`
			// candidates (512 by default, about 34s of 60 fps traffic that never
			// authenticates); the noise phase spans two of those cycles.
			since = new Date()
			const wrongKey = wrongKeyOf(key.keyHex)
			const noise = makeSender(ctx.config, 6009, 1019, wrongKey, {
				roc: 0,
				durationS: 240,
				fps: 60,
			})
			const noiseRun = noise.run()
			await new Promise((resolve) => setTimeout(resolve, 90_000))
			step(
				'noise has walked the search past the answer; starting the real sender',
			)

			// Phase three: the real sender returns at rollover 12, a counter the
			// climb passed within the first seconds of the noise - five above the
			// floor, inside the re-sweep window, unreachable by any climb. The
			// noise keeps running, because it is what keeps the trials advancing;
			// both stop once the authentication is in the log.
			const sender = makeSender(ctx.config, 6009, 1019, key.keyHex, {
				roc: 12,
				durationS: 150,
			})
			const senderRun = sender.run()
			try {
				const lines = await waitForLogLines(
					ctx.config.region,
					ctx.config.logGroup,
					`SRTP traffic authenticated.*"port":6009`,
					since,
					240_000,
				)
				assert.ok(lines.length > 0, 'the walked-past sender must be found')
				// The trial count is the proof the search actually came back
				// down rather than climbing into the answer by luck: the counter
				// sits inside the re-sweep band, and the only way back to it is
				// past a full candidate cycle, so the confirmation must carry
				// more than one cycle's worth of trials.
				const trials = lines
					.map((line) => /"trials":(\d+)/.exec(line)?.[1])
					.map((match) => (match === undefined ? 0 : Number(match)))
					.reduce((max, n) => Math.max(max, n), 0)
				assert.ok(
					trials > 512,
					`the confirmation must come after a full candidate cycle was walked (trials: ${String(trials)})`,
				)
				await waitForStreamIngestion(
					ctx.config.region,
					streamNameFor(ctx.config, 6009),
					since,
				)
			} finally {
				sender.stop()
				noise.stop()
				await Promise.all([senderRun, noiseRun])
			}
		},
	},
]
