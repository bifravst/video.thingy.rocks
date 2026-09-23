import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import dgram from 'node:dgram'
import {
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, describe, it } from 'node:test'

import {
	hasGstElements,
	startHelper,
	type Helper,
} from '../backend/src/testing/srtpHelper.ts'

const SENDER = 'scripts/stream-testsrc-to-srtp.py'
const SSRC = 42

/** A key of the right shape; each test gets its own so a leak is attributable. */
const freshKey = (): string => randomBytes(30).toString('hex')

/** Runs the sender to completion - for the cases that refuse before streaming. */
const run = (
	args: string[],
	options: { input?: string; env?: NodeJS.ProcessEnv } = {},
): { status: number | null; stderr: string } => {
	const result = spawnSync('python3', [SENDER, ...args], {
		input: options.input ?? '',
		encoding: 'utf8',
		timeout: 30_000,
		env: { ...process.env, ...options.env },
	})
	return { status: result.status, stderr: result.stderr }
}

/**
 * Every process on this machine whose command line contains `secret`.
 *
 * All of them, not just the sender, because the thing being ruled out is the key
 * reaching *any* argument vector - including one belonging to something the sender
 * starts, which is exactly how the shell version leaked it through gst-launch-1.0.
 */
const processesExposing = (secret: string): string[] =>
	readdirSync('/proc')
		.filter((entry) => /^\d+$/.test(entry))
		.filter((pid) => {
			try {
				return readFileSync(`/proc/${pid}/cmdline`, 'latin1').includes(secret)
			} catch {
				// Gone, or not ours to read - neither can be leaking to us.
				return false
			}
		})

void describe('stream-testsrc-to-srtp.py', () => {
	/**
	 * The refusals run without GStreamer: they happen before it is imported, so they
	 * hold on any machine that can run Python.
	 */
	void describe('refusing to expose the key', () => {
		// The shell sender took the key as its third positional argument. That habit is
		// the one worth breaking, so a key offered that way is refused with the reason
		// rather than as a usage error.
		void it('refuses a key among its arguments', () => {
			const { status, stderr } = run(['127.0.0.1', '6000', freshKey()])
			assert.notStrictEqual(status, 0)
			assert.match(stderr, /key-shaped material in the arguments/)
		})

		void it('refuses a key hidden in an option value', () => {
			const { status, stderr } = run([
				'127.0.0.1',
				'6000',
				`--ssrc=${freshKey()}`,
			])
			assert.notStrictEqual(status, 0)
			assert.match(stderr, /key-shaped material in the arguments/)
		})

		/**
		 * srtpenc copies the key into the caps it sends downstream, and GStreamer logs
		 * caps above level 3 - checked against the element rather than assumed, which
		 * is why the receiver's threshold is the right one here as well.
		 */
		for (const debug of ['4', '*:5', 'srtpenc:9', '2,udpsink:6']) {
			void it(`refuses GST_DEBUG=${debug}`, () => {
				const { status, stderr } = run(['127.0.0.1', '6000'], {
					input: `${freshKey()}\n`,
					env: { GST_DEBUG: debug },
				})
				assert.notStrictEqual(status, 0)
				assert.match(stderr, /would log caps/)
			})
		}

		// A level that does not log caps must not be refused: the check is about the
		// key, not about debugging.
		void it('allows GST_DEBUG=3', () => {
			const { stderr } = run(['127.0.0.1', '6000'], {
				input: 'not a key\n',
				env: { GST_DEBUG: '3' },
			})
			assert.doesNotMatch(stderr, /would log caps/)
			assert.match(stderr, /exactly 60 hex characters/)
		})

		void it('refuses GST_DEBUG_DUMP_DOT_DIR', () => {
			const { status, stderr } = run(['127.0.0.1', '6000'], {
				input: `${freshKey()}\n`,
				env: { GST_DEBUG_DUMP_DOT_DIR: tmpdir() },
			})
			assert.notStrictEqual(status, 0)
			assert.match(stderr, /pipeline graphs containing the SRTP key/)
		})
	})

	void describe('validating what it is given', () => {
		const dir = mkdtempSync(join(tmpdir(), 'srtp-sender-'))
		after(() => rmSync(dir, { recursive: true, force: true }))

		for (const [port, why] of [
			['5000', 'an unencrypted port'],
			['06000', 'a non-canonical form'],
			['6010', 'one past the range'],
		] as const) {
			void it(`rejects port ${port}, ${why}`, () => {
				const { status, stderr } = run(['127.0.0.1', port], {
					input: `${freshKey()}\n`,
				})
				assert.notStrictEqual(status, 0)
				assert.match(stderr, /port must be one of 6000-6009/)
			})
		}

		for (const ssrc of ['-1', '0123', '4294967296', '99999999999']) {
			void it(`rejects SSRC ${ssrc}`, () => {
				const { status, stderr } = run(['127.0.0.1', '6000', '--ssrc', ssrc], {
					input: `${freshKey()}\n`,
				})
				assert.notStrictEqual(status, 0)
				assert.match(stderr, /ssrc must be a canonical decimal uint32/)
			})
		}

		void it('rejects a key of the wrong length', () => {
			const { status, stderr } = run(['127.0.0.1', '6000'], {
				input: `${freshKey().slice(0, 58)}\n`,
			})
			assert.notStrictEqual(status, 0)
			assert.match(stderr, /exactly 60 hex characters/)
		})

		void it('rejects an empty stdin rather than streaming with no key', () => {
			const { status, stderr } = run(['127.0.0.1', '6000'])
			assert.notStrictEqual(status, 0)
			assert.match(stderr, /exactly 60 hex characters/)
		})

		void it('reads the key file it is pointed at', () => {
			const path = join(dir, 'short.txt')
			writeFileSync(path, 'abc\n', { mode: 0o600 })
			const { status, stderr } = run(['127.0.0.1', '6000', '--key-file', path])
			assert.notStrictEqual(status, 0)
			assert.match(
				stderr,
				/exactly 60 hex characters/,
				'the file was read: its content is what got rejected',
			)
		})

		void it('reports a key file it cannot read', () => {
			const { status, stderr } = run([
				'127.0.0.1',
				'6000',
				'--key-file',
				join(dir, 'missing.txt'),
			])
			assert.notStrictEqual(status, 0)
			assert.match(stderr, /cannot read key file/)
		})
	})

	/**
	 * What the sender needs, stated once.
	 *
	 * The guide used to carry its own list - an install command missing two of the
	 * four plugin packages, and a probe checking three of the six elements, which
	 * printed "ok" on a machine the sender would then refuse to run on. Both drifted
	 * from the script in the commit that wrote them. The guide now defers to the
	 * script: its probe is the sender's own --check, and its install command is
	 * compared with the packages the script names.
	 */
	void describe('its prerequisites', () => {
		const guide = readFileSync('docs/TESTING-SRTP-INGESTION.md', 'utf8')
		const bindings = hasGstElements([])
			? false
			: 'requires python3 GStreamer bindings'

		/** The packages the sender says it needs, read from the script itself. */
		const requiredPackages = (): string[] =>
			JSON.parse(
				spawnSync(
					'python3',
					[
						'-c',
						'import json, runpy, sys\n' +
							"print(json.dumps(runpy.run_path(sys.argv[1], run_name='packages')['required_packages']()))",
						SENDER,
					],
					{ encoding: 'utf8', timeout: 30_000 },
				).stdout,
			) as string[]

		void it('has an install command in the guide naming every package it needs', () => {
			const install = guide
				.split('\n')
				.find((line) => line.trim().startsWith('sudo apt install'))
			assert.ok(
				install !== undefined,
				'the guide should give an install command',
			)
			const listed = install.trim().split(/\s+/).slice(3)
			const needed = requiredPackages()
			assert.ok(needed.length > 0)
			assert.deepStrictEqual(
				needed.filter((pkg) => !listed.includes(pkg)),
				[],
				`the guide's install command is missing packages the sender needs: ${install}`,
			)
		})

		void it('is checked in the guide with its own --check, not a separate list', () => {
			assert.ok(guide.includes('./scripts/stream-testsrc-to-srtp.py --check'))
		})

		void it(
			'passes --check on a machine with everything installed',
			{
				skip: hasGstElements([
					'videotestsrc',
					'videoconvert',
					'x264enc',
					'rtph264pay',
					'srtpenc',
					'udpsink',
				])
					? false
					: 'requires every element the sender uses',
			},
			() => {
				const result = spawnSync('python3', [SENDER, '--check'], {
					encoding: 'utf8',
					timeout: 30_000,
				})
				assert.strictEqual(result.status, 0, result.stdout + result.stderr)
				assert.strictEqual(result.stdout.trim(), 'ok')
			},
		)

		// The case --check exists for: a machine missing plugins. Simulated by pointing
		// GStreamer at an empty plugin path and a fresh registry.
		void it(
			'names every missing element and its package',
			{ skip: bindings },
			() => {
				const registry = mkdtempSync(join(tmpdir(), 'gst-registry-'))
				try {
					const result = spawnSync('python3', [SENDER, '--check'], {
						encoding: 'utf8',
						timeout: 30_000,
						env: {
							...process.env,
							GST_PLUGIN_SYSTEM_PATH_1_0: join(registry, 'none'),
							GST_PLUGIN_PATH_1_0: join(registry, 'none'),
							GST_REGISTRY_1_0: join(registry, 'registry.bin'),
						},
					})
					assert.strictEqual(result.status, 1)
					for (const element of [
						'videotestsrc',
						'videoconvert',
						'x264enc',
						'rtph264pay',
						'srtpenc',
						'udpsink',
					]) {
						assert.match(
							result.stdout,
							new RegExp(`missing: ${element} \\(ships in `),
						)
					}
					assert.match(
						result.stdout,
						/install with: sudo apt install .*gstreamer1\.0-plugins-good/,
					)
				} finally {
					rmSync(registry, { recursive: true, force: true })
				}
			},
		)
	})

	/**
	 * Against the real receiver, with real libsrtp on both ends.
	 *
	 * The point of this rewrite is a property of the running process, so it is
	 * checked on the running process: while the sender streams, no command line on
	 * the machine holds its key, and the receiver authenticates what it sends. The
	 * second half matters as much as the first - a sender that set the key wrongly,
	 * or not at all, would keep it out of /proc just as well.
	 */
	void describe(
		'streaming',
		{
			skip: hasGstElements([
				'srtpdec',
				'srtpenc',
				'x264enc',
				'rtph264pay',
				'videotestsrc',
			])
				? false
				: 'requires python3 GStreamer bindings with srtp, x264enc and rtph264pay',
		},
		() => {
			/**
			 * Forwards an ingest port to the helper's relay port.
			 *
			 * The sender only accepts the real ingest ports, and the helper binds
			 * whatever the OS gives it, so this sits in between - on the first of
			 * 6000-6009 that is free.
			 */
			const relayTo = async (
				helper: Helper,
			): Promise<{ port: number; close: () => void }> => {
				const inbound = dgram.createSocket('udp4')
				const outbound = dgram.createSocket('udp4')
				for (let port = 6000; port <= 6009; port++) {
					const bound = await new Promise<boolean>((resolve) => {
						inbound.once('error', () => resolve(false))
						inbound.bind(port, '127.0.0.1', () => resolve(true))
					})
					if (!bound) continue
					inbound.on('message', (datagram) => {
						outbound.send(datagram, helper.relayPort, '127.0.0.1')
					})
					return {
						port,
						close: () => {
							inbound.close()
							outbound.close()
						},
					}
				}
				throw new Error('no port in 6000-6009 is free on 127.0.0.1')
			}

			const streamWith = async (
				deliver: 'stdin' | 'key-file',
			): Promise<void> => {
				const key = freshKey()
				const helper = await startHelper({ key, ssrc: SSRC })
				const relay = await relayTo(helper)
				const dir = mkdtempSync(join(tmpdir(), 'srtp-sender-'))
				const keyFile = join(dir, 'key.txt')
				writeFileSync(keyFile, `${key}\n`, { mode: 0o600 })

				const sender = spawn(
					'python3',
					[
						SENDER,
						'127.0.0.1',
						String(relay.port),
						'--ssrc',
						String(SSRC),
						...(deliver === 'key-file' ? ['--key-file', keyFile] : []),
					],
					{ stdio: ['pipe', 'pipe', 'pipe'] },
				)
				let senderStderr = ''
				sender.stderr.setEncoding('utf8')
				sender.stderr.on('data', (chunk: string) => {
					senderStderr += chunk
				})
				sender.stdin.end(deliver === 'stdin' ? `${key}\n` : '')

				try {
					const confirmed = await helper.waitFor(
						(m) => m.t === 'auth' && m.status === 'ok' && m.first,
						20_000,
					)
					assert.ok(confirmed.t === 'auth' && confirmed.status === 'ok')
					assert.strictEqual(
						confirmed.roc,
						0,
						'a new session starts at rollover counter zero',
					)

					// Still streaming, which is when the old sender exposed the key.
					assert.strictEqual(sender.exitCode, null, senderStderr)
					assert.deepStrictEqual(
						processesExposing(key),
						[],
						'the key must not appear in any command line on the machine',
					)
					assert.ok(
						!readFileSync(
							`/proc/${String(sender.pid)}/environ`,
							'latin1',
						).includes(key),
						'nor in the sender environment',
					)

					// Ctrl+C is how a sender is stopped, so it is a clean exit.
					const exited = new Promise<number | null>((resolve) => {
						sender.once('exit', (code) => resolve(code))
					})
					sender.kill('SIGINT')
					assert.strictEqual(await exited, 0, senderStderr)
				} finally {
					if (sender.exitCode === null) sender.kill('SIGKILL')
					relay.close()
					await helper.stop()
					rmSync(dir, { recursive: true, force: true })
				}
			}

			void it(
				'is authenticated by the receiver with the key from stdin, which no command line holds',
				{ timeout: 60_000 },
				async () => streamWith('stdin'),
			)

			void it(
				'is authenticated by the receiver with the key from a file, which no command line holds',
				{ timeout: 60_000 },
				async () => streamWith('key-file'),
			)
		},
	)
})
