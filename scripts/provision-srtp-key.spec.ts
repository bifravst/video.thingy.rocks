import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, it } from 'node:test'

const SCRIPT = 'scripts/provision-srtp-key.sh'

/**
 * Stands in for the AWS CLI: records its arguments, the request file's mode and
 * contents, and then takes as long as it is told to, so a test can signal the script
 * while it is waiting on the call.
 */
const FAKE_AWS = `#!/bin/bash
printf '%s\\n' "$*" >> "$FAKE_AWS_LOG.argv"
for arg in "$@"; do
  case "$arg" in
    file://*)
      stat -c %a "\${arg#file://}" > "$FAKE_AWS_LOG.mode"
      cp "\${arg#file://}" "$FAKE_AWS_LOG.payload"
      ;;
  esac
done
sleep "\${FAKE_AWS_SLEEP:-0}"
`

type Run = {
	exited: Promise<{
		code: number | null
		signal: NodeJS.Signals | null
		stdout: string
	}>
	signal: (name: NodeJS.Signals) => void
}

void describe('provision-srtp-key.sh', () => {
	let dir: string
	let log: string
	let scratch: string

	beforeEach(() => {
		dir = mkdtempSync(join(tmpdir(), 'provision-srtp-key-'))
		mkdirSync(join(dir, 'bin'))
		writeFileSync(join(dir, 'bin', 'aws'), FAKE_AWS, { mode: 0o755 })
		// The script's own TMPDIR, so what it leaves behind can be listed.
		scratch = join(dir, 'tmp')
		mkdirSync(scratch)
		log = join(dir, 'aws')
	})

	afterEach(() => {
		rmSync(dir, { recursive: true, force: true })
	})

	const start = (key: string, awsSleepSeconds = 0): Run => {
		const child = spawn('bash', [SCRIPT, '6000', '3735928559'], {
			stdio: ['pipe', 'pipe', 'pipe'],
			env: {
				...process.env,
				PATH: `${join(dir, 'bin')}:${process.env.PATH ?? ''}`,
				TMPDIR: scratch,
				FAKE_AWS_LOG: log,
				FAKE_AWS_SLEEP: String(awsSleepSeconds),
			},
		})
		let stdout = ''
		child.stdout.setEncoding('utf8')
		child.stdout.on('data', (chunk: string) => {
			stdout += chunk
		})
		child.stdin.end(`${key}\n`)
		return {
			exited: new Promise((resolve) => {
				child.once('exit', (code, signal) => resolve({ code, signal, stdout }))
			}),
			signal: (name) => child.kill(name),
		}
	}

	/** Resolves once the fake AWS CLI has been reached. */
	const awsCalled = async (): Promise<void> => {
		const deadline = Date.now() + 10_000
		while (!existsSync(`${log}.payload`)) {
			if (Date.now() > deadline) throw new Error('the AWS CLI was never called')
			await new Promise((resolve) => setTimeout(resolve, 20))
		}
	}

	const requestFilesLeft = (): string[] =>
		readdirSync(scratch).filter((name) => name.startsWith('srtp-key-request.'))

	void it('provisions the key without it reaching an argument vector', async () => {
		const key = randomBytes(30).toString('hex')
		const { code, stdout } = await start(key).exited

		assert.strictEqual(code, 0)
		assert.match(stdout, /Done\./)

		const request = JSON.parse(readFileSync(`${log}.payload`, 'utf8')) as {
			Name: string
			Type: string
			Value: string
		}
		assert.strictEqual(request.Type, 'SecureString')
		assert.strictEqual((JSON.parse(request.Value) as { key: string }).key, key)

		assert.ok(
			!readFileSync(`${log}.argv`, 'utf8').includes(key),
			'the key must reach the AWS CLI through the request file only',
		)
		assert.strictEqual(
			readFileSync(`${log}.mode`, 'utf8').trim(),
			'600',
			'the request file holds the key, so nobody else may read it',
		)
		assert.deepStrictEqual(requestFilesLeft(), [])
	})

	/**
	 * A signal ends the script, as it would without a trap.
	 *
	 * A trap on a signal replaces the signal's default action, and when the handler
	 * returns bash carries on from where it was interrupted. The trap used to delete
	 * the request file and return, so an interrupted run went on - it reported
	 * success, and one interrupted before the request was written would have
	 * recreated it by plain redirection, with the caller's umask instead of 0600.
	 *
	 * The signal is sent while the AWS call is in flight, because that is the one
	 * point where it can be delivered deterministically: bash runs the trap as soon
	 * as the call returns. The narrower window between creating the file and
	 * writing it has no such hold point, so it is covered by the same handler rather
	 * than by a test of its own.
	 *
	 * Either way of stopping counts - exiting with 128 plus the signal number, or
	 * being ended by the signal itself. The resuming trap covered only INT and TERM,
	 * so HUP always ended the script and was never affected; it is here so that it
	 * stays that way.
	 */
	for (const [name, status] of [
		['SIGHUP', 129],
		['SIGINT', 130],
		['SIGTERM', 143],
	] as const) {
		void it(`stops on ${name} instead of carrying on`, async () => {
			const run = start(randomBytes(30).toString('hex'), 1)
			await awsCalled()
			run.signal(name)
			const { code, signal, stdout } = await run.exited

			assert.ok(
				code === status || signal === name,
				`a ${name} must end the script, but it exited with ${String(code)}`,
			)
			assert.doesNotMatch(
				stdout,
				/Done\./,
				'an interrupted run must not report success',
			)
			assert.deepStrictEqual(
				requestFilesLeft(),
				[],
				'the request file holds the key, so it must not outlive the script',
			)
		})
	}
})
