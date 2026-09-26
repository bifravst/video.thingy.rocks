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
 * while it is waiting on the call. Answers `ssm get-parameter` with the generation the
 * test primes (`FAKE_AWS_GENERATION`), or not-found when unprimed - the port's first
 * provisioning.
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
if [ "$1" = "ssm" ] && [ "$2" = "get-parameter" ]; then
  if [ -n "\${FAKE_AWS_GENERATION:-}" ]; then
    printf '%s\\n' "$FAKE_AWS_GENERATION"
    exit 0
  fi
  exit 1
fi
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

	const start = (
		key: string,
		awsSleepSeconds = 0,
		extraEnv: Record<string, string> = {},
	): Run => {
		const child = spawn('bash', [SCRIPT, '6000', '3735928559'], {
			stdio: ['pipe', 'pipe', 'pipe'],
			env: {
				...process.env,
				PATH: `${join(dir, 'bin')}:${process.env.PATH ?? ''}`,
				TMPDIR: scratch,
				FAKE_AWS_LOG: log,
				FAKE_AWS_SLEEP: String(awsSleepSeconds),
				...extraEnv,
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
			KeyId?: string
		}
		assert.strictEqual(request.Type, 'SecureString')
		assert.strictEqual((JSON.parse(request.Value) as { key: string }).key, key)
		// Every provisioning run stamps the key with a generation: the fence the
		// replay floor's rotation condition compares against, so a later
		// provisioning of the same port is always strictly newer than an
		// earlier one (unix time is monotonic enough for that by construction).
		const value = JSON.parse(request.Value) as { generation?: number }
		assert.ok(
			typeof value.generation === 'number' &&
				Number.isInteger(value.generation) &&
				value.generation > 0,
			'the parameter value must carry a positive integer generation',
		)

		const argv = readFileSync(`${log}.argv`, 'utf8')
		assert.ok(
			!argv.includes(key),
			'the key must reach the AWS CLI through the request file only',
		)
		// The instance role has no kms:Decrypt grant, which only works for parameters
		// encrypted with the AWS managed key (see the SRTP key grant in StreamingStack).
		assert.strictEqual(
			request.KeyId,
			undefined,
			'the parameter must be encrypted with aws/ssm, which the instance role can use without a KMS grant',
		)
		assert.doesNotMatch(argv, /--key-id/)
		assert.strictEqual(
			readFileSync(`${log}.mode`, 'utf8').trim(),
			'600',
			'the request file holds the key, so nobody else may read it',
		)
		assert.deepStrictEqual(requestFilesLeft(), [])
	})

	/**
	 * The generation counter, not the clock: two provisions of the same port
	 * within one second must get different generations, because the replay
	 * floor's rotation fence only lets a strictly newer generation replace a
	 * row's identity. Unix seconds gave both the same value, which left the
	 * newer key unable to persist its floor - silently, since a key that
	 * cannot write its floor still ingests.
	 */
	void it('gives a same-second re-provisioning a strictly newer generation', async () => {
		const first = await start(randomBytes(30).toString('hex')).exited
		assert.strictEqual(first.code, 0)
		const request1 = JSON.parse(readFileSync(`${log}.payload`, 'utf8')) as {
			Value: string
		}
		const gen1 = (JSON.parse(request1.Value) as { generation: number })
			.generation

		// The second run happens immediately - same unix second, most runs -
		// and reads back the generation the first wrote.
		const second = await start(randomBytes(30).toString('hex'), 0, {
			FAKE_AWS_GENERATION: String(gen1),
		}).exited
		assert.strictEqual(second.code, 0)
		const request2 = JSON.parse(readFileSync(`${log}.payload`, 'utf8')) as {
			Value: string
		}
		const gen2 = (JSON.parse(request2.Value) as { generation: number })
			.generation
		assert.ok(
			gen2 > gen1,
			`the generation must advance within one second (${String(gen1)} -> ${String(gen2)})`,
		)
		// One higher, not merely different: the counter, never the clock.
		assert.strictEqual(gen2, gen1 + 1)

		// And the counter is written before the key, so a run that fails in
		// between skips a generation instead of repeating one.
		const calls = readFileSync(`${log}.argv`, 'utf8').trim().split('\n')
		const counterPut = calls.findIndex((line) =>
			line.includes('/srtp/port/6000/generation --type String'),
		)
		const keyPut = calls.findIndex((line) => line.includes('file://'))
		assert.ok(counterPut >= 0, 'the generation parameter must be written')
		assert.ok(
			counterPut < keyPut,
			'the generation counter must be written before the key parameter',
		)
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
