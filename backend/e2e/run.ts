import assert from 'node:assert/strict'
import { cases } from './cases.ts'
import {
	acquireSuiteLock,
	codeBucketFor,
	discoverStack,
	fleetInstanceIds,
	refreshSuiteLock,
	releaseSuiteLock,
	restartBackend,
	waitForLogLines,
} from './stack.ts'

/**
 * The e2e runner: the acceptance test this feature never had.
 *
 * It runs against a deployed stack, so before anything else it makes sure that is
 * what it is looking at - and it will not touch a stack that is not explicitly
 * named. Provisioning a port's key takes an `--only` filter to run one case, and
 * everything it provisions is a parameter this suite owns (a fresh key per port,
 * overwritten between runs).
 *
 * Usage:
 *   npm run test:e2e [-- --only wrap]
 */

const args = process.argv.slice(2)
const onlyIndex = args.indexOf('--only')
const only = onlyIndex !== -1 ? args[onlyIndex + 1] : undefined
const selected =
	only === undefined ? cases : cases.filter((c) => c.name.includes(only))
assert.ok(selected.length > 0, `no case matches --only ${String(only)}`)

const stackName = process.env.STREAMING_STACK_NAME ?? `video-streaming-2026-05`
const region =
	process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION ?? 'eu-central-1'

const log = (message: string): void => {
	console.log(`[e2e ${new Date().toISOString()}] ${message}`)
}

/**
 * A net under the whole suite: once, mid-case, a promise that never settled
 * drained the event loop and Node exited 0 silently with cases still unrun -
 * a clean exit code that looks like success. Nothing else runs after the
 * summary on a successful run, so if the loop ever empties before that, it is
 * the same bug again and must be visible rather than silent - including to
 * whatever automation reads the exit code, which is why this fails the run
 * and not only says so on the console.
 */
let suiteDone = false
process.on('beforeExit', (code) => {
	if (suiteDone) return
	console.error(
		`[e2e] the event loop drained with the suite unfinished (exit code ${String(code)}): an await never settled, which is a bug in the suite, not a pass. The last line above is where it stopped.`,
	)
	process.exitCode = 1
})

const main = async (): Promise<void> => {
	const runId = `e2e-${new Date().toISOString()}-${String(process.pid)}`
	log(`discovering stack ${stackName} in ${region}`)
	const config = await discoverStack(region, stackName)
	const instances = await fleetInstanceIds(region, config.autoScalingGroupName)
	assert.ok(instances.length > 0, 'no running instances in the fleet')
	log(
		`stack: NLB ${config.nlbHost}, table ${config.tableName}, instances ${instances.join(', ')}`,
	)

	// One suite per stack, always. Two runs against the same stack corrupt
	// each other in ways that look exactly like product bugs: keys provisioned
	// twice under one fleet, restarts mid-case, and unauthenticated traffic
	// from one run walking the other's rollover-counter search past the
	// answer - which is precisely how wrap and climb failed on 2026-09-25.
	await acquireSuiteLock(region, config.tableName, runId)
	log('acquired the stack lock for this run')
	let released = false
	const release = async (): Promise<void> => {
		if (released) return Promise.resolve()
		released = true
		return releaseSuiteLock(region, config.tableName, runId)
	}
	for (const sig of ['SIGINT', 'SIGTERM'] as const) {
		process.once(sig, () => {
			void release()
				.catch(() => undefined)
				.finally(() => process.exit(130))
		})
	}

	// Provision every selected case's initial key, then put the deployed backend
	// code on the instances and restart the service. The restart has to deploy the
	// code too: instances only pick it up at boot, so a fleet that predates the
	// deploy would otherwise still run whatever it booted with.
	//
	// The readiness window starts BEFORE the restart, not after it: the service
	// logs 'SRTP transport started' a fraction of a second inside the restart
	// command, so a window that starts once the command has completed can sit
	// entirely after the one moment that line is ever written.
	const since = new Date()
	const keys = new Map<string, { keyHex: string }>()
	try {
		for (const c of selected) {
			log(`provisioning port ${String(c.port)} (${c.name})`)
			keys.set(c.name, await c.provision(region, stackName))
		}
		// The lock row's timestamp says "still running" to any concurrent
		// acquirer; provisioning is the longest phase without a case log line.
		await refreshSuiteLock(region, config.tableName, runId)
		const codeBucket = await codeBucketFor(region, stackName)
		log(
			`deploying the backend code to the fleet and restarting the service (${instances.length} instances, code bucket ${codeBucket})`,
		)
		await restartBackend(region, instances, codeBucket)

		// Wait for the service to say, in its own log, that the SRTP transport is up -
		// and fail fast, with the reason, on the failures that otherwise cost
		// three minutes of streaming into a port nothing is listening on.
		log('waiting for the SRTP transport to report that it started')
		const startup = await waitForLogLines(
			region,
			config.logGroup,
			'SRTP transport (started|has no usable keys|disabled)',
			since,
			5 * 60_000,
		).catch(() => [] as string[])
		if (startup.length === 0) {
			throw new Error(
				`the service never logged anything about the SRTP transport. Either the log group is wrong (using ${config.logGroup}, override with E2E_LOG_GROUP) or the deployed backend is not the SRTP one - re-run npm run cdk:prod:deploy from the srtp-ingest-v3 branch and let it complete.`,
			)
		}
		const line = startup[startup.length - 1] ?? ''
		if (line.includes('has no usable keys') || line.includes('disabled')) {
			throw new Error(`the SRTP transport did not start: ${line.slice(0, 400)}`)
		}

		// The transport being up is not the helpers being up: a helper reaching
		// 'searching' proves srtp_port.py actually runs on the fleet - the Python
		// bindings, the built srtpdec element and the floor read all work. A
		// 'missing-element' fatal here would otherwise surface three minutes into
		// the first case as a silent drop.
		log("waiting for a port's helper to report that it is searching")
		const helperUp = await waitForLogLines(
			region,
			config.logGroup,
			'Port state.*"from":"starting","to":"searching"',
			since,
			5 * 60_000,
		).catch(() => [] as string[])
		if (helperUp.length === 0) {
			throw new Error(
				"no SRTP helper reached 'searching': the helpers cannot run on the fleet. Check the application log for 'SRTP helper fatal' - on Amazon Linux 2023 that usually means the srtpdec build (install-gst-srtp-plugin.sh) failed.",
			)
		}
		log('SRTP transport started, helpers running; running cases')

		const failures: { name: string; error: unknown }[] = []
		for (const c of selected) {
			log(`running: ${c.name}`)
			const started = Date.now()
			try {
				await c.run(
					{ config, instances, codeBucket },
					keys.get(c.name) as { keyHex: string },
				)
				log(
					`passed: ${c.name} (${String(Math.round((Date.now() - started) / 1000))}s)`,
				)
			} catch (error) {
				failures.push({ name: c.name, error })
				log(`FAILED: ${c.name}`)
				console.error(error)
			}
			// A case that restarts the fleet (climb, restart recovery,
			// rotation) must not hold the lock's refresh hostage: refresh after
			// every case so a concurrent acquirer still sees this run.
			await refreshSuiteLock(region, config.tableName, runId).catch(
				(error: unknown) => {
					throw error
				},
			)
		}

		if (failures.length > 0) {
			console.error(`\n${String(failures.length)} case(s) failed:`)
			for (const f of failures) {
				console.error(` - ${f.name}: ${String(f.error)}`)
			}
			suiteDone = true
			await release()
			process.exit(1)
		}
	} finally {
		await release()
	}
	suiteDone = true
	log(`all ${String(selected.length)} case(s) passed`)
}

void main().catch((error) => {
	console.error(error)
	process.exit(1)
})
