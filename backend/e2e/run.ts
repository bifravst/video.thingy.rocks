import assert from 'node:assert/strict'
import { cases } from './cases.ts'
import {
	codeBucketFor,
	discoverStack,
	fleetInstanceIds,
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

const main = async (): Promise<void> => {
	log(`discovering stack ${stackName} in ${region}`)
	const config = await discoverStack(region, stackName)
	const instances = await fleetInstanceIds(region, config.autoScalingGroupName)
	assert.ok(instances.length > 0, 'no running instances in the fleet')
	log(
		`stack: NLB ${config.nlbHost}, table ${config.tableName}, instances ${instances.join(', ')}`,
	)

	// Provision every selected case's initial key, then put the deployed backend
	// code on the instances and restart the service. The restart has to deploy the
	// code too: instances only pick it up at boot, so a fleet that predates the
	// deploy would otherwise still run whatever it booted with.
	const keys = new Map<string, { keyHex: string }>()
	for (const c of selected) {
		log(`provisioning port ${String(c.port)} (${c.name})`)
		keys.set(c.name, await c.provision(region, stackName))
	}
	const codeBucket = await codeBucketFor(region, stackName)
	log(
		`deploying the backend code to the fleet and restarting the service (${instances.length} instances, code bucket ${codeBucket})`,
	)
	await restartBackend(region, instances, codeBucket)

	// Wait for the service to say, in its own log, that the SRTP transport is up -
	// and fail fast, with the reason, on the two startup failures that otherwise
	// cost three minutes of streaming into a port nothing is listening on.
	const since = new Date()
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
	log('SRTP transport started; running cases')

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
	}

	if (failures.length > 0) {
		console.error(`\n${String(failures.length)} case(s) failed:`)
		for (const f of failures) {
			console.error(` - ${f.name}: ${String(f.error)}`)
		}
		process.exit(1)
	}
	log(`all ${String(selected.length)} case(s) passed`)
}

void main().catch((error) => {
	console.error(error)
	process.exit(1)
})
