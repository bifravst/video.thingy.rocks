import assert from 'node:assert/strict'
import { cases } from './cases.ts'
import { discoverStack, fleetInstanceIds, restartBackend } from './stack.ts'

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
 *   E2E_STACK_NAME=video-streaming-2026-05 AWS_REGION=eu-central-1 \
 *     npm run test:e2e [-- --only wrap]
 */

const args = process.argv.slice(2)
const onlyIndex = args.indexOf('--only')
const only = onlyIndex !== -1 ? args[onlyIndex + 1] : undefined
const selected =
	only === undefined ? cases : cases.filter((c) => c.name.includes(only))
assert.ok(selected.length > 0, `no case matches --only ${String(only)}`)

const stackName = process.env.E2E_STACK_NAME ?? process.env.STACK_NAME
assert.ok(
	stackName !== undefined && stackName !== '',
	'E2E_STACK_NAME (or STACK_NAME) must name the deployed stack to test',
)
const region = process.env.AWS_REGION ?? 'eu-central-1'

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

	// Provision every selected case's initial key, then restart the service once
	// so it loads them the way production does: keys are resolved at process
	// start, and rotation is a reprovision-plus-restart by design.
	const keys = new Map<string, { keyHex: string }>()
	for (const c of selected) {
		log(`provisioning port ${String(c.port)} (${c.name})`)
		keys.set(c.name, await c.provision(region, stackName))
	}
	log(
		`restarting the fleet so the service loads the keys (${instances.length} instances)`,
	)
	await restartBackend(region, instances)
	await new Promise((resolve) => setTimeout(resolve, 15_000))

	const failures: { name: string; error: unknown }[] = []
	for (const c of selected) {
		log(`running: ${c.name}`)
		const started = Date.now()
		try {
			await c.run({ config, instances }, keys.get(c.name) as { keyHex: string })
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
