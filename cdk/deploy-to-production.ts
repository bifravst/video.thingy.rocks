import {
	DiffMethod,
	FileContext,
	StackSelectionStrategy,
	Toolkit,
} from '@aws-cdk/toolkit-lib'
import {
	DescribeAvailabilityZonesCommand,
	EC2Client,
} from '@aws-sdk/client-ec2'
import { IAMClient } from '@aws-sdk/client-iam'
import { ensureGitHubOIDCProvider } from '@bifravst/ci'
import commandLineArgs from 'command-line-args'
import { readFile } from 'node:fs/promises'
import path from 'node:path'
import { env } from '../aws/env.ts'
import { repoInfo } from './helper/repo.ts'
import { ProdApp } from './ProdApp.ts'

const options = commandLineArgs([
	{
		name: 'diff',
		type: Boolean,
		defaultValue: false,
	},
	{
		name: 'destroy',
		type: Boolean,
		defaultValue: false,
	},
	{
		name: 'stackName',
		type: String,
	},
])

const iam = new IAMClient({})
const accountEnv = await env()
const ec2Client = new EC2Client()
const availabilityZones = new Set<string>(
	(
		await ec2Client.send(new DescribeAvailabilityZonesCommand({}))
	).AvailabilityZones?.map((zone) => zone.ZoneName!) ?? [],
)

const version = process.env.VERSION ?? '0.0.0-development'

const ctx = {
	...JSON.parse(
		await readFile(path.join(process.cwd(), 'cdk.context.json'), 'utf-8'),
	),
	version,
}

console.log('Availability Zones:', Array.from(availabilityZones).join(', '))

const app = new ProdApp({
	repository: repoInfo(),
	gitHubOICDProviderArn: await ensureGitHubOIDCProvider({
		iam,
	}),
	context: ctx,
	env: accountEnv,
	availabilityZones,
})

const cdk = new Toolkit()

const cx = await cdk.fromAssemblyBuilder(async () => app.synth(), {
	contextStore: new FileContext(path.join(process.cwd(), 'cdk.context.json')),
})

const stacks =
	options.stackName !== undefined
		? {
				strategy: StackSelectionStrategy.PATTERN_MUST_MATCH_SINGLE,
				patterns: [options.stackName],
			}
		: undefined

if (options.diff === true) {
	await cdk.diff(cx, { method: DiffMethod.TemplateOnly(), stacks })
} else if (options.destroy === true) {
	await cdk.destroy(cx, {
		stacks,
	})
} else {
	await cdk.deploy(cx, {
		stacks,
	})
}
