#!/usr/bin/env node
/**
 * Deploy CDK stack to test AWS account
 *
 * This script deploys the NTN Video Streaming stack to a test environment
 * for integration testing and validation.
 *
 * Usage:
 *   node --experimental-transform-types test/deploy-test-stack.ts [--destroy] [--diff]
 *
 * Options:
 *   --destroy  Destroy the test stack instead of deploying
 *   --diff     Show differences before deploying
 */

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
import { App } from 'aws-cdk-lib'
import commandLineArgs from 'command-line-args'
import path from 'node:path'
import { StreamingStack } from '../cdk/StreamingStack.ts'

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
])

const stackNameWithDefault =
	process.env.TEST_STACK_NAME ?? 'NTNVideoStreamingTest'
const regionWithDefault = process.env.AWS_REGION ?? 'us-east-1'

console.log(`Test stack: ${stackNameWithDefault}`)
console.log(`Region: ${regionWithDefault}`)

// Get available availability zones
const ec2Client = new EC2Client({ region: regionWithDefault })
const azResponse = await ec2Client.send(
	new DescribeAvailabilityZonesCommand({
		Filters: [
			{
				Name: 'region-name',
				Values: [regionWithDefault],
			},
		],
	}),
)

const availabilityZones = new Set(
	azResponse.AvailabilityZones?.map((az) => az.ZoneName ?? '').filter(
		(name) => name !== '',
	) ?? [],
)

if (availabilityZones.size === 0) {
	throw new Error(`No availability zones found in region ${regionWithDefault}`)
}

console.log(
	`Using availability zones: ${Array.from(availabilityZones).join(', ')}`,
)

// Create CDK app
const app = new App()

// Create test stack
new StreamingStack(app, stackNameWithDefault, {
	env: {
		account: process.env.CDK_DEFAULT_ACCOUNT,
		region: regionWithDefault,
	},
	availabilityZones,
})

// Use CDK Toolkit for deployment
const cdk = new Toolkit()

const cx = await cdk.fromAssemblyBuilder(async () => app.synth(), {
	contextStore: new FileContext(path.join(process.cwd(), 'cdk.context.json')),
})

const stacks = {
	strategy: StackSelectionStrategy.PATTERN_MUST_MATCH_SINGLE,
	patterns: [stackNameWithDefault],
}

if (options.diff === true) {
	console.log('\nShowing differences...')
	await cdk.diff(cx, { method: DiffMethod.TemplateOnly(), stacks })
} else if (options.destroy === true) {
	console.log('\nDestroying stack...')
	await cdk.destroy(cx, { stacks })
} else {
	console.log('\nDeploying stack...')
	await cdk.deploy(cx, { stacks })
	console.log('\nDeployment complete!')
}
