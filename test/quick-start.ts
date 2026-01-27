#!/usr/bin/env node
/**
 * Quick Start Script for Test Environment
 *
 * This script helps set up and validate the test environment quickly.
 * It performs the following steps:
 * 1. Check prerequisites
 * 2. Deploy test stack (optional)
 * 3. Get EC2 instance IP
 * 4. Run a quick validation test
 *
 * Usage:
 *   node --experimental-transform-types test/quick-start.ts
 */

import { execSync } from 'node:child_process'
import { parseArgs } from 'node:util'

const { values } = parseArgs({
	options: {
		'skip-deploy': { type: 'boolean', default: false },
		'stack-name': { type: 'string', default: 'NTNVideoStreamingTest' },
		region: { type: 'string', default: 'us-east-1' },
	},
})

const skipDeploy = values['skip-deploy'] ?? false
const stackName = values['stack-name'] ?? 'NTNVideoStreamingTest'
const region = values.region ?? 'us-east-1'

console.log('NTN Video Streaming - Test Environment Quick Start')
console.log('==================================================')
console.log()

/**
 * Execute command and return output
 */
const exec = (command: string, silent = false): string => {
	try {
		const output = execSync(command, {
			encoding: 'utf-8',
			stdio: silent ? 'pipe' : 'inherit',
		})
		return output.trim()
	} catch (error) {
		if (error instanceof Error) {
			throw new Error(`Command failed: ${command}\n${error.message}`)
		}
		throw error
	}
}

/**
 * Check if command exists
 */
const commandExists = (command: string): boolean => {
	try {
		exec(`which ${command}`, true)
		return true
	} catch {
		return false
	}
}

/**
 * Step 1: Check prerequisites
 */
const checkPrerequisites = (): void => {
	console.log('Step 1: Checking prerequisites...')

	const checks = [
		{ name: 'Node.js', command: 'node', version: 'node --version' },
		{ name: 'npm', command: 'npm', version: 'npm --version' },
		{ name: 'AWS CLI', command: 'aws', version: 'aws --version' },
		{ name: 'CDK', command: 'cdk', version: 'cdk --version' },
	]

	for (const check of checks) {
		if (commandExists(check.command)) {
			const version = exec(check.version, true)
			console.log(`  ✓ ${check.name}: ${version}`)
		} else {
			console.log(`  ✗ ${check.name}: Not found`)
			console.log(`    Please install ${check.name}`)
			process.exit(1)
		}
	}

	// Check AWS credentials
	try {
		exec('aws sts get-caller-identity', true)
		console.log('  ✓ AWS credentials configured')
	} catch {
		console.log('  ✗ AWS credentials not configured')
		console.log('    Run: aws configure')
		process.exit(1)
	}

	console.log()
}

/**
 * Step 2: Deploy test stack
 */
const deployStack = (): void => {
	if (skipDeploy) {
		console.log('Step 2: Skipping stack deployment (--skip-deploy)')
		console.log()
		return
	}

	console.log('Step 2: Deploying test stack...')
	console.log(`  Stack name: ${stackName}`)
	console.log(`  Region: ${region}`)
	console.log()

	try {
		// Check if stack exists
		const stackStatus = exec(
			`aws cloudformation describe-stacks --stack-name ${stackName} --region ${region} --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "NOT_FOUND"`,
			true,
		)

		if (stackStatus !== 'NOT_FOUND') {
			console.log(`  Stack already exists with status: ${stackStatus}`)
			console.log('  Skipping deployment')
		} else {
			console.log('  Synthesizing CDK stack...')
			exec(`node --experimental-transform-types test/deploy-test-stack.ts`)

			console.log()
			console.log('  Stack synthesized successfully!')
			console.log()
			console.log('  To deploy the stack, run:')
			console.log(`    cdk deploy ${stackName}`)
			console.log()
			console.log('  Note: Deployment will take 10-15 minutes')
			console.log('  Run this script again with --skip-deploy after deployment')
			process.exit(0)
		}
	} catch (error) {
		console.log('  ✗ Error deploying stack')
		if (error instanceof Error) {
			console.log(`    ${error.message}`)
		}
		process.exit(1)
	}

	console.log()
}

/**
 * Step 3: Get stack outputs
 */
const getStackOutputs = (): Record<string, string> => {
	console.log('Step 3: Getting stack outputs...')

	try {
		const outputsJson = exec(
			`aws cloudformation describe-stacks --stack-name ${stackName} --region ${region} --query "Stacks[0].Outputs" --output json`,
			true,
		)

		const outputs = JSON.parse(outputsJson)
		const outputMap: Record<string, string> = {}

		for (const output of outputs) {
			outputMap[output.OutputKey] = output.OutputValue
			console.log(`  ${output.OutputKey}: ${output.OutputValue}`)
		}

		console.log()
		return outputMap
	} catch (error) {
		console.log('  ✗ Error getting stack outputs')
		if (error instanceof Error) {
			console.log(`    ${error.message}`)
		}
		process.exit(1)
	}
}

/**
 * Step 4: Get EC2 instance IP
 */
const getEC2InstanceIP = (): string | null => {
	console.log('Step 4: Getting EC2 instance IP...')

	try {
		const instancesJson = exec(
			`aws ec2 describe-instances --region ${region} --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" "Name=instance-state-name,Values=running" --query "Reservations[*].Instances[*].[PublicIpAddress]" --output json`,
			true,
		)

		const instances = JSON.parse(instancesJson)

		if (instances.length === 0 || instances[0].length === 0) {
			console.log('  ⚠ No running EC2 instances found')
			console.log('    The Auto Scaling Group may still be launching instances')
			console.log('    Wait a few minutes and try again')
			return null
		}

		const ip = instances[0][0][0]
		console.log(`  ✓ EC2 instance IP: ${ip}`)
		console.log()
		return ip
	} catch (error) {
		console.log('  ✗ Error getting EC2 instance IP')
		if (error instanceof Error) {
			console.log(`    ${error.message}`)
		}
		return null
	}
}

/**
 * Step 5: Print next steps
 */
const printNextSteps = (
	outputs: Record<string, string>,
	ec2IP: string | null,
): void => {
	console.log('Step 5: Next steps')
	console.log('==================')
	console.log()

	if (ec2IP === undefined || ec2IP === null || ec2IP === "") {
		console.log('Wait for EC2 instances to launch, then:')
		console.log()
	}

	console.log('1. Test UDP packet generation:')
	console.log()
	if (ec2IP) {
		console.log(`   npm run test:generate-packets -- \\`)
		console.log(`     --host ${ec2IP} \\`)
		console.log(`     --port 5000 \\`)
		console.log(`     --duration 30`)
	} else {
		console.log(`   npm run test:generate-packets -- \\`)
		console.log(`     --host <EC2_IP> \\`)
		console.log(`     --port 5000 \\`)
		console.log(`     --duration 30`)
	}
	console.log()

	console.log('2. Run integration tests:')
	console.log()
	if (ec2IP) {
		console.log(`   npm run test:integration -- \\`)
		console.log(`     --host ${ec2IP} \\`)
		console.log(`     --bucket-name ${outputs.VideoBucketName} \\`)
		console.log(`     --table-name ${outputs.DynamoDBTableName} \\`)
		console.log(`     --port 5000`)
	} else {
		console.log(`   npm run test:integration -- \\`)
		console.log(`     --host <EC2_IP> \\`)
		console.log(`     --bucket-name ${outputs.VideoBucketName} \\`)
		console.log(`     --table-name ${outputs.DynamoDBTableName} \\`)
		console.log(`     --port 5000`)
	}
	console.log()

	console.log('3. Check DynamoDB:')
	console.log()
	console.log(`   aws dynamodb get-item \\`)
	console.log(`     --table-name ${outputs.DynamoDBTableName} \\`)
	console.log(`     --key '{"port": {"N": "5000"}}'`)
	console.log()

	console.log('4. Check S3 bucket:')
	console.log()
	console.log(`   aws s3 ls s3://${outputs.VideoBucketName}/raw/5000/`)
	console.log(`   aws s3 ls s3://${outputs.VideoBucketName}/hls/5000/`)
	console.log()

	console.log('5. View CloudWatch logs:')
	console.log()
	console.log(`   aws logs tail /ntn-video-streaming/udp-listener --follow`)
	console.log()

	console.log('For more details, see test/SETUP.md')
	console.log()

	// Save environment variables
	console.log('Environment variables for testing:')
	console.log()
	console.log(`export TEST_STACK_NAME=${stackName}`)
	console.log(`export AWS_REGION=${region}`)
	if (ec2IP) {
		console.log(`export TEST_EC2_IP=${ec2IP}`)
	}
	console.log(`export TEST_BUCKET_NAME=${outputs.VideoBucketName}`)
	console.log(`export TEST_TABLE_NAME=${outputs.DynamoDBTableName}`)
	console.log()
}

/**
 * Main function
 */
const main = async (): Promise<void> => {
	try {
		checkPrerequisites()
		deployStack()
		const outputs = getStackOutputs()
		const ec2IP = getEC2InstanceIP()
		printNextSteps(outputs, ec2IP)
	} catch (error) {
		console.error('Error:', error)
		process.exit(1)
	}
}

// Run the script
void main()
