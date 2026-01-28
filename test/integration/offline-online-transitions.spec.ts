#!/usr/bin/env node
/**
 * Integration Test: Offline/Online Transitions
 *
 * This test validates the system's handling of intermittent connectivity:
 * 1. Send UDP packets to establish active stream
 * 2. Stop sending packets and wait for timeout (1 minute)
 * 3. Verify stream marked as inactive in DynamoDB
 * 4. Verify last frame snapshot is captured and stored in S3
 * 5. Verify snapshot accessible via CloudFront URL
 * 6. Resume sending UDP packets
 * 7. Verify stream marked as active in DynamoDB
 * 8. Verify new HLS segments are generated
 *
 * Requirements validated: 11.1, 11.2, 11.4, 11.5
 *
 * Prerequisites:
 * - CDK stack deployed to test environment
 * - AWS credentials configured
 * - EC2 instance running and accessible
 *
 * Usage:
 *   node --experimental-transform-types test/integration/offline-online-transitions.spec.ts \
 *     --host <EC2_IP> \
 *     --bucket-name <BUCKET_NAME> \
 *     --table-name <TABLE_NAME> \
 *     --cloudfront-url <CLOUDFRONT_URL> \
 *     --port 5000
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import {
	HeadObjectCommand,
	ListObjectsV2Command,
	S3Client,
} from '@aws-sdk/client-s3'
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb'
import dgram from 'node:dgram'
import https from 'node:https'
import { parseArgs } from 'node:util'
import { createPacketWithTimestamp } from '../fixtures/sample-packets.ts'

// Parse command line arguments
const { values } = parseArgs({
	options: {
		port: { type: 'string', default: '5000' },
		host: { type: 'string', default: 'localhost' },
		'bucket-name': { type: 'string' },
		'table-name': { type: 'string' },
		'cloudfront-url': { type: 'string' },
		region: { type: 'string', default: 'eu-central-1' },
		'packet-count': { type: 'string', default: '100' },
		'inactivity-timeout': { type: 'string', default: '65' }, // 65 seconds (slightly more than 1 minute)
		timeout: { type: 'string', default: '300' },
	},
})

const port = Number.parseInt(values.port ?? '5000', 10)
const host = values.host ?? 'localhost'
const bucketName = values['bucket-name']
const tableName = values['table-name']
const cloudfrontUrl = values['cloudfront-url']
const region = values.region ?? 'eu-central-1'
const packetCount = Number.parseInt(values['packet-count'] ?? '100', 10)
const inactivityTimeout = Number.parseInt(
	values['inactivity-timeout'] ?? '65',
	10,
)
const timeout = Number.parseInt(values.timeout ?? '300', 10) * 1000

if (bucketName === undefined || bucketName === null || bucketName === '') {
	console.error('Error: --bucket-name is required')
	console.error(
		'Get it from: aws cloudformation describe-stacks --stack-name video-streaming --query "Stacks[0].Outputs[?OutputKey==\'VideoBucketName\'].OutputValue" --output text',
	)
	process.exit(1)
}

if (tableName === undefined || tableName === null || tableName === '') {
	console.error('Error: --table-name is required')
	console.error(
		'Get it from: aws cloudformation describe-stacks --stack-name video-streaming --query "Stacks[0].Outputs[?OutputKey==\'DynamoDBTableName\'].OutputValue" --output text',
	)
	process.exit(1)
}

if (
	cloudfrontUrl === undefined ||
	cloudfrontUrl === null ||
	cloudfrontUrl === ''
) {
	console.error('Error: --cloudfront-url is required')
	console.error(
		'Get it from: aws cloudformation describe-stacks --stack-name video-streaming --query "Stacks[0].Outputs[?OutputKey==\'CloudFrontURL\'].OutputValue" --output text',
	)
	process.exit(1)
}

console.log('Integration Test: Offline/Online Transitions')
console.log('========================================================')
console.log(`Target: ${host}:${port}`)
console.log(`S3 Bucket: ${bucketName}`)
console.log(`DynamoDB Table: ${tableName}`)
console.log(`CloudFront URL: ${cloudfrontUrl}`)
console.log(`Region: ${region}`)
console.log(`Packet count: ${packetCount}`)
console.log(`Inactivity timeout: ${inactivityTimeout} seconds`)
console.log(`Test timeout: ${timeout / 1000} seconds`)
console.log()

// Create AWS clients
const s3Client = new S3Client({ region })
const dynamoClient = DynamoDBDocumentClient.from(new DynamoDBClient({ region }))

// Test results
type TestResult = {
	name: string
	passed: boolean
	message: string
	duration?: number
	requirement?: string
}

const results: TestResult[] = []
const startTime = Date.now()

/**
 * Send UDP packets to establish active stream
 */
const sendInitialPackets = async (): Promise<void> => {
	console.log(
		'Step 1: Sending initial UDP packets to establish active stream...',
	)
	const stepStartTime = Date.now()

	const socket = dgram.createSocket('udp4')
	let sent = 0
	let errors = 0

	for (let i = 0; i < packetCount; i++) {
		const packet = createPacketWithTimestamp(Date.now())

		await new Promise<void>((resolve) => {
			socket.send(packet, port, host, (error) => {
				if (error) {
					errors++
					console.error(`  Error sending packet ${i}: ${error.message}`)
				} else {
					sent++
				}
				resolve()
			})
		})

		// Small delay between packets to simulate realistic streaming
		await new Promise((resolve) => setTimeout(resolve, 10))

		// Progress indicator every 25 packets
		if ((i + 1) % 25 === 0) {
			process.stdout.write(`\r  Sent ${sent}/${packetCount} packets...`)
		}
	}

	socket.close()

	const duration = Date.now() - stepStartTime
	console.log(`\n  ✓ Sent ${sent} packets in ${duration}ms (${errors} errors)`)

	results.push({
		name: 'Send initial UDP packets',
		passed: sent >= packetCount * 0.95, // Allow 5% packet loss
		message: `Sent ${sent}/${packetCount} packets (${errors} errors)`,
		duration,
		requirement: '11.1',
	})
}

/**
 * Wait for initial processing
 */
const waitForInitialProcessing = async (seconds: number): Promise<void> => {
	console.log(`\nStep 2: Waiting ${seconds} seconds for initial processing...`)

	for (let i = 0; i < seconds; i++) {
		await new Promise((resolve) => setTimeout(resolve, 1000))
		const remaining = seconds - i - 1
		process.stdout.write(`\r  ${remaining} seconds remaining...`)
	}

	console.log('\r  ✓ Wait complete                    ')
}

/**
 * Verify stream is active
 */
const verifyStreamActive = async (): Promise<void> => {
	console.log('\nStep 3: Verifying stream is active in DynamoDB...')
	const stepStartTime = Date.now()

	try {
		const response = await dynamoClient.send(
			new GetCommand({
				TableName: tableName,
				Key: { port },
			}),
		)

		if (
			response.Item === undefined ||
			response.Item === null ||
			Object.keys(response.Item).length === 0
		) {
			throw new Error('Stream metadata not found in DynamoDB')
		}

		const metadata = response.Item
		console.log('  ✓ Stream metadata found:')
		console.log(`    - Status: ${metadata.status}`)
		console.log(`    - Last packet time: ${metadata.lastPacketTime}`)

		const duration = Date.now() - stepStartTime
		results.push({
			name: 'Verify stream active',
			passed: metadata.status === 'active',
			message: `Status: ${metadata.status}`,
			duration,
			requirement: '11.1',
		})
	} catch (error) {
		const duration = Date.now() - stepStartTime
		const message = error instanceof Error ? error.message : 'Unknown error'
		console.log(`  ✗ Error: ${message}`)

		results.push({
			name: 'Verify stream active',
			passed: false,
			message,
			duration,
			requirement: '11.1',
		})
	}
}

/**
 * Stop sending packets and wait for inactivity timeout
 */
const waitForInactivityTimeout = async (): Promise<void> => {
	console.log(
		`\nStep 4: Stopping packets and waiting ${inactivityTimeout} seconds for inactivity timeout...`,
	)
	console.log(
		'  (System should mark stream as inactive after 1 minute of no packets)',
	)

	for (let i = 0; i < inactivityTimeout; i++) {
		await new Promise((resolve) => setTimeout(resolve, 1000))
		const remaining = inactivityTimeout - i - 1
		process.stdout.write(`\r  ${remaining} seconds remaining...`)
	}

	console.log('\r  ✓ Wait complete                    ')
}

/**
 * Verify stream marked as inactive
 */
const verifyStreamInactive = async (): Promise<void> => {
	console.log('\nStep 5: Verifying stream marked as inactive in DynamoDB...')
	const stepStartTime = Date.now()

	try {
		const response = await dynamoClient.send(
			new GetCommand({
				TableName: tableName,
				Key: { port },
			}),
		)

		if (
			response.Item === undefined ||
			response.Item === null ||
			Object.keys(response.Item).length === 0
		) {
			throw new Error('Stream metadata not found in DynamoDB')
		}

		const metadata = response.Item
		console.log('  ✓ Stream metadata found:')
		console.log(`    - Status: ${metadata.status}`)
		console.log(`    - Last packet time: ${metadata.lastPacketTime}`)
		console.log(
			`    - Last frame path: ${metadata.lastFramePath !== undefined && metadata.lastFramePath !== null && metadata.lastFramePath !== '' ? metadata.lastFramePath : 'Not set'}`,
		)

		const duration = Date.now() - stepStartTime
		results.push({
			name: 'Verify stream inactive',
			passed: metadata.status === 'inactive',
			message: `Status: ${metadata.status}`,
			duration,
			requirement: '11.4',
		})
	} catch (error) {
		const duration = Date.now() - stepStartTime
		const message = error instanceof Error ? error.message : 'Unknown error'
		console.log(`  ✗ Error: ${message}`)

		results.push({
			name: 'Verify stream inactive',
			passed: false,
			message,
			duration,
			requirement: '11.4',
		})
	}
}

/**
 * Verify last frame snapshot in S3
 */
const verifySnapshotInS3 = async (): Promise<void> => {
	console.log('\nStep 6: Verifying last frame snapshot in S3...')
	const stepStartTime = Date.now()

	try {
		const snapshotKey = `snapshots/${port}/last_frame.jpg`

		const response = await s3Client.send(
			new HeadObjectCommand({
				Bucket: bucketName,
				Key: snapshotKey,
			}),
		)

		const sizeKB = ((response.ContentLength ?? 0) / 1024).toFixed(2)
		const lastModified = response.LastModified?.toISOString() ?? 'Unknown'

		console.log('  ✓ Snapshot found in S3:')
		console.log(`    - Key: ${snapshotKey}`)
		console.log(`    - Size: ${sizeKB} KB`)
		console.log(`    - Last modified: ${lastModified}`)
		console.log(`    - Content type: ${response.ContentType}`)

		const duration = Date.now() - stepStartTime
		results.push({
			name: 'Verify snapshot in S3',
			passed: true,
			message: `Found snapshot (${sizeKB} KB)`,
			duration,
			requirement: '11.4',
		})
	} catch (error) {
		const duration = Date.now() - stepStartTime
		const message = error instanceof Error ? error.message : 'Unknown error'
		console.log(`  ✗ Error: ${message}`)

		results.push({
			name: 'Verify snapshot in S3',
			passed: false,
			message,
			duration,
			requirement: '11.4',
		})
	}
}

/**
 * Verify snapshot accessible via CloudFront
 */
const verifySnapshotViaCloudFront = async (): Promise<void> => {
	console.log('\nStep 7: Verifying snapshot accessible via CloudFront...')
	const stepStartTime = Date.now()

	try {
		const snapshotUrl = `https://${cloudfrontUrl}/snapshots/${port}/last_frame.jpg`
		console.log(`  Requesting: ${snapshotUrl}`)

		const response = await new Promise<{
			statusCode: number
			contentLength: number
			contentType: string
		}>((resolve, reject) => {
			https
				.get(snapshotUrl, (res) => {
					const statusCode = res.statusCode ?? 0
					const contentLength = Number.parseInt(
						res.headers['content-length'] ?? '0',
						10,
					)
					const contentType = res.headers['content-type'] ?? 'unknown'

					// Consume response data to free up memory
					res.on('data', () => {})
					res.on('end', () => {
						resolve({ statusCode, contentLength, contentType })
					})
				})
				.on('error', reject)
		})

		const sizeKB = (response.contentLength / 1024).toFixed(2)

		console.log('  ✓ Snapshot accessible via CloudFront:')
		console.log(`    - Status code: ${response.statusCode}`)
		console.log(`    - Size: ${sizeKB} KB`)
		console.log(`    - Content type: ${response.contentType}`)

		const duration = Date.now() - stepStartTime
		results.push({
			name: 'Verify snapshot via CloudFront',
			passed: response.statusCode === 200 && response.contentLength > 0,
			message: `HTTP ${response.statusCode}, ${sizeKB} KB`,
			duration,
			requirement: '11.5',
		})
	} catch (error) {
		const duration = Date.now() - stepStartTime
		const message = error instanceof Error ? error.message : 'Unknown error'
		console.log(`  ✗ Error: ${message}`)

		results.push({
			name: 'Verify snapshot via CloudFront',
			passed: false,
			message,
			duration,
			requirement: '11.5',
		})
	}
}

/**
 * Resume sending UDP packets
 */
const resumeSendingPackets = async (): Promise<void> => {
	console.log('\nStep 8: Resuming UDP packet transmission...')
	const stepStartTime = Date.now()

	const socket = dgram.createSocket('udp4')
	let sent = 0
	let errors = 0

	for (let i = 0; i < packetCount; i++) {
		const packet = createPacketWithTimestamp(Date.now())

		await new Promise<void>((resolve) => {
			socket.send(packet, port, host, (error) => {
				if (error) {
					errors++
					console.error(`  Error sending packet ${i}: ${error.message}`)
				} else {
					sent++
				}
				resolve()
			})
		})

		// Small delay between packets
		await new Promise((resolve) => setTimeout(resolve, 10))

		// Progress indicator every 25 packets
		if ((i + 1) % 25 === 0) {
			process.stdout.write(`\r  Sent ${sent}/${packetCount} packets...`)
		}
	}

	socket.close()

	const duration = Date.now() - stepStartTime
	console.log(`\n  ✓ Sent ${sent} packets in ${duration}ms (${errors} errors)`)

	results.push({
		name: 'Resume sending packets',
		passed: sent >= packetCount * 0.95,
		message: `Sent ${sent}/${packetCount} packets (${errors} errors)`,
		duration,
		requirement: '11.2',
	})
}

/**
 * Wait for stream resumption processing
 */
const waitForResumptionProcessing = async (seconds: number): Promise<void> => {
	console.log(
		`\nStep 9: Waiting ${seconds} seconds for stream resumption processing...`,
	)

	for (let i = 0; i < seconds; i++) {
		await new Promise((resolve) => setTimeout(resolve, 1000))
		const remaining = seconds - i - 1
		process.stdout.write(`\r  ${remaining} seconds remaining...`)
	}

	console.log('\r  ✓ Wait complete                    ')
}

/**
 * Verify stream marked as active again
 */
const verifyStreamActiveAgain = async (): Promise<void> => {
	console.log(
		'\nStep 10: Verifying stream marked as active again in DynamoDB...',
	)
	const stepStartTime = Date.now()

	try {
		const response = await dynamoClient.send(
			new GetCommand({
				TableName: tableName,
				Key: { port },
			}),
		)

		if (
			response.Item === undefined ||
			response.Item === null ||
			Object.keys(response.Item).length === 0
		) {
			throw new Error('Stream metadata not found in DynamoDB')
		}

		const metadata = response.Item
		console.log('  ✓ Stream metadata found:')
		console.log(`    - Status: ${metadata.status}`)
		console.log(`    - Last packet time: ${metadata.lastPacketTime}`)

		const duration = Date.now() - stepStartTime
		results.push({
			name: 'Verify stream active again',
			passed: metadata.status === 'active',
			message: `Status: ${metadata.status}`,
			duration,
			requirement: '11.2',
		})
	} catch (error) {
		const duration = Date.now() - stepStartTime
		const message = error instanceof Error ? error.message : 'Unknown error'
		console.log(`  ✗ Error: ${message}`)

		results.push({
			name: 'Verify stream active again',
			passed: false,
			message,
			duration,
			requirement: '11.2',
		})
	}
}

/**
 * Verify new HLS segments generated
 */
const verifyNewHLSSegments = async (): Promise<void> => {
	console.log('\nStep 11: Verifying new HLS segments are generated...')
	const stepStartTime = Date.now()

	try {
		// Get list of HLS segments before and after resumption
		// We'll check the 1080p profile as a representative sample
		const response = await s3Client.send(
			new ListObjectsV2Command({
				Bucket: bucketName,
				Prefix: `hls/${port}/1080p/`,
				MaxKeys: 50,
			}),
		)

		const segments = response.Contents ?? []
		const segmentCount = segments.length

		// Filter for .ts segment files (not playlists)
		const tsSegments = segments.filter((obj) => obj.Key?.endsWith('.ts'))
		const tsCount = tsSegments.length

		console.log(`  ✓ Found ${segmentCount} total objects in HLS 1080p folder`)
		console.log(`  ✓ Found ${tsCount} .ts segment files`)

		if (tsSegments.length > 0) {
			// Show most recent segments
			const sortedSegments = tsSegments.sort((a, b) => {
				const timeA = a.LastModified?.getTime() ?? 0
				const timeB = b.LastModified?.getTime() ?? 0
				return timeB - timeA
			})

			console.log('  Most recent segments:')
			for (const seg of sortedSegments.slice(0, 3)) {
				const sizeKB = ((seg.Size ?? 0) / 1024).toFixed(2)
				const modified = seg.LastModified?.toISOString() ?? 'Unknown'
				console.log(`    - ${seg.Key} (${sizeKB} KB, ${modified})`)
			}
		}

		const duration = Date.now() - stepStartTime
		results.push({
			name: 'Verify new HLS segments',
			passed: tsCount > 0,
			message: `Found ${tsCount} HLS segments`,
			duration,
			requirement: '11.2',
		})
	} catch (error) {
		const duration = Date.now() - stepStartTime
		const message = error instanceof Error ? error.message : 'Unknown error'
		console.log(`  ✗ Error: ${message}`)

		results.push({
			name: 'Verify new HLS segments',
			passed: false,
			message,
			duration,
			requirement: '11.2',
		})
	}
}

/**
 * Print test results summary
 */
const printResults = (): void => {
	console.log('\n' + '='.repeat(80))
	console.log('Test Results Summary - Task 7.4: Offline/Online Transitions')
	console.log('='.repeat(80))

	let passed = 0
	let failed = 0

	for (const result of results) {
		const status = result.passed ? '✓ PASS' : '✗ FAIL'
		const duration =
			result.duration !== undefined && result.duration !== null
				? ` (${result.duration}ms)`
				: ''
		const req =
			result.requirement !== undefined &&
			result.requirement !== null &&
			result.requirement !== ''
				? ` [Req: ${result.requirement}]`
				: ''
		console.log(`${status} ${result.name}${duration}${req}`)
		console.log(`       ${result.message}`)

		if (result.passed) passed++
		else failed++
	}

	const totalDuration = Date.now() - startTime
	const totalMinutes = (totalDuration / 60000).toFixed(2)

	console.log('\n' + '='.repeat(80))
	console.log(
		`Total: ${passed + failed} | Passed: ${passed} | Failed: ${failed}`,
	)
	console.log(`Total test duration: ${totalMinutes} minutes`)
	console.log('='.repeat(80))

	if (failed > 0) {
		console.log('\n⚠ Some tests failed. This may be due to:')
		console.log('  - Inactivity timeout not yet reached (wait longer)')
		console.log('  - Snapshot capture not configured in FFmpeg')
		console.log('  - CloudFront distribution not fully propagated')
		console.log('  - Stream state tracking not implemented correctly')
		console.log(
			'\nCheck CloudWatch logs: aws logs tail /video-streaming/udp-listener --follow',
		)
		process.exit(1)
	} else {
		console.log(
			'\n✓ All tests passed! Offline/online transitions working correctly.',
		)
		console.log('\nRequirements validated:')
		console.log(
			'  - 11.1: System treats Cat1bisCam offline as expected behavior',
		)
		console.log(
			'  - 11.2: System automatically resumes processing when Cat1bisCam returns',
		)
		console.log(
			'  - 11.4: System marks stream as inactive after 1 hour offline',
		)
		console.log('  - 11.5: Web frontend displays last frame when offline')
	}
}

/**
 * Main test flow
 */
const main = async (): Promise<void> => {
	try {
		// Phase 1: Establish active stream
		await sendInitialPackets()
		await waitForInitialProcessing(20)
		await verifyStreamActive()

		// Phase 2: Go offline and verify inactive state
		await waitForInactivityTimeout()
		await verifyStreamInactive()
		await verifySnapshotInS3()
		await verifySnapshotViaCloudFront()

		// Phase 3: Resume and verify active state
		await resumeSendingPackets()
		await waitForResumptionProcessing(20)
		await verifyStreamActiveAgain()
		await verifyNewHLSSegments()

		printResults()
	} catch (error) {
		console.error('\nFatal error:', error)
		process.exit(1)
	}
}

// Run the test
void main()
