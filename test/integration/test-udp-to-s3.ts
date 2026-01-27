#!/usr/bin/env node
/**
 * Integration Test: UDP to S3 Storage Flow
 *
 * Tests the complete flow from UDP packet reception to S3 storage:
 * 1. Send test UDP packets to a port
 * 2. Verify HLS segments appear in S3
 * 3. Verify raw segments are stored
 * 4. Verify DynamoDB metadata is updated
 * 5. Verify manifests are generated
 *
 * Prerequisites:
 * - CDK stack deployed to test environment
 * - AWS credentials configured
 * - EC2 instance running and accessible
 *
 * Usage:
 *   node --experimental-transform-types test/integration/test-udp-to-s3.ts
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import {
	GetObjectCommand,
	ListObjectsV2Command,
	S3Client,
} from '@aws-sdk/client-s3'
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb'
import dgram from 'node:dgram'
import { parseArgs } from 'node:util'
import { createPacketWithTimestamp } from '../fixtures/sample-packets.js'

// Parse command line arguments
const { values } = parseArgs({
	options: {
		port: { type: 'string', default: '5000' },
		host: { type: 'string', default: 'localhost' },
		'bucket-name': { type: 'string' },
		'table-name': { type: 'string' },
		region: { type: 'string', default: 'us-east-1' },
		'packet-count': { type: 'string', default: '100' },
		timeout: { type: 'string', default: '60' },
	},
})

const port = Number.parseInt(values.port ?? '5000', 10)
const host = values.host ?? 'localhost'
const bucketName = values['bucket-name']
const tableName = values['table-name']
const region = values.region ?? 'us-east-1'
const packetCount = Number.parseInt(values['packet-count'] ?? '100', 10)
const timeout = Number.parseInt(values.timeout ?? '60', 10) * 1000

if (bucketName === undefined || bucketName === null || bucketName === "") {
	console.error('Error: --bucket-name is required')
	process.exit(1)
}

if (tableName === undefined || tableName === null || tableName === "") {
	console.error('Error: --table-name is required')
	process.exit(1)
}

console.log('Integration Test: UDP to S3 Storage Flow')
console.log('=========================================')
console.log(`Target: ${host}:${port}`)
console.log(`S3 Bucket: ${bucketName}`)
console.log(`DynamoDB Table: ${tableName}`)
console.log(`Region: ${region}`)
console.log(`Packet count: ${packetCount}`)
console.log(`Timeout: ${timeout / 1000} seconds`)
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
}

const results: TestResult[] = []

/**
 * Send UDP packets
 */
const sendPackets = async (): Promise<void> => {
	console.log('Step 1: Sending UDP packets...')
	const startTime = Date.now()

	const socket = dgram.createSocket('udp4')
	let sent = 0

	for (let i = 0; i < packetCount; i++) {
		const packet = createPacketWithTimestamp(Date.now())

		await new Promise<void>((resolve, reject) => {
			socket.send(packet, port, host, (error) => {
				if (error) {
					reject(error)
				} else {
					sent++
					resolve()
				}
			})
		})

		// Small delay between packets
		await new Promise((resolve) => setTimeout(resolve, 10))
	}

	socket.close()

	const duration = Date.now() - startTime
	console.log(`  ✓ Sent ${sent} packets in ${duration}ms`)

	results.push({
		name: 'Send UDP packets',
		passed: sent === packetCount,
		message: `Sent ${sent}/${packetCount} packets`,
		duration,
	})
}

/**
 * Wait for processing
 */
const waitForProcessing = async (seconds: number): Promise<void> => {
	console.log(`\nStep 2: Waiting ${seconds} seconds for processing...`)
	await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
	console.log('  ✓ Wait complete')
}

/**
 * Check DynamoDB metadata
 */
const checkDynamoDBMetadata = async (): Promise<void> => {
	console.log('\nStep 3: Checking DynamoDB metadata...')
	const startTime = Date.now()

	try {
		const response = await dynamoClient.send(
			new GetCommand({
				TableName: tableName,
				Key: { port },
			}),
		)

		if (response.Item === undefined || response.Item === null || Object.keys(response.Item).length === 0) {
			throw new Error('Stream metadata not found in DynamoDB')
		}

		const metadata = response.Item
		console.log('  ✓ Metadata found:')
		console.log(`    - Status: ${metadata.status}`)
		console.log(`    - Last packet time: ${metadata.lastPacketTime}`)
		console.log(`    - HLS manifest: ${metadata.hlsManifestPath}`)
		console.log(`    - Raw stream: ${metadata.rawStreamPath}`)

		const duration = Date.now() - startTime
		results.push({
			name: 'Check DynamoDB metadata',
			passed: metadata.status === 'active',
			message: `Status: ${metadata.status}`,
			duration,
		})
	} catch (error) {
		const duration = Date.now() - startTime
		const message = error instanceof Error ? error.message : 'Unknown error'
		console.log(`  ✗ Error: ${message}`)

		results.push({
			name: 'Check DynamoDB metadata',
			passed: false,
			message,
			duration,
		})
	}
}

/**
 * Check S3 raw segments
 */
const checkRawSegments = async (): Promise<void> => {
	console.log('\nStep 4: Checking S3 raw segments...')
	const startTime = Date.now()

	try {
		const response = await s3Client.send(
			new ListObjectsV2Command({
				Bucket: bucketName,
				Prefix: `raw/${port}/`,
				MaxKeys: 10,
			}),
		)

		const count = response.Contents?.length ?? 0
		console.log(`  ✓ Found ${count} raw segments`)

		if (count > 0 && response.Contents) {
			console.log('  Sample segments:')
			for (const obj of response.Contents.slice(0, 3)) {
				console.log(`    - ${obj.Key} (${obj.Size} bytes)`)
			}
		}

		const duration = Date.now() - startTime
		results.push({
			name: 'Check S3 raw segments',
			passed: count > 0,
			message: `Found ${count} segments`,
			duration,
		})
	} catch (error) {
		const duration = Date.now() - startTime
		const message = error instanceof Error ? error.message : 'Unknown error'
		console.log(`  ✗ Error: ${message}`)

		results.push({
			name: 'Check S3 raw segments',
			passed: false,
			message,
			duration,
		})
	}
}

/**
 * Check HLS segments
 */
const checkHLSSegments = async (): Promise<void> => {
	console.log('\nStep 5: Checking HLS segments...')
	const startTime = Date.now()

	try {
		const profiles = ['1080p', '720p', '480p', '360p']
		let totalSegments = 0

		for (const profile of profiles) {
			const response = await s3Client.send(
				new ListObjectsV2Command({
					Bucket: bucketName,
					Prefix: `hls/${port}/${profile}/`,
					MaxKeys: 10,
				}),
			)

			const count = response.Contents?.length ?? 0
			totalSegments += count
			console.log(`  ✓ ${profile}: ${count} segments`)
		}

		const duration = Date.now() - startTime
		results.push({
			name: 'Check HLS segments',
			passed: totalSegments > 0,
			message: `Found ${totalSegments} total segments`,
			duration,
		})
	} catch (error) {
		const duration = Date.now() - startTime
		const message = error instanceof Error ? error.message : 'Unknown error'
		console.log(`  ✗ Error: ${message}`)

		results.push({
			name: 'Check HLS segments',
			passed: false,
			message,
			duration,
		})
	}
}

/**
 * Check HLS manifests
 */
const checkHLSManifests = async (): Promise<void> => {
	console.log('\nStep 6: Checking HLS manifests...')
	const startTime = Date.now()

	try {
		// Check master manifest
		const masterResponse = await s3Client.send(
			new GetObjectCommand({
				Bucket: bucketName,
				Key: `hls/${port}/master.m3u8`,
			}),
		)

		const masterContent = await masterResponse.Body?.transformToString()
		console.log('  ✓ Master manifest found')

		if (masterContent) {
			const lines = masterContent.split('\n').length
			console.log(`    - ${lines} lines`)
		}

		// Check variant playlists
		const profiles = ['1080p', '720p', '480p', '360p']
		let variantCount = 0

		for (const profile of profiles) {
			try {
				await s3Client.send(
					new GetObjectCommand({
						Bucket: bucketName,
						Key: `hls/${port}/${profile}/playlist.m3u8`,
					}),
				)
				variantCount++
			} catch {
				// Variant not found
			}
		}

		console.log(`  ✓ Found ${variantCount}/4 variant playlists`)

		const duration = Date.now() - startTime
		results.push({
			name: 'Check HLS manifests',
			passed: variantCount > 0,
			message: `Found ${variantCount}/4 variants`,
			duration,
		})
	} catch (error) {
		const duration = Date.now() - startTime
		const message = error instanceof Error ? error.message : 'Unknown error'
		console.log(`  ✗ Error: ${message}`)

		results.push({
			name: 'Check HLS manifests',
			passed: false,
			message,
			duration,
		})
	}
}

/**
 * Print test results
 */
const printResults = (): void => {
	console.log('\n' + '='.repeat(50))
	console.log('Test Results')
	console.log('='.repeat(50))

	let passed = 0
	let failed = 0

	for (const result of results) {
		const status = result.passed ? '✓ PASS' : '✗ FAIL'
		const duration = result.duration ? ` (${result.duration}ms)` : ''
		console.log(`${status} ${result.name}${duration}`)
		console.log(`       ${result.message}`)

		if (result.passed) passed++
		else failed++
	}

	console.log('\n' + '='.repeat(50))
	console.log(
		`Total: ${passed + failed} | Passed: ${passed} | Failed: ${failed}`,
	)
	console.log('='.repeat(50))

	if (failed > 0) {
		process.exit(1)
	}
}

/**
 * Main test flow
 */
const main = async (): Promise<void> => {
	try {
		await sendPackets()
		await waitForProcessing(10)
		await checkDynamoDBMetadata()
		await checkRawSegments()
		await checkHLSSegments()
		await checkHLSManifests()

		printResults()
	} catch (error) {
		console.error('\nFatal error:', error)
		process.exit(1)
	}
}

// Run the test
void main()
