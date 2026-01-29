import assert from 'node:assert'
import { describe, it } from 'node:test'
import { StreamDynamoDBClient } from './StreamDynamoDBClient.ts'
import type { AWSConfig } from './aws-auth.ts'

void describe('StreamDynamoDBClient', () => {
	const mockAwsConfig: AWSConfig = {
		region: 'us-east-1',
		credentials: {
			accessKeyId: 'test',
			secretAccessKey: 'test',
			sessionToken: 'test',
		},
	}

	const tableName = 'TestStreamMetadata'
	const cloudFrontDomain = 'test.cloudfront.net'

	void it('should build CloudFront URLs correctly', () => {
		const client = new StreamDynamoDBClient(
			mockAwsConfig,
			tableName,
			cloudFrontDomain,
		)

		// Access private method through type assertion for testing
		const buildUrl = (client as any).buildCloudFrontUrl.bind(client)

		const s3Path = 's3://bucket-name/snapshots/5000/last_frame.jpg'
		const expectedUrl =
			'https://test.cloudfront.net/snapshots/5000/last_frame.jpg'

		const result = buildUrl(s3Path)
		assert.strictEqual(result, expectedUrl)
	})

	void it('should handle paths without s3:// prefix', () => {
		const client = new StreamDynamoDBClient(
			mockAwsConfig,
			tableName,
			cloudFrontDomain,
		)
		const buildUrl = (client as any).buildCloudFrontUrl.bind(client)

		const path = 'snapshots/5000/last_frame.jpg'
		const expectedUrl =
			'https://test.cloudfront.net/snapshots/5000/last_frame.jpg'

		const result = buildUrl(path)
		assert.strictEqual(result, expectedUrl)
	})
})
