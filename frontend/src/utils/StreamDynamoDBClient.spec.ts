import assert from 'node:assert'
import { describe, it } from 'node:test'
import type { StreamMetadata } from '../types.js'
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

	void describe('buildCloudFrontUrl', () => {
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

	void describe('buildHlsManifestUrl', () => {
		void it('should construct master.m3u8 URL for new streams', () => {
			const client = new StreamDynamoDBClient(
				mockAwsConfig,
				tableName,
				cloudFrontDomain,
			)
			const buildHlsUrl = (client as any).buildHlsManifestUrl.bind(client)

			const port = 5000
			const metadata: StreamMetadata = {
				port,
				status: 'active',
				lastPacketTime: new Date().toISOString(),
				lastFramePath: 'snapshots/5000/last_frame.jpg',
				hlsManifestPath: '',
				rawStreamPath: '',
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			}

			const result = buildHlsUrl(port, metadata)
			const expectedUrl = 'https://test.cloudfront.net/hls/5000/master.m3u8'

			assert.strictEqual(result, expectedUrl)
		})

		void it('should construct master.m3u8 URL for different ports', () => {
			const client = new StreamDynamoDBClient(
				mockAwsConfig,
				tableName,
				cloudFrontDomain,
			)
			const buildHlsUrl = (client as any).buildHlsManifestUrl.bind(client)

			const testPorts = [5000, 5001, 5002, 6000]

			for (const port of testPorts) {
				const metadata: StreamMetadata = {
					port,
					status: 'active',
					lastPacketTime: new Date().toISOString(),
					lastFramePath: '',
					hlsManifestPath: '',
					rawStreamPath: '',
					createdAt: new Date().toISOString(),
					updatedAt: new Date().toISOString(),
				}

				const result = buildHlsUrl(port, metadata)
				const expectedUrl = `https://test.cloudfront.net/hls/${port}/master.m3u8`

				assert.strictEqual(result, expectedUrl, `Failed for port ${port}`)
			}
		})

		void it('should fallback to 1080p profile playlist for legacy streams', () => {
			const client = new StreamDynamoDBClient(
				mockAwsConfig,
				tableName,
				cloudFrontDomain,
			)
			const buildHlsUrl = (client as any).buildHlsManifestUrl.bind(client)

			const port = 5000
			const metadata: StreamMetadata = {
				port,
				status: 'active',
				lastPacketTime: new Date().toISOString(),
				lastFramePath: '',
				hlsManifestPath: 's3://bucket/hls/5000/1080p/playlist.m3u8',
				rawStreamPath: '',
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			}

			const result = buildHlsUrl(port, metadata)
			const expectedUrl =
				'https://test.cloudfront.net/hls/5000/1080p/playlist.m3u8'

			assert.strictEqual(result, expectedUrl)
		})

		void it('should fallback to 720p profile playlist for legacy streams', () => {
			const client = new StreamDynamoDBClient(
				mockAwsConfig,
				tableName,
				cloudFrontDomain,
			)
			const buildHlsUrl = (client as any).buildHlsManifestUrl.bind(client)

			const port = 5001
			const metadata: StreamMetadata = {
				port,
				status: 'active',
				lastPacketTime: new Date().toISOString(),
				lastFramePath: '',
				hlsManifestPath: 'hls/5001/720p/playlist.m3u8',
				rawStreamPath: '',
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			}

			const result = buildHlsUrl(port, metadata)
			const expectedUrl =
				'https://test.cloudfront.net/hls/5001/720p/playlist.m3u8'

			assert.strictEqual(result, expectedUrl)
		})

		void it('should fallback to 480p profile playlist for legacy streams', () => {
			const client = new StreamDynamoDBClient(
				mockAwsConfig,
				tableName,
				cloudFrontDomain,
			)
			const buildHlsUrl = (client as any).buildHlsManifestUrl.bind(client)

			const port = 5002
			const metadata: StreamMetadata = {
				port,
				status: 'active',
				lastPacketTime: new Date().toISOString(),
				lastFramePath: '',
				hlsManifestPath: 'hls/5002/480p/playlist.m3u8',
				rawStreamPath: '',
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			}

			const result = buildHlsUrl(port, metadata)
			const expectedUrl =
				'https://test.cloudfront.net/hls/5002/480p/playlist.m3u8'

			assert.strictEqual(result, expectedUrl)
		})

		void it('should fallback to 360p profile playlist for legacy streams', () => {
			const client = new StreamDynamoDBClient(
				mockAwsConfig,
				tableName,
				cloudFrontDomain,
			)
			const buildHlsUrl = (client as any).buildHlsManifestUrl.bind(client)

			const port = 5003
			const metadata: StreamMetadata = {
				port,
				status: 'active',
				lastPacketTime: new Date().toISOString(),
				lastFramePath: '',
				hlsManifestPath: 'hls/5003/360p/playlist.m3u8',
				rawStreamPath: '',
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			}

			const result = buildHlsUrl(port, metadata)
			const expectedUrl =
				'https://test.cloudfront.net/hls/5003/360p/playlist.m3u8'

			assert.strictEqual(result, expectedUrl)
		})

		void it('should use master.m3u8 when hlsManifestPath points to master playlist', () => {
			const client = new StreamDynamoDBClient(
				mockAwsConfig,
				tableName,
				cloudFrontDomain,
			)
			const buildHlsUrl = (client as any).buildHlsManifestUrl.bind(client)

			const port = 5000
			const metadata: StreamMetadata = {
				port,
				status: 'active',
				lastPacketTime: new Date().toISOString(),
				lastFramePath: '',
				hlsManifestPath: 's3://bucket/hls/5000/master.m3u8',
				rawStreamPath: '',
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			}

			const result = buildHlsUrl(port, metadata)
			const expectedUrl = 'https://test.cloudfront.net/hls/5000/master.m3u8'

			assert.strictEqual(result, expectedUrl)
		})

		void it('should use master.m3u8 when hlsManifestPath is undefined', () => {
			const client = new StreamDynamoDBClient(
				mockAwsConfig,
				tableName,
				cloudFrontDomain,
			)
			const buildHlsUrl = (client as any).buildHlsManifestUrl.bind(client)

			const port = 5000
			const metadata: StreamMetadata = {
				port,
				status: 'active',
				lastPacketTime: new Date().toISOString(),
				lastFramePath: '',
				hlsManifestPath: '',
				rawStreamPath: '',
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			}

			const result = buildHlsUrl(port, metadata)
			const expectedUrl = 'https://test.cloudfront.net/hls/5000/master.m3u8'

			assert.strictEqual(result, expectedUrl)
		})
	})
})
