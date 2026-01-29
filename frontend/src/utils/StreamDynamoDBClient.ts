import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import {
	DynamoDBDocumentClient,
	GetCommand,
	ScanCommand,
} from '@aws-sdk/lib-dynamodb'
import type {
	StreamDetailResponse,
	StreamMetadata,
	StreamSummary,
} from '../types.js'
import type { AWSConfig } from './aws-auth.js'

export type StreamListResponse = {
	streams: StreamSummary[]
}

export class StreamDynamoDBClient {
	private readonly docClient: DynamoDBDocumentClient
	private readonly tableName: string
	private readonly cloudFrontDomain: string

	constructor(
		awsConfig: AWSConfig,
		tableName: string,
		cloudFrontDomain: string,
	) {
		const client = new DynamoDBClient({
			region: awsConfig.region,
			credentials: awsConfig.credentials,
		})

		this.docClient = DynamoDBDocumentClient.from(client)
		this.tableName = tableName
		this.cloudFrontDomain = cloudFrontDomain
	}

	/**
	 * List all streams from DynamoDB
	 */
	async listStreams(): Promise<StreamListResponse> {
		try {
			const command = new ScanCommand({
				TableName: this.tableName,
			})

			const result = await this.docClient.send(command)

			const streams: StreamSummary[] = (result.Items ?? []).map((item) => {
				const metadata = item as StreamMetadata
				return {
					port: metadata.port,
					status: metadata.status,
					lastPacketTime: metadata.lastPacketTime,
					thumbnailUrl: this.buildCloudFrontUrl(metadata.lastFramePath),
				}
			})

			return { streams }
		} catch (error) {
			console.error('[StreamDynamoDBClient] Failed to list streams:', error)
			throw new Error('Failed to fetch stream list from DynamoDB')
		}
	}

	/**
	 * Get detailed information for a specific stream
	 */
	async getStreamDetail(port: number): Promise<StreamDetailResponse> {
		try {
			const command = new GetCommand({
				TableName: this.tableName,
				Key: { port },
			})

			const result = await this.docClient.send(command)

			if (!result.Item) {
				throw new Error(`Stream with port ${port} not found`)
			}

			const metadata = result.Item as StreamMetadata

			return {
				port: metadata.port,
				status: metadata.status,
				lastPacketTime: metadata.lastPacketTime,
				hlsManifestUrl: this.buildCloudFrontUrl(metadata.hlsManifestPath),
				rawStreamUrl: this.buildCloudFrontUrl(metadata.rawStreamPath),
				lastFrameUrl: this.buildCloudFrontUrl(metadata.lastFramePath),
				metadata: {
					createdAt: metadata.createdAt,
					updatedAt: metadata.updatedAt,
				},
			}
		} catch (error) {
			console.error(
				`[StreamDynamoDBClient] Failed to get stream ${port}:`,
				error,
			)
			if (error instanceof Error && error.message.includes('not found')) {
				throw error
			}
			throw new Error(`Failed to fetch stream ${port} from DynamoDB`)
		}
	}

	/**
	 * Get stream status (lightweight query with projection)
	 */
	async getStreamStatus(
		port: number,
	): Promise<{ status: string; lastPacketTime: string }> {
		try {
			const command = new GetCommand({
				TableName: this.tableName,
				Key: { port },
				ProjectionExpression: 'status, lastPacketTime',
			})

			const result = await this.docClient.send(command)

			if (!result.Item) {
				throw new Error(`Stream with port ${port} not found`)
			}

			return {
				status: result.Item.status as string,
				lastPacketTime: result.Item.lastPacketTime as string,
			}
		} catch (error) {
			console.error(
				`[StreamDynamoDBClient] Failed to get stream status for ${port}:`,
				error,
			)
			throw new Error(`Failed to fetch stream status for port ${port}`)
		}
	}

	/**
	 * Build CloudFront URL from S3 path
	 */
	private buildCloudFrontUrl(s3Path: string): string {
		// Remove s3://bucket/ prefix if present
		const path = s3Path.replace(/^s3:\/\/[^/]+\//, '')
		return `https://${this.cloudFrontDomain}/${path}`
	}
}
