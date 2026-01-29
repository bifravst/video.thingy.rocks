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
					status: metadata.status ?? 'inactive',
					lastPacketTime: metadata.lastPacketTime ?? new Date().toISOString(),
					thumbnailUrl:
						this.buildCloudFrontUrl(metadata.lastFramePath) ||
						'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="300" height="200"%3E%3Crect fill="%23ddd" width="300" height="200"/%3E%3Ctext x="50%25" y="50%25" text-anchor="middle" dy=".3em" fill="%23999"%3ENo Image%3C/text%3E%3C/svg%3E',
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

			// Construct master playlist URL
			// Format: hls/{port}/master.m3u8
			// Fallback to 1080p profile playlist for backward compatibility
			const hlsManifestUrl = this.buildHlsManifestUrl(port, metadata)

			return {
				port: metadata.port,
				status: metadata.status ?? 'inactive',
				lastPacketTime: metadata.lastPacketTime ?? new Date().toISOString(),
				hlsManifestUrl,
				rawStreamUrl: this.buildCloudFrontUrl(metadata.rawStreamPath) || '',
				lastFrameUrl:
					this.buildCloudFrontUrl(metadata.lastFramePath) ||
					'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="800" height="600"%3E%3Crect fill="%23333" width="800" height="600"/%3E%3Ctext x="50%25" y="50%25" text-anchor="middle" dy=".3em" fill="%23999" font-size="24"%3ENo Image Available%3C/text%3E%3C/svg%3E',
				metadata: {
					createdAt: metadata.createdAt ?? new Date().toISOString(),
					updatedAt: metadata.updatedAt ?? new Date().toISOString(),
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
	private buildCloudFrontUrl(s3Path: string | undefined): string {
		// Handle undefined or empty paths
		if (s3Path === undefined || s3Path === null) {
			return ''
		}

		// Remove s3://bucket/ prefix if present
		const path = s3Path.replace(/^s3:\/\/[^/]+\//, '')
		return `https://${this.cloudFrontDomain}/${path}`
	}

	/**
	 * Build HLS manifest URL for a stream
	 * Constructs master.m3u8 URL with fallback to 1080p profile playlist
	 * for backward compatibility
	 */
	private buildHlsManifestUrl(port: number, metadata: StreamMetadata): string {
		// Primary: Use master playlist for multi-bitrate streaming
		// Format: hls/{port}/master.m3u8
		const masterPlaylistPath = `hls/${port}/master.m3u8`
		const masterPlaylistUrl = `https://${this.cloudFrontDomain}/${masterPlaylistPath}`

		// Fallback: If hlsManifestPath exists in metadata and points to a profile-specific playlist,
		// use it for backward compatibility with streams that don't have master playlists yet
		if (metadata.hlsManifestPath) {
			const legacyUrl = this.buildCloudFrontUrl(metadata.hlsManifestPath)
			// If the legacy path contains a profile (e.g., 1080p/playlist.m3u8),
			// it means this is an old stream without master playlist
			if (
				legacyUrl.includes('/1080p/') ||
				legacyUrl.includes('/720p/') ||
				legacyUrl.includes('/480p/') ||
				legacyUrl.includes('/360p/')
			) {
				return legacyUrl
			}
		}

		// Default to master playlist URL
		// Note: The player will handle 404 errors if master playlist doesn't exist yet
		return masterPlaylistUrl
	}
}
