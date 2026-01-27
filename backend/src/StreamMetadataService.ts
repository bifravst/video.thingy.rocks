import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import {
	DynamoDBDocumentClient,
	GetCommand,
	PutCommand,
	UpdateCommand,
} from '@aws-sdk/lib-dynamodb'

export type StreamMetadata = {
	port: number
	status: 'active' | 'inactive'
	lastPacketTime: string // ISO 8601
	lastFramePath?: string
	hlsManifestPath?: string
	rawStreamPath?: string
	createdAt: string
	updatedAt: string
}

export type StreamMetadataServiceConfig = {
	tableName: string
	region?: string
}

export class StreamMetadataService {
	private readonly docClient: DynamoDBDocumentClient
	private readonly tableName: string

	constructor(config: StreamMetadataServiceConfig) {
		const client = new DynamoDBClient({
			region: config.region ?? 'us-east-1',
		})
		this.docClient = DynamoDBDocumentClient.from(client)
		this.tableName = config.tableName
	}

	async updateStreamStatus(
		port: number,
		status: 'active' | 'inactive',
	): Promise<void> {
		const now = new Date().toISOString()

		try {
			// Try to update existing item
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression: 'SET #status = :status, updatedAt = :updatedAt',
					ExpressionAttributeNames: {
						'#status': 'status',
					},
					ExpressionAttributeValues: {
						':status': status,
						':updatedAt': now,
					},
					ConditionExpression: 'attribute_exists(port)',
				}),
			)
			console.log(
				`[StreamMetadataService] Updated stream status for port ${port} to ${status}`,
			)
		} catch (error: unknown) {
			// If item doesn't exist, create it
			if (
				error instanceof Error &&
				error.name === 'ConditionalCheckFailedException'
			) {
				await this.createStreamMetadata(port, status)
			} else {
				console.error(
					`[StreamMetadataService] Error updating stream status for port ${port}:`,
					error,
				)
				throw error
			}
		}
	}

	async updateLastPacketTime(port: number, timestamp: Date): Promise<void> {
		const isoTimestamp = timestamp.toISOString()
		const now = new Date().toISOString()

		try {
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression:
						'SET lastPacketTime = :lastPacketTime, updatedAt = :updatedAt',
					ExpressionAttributeValues: {
						':lastPacketTime': isoTimestamp,
						':updatedAt': now,
					},
				}),
			)
			console.log(
				`[StreamMetadataService] Updated last packet time for port ${port}`,
			)
		} catch (error) {
			console.error(
				`[StreamMetadataService] Error updating last packet time for port ${port}:`,
				error,
			)
			throw error
		}
	}

	async updateLastFramePath(port: number, s3Path: string): Promise<void> {
		const now = new Date().toISOString()

		try {
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression:
						'SET lastFramePath = :lastFramePath, updatedAt = :updatedAt',
					ExpressionAttributeValues: {
						':lastFramePath': s3Path,
						':updatedAt': now,
					},
				}),
			)
			console.log(
				`[StreamMetadataService] Updated last frame path for port ${port}`,
			)
		} catch (error) {
			console.error(
				`[StreamMetadataService] Error updating last frame path for port ${port}:`,
				error,
			)
			throw error
		}
	}

	async updateHlsManifestPath(port: number, s3Path: string): Promise<void> {
		const now = new Date().toISOString()

		try {
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression:
						'SET hlsManifestPath = :hlsManifestPath, updatedAt = :updatedAt',
					ExpressionAttributeValues: {
						':hlsManifestPath': s3Path,
						':updatedAt': now,
					},
				}),
			)
			console.log(
				`[StreamMetadataService] Updated HLS manifest path for port ${port}`,
			)
		} catch (error) {
			console.error(
				`[StreamMetadataService] Error updating HLS manifest path for port ${port}:`,
				error,
			)
			throw error
		}
	}

	async updateRawStreamPath(port: number, s3Path: string): Promise<void> {
		const now = new Date().toISOString()

		try {
			await this.docClient.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: { port },
					UpdateExpression:
						'SET rawStreamPath = :rawStreamPath, updatedAt = :updatedAt',
					ExpressionAttributeValues: {
						':rawStreamPath': s3Path,
						':updatedAt': now,
					},
				}),
			)
			console.log(
				`[StreamMetadataService] Updated raw stream path for port ${port}`,
			)
		} catch (error) {
			console.error(
				`[StreamMetadataService] Error updating raw stream path for port ${port}:`,
				error,
			)
			throw error
		}
	}

	async getStreamMetadata(port: number): Promise<StreamMetadata | null> {
		try {
			const result = await this.docClient.send(
				new GetCommand({
					TableName: this.tableName,
					Key: { port },
				}),
			)
			return result.Item !== undefined ? (result.Item as StreamMetadata) : null
		} catch (error) {
			console.error(
				`[StreamMetadataService] Error getting stream metadata for port ${port}:`,
				error,
			)
			throw error
		}
	}

	private async createStreamMetadata(
		port: number,
		status: 'active' | 'inactive',
	): Promise<void> {
		const now = new Date().toISOString()

		const metadata: StreamMetadata = {
			port,
			status,
			lastPacketTime: now,
			createdAt: now,
			updatedAt: now,
		}

		try {
			await this.docClient.send(
				new PutCommand({
					TableName: this.tableName,
					Item: metadata,
				}),
			)
			console.log(
				`[StreamMetadataService] Created stream metadata for port ${port}`,
			)
		} catch (error) {
			console.error(
				`[StreamMetadataService] Error creating stream metadata for port ${port}:`,
				error,
			)
			throw error
		}
	}
}
