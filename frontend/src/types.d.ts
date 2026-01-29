declare module '*.css'

// DynamoDB Stream Metadata (matches backend schema)
export type StreamMetadata = {
	port: number
	status: 'active' | 'inactive'
	lastPacketTime: string // ISO 8601 timestamp
	lastFramePath: string // S3 path to snapshot
	hlsManifestPath: string // S3 path to master.m3u8
	rawStreamPath: string // S3 path prefix for raw segments
	createdAt: string // ISO 8601 timestamp
	updatedAt: string // ISO 8601 timestamp
}

// Stream summary for list view
export type StreamSummary = {
	port: number
	status: 'active' | 'inactive'
	lastPacketTime: string
	thumbnailUrl: string // CloudFront URL for snapshot
}

// Stream detail for player view
export type StreamDetailResponse = {
	port: number
	status: 'active' | 'inactive'
	lastPacketTime: string
	hlsManifestUrl: string // CloudFront URL for HLS manifest
	rawStreamUrl: string // CloudFront URL for raw stream
	lastFrameUrl: string // CloudFront URL for snapshot
	metadata: {
		createdAt: string
		updatedAt: string
	}
}
