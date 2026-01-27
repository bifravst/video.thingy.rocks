# Design Document: NTN Video Streaming

## Overview

This design document describes a serverless video streaming system for
Non-Terrestrial Network Connected Cameras (NTNCam devices) deployed on AWS using
CDK infrastructure written in TypeScript. The system receives UDP video streams
from multiple cameras on ports 5000-5009, processes them for both raw and
adaptive bitrate delivery, and provides a web interface for viewing.

The architecture leverages AWS serverless components to minimize operational
overhead while handling intermittent connectivity patterns typical of
non-terrestrial networks. Key design decisions include:

- **EC2-based UDP ingestion** instead of MediaLive (which doesn't support raw
  UDP)
- **FFmpeg for transcoding** to HLS adaptive bitrate formats
- **S3 + CloudFront** for scalable video segment delivery
- **DynamoDB** for stream metadata and state management
- **React-based web frontend** hosted on S3/CloudFront with direct DynamoDB
  access

The system treats camera offline periods as normal operation, preserving the
last received frame and automatically resuming processing when cameras begin
sending data again.

## Architecture

### High-Level Architecture

```
NTNCam Devices (UDP 5000-5009)
         |
         v
    [EC2 UDP Listener Fleet]
         |
         v
    [FFmpeg Transcoding]
         |
         +---> [S3: Raw Segments]
         |
         +---> [S3: HLS Segments (multiple bitrates)]
         |
         v
    [DynamoDB: Stream Metadata]
         |
         v
    [CloudFront Distribution]
         |
         v
    [Web Frontend (React)]
```

### Component Architecture

#### 1. UDP Ingestion Layer (EC2 Auto Scaling Group)

- **EC2 instances** running Node.js UDP listeners on ports 5000-5009
- **Auto Scaling Group** with target tracking based on network throughput
- **Network Load Balancer** for health checks and traffic distribution
- Each instance listens on all 10 ports simultaneously
- Incoming packets are buffered and written to local disk in chunks

**Rationale**: MediaLive requires RTP push (not raw UDP) and doesn't support the
port-based identification scheme. EC2 provides the flexibility needed for raw
UDP ingestion across 10 ports.

#### 2. Stream Processing Layer (FFmpeg on EC2)

- **FFmpeg processes** running on the same EC2 instances as UDP listeners
- Separate FFmpeg process per active stream (port)
- Transcoding pipeline:
  - Input: Raw UDP stream buffer
  - Output 1: Raw stream segments (original bitrate) → S3
  - Output 2: HLS adaptive bitrate (1080p, 720p, 480p, 360p) → S3
- Segments are 6 seconds duration for balance between latency and efficiency
- Reception timestamps embedded in segment metadata

**Rationale**: Co-locating FFmpeg with UDP listeners minimizes latency and
avoids network transfer of raw video data. FFmpeg provides robust HLS packaging
with adaptive bitrate support.

#### 3. Storage Layer (S3)

**Bucket Structure**:

```
ntn-video-streams/
├── raw/
│   └── {port}/
│       └── {timestamp}.ts
├── hls/
│   └── {port}/
│       ├── master.m3u8
│       ├── 1080p/
│       │   ├── playlist.m3u8
│       │   └── segment_{n}.ts
│       ├── 720p/
│       ├── 480p/
│       └── 360p/
└── snapshots/
    └── {port}/
        └── last_frame.jpg
```

**Lifecycle Policies**:

- Delete objects older than 30 days
- Transition to Intelligent-Tiering after 7 days (optional optimization)

**Rationale**: S3 provides durable, scalable storage with built-in lifecycle
management. The hierarchical structure supports both raw and adaptive bitrate
access patterns.

#### 4. Metadata Layer (DynamoDB)

**Table: StreamMetadata**

- Partition Key: `port` (number, 5000-5009)
- Attributes:
  - `status`: "active" | "inactive"
  - `lastPacketTime`: timestamp (ISO 8601)
  - `lastFramePath`: S3 path to snapshot
  - `hlsManifestPath`: S3 path to master.m3u8
  - `rawStreamPath`: S3 path prefix for raw segments
  - `createdAt`: timestamp
  - `updatedAt`: timestamp

**Table: StreamEvents** (optional, for monitoring)

- Partition Key: `port` (number)
- Sort Key: `timestamp` (number)
- Attributes:
  - `eventType`: "stream_start" | "stream_stop" | "packet_received"
  - `metadata`: JSON object

**Rationale**: DynamoDB provides low-latency access to stream state for the web
frontend and monitoring systems. The simple key structure supports efficient
queries by port number.

#### 4. Control Plane (Direct DynamoDB Access)

The web frontend accesses DynamoDB directly using AWS SDK with appropriate IAM
credentials (via Cognito Identity Pool for unauthenticated access or Cognito
User Pool for authenticated access).

**DynamoDB Access Pattern**:

- Frontend uses AWS SDK for JavaScript to query DynamoDB
- Cognito Identity Pool provides temporary AWS credentials
- IAM role attached to identity pool allows read-only access to StreamMetadata
  table
- No Lambda or API Gateway needed for simple CRUD operations

**Queries**:

```typescript
// List all streams
const params = {
  TableName: "StreamMetadata",
};
const result = await dynamodb.scan(params).promise();

// Get specific stream
const params = {
  TableName: "StreamMetadata",
  Key: { port: 5000 },
};
const result = await dynamodb.getItem(params).promise();
```

**Rationale**: Direct DynamoDB access eliminates the need for Lambda functions
and API Gateway, reducing latency and cost. Cognito provides secure, temporary
credentials for frontend access. This is appropriate for read-only operations
where the frontend doesn't need complex business logic.

#### 5. Web Frontend (React SPA)

**Components**:

- **StreamList**: Displays grid of all streams with thumbnails and status
- **StreamPlayer**: Video player with raw/adaptive bitrate toggle
- **StreamInfo**: Displays metadata (port, status, timestamp, bitrate)
- **VideoPlayer**: Wrapper around video.js or hls.js for HLS playback

**State Management**:

- React Context for global stream list
- AWS SDK for JavaScript to query DynamoDB directly
- Polling every 5 seconds for status updates via DynamoDB queries
- WebSocket connection for real-time updates (optional enhancement)

**Video Playback**:

- **hls.js** library for HLS adaptive bitrate in browsers
- Automatic quality switching based on bandwidth
- Manual quality selection option
- Fallback to last frame image when stream is inactive

**Hosting**:

- Static files hosted in S3
- CloudFront distribution for global delivery
- HTTPS enforced

**Rationale**: React provides a modern, component-based architecture. hls.js is
the industry standard for HLS playback in browsers. S3+CloudFront hosting is
cost-effective and scalable.

## Components and Interfaces

### UDP Listener Service (Node.js)

**Interface**:

```typescript
interface UDPListenerConfig {
  portRange: { start: number; end: number };
  bufferSize: number;
  flushInterval: number; // milliseconds
  outputDirectory: string;
}

class UDPListener {
  constructor(config: UDPListenerConfig);
  start(): Promise<void>;
  stop(): Promise<void>;
  getActiveStreams(): number[]; // Returns list of active ports
}

interface PacketHandler {
  onPacket(port: number, data: Buffer, timestamp: Date): void;
  onStreamStart(port: number): void;
  onStreamStop(port: number, inactivityDuration: number): void;
}
```

**Implementation Details**:

- Uses Node.js `dgram` module for UDP sockets
- One socket per port (10 sockets total)
- Packets buffered in memory up to `bufferSize` or `flushInterval`
- Flushed to disk as `.ts` (MPEG-TS) files
- Monitors inactivity: if no packets for 1 minute, marks stream as inactive

### FFmpeg Transcoding Service

**Interface**:

```typescript
interface TranscodingConfig {
  inputPath: string;
  outputPaths: {
    raw: string;
    hls: string;
  };
  hlsProfiles: BitrateProfile[];
  segmentDuration: number; // seconds
}

interface BitrateProfile {
  name: string; // "1080p", "720p", etc.
  resolution: string; // "1920x1080"
  videoBitrate: string; // "5000k"
  audioBitrate: string; // "128k"
}

class FFmpegTranscoder {
  constructor(config: TranscodingConfig);
  start(): Promise<void>;
  stop(): Promise<void>;
  getStatus(): TranscodingStatus;
}

interface TranscodingStatus {
  isRunning: boolean;
  currentSegment: number;
  lastError?: string;
}
```

**FFmpeg Command Template**:

```bash
ffmpeg -i pipe:0 \
  -c:v copy -f segment -segment_time 6 -segment_format mpegts \
  s3://bucket/raw/{port}/%Y%m%d_%H%M%S.ts \
  -filter_complex "[0:v]split=4[v1][v2][v3][v4]" \
  -map "[v1]" -s 1920x1080 -b:v 5000k -c:v libx264 -preset fast \
    -f hls -hls_time 6 -hls_list_size 10 -hls_flags delete_segments \
    s3://bucket/hls/{port}/1080p/playlist.m3u8 \
  -map "[v2]" -s 1280x720 -b:v 3000k -c:v libx264 -preset fast \
    -f hls -hls_time 6 -hls_list_size 10 -hls_flags delete_segments \
    s3://bucket/hls/{port}/720p/playlist.m3u8 \
  -map "[v3]" -s 854x480 -b:v 1500k -c:v libx264 -preset fast \
    -f hls -hls_time 6 -hls_list_size 10 -hls_flags delete_segments \
    s3://bucket/hls/{port}/480p/playlist.m3u8 \
  -map "[v4]" -s 640x360 -b:v 800k -c:v libx264 -preset fast \
    -f hls -hls_time 6 -hls_list_size 10 -hls_flags delete_segments \
    s3://bucket/hls/{port}/360p/playlist.m3u8 \
  -vf "select='eq(pict_type,I)'" -vsync vfr -frames:v 1 \
    s3://bucket/snapshots/{port}/last_frame.jpg
```

### Stream Metadata Service (Lambda)

**Interface**:

```typescript
interface StreamMetadata {
  port: number;
  status: "active" | "inactive";
  lastPacketTime: string; // ISO 8601
  lastFramePath: string;
  hlsManifestPath: string;
  rawStreamPath: string;
  createdAt: string;
  updatedAt: string;
}

interface StreamMetadataService {
  listStreams(): Promise<StreamMetadata[]>;
  getStream(port: number): Promise<StreamMetadata | null>;
  updateStreamStatus(
    port: number,
    status: "active" | "inactive",
  ): Promise<void>;
  updateLastPacketTime(port: number, timestamp: Date): Promise<void>;
}
```

**DynamoDB Operations**:

- `listStreams()`: Scan operation
- `getStream()`: GetItem operation
- `updateStreamStatus()`: UpdateItem operation (called by EC2 instances)
- `updateLastPacketTime()`: UpdateItem operation (called by EC2 instances)

**Note**: The web frontend performs read-only operations (listStreams,
getStream) directly using AWS SDK. The EC2 instances perform write operations
(updateStreamStatus, updateLastPacketTime).

### Web Frontend DynamoDB Client

**Interface**:

```typescript
interface StreamListResponse {
  streams: StreamSummary[];
}

interface StreamSummary {
  port: number;
  status: "active" | "inactive";
  lastPacketTime: string;
  thumbnailUrl: string;
}

interface StreamDetailResponse {
  port: number;
  status: "active" | "inactive";
  lastPacketTime: string;
  hlsManifestUrl: string;
  rawStreamUrl: string;
  lastFrameUrl: string;
  metadata: {
    createdAt: string;
    updatedAt: string;
  };
}

class StreamDynamoDBClient {
  private dynamodb: AWS.DynamoDB.DocumentClient;

  constructor(credentials: AWS.Credentials) {
    this.dynamodb = new AWS.DynamoDB.DocumentClient({
      credentials,
      region: "us-east-1",
    });
  }

  async listStreams(): Promise<StreamListResponse> {
    const params = {
      TableName: "StreamMetadata",
    };
    const result = await this.dynamodb.scan(params).promise();
    return {
      streams: result.Items as StreamSummary[],
    };
  }

  async getStreamDetail(port: number): Promise<StreamDetailResponse> {
    const params = {
      TableName: "StreamMetadata",
      Key: { port },
    };
    const result = await this.dynamodb.get(params).promise();
    return result.Item as StreamDetailResponse;
  }

  async getStreamStatus(
    port: number,
  ): Promise<{ status: string; lastPacketTime: string }> {
    const params = {
      TableName: "StreamMetadata",
      Key: { port },
      ProjectionExpression: "status, lastPacketTime",
    };
    const result = await this.dynamodb.get(params).promise();
    return result.Item as { status: string; lastPacketTime: string };
  }
}
```

## Data Models

### Stream State Machine

```
[Unknown] --first packet--> [Active]
[Active] --1 minute no packets--> [Inactive]
[Inactive] --new packet--> [Active]
[Active] --30 days--> [Expired/Deleted]
[Inactive] --30 days--> [Expired/Deleted]
```

### Video Segment Model

```typescript
interface VideoSegment {
  port: number;
  segmentNumber: number;
  timestamp: Date; // Reception timestamp
  duration: number; // seconds
  s3Path: string;
  format: "raw" | "hls";
  profile?: string; // For HLS: "1080p", "720p", etc.
  size: number; // bytes
}
```

### HLS Manifest Model

```typescript
interface HLSManifest {
  port: number;
  masterPlaylistUrl: string;
  variantPlaylists: {
    profile: string;
    resolution: string;
    bandwidth: number;
    playlistUrl: string;
  }[];
  lastUpdated: Date;
}
```

### Snapshot Model

```typescript
interface Snapshot {
  port: number;
  captureTime: Date; // Reception timestamp
  s3Path: string;
  format: "jpeg";
  resolution: string;
  size: number;
}
```

## CDK Infrastructure

### Stack Structure

```typescript
class NTNVideoStreamingStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // VPC for EC2 instances
    const vpc = new ec2.Vpc(this, 'StreamingVPC', {
      maxAzs: 2,
      natGateways: 1
    });

    // S3 buckets
    const videoBucket = new s3.Bucket(this, 'VideoBucket', {
      lifecycleRules: [{
        expiration: cdk.Duration.days(30)
      }],
      cors: [/* CORS config */]
    });

    // DynamoDB table
    const streamTable = new dynamodb.Table(this, 'StreamMetadata', {
      partitionKey: { name: 'port', type: dynamodb.AttributeType.NUMBER },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST
    });

    // Cognito Identity Pool for unauthenticated access
    const identityPool = new cognito.CfnIdentityPool(this, 'StreamViewerIdentityPool', {
      allowUnauthenticatedIdentities: true
    });

    // IAM role for unauthenticated users (read-only DynamoDB access)
    const unauthRole = new iam.Role(this, 'UnauthRole', {
      assumedBy: new iam.FederatedPrincipal(
        'cognito-identity.amazonaws.com',
        {
          StringEquals: {
            'cognito-identity.amazonaws.com:aud': identityPool.ref
          },
          'ForAnyValue:StringLike': {
            'cognito-identity.amazonaws.com:amr': 'unauthenticated'
          }
        },
        'sts:AssumeRoleWithWebIdentity'
      )
    });

    // Grant read-only access to DynamoDB
    streamTable.grantReadData(unauthRole);

    // EC2 Auto Scaling Group for UDP listeners
    const asg = new autoscaling.AutoScalingGroup(this, 'UDPListenerASG', {
      vpc,
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.C5, ec2.InstanceSize.XLARGE),
      machineImage: ec2.MachineImage.latestAmazonLinux2(),
      minCapacity: 2,
      maxCapacity: 10,
      userData: /* User data script */
    });

    // Security group: allow UDP 5000-5009
    asg.connections.allowFromAnyIpv4(ec2.Port.udpRange(5000, 5009));

    // CloudFront distribution
    const distribution = new cloudfront.Distribution(this, 'StreamingDistribution', {
      defaultBehavior: {
        origin: new origins.S3Origin(videoBucket),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS
      }
    });

    // Frontend bucket
    const frontendBucket = new s3.Bucket(this, 'FrontendBucket', {
      websiteIndexDocument: 'index.html',
      publicReadAccess: true
    });

    // Outputs
    new cdk.CfnOutput(this, 'IdentityPoolId', {
      value: identityPool.ref
    });
    new cdk.CfnOutput(this, 'DynamoDBTableName', {
      value: streamTable.tableName
    });
    new cdk.CfnOutput(this, 'CloudFrontURL', {
      value: distribution.distributionDomainName
    });
  }
}
```

### IAM Roles and Policies

**EC2 Instance Role**:

- S3: PutObject, GetObject on video bucket
- DynamoDB: PutItem, UpdateItem, GetItem on stream table
- CloudWatch: PutMetricData, PutLogEvents

**Cognito Unauthenticated Role** (for web frontend):

- DynamoDB: GetItem, Scan on stream table (read-only)
- S3: GetObject on video bucket via CloudFront (no direct S3 access needed)

**Frontend (CloudFront)**:

- S3: GetObject on video bucket (via OAI)
- S3: GetObject on frontend bucket

## Correctness Properties

A property is a characteristic or behavior that should hold true across all
valid executions of a system—essentially, a formal statement about what the
system should do. Properties serve as the bridge between human-readable
specifications and machine-verifiable correctness guarantees.

### Property 1: Packet Buffering and Forwarding

_For any_ UDP packet received on any port in the range 5000-5009, the
Video_Ingestion_Service should buffer the packet and forward it to the
Stream_Processing_Service with the port number as the Stream_Identifier.

**Validates: Requirements 1.2, 1.4, 1.5**

### Property 2: Concurrent Stream Handling

_For any_ set of concurrent UDP streams on different ports, the
Video_Ingestion_Service should process all streams simultaneously without
interference between streams.

**Validates: Requirements 1.3, 6.1**

### Property 3: Dual Stream Output

_For any_ video stream processed by the Stream_Processing_Service, both a raw
stream (identical to input) and multiple HLS bitrate variants should be
produced.

**Validates: Requirements 2.2, 2.3**

### Property 4: HLS Format Validity

_For any_ transcoded stream output, the HLS manifest files (master.m3u8 and
variant playlists) should be valid and parseable according to the HLS
specification.

**Validates: Requirements 2.4**

### Property 5: Error Isolation

_For any_ stream that encounters a processing error, other concurrent streams
should continue processing without interruption.

**Validates: Requirements 2.5, 6.4**

### Property 6: Segment Storage and Accessibility

_For any_ processed video segment, it should be stored in S3 and accessible via
CloudFront with the correct path structure.

**Validates: Requirements 3.1**

### Property 7: Concurrent Viewer Support

_For any_ stream, multiple viewers should be able to request and receive the
stream simultaneously without errors.

**Validates: Requirements 3.3**

### Property 8: Reception Timestamp Embedding

_For any_ transcoded video segment, the Reception_Timestamp metadata should be
embedded and retrievable.

**Validates: Requirements 3.5**

### Property 9: Stream List Display

_For any_ set of known streams, the Web_Frontend should display all streams with
their Stream_Identifiers, online status, and metadata.

**Validates: Requirements 4.1, 4.5**

### Property 10: Adaptive Bitrate Quality Switching

_For any_ stream in adaptive bitrate mode, when bandwidth changes are simulated,
the player should switch to an appropriate quality level.

**Validates: Requirements 4.4, 5.2, 5.3**

### Property 11: Stream Auto-Detection

_For any_ new stream that begins sending packets on a port in the range, the
system should automatically detect and begin processing the stream without
manual configuration.

**Validates: Requirements 6.2**

### Property 12: Last Frame Preservation

_For any_ stream that stops sending packets, the system should preserve the last
received frame and make it available for display.

**Validates: Requirements 6.3, 11.3**

### Property 13: Stream Resumption

_For any_ stream that resumes sending packets after being offline, the system
should automatically resume processing and the Web_Frontend should transition
from still image to live video.

**Validates: Requirements 11.2, 11.7**

### Property 14: Active Stream Count Accuracy

_For any_ sequence of stream start and stop events, the system should maintain
an accurate count of active streams by port number.

**Validates: Requirements 6.5**

### Property 15: Stream Metrics Emission

_For any_ active stream, the system should emit metrics including packet loss
rate and bitrate.

**Validates: Requirements 8.1**

### Property 16: Stream Status API

_For any_ request to the stream status API, it should return accurate status
information for all active streams.

**Validates: Requirements 8.4**

### Property 17: Malformed Packet Handling

_For any_ malformed UDP packet received, the system should discard it and
continue processing subsequent valid packets without interruption.

**Validates: Requirements 9.1**

## Error Handling

### UDP Ingestion Errors

**Malformed Packets**:

- Detection: Validate packet structure before buffering
- Action: Discard packet, log warning (not error), continue processing
- Recovery: No recovery needed, next packet processed normally

**Buffer Overflow**:

- Detection: Monitor buffer size before accepting packets
- Action: Drop oldest packets (FIFO), log warning
- Recovery: Resume normal buffering when space available

**Port Binding Failures**:

- Detection: Socket creation/binding errors on startup
- Action: Log error, retry binding up to 3 times with exponential backoff
- Recovery: If all retries fail, mark port as unavailable and continue with
  other ports

### Transcoding Errors

**FFmpeg Process Crash**:

- Detection: Process exit with non-zero code
- Action: Log error with stream context, restart FFmpeg process for that stream
- Recovery: Retry up to 3 times, if all fail, mark stream as failed and alert

**Invalid Input Format**:

- Detection: FFmpeg reports unsupported codec or format
- Action: Log error, attempt to process with different FFmpeg parameters
- Recovery: If recovery fails, store raw stream only without transcoding

**S3 Upload Failures**:

- Detection: S3 PutObject returns error
- Action: Buffer segments in memory (up to 60 seconds), retry upload
- Recovery: If memory buffer full, drop oldest segments and log error

### Storage Errors

**S3 Service Unavailable**:

- Detection: S3 API returns 503 or timeout
- Action: Buffer in memory, retry with exponential backoff
- Recovery: Resume normal operation when S3 available

**Insufficient Storage**:

- Detection: S3 quota exceeded or PutObject fails with storage error
- Action: Trigger emergency cleanup of oldest segments
- Recovery: Resume normal operation after cleanup

### Frontend Errors

**Stream Not Found**:

- Detection: DynamoDB query returns no item
- Action: Display "Stream not available" message to user
- Recovery: User can retry or select different stream

**Playback Errors**:

- Detection: hls.js reports playback error
- Action: Attempt to reload stream, fall back to lower quality
- Recovery: If all qualities fail, display last frame snapshot

**Network Interruption**:

- Detection: Loss of connectivity to DynamoDB or CloudFront
- Action: Display "Connection lost" message, attempt to re-establish connection
  every 5 seconds
- Recovery: Resume playback when connection restored

### Monitoring and Alerting

**CloudWatch Alarms**:

- High packet loss rate (>5%) for any stream
- FFmpeg process failures (>3 in 5 minutes)
- S3 upload failures (>10 in 5 minutes)
- DynamoDB throttling errors
- EC2 instance CPU >80% for 5 minutes

**Logging Strategy**:

- INFO: Stream start/stop, normal state transitions
- WARN: Packet loss, buffer pressure, retry attempts
- ERROR: Process crashes, unrecoverable failures, security violations
- All logs include: timestamp, port number, stream identifier, context

## Testing Strategy

### Dual Testing Approach

This system requires both unit tests and property-based tests for comprehensive
coverage:

**Unit Tests**: Focus on specific examples, edge cases, and integration points

- Specific packet formats and edge cases
- Error conditions and recovery paths
- CDK infrastructure configuration validation
- UI component rendering with specific data

**Property-Based Tests**: Verify universal properties across all inputs

- Packet handling across random ports and data
- Concurrent stream processing with random stream counts
- Stream state transitions with random timing
- HLS manifest validity across random video inputs

Both approaches are complementary and necessary. Unit tests catch concrete bugs
in specific scenarios, while property tests verify general correctness across
the input space.

### Property-Based Testing Configuration

**Library**: fast-check (for TypeScript/JavaScript components)

**Configuration**:

- Minimum 100 iterations per property test
- Each test tagged with: **Feature: ntn-video-streaming, Property {number}:
  {property_text}**
- Generators for: UDP packets, port numbers (5000-5009), video segments, stream
  metadata
- Shrinking enabled to find minimal failing examples

**Example Property Test Structure**:

```typescript
import fc from "fast-check";

// Feature: ntn-video-streaming, Property 1: Packet Buffering and Forwarding
test("UDP packets are buffered and forwarded with correct port identifier", () => {
  fc.assert(
    fc.property(
      fc.integer({ min: 5000, max: 5009 }), // port
      fc.uint8Array({ minLength: 100, maxLength: 1500 }), // packet data
      (port, packetData) => {
        const listener = new UDPListener(config);
        const result = listener.handlePacket(port, packetData);

        expect(result.buffered).toBe(true);
        expect(result.streamIdentifier).toBe(port);
        expect(result.forwardedData).toEqual(packetData);
      },
    ),
    { numRuns: 100 },
  );
});
```

### Unit Testing Focus Areas

**UDP Ingestion**:

- Port binding on startup
- Malformed packet rejection
- Buffer overflow handling
- Graceful shutdown

**FFmpeg Transcoding**:

- Process lifecycle management
- Error recovery and retries
- S3 upload integration
- Snapshot capture timing

**API Endpoints**:

No API Gateway needed. The web frontend accesses DynamoDB directly using AWS SDK
with Cognito credentials.

**Frontend Components**:

- Stream list rendering
- Video player initialization
- Adaptive bitrate UI controls
- Offline stream display

### Integration Testing

**End-to-End Flows**:

1. Send UDP packets → Verify HLS segments in S3
2. Query DynamoDB from frontend → Verify stream metadata
3. Request stream via CloudFront → Verify delivery
4. Simulate camera offline → Verify snapshot display
5. Camera resumes sending packets → Verify automatic transition to live

**Load Testing**:

- 10 concurrent streams for 15 minutes
- Measure: latency, packet loss, CPU/memory usage
- Verify: no stream interference, accurate metrics

**Chaos Testing**:

- Random EC2 instance termination
- S3 service degradation simulation
- Network partition between components
- Verify: graceful degradation, automatic recovery
