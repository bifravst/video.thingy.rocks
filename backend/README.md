# NTN Video Streaming Backend

UDP listener and stream processing service for Non-Terrestrial Network Connected
Cameras.

## Overview

This service receives UDP video streams from NTNCam devices on ports 5000-5009,
buffers the data, tracks stream state, and integrates with AWS services
(DynamoDB, S3) for metadata and storage.

## Components Implemented

### UDPListener

- Listens for UDP packets on ports 5000-5009
- Validates incoming packets
- Handles port binding failures with exponential backoff retry
- Emits events for packet reception

### PacketBuffer

- Buffers incoming packets in memory
- Flushes to disk when size or time threshold is reached
- Handles buffer overflow with FIFO packet dropping
- Writes MPEG-TS files to local storage

### StreamStateManager

- Tracks active/inactive state for each stream
- Detects stream start (first packet on a port)
- Detects stream stop (no packets for 1 minute)
- Emits events for state transitions

### StreamMetadataService

- Integrates with DynamoDB to store stream metadata
- Updates stream status (active/inactive)
- Updates last packet timestamps
- Stores S3 paths for HLS manifests and snapshots

## Directory Structure

```
backend/
├── src/
│   ├── UDPListener.ts              # UDP packet reception
│   ├── UDPListener.spec.ts         # UDP listener tests
│   ├── PacketBuffer.ts             # Packet buffering and disk writing
│   ├── PacketBuffer.spec.ts        # Buffer tests
│   ├── StreamStateManager.ts       # Stream state tracking
│   ├── StreamStateManager.spec.ts  # State manager tests
│   ├── StreamMetadataService.ts    # DynamoDB integration
│   └── index.ts                    # Main service entry point
├── package.json
└── README.md
```

## Requirements

- Node.js v24 or higher
- NPM v11 or higher

## Installation

```bash
npm install
```

## Configuration

Environment variables:

- `OUTPUT_DIR` - Directory for buffered video files (default:
  `/tmp/video-streams`)
- `DYNAMODB_TABLE_NAME` - DynamoDB table name (default: `StreamMetadata`)
- `AWS_REGION` - AWS region (default: `us-east-1`)

## Running

```bash
node --experimental-transform-types src/index.ts
```

## Testing

```bash
npm test
```

All tests pass successfully:

- UDPListener: Port binding, packet reception, error handling
- PacketBuffer: Buffering, flushing, overflow handling
- StreamStateManager: State transitions, active stream tracking

## Architecture

```
UDP Packets (ports 5000-5009)
    ↓
UDPListener (validates & receives)
    ↓
PacketBuffer (buffers & flushes) → Local Disk (MPEG-TS files)
    ↓
StreamStateManager (tracks state)
    ↓
StreamMetadataService → DynamoDB
```

## Requirements Implemented

- **Requirement 1.1**: UDP ingestion on ports 5000-5009 ✓
- **Requirement 1.2**: Packet buffering ✓
- **Requirement 1.4**: Port-based stream identification ✓
- **Requirement 1.5**: Stream state tracking ✓
- **Requirement 6.2**: Stream metadata in DynamoDB ✓
- **Requirement 6.5**: Active stream count tracking ✓
- **Requirement 8.4**: Stream status API support ✓
- **Requirement 9.1**: Error handling for malformed packets ✓
- **Requirement 11.4**: Inactivity detection (1 minute timeout) ✓

## Dependencies

- `@aws-sdk/client-s3` - S3 operations for video storage
- `@aws-sdk/client-dynamodb` - DynamoDB operations for stream metadata
- `@aws-sdk/lib-dynamodb` - DynamoDB document client
- `@aws-sdk/client-cloudwatch` - CloudWatch metrics emission

## Development Dependencies

- `typescript` - TypeScript compiler
- `@types/node` - Node.js type definitions
