# NTN Video Streaming Backend

This directory contains the backend services for the NTN Video Streaming system,
including:

- UDP listener service (ports 5000-5009)
- FFmpeg transcoding service
- Stream state management
- AWS service integrations (S3, DynamoDB, CloudWatch)

## Directory Structure

```
backend/
├── src/           # Source code
├── tests/         # Test files (*.spec.ts)
├── package.json   # Node.js dependencies and scripts
└── README.md      # This file
```

## Requirements

- Node.js v24 or higher
- NPM v11 or higher

## Installation

```bash
npm install
```

## Testing

```bash
npm test
```

## Dependencies

- `@aws-sdk/client-s3` - S3 operations for video storage
- `@aws-sdk/client-dynamodb` - DynamoDB operations for stream metadata
- `@aws-sdk/lib-dynamodb` - DynamoDB document client
- `@aws-sdk/client-cloudwatch` - CloudWatch metrics emission

## Development Dependencies

- `typescript` - TypeScript compiler
- `@types/node` - Node.js type definitions
