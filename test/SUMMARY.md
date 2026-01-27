# Test Environment Summary

This document summarizes the test environment setup for Task 7.1.

## What Was Created

### 1. Test Infrastructure Scripts

- **`deploy-test-stack.ts`** - CDK deployment script for test environment
  - Synthesizes the StreamingStack for testing
  - Configures availability zones
  - Outputs deployment instructions

- **`udp-packet-generator.ts`** - UDP packet generator for testing
  - Generates MPEG-TS formatted UDP packets
  - Supports multiple test patterns (color-bars, test-card, random)
  - Configurable bitrate, packet size, and duration
  - Real-time statistics and progress reporting

- **`quick-start.ts`** - Quick setup and validation script
  - Checks prerequisites (Node.js, AWS CLI, CDK)
  - Validates AWS credentials
  - Gets stack outputs and EC2 instance IPs
  - Provides next steps and environment variables

### 2. Test Fixtures

- **`fixtures/sample-packets.ts`** - Pre-generated MPEG-TS packets
  - Valid packets with proper structure
  - Malformed packets for error testing
  - Packet generation utilities
  - Validation functions

- **`fixtures/test-metadata.json`** - Sample stream metadata
  - Example stream states (active, inactive)
  - Test scenarios (new stream, stopped stream, intermittent)
  - Expected HLS profiles and configurations

- **`fixtures/expected-manifests/`** - Expected HLS manifest files
  - `master.m3u8` - Master playlist
  - `1080p-playlist.m3u8` - 1080p variant
  - `720p-playlist.m3u8` - 720p variant

### 3. Integration Tests

- **`integration/test-udp-to-s3.ts`** - End-to-end integration test
  - Sends UDP packets to EC2 instance
  - Verifies DynamoDB metadata updates
  - Checks S3 raw segments
  - Validates HLS segments and manifests
  - Measures latency and reports results

### 4. Documentation

- **`README.md`** - Overview and quick reference
- **`SETUP.md`** - Detailed setup guide with step-by-step instructions
- **`SUMMARY.md`** - This file

### 5. NPM Scripts

Added to `package.json`:

```json
{
  "test:quick-start": "Quick setup and validation",
  "test:deploy": "Deploy test stack",
  "test:generate-packets": "Generate UDP test packets",
  "test:integration": "Run integration tests"
}
```

## Test Environment Components

### AWS Resources (Created by CDK Stack)

1. **VPC** - Virtual Private Cloud with public/private subnets
2. **EC2 Auto Scaling Group** - Runs UDP listener and FFmpeg transcoding
3. **S3 Bucket** - Stores video segments and snapshots
4. **DynamoDB Table** - Stores stream metadata
5. **CloudFront Distribution** - Delivers video content
6. **Cognito Identity Pool** - Provides unauthenticated access
7. **CloudWatch Alarms** - Monitors system health
8. **Security Groups** - Controls network access

### Test Data

1. **Sample Packets** - Pre-generated MPEG-TS packets for unit tests
2. **Test Metadata** - Sample stream metadata for DynamoDB
3. **Expected Manifests** - Reference HLS manifests for validation

## Usage

### Quick Start

```bash
# Run quick start script
npm run test:quick-start

# Follow the instructions to deploy and test
```

### Manual Setup

```bash
# 1. Deploy test stack
npm run test:deploy
cdk deploy NTNVideoStreamingTest

# 2. Generate test packets
npm run test:generate-packets -- \
  --host <EC2_IP> \
  --port 5000 \
  --duration 60

# 3. Run integration tests
npm run test:integration -- \
  --host <EC2_IP> \
  --bucket-name <BUCKET_NAME> \
  --table-name <TABLE_NAME>
```

## Test Scenarios

### 1. Basic UDP Ingestion

- Send packets to port 5000
- Verify packets are received and buffered
- Check DynamoDB metadata is created

### 2. Multi-Stream Testing

- Send packets to multiple ports (5000-5009)
- Verify concurrent stream handling
- Check stream isolation

### 3. Offline/Online Transitions

- Start stream, stop, wait, resume
- Verify last frame preservation
- Check automatic resumption

### 4. Error Handling

- Send malformed packets
- Simulate S3 failures
- Kill FFmpeg process
- Verify graceful recovery

### 5. Load Testing

- 10 concurrent streams for 15 minutes
- Measure packet loss and latency
- Monitor Auto Scaling behavior

## Validation Checklist

- [x] Test infrastructure scripts created
- [x] UDP packet generator implemented
- [x] Test fixtures created
- [x] Integration test script created
- [x] Documentation written
- [x] NPM scripts added
- [x] Syntax validation passed

## Next Steps

After completing Task 7.1, proceed to:

1. **Task 7.2** - Test UDP to S3 storage flow
2. **Task 7.3** - Test concurrent viewer support (property test)
3. **Task 7.4** - Test offline/online transitions
4. **Task 7.5** - Test error scenarios
5. **Task 7.6** - Test security configurations
6. **Task 7.7** - Load testing
7. **Task 7.8** - Verify HLS playback compatibility
8. **Task 7.9** - Create deployment documentation

## Requirements Validated

This task validates the following requirements:

- **Requirement 1.1** - UDP Video Ingestion (test infrastructure)
- **Requirement 7.1** - CDK Infrastructure Deployment (test deployment)

## Notes

- All test scripts use TypeScript with experimental transform types
- Test fixtures are version controlled (except large video files)
- Integration tests require deployed AWS infrastructure
- Quick start script automates common setup tasks
- Documentation provides detailed troubleshooting guidance

## Troubleshooting

See `SETUP.md` for detailed troubleshooting steps, including:

- EC2 instance not receiving packets
- No segments in S3
- DynamoDB not updating
- High packet loss
- Network connectivity issues

## Cleanup

To clean up test resources:

```bash
# Destroy CDK stack
cdk destroy NTNVideoStreamingTest

# Or manually delete resources
aws s3 rm s3://<BUCKET_NAME> --recursive
aws dynamodb delete-table --table-name <TABLE_NAME>
```
