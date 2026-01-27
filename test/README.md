# Test Environment

This directory contains test infrastructure for the NTN Video Streaming system.

## Contents

- `deploy-test-stack.ts` - Script to deploy CDK stack to test AWS account
- `udp-packet-generator.ts` - UDP packet generator for testing video ingestion
- `fixtures/` - Test data and fixtures
- `integration/` - Integration test scripts
  - `udp-to-s3-flow.spec.ts` - Task 7.2: UDP to S3 storage flow test
  - `offline-online-transitions.spec.ts` - Task 7.4: Offline/online transitions
    test

## Quick Start

### 1. Deploy Test Stack

```bash
npm run test:deploy
```

### 2. Run Task 7.2: UDP to S3 Flow Test

```bash
# Get stack outputs
export TEST_BUCKET_NAME=$(aws cloudformation describe-stacks \
  --stack-name NTNVideoStreamingTest \
  --query "Stacks[0].Outputs[?OutputKey=='VideoBucketName'].OutputValue" \
  --output text)

export TEST_TABLE_NAME=$(aws cloudformation describe-stacks \
  --stack-name NTNVideoStreamingTest \
  --query "Stacks[0].Outputs[?OutputKey=='DynamoDBTableName'].OutputValue" \
  --output text)

export TEST_EC2_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

# Run test
npm run test:task-7.2 -- \
  --host $TEST_EC2_IP \
  --bucket-name $TEST_BUCKET_NAME \
  --table-name $TEST_TABLE_NAME \
  --port 5000
```

### 3. Run Task 7.4: Offline/Online Transitions Test

```bash
# Get CloudFront URL (in addition to above exports)
export TEST_CLOUDFRONT_URL=$(aws cloudformation describe-stacks \
  --stack-name NTNVideoStreamingTest \
  --query "Stacks[0].Outputs[?OutputKey=='CloudFrontURL'].OutputValue" \
  --output text)

# Run test
npm run test:task-7.4 -- \
  --host $TEST_EC2_IP \
  --bucket-name $TEST_BUCKET_NAME \
  --table-name $TEST_TABLE_NAME \
  --cloudfront-url $TEST_CLOUDFRONT_URL \
  --port 5000
```

## Usage

### Deploy Test Stack

```bash
npm run test:deploy
```

This will deploy the CDK stack to your test AWS account. Make sure you have:

- AWS credentials configured
- Appropriate permissions to create resources
- CDK bootstrapped in your account/region

### Generate Test UDP Packets

```bash
node --experimental-transform-types test/udp-packet-generator.ts --port 5000 --duration 60
```

Options:

- `--port` - Target port (5000-5009)
- `--host` - Target host (default: localhost)
- `--duration` - Duration in seconds (default: 60)
- `--bitrate` - Bitrate in kbps (default: 1000)
- `--packet-size` - Packet size in bytes (default: 1316)

### Run Integration Tests

#### Task 7.2: UDP to S3 Storage Flow

Tests the complete end-to-end flow from UDP packet reception to S3 storage.

```bash
npm run test:task-7.2 -- \
  --host <EC2_IP> \
  --bucket-name <BUCKET_NAME> \
  --table-name <TABLE_NAME> \
  --port 5000
```

See `TASK-7.2-EXECUTION-GUIDE.md` for detailed instructions.

#### Task 7.4: Offline/Online Transitions

Tests the system's handling of intermittent NTNCam connectivity.

```bash
npm run test:task-7.4 -- \
  --host <EC2_IP> \
  --bucket-name <BUCKET_NAME> \
  --table-name <TABLE_NAME> \
  --cloudfront-url <CLOUDFRONT_URL> \
  --port 5000
```

See `TASK-7.4-EXECUTION-GUIDE.md` for detailed instructions.

**Note:** Task 7.4 takes approximately 2-3 minutes to complete due to the
inactivity timeout requirement.

## Test Fixtures

The `fixtures/` directory contains:

- Sample video files for testing
- Pre-generated MPEG-TS packets
- Expected HLS manifest files
- Test metadata

## Environment Variables

Set these environment variables for testing:

- `AWS_REGION` - AWS region for deployment (default: us-east-1)
- `TEST_STACK_NAME` - Name for test stack (default: NTNVideoStreamingTest)
- `TEST_UDP_HOST` - Target host for UDP packets (default: localhost)
