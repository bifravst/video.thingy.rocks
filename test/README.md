# Test Environment

This directory contains test infrastructure for the Video Streaming system.

## Contents

- `udp-packet-generator.ts` - UDP packet generator for testing video ingestion
- `fixtures/` - Test data and fixtures
- `integration/` - Integration test scripts
  - `udp-to-s3-flow.spec.ts` - Task 7.2: UDP to S3 storage flow test
  - `offline-online-transitions.spec.ts` - Task 7.4: Offline/online transitions
    test

## Quick Start

### 1. Deploy or update the stack

```bash
npm run cdk:prod:deploy
```

### 2. View EC2 Instance Logs

After deployment, you can view the CloudWatch Logs to debug issues:

```bash
# View application logs (systemd journal for video-streaming service)
./scripts/view-logs.sh /video-streaming/application

# View system logs
./scripts/view-logs.sh /video-streaming/system

# View cloud-init logs (useful for debugging startup issues)
./scripts/view-logs.sh /video-streaming/cloud-init
```

Or use AWS CLI directly:

```bash
# List all log groups
aws logs describe-log-groups --log-group-name-prefix "/video-streaming"

# Tail application logs
aws logs tail /video-streaming/application --follow --format short

# View last hour of logs
aws logs tail /video-streaming/application --since 1h --format short
```

### 3. Run Task 7.2: UDP to S3 Flow Test

```bash
# Get stack outputs
export BUCKET_NAME=$(aws cloudformation describe-stacks \
  --stack-name video-streaming \
  --query "Stacks[0].Outputs[?OutputKey=='VideoBucketName'].OutputValue" \
  --output text)

export TABLE_NAME=$(aws cloudformation describe-stacks \
  --stack-name video-streaming \
  --query "Stacks[0].Outputs[?OutputKey=='DynamoDBTableName'].OutputValue" \
  --output text)

export ASG_NAME=$(aws autoscaling describe-auto-scaling-groups --query "AutoScalingGroups[?contains(AutoScalingGroupName, 'UDPListenerASG')].AutoScalingGroupName" --output text)
export EC2_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=$ASG_NAME" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

# Output

echo $BUCKET_NAME
echo $TABLE_NAME
echo $EC2_IP

# Run test
npm run test:udp-to-s3 -- \
  --host $EC2_IP \
  --bucket-name $BUCKET_NAME \
  --table-name $TABLE_NAME \
  --port 5000
```

### 4. Run Task 7.4: Offline/Online Transitions Test

```bash
# Get CloudFront URL (in addition to above exports)
export CLOUDFRONT_URL=$(aws cloudformation describe-stacks \
  --stack-name video-streaming \
  --query "Stacks[0].Outputs[?OutputKey=='CloudFrontURL'].OutputValue" \
  --output text)

# Run test
npm run test:offline-online-transition -- \
  --host $EC2_IP \
  --bucket-name $BUCKET_NAME \
  --table-name $TABLE_NAME \
  --cloudfront-url $CLOUDFRONT_URL \
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

#### UDP to S3 Storage Flow

Tests the complete end-to-end flow from UDP packet reception to S3 storage.

```bash
npm run test:udp-to-s3 -- \
  --host <EC2_IP> \
  --bucket-name <BUCKET_NAME> \
  --table-name <TABLE_NAME> \
  --port 5000
```

#### Offline/Online Transitions

Tests the system's handling of intermittent Cat1bisCam connectivity.

```bash
npm run test:offline-online-transition -- \
  --host <EC2_IP> \
  --bucket-name <BUCKET_NAME> \
  --table-name <TABLE_NAME> \
  --cloudfront-url <CLOUDFRONT_URL> \
  --port 5000
```

**Note:** This takes approximately 2-3 minutes to complete due to the inactivity
timeout requirement.

## Test Fixtures

The `fixtures/` directory contains:

- Sample video files for testing
- Pre-generated MPEG-TS packets
- Expected HLS manifest files
- Test metadata

## Environment Variables

Set these environment variables for testing:

- `AWS_REGION` - AWS region for deployment (default: eu-central-1)
- `TEST_STACK_NAME` - Name for test stack (default: video-streaming)
- `TEST_UDP_HOST` - Target host for UDP packets (default: localhost)
