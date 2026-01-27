# Task 7.2 Execution Guide: UDP to S3 Storage Flow Test

## Overview

This guide provides step-by-step instructions for executing Task 7.2: Test UDP
to S3 storage flow.

**Task Requirements:**

- Send test UDP packets to port 5000
- Verify HLS segments appear in S3 within expected time
- Verify raw segments are stored in correct S3 paths
- Verify DynamoDB metadata is created and updated
- Verify master.m3u8 and variant playlists are generated
- Measure end-to-end latency from UDP to S3

**Requirements Validated:** 1.1, 2.1, 3.1

## Prerequisites

1. **AWS Account** with appropriate permissions
2. **AWS CLI** configured with credentials
3. **Node.js v24+** installed
4. **CDK Stack** deployed to test environment

## Step 1: Deploy the Test Stack

The test stack must be deployed before running integration tests.

### Option A: Deploy using the deployment script

```bash
# Deploy the stack
npm run test:deploy
```

This will:

1. Synthesize the CDK stack
2. Show IAM and security group changes
3. Ask for approval
4. Deploy all resources (takes 10-15 minutes)

### Option B: Manual deployment

```bash
# Synthesize the stack
node --experimental-transform-types test/deploy-test-stack.ts

# Deploy using CDK CLI
cdk deploy NTNVideoStreamingTest
```

### Deployment Outputs

After deployment completes, note these outputs:

- `IdentityPoolId` - Cognito Identity Pool ID
- `DynamoDBTableName` - DynamoDB table name
- `VideoBucketName` - S3 bucket name
- `CloudFrontURL` - CloudFront distribution URL
- `VPCId` - VPC ID
- `AlarmTopicArn` - SNS topic ARN

Save these for testing:

```bash
export TEST_BUCKET_NAME=$(aws cloudformation describe-stacks \
  --stack-name NTNVideoStreamingTest \
  --query "Stacks[0].Outputs[?OutputKey=='VideoBucketName'].OutputValue" \
  --output text)

export TEST_TABLE_NAME=$(aws cloudformation describe-stacks \
  --stack-name NTNVideoStreamingTest \
  --query "Stacks[0].Outputs[?OutputKey=='DynamoDBTableName'].OutputValue" \
  --output text)

echo "Bucket: $TEST_BUCKET_NAME"
echo "Table: $TEST_TABLE_NAME"
```

## Step 2: Get EC2 Instance IP

Find the public IP of the running EC2 instance:

```bash
export TEST_EC2_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

echo "EC2 IP: $TEST_EC2_IP"
```

**Note:** If no IP is returned, the EC2 instances may still be launching. Wait
2-3 minutes and try again.

## Step 3: Verify EC2 Instance is Ready

Check that the UDP listener service is running:

```bash
# Check CloudWatch logs
aws logs tail /ntn-video-streaming/udp-listener --follow
```

You should see log messages indicating the service has started and is listening
on ports 5000-5009.

Press Ctrl+C to stop following logs.

## Step 4: Run the Integration Test

Execute the comprehensive integration test:

```bash
node --experimental-transform-types test/integration/udp-to-s3-flow.spec.ts \
  --host $TEST_EC2_IP \
  --bucket-name $TEST_BUCKET_NAME \
  --table-name $TEST_TABLE_NAME \
  --port 5000 \
  --packet-count 200 \
  --timeout 120
```

### Test Parameters

- `--host` - EC2 instance public IP
- `--bucket-name` - S3 bucket name from stack outputs
- `--table-name` - DynamoDB table name from stack outputs
- `--port` - UDP port to test (5000-5009)
- `--packet-count` - Number of UDP packets to send (default: 200)
- `--timeout` - Maximum wait time in seconds (default: 120)
- `--region` - AWS region (default: us-east-1)

### Expected Output

The test will:

1. **Send UDP packets** - Sends 200 test packets to the EC2 instance
2. **Wait for processing** - Waits 30 seconds for FFmpeg to process the stream
3. **Check DynamoDB** - Verifies stream metadata is created with status "active"
4. **Check raw segments** - Verifies raw .ts files are in S3 at `raw/5000/`
5. **Check HLS segments** - Verifies transcoded segments for all bitrate
   profiles
6. **Check manifests** - Verifies master.m3u8 and variant playlists exist
7. **Measure latency** - Reports end-to-end latency from UDP to S3

### Success Criteria

All tests should pass:

- ✓ Send UDP packets
- ✓ Check DynamoDB metadata (status: active)
- ✓ Check S3 raw segments (count > 0)
- ✓ Check HLS segments (count > 0)
- ✓ Check HLS manifests (master + variants found)
- ✓ Measure end-to-end latency (< 120s)

## Step 5: Manual Verification (Optional)

### Verify DynamoDB

```bash
aws dynamodb get-item \
  --table-name $TEST_TABLE_NAME \
  --key '{"port": {"N": "5000"}}'
```

Expected output:

```json
{
  "Item": {
    "port": { "N": "5000" },
    "status": { "S": "active" },
    "lastPacketTime": { "S": "2024-01-27T10:30:45.123Z" },
    "hlsManifestPath": { "S": "hls/5000/master.m3u8" },
    "rawStreamPath": { "S": "raw/5000/" },
    "lastFramePath": { "S": "snapshots/5000/last_frame.jpg" }
  }
}
```

### Verify S3 Raw Segments

```bash
aws s3 ls s3://$TEST_BUCKET_NAME/raw/5000/ --recursive
```

Expected: Multiple .ts files with timestamps

### Verify HLS Segments

```bash
# Check master manifest
aws s3 ls s3://$TEST_BUCKET_NAME/hls/5000/

# Check 1080p segments
aws s3 ls s3://$TEST_BUCKET_NAME/hls/5000/1080p/

# Check 720p segments
aws s3 ls s3://$TEST_BUCKET_NAME/hls/5000/720p/
```

### Download and Inspect Master Manifest

```bash
aws s3 cp s3://$TEST_BUCKET_NAME/hls/5000/master.m3u8 - | cat
```

Expected format:

```
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080
1080p/playlist.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720
720p/playlist.m3u8
...
```

## Step 6: Test Multiple Ports (Optional)

Test concurrent streams on different ports:

```bash
# Terminal 1 - Port 5000
node --experimental-transform-types test/integration/udp-to-s3-flow.spec.ts \
  --host $TEST_EC2_IP \
  --bucket-name $TEST_BUCKET_NAME \
  --table-name $TEST_TABLE_NAME \
  --port 5000 &

# Terminal 2 - Port 5001
node --experimental-transform-types test/integration/udp-to-s3-flow.spec.ts \
  --host $TEST_EC2_IP \
  --bucket-name $TEST_BUCKET_NAME \
  --table-name $TEST_TABLE_NAME \
  --port 5001 &

# Wait for both to complete
wait
```

## Troubleshooting

### Test Fails: "Stream metadata not found in DynamoDB"

**Cause:** UDP packets not reaching EC2 or service not running

**Solutions:**

1. Check security group allows UDP 5000-5009 from your IP
2. Verify EC2 instance is running:
   `aws ec2 describe-instances --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*"`
3. Check CloudWatch logs:
   `aws logs tail /ntn-video-streaming/udp-listener --follow`
4. SSH into EC2 and check service: `systemctl status ntn-video-streaming`

### Test Fails: "No raw segments found"

**Cause:** FFmpeg not processing packets or S3 upload failing

**Solutions:**

1. Check CloudWatch logs for FFmpeg errors
2. Verify EC2 instance has S3 write permissions
3. Check disk space on EC2 instance
4. Increase wait time: `--timeout 180`

### Test Fails: "No HLS segments found"

**Cause:** Transcoding takes longer than expected

**Solutions:**

1. Increase wait time: `--timeout 300`
2. Check FFmpeg is installed: SSH to EC2 and run `ffmpeg -version`
3. Check CloudWatch metrics for CPU usage (may need larger instance)
4. Verify FFmpeg processes are running: `ps aux | grep ffmpeg`

### Test Fails: "Manifests not found"

**Cause:** HLS packaging not complete

**Solutions:**

1. Wait longer and re-run test
2. Check FFmpeg command in CloudWatch logs
3. Verify S3 bucket permissions
4. Check for FFmpeg errors in logs

### High Packet Loss

**Cause:** Network issues or EC2 instance overloaded

**Solutions:**

1. Reduce packet rate: `--packet-count 100`
2. Check network connectivity to EC2
3. Verify Auto Scaling Group is scaling appropriately
4. Check EC2 instance CPU/memory usage

## Cleanup

After testing, destroy the test stack to avoid charges:

```bash
# Option 1: Using deployment script
npm run test:deploy -- --destroy

# Option 2: Using CDK CLI
cdk destroy NTNVideoStreamingTest
```

**Note:** This will delete all resources including S3 buckets and DynamoDB
tables.

## Success Criteria for Task 7.2

Task 7.2 is complete when:

- [x] Test stack successfully deployed
- [x] Integration test script created
- [x] UDP packets successfully sent to EC2 instance
- [x] DynamoDB metadata created and updated
- [x] Raw segments stored in S3 at correct paths
- [x] HLS segments generated for all bitrate profiles
- [x] Master manifest and variant playlists created
- [x] End-to-end latency measured and documented
- [x] All tests pass with no errors

## Next Steps

After completing Task 7.2:

1. **Task 7.3** - Write property test for concurrent viewer support
2. **Task 7.4** - Test offline/online transitions
3. **Task 7.5** - Test error scenarios
4. **Task 7.6** - Test security configurations
5. **Task 7.7** - Load testing
6. **Task 7.8** - Verify HLS playback compatibility

## References

- Requirements: `.kiro/specs/ntn-video-streaming/requirements.md`
- Design: `.kiro/specs/ntn-video-streaming/design.md`
- Tasks: `.kiro/specs/ntn-video-streaming/tasks.md`
- Setup Guide: `test/SETUP.md`
- Test Summary: `test/SUMMARY.md`
