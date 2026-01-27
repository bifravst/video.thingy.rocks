# Task 7.4 Execution Guide: Offline/Online Transitions Test

## Overview

This guide provides step-by-step instructions for executing Task 7.4: Test
offline/online transitions.

**Task Requirements:**

- Send UDP packets to establish active stream
- Stop sending packets and wait for timeout (1 minute)
- Verify stream marked as inactive in DynamoDB
- Verify last frame snapshot is captured and stored in S3
- Verify snapshot accessible via CloudFront URL
- Resume sending UDP packets
- Verify stream marked as active in DynamoDB
- Verify new HLS segments are generated

**Requirements Validated:** 11.1, 11.2, 11.4, 11.5

## Prerequisites

1. **AWS Account** with appropriate permissions
2. **AWS CLI** configured with credentials
3. **Node.js v24+** installed
4. **CDK Stack** deployed to test environment (from Task 7.1)
5. **Task 7.2** completed successfully (UDP to S3 flow working)

## Important Notes

⚠️ **This test takes approximately 3-5 minutes to complete** due to the 1-minute
inactivity timeout requirement.

⚠️ **The test validates Requirement 11.4** which states: "WHEN an NTNCam device
has been offline for more than 1 hour, THE System SHALL mark the stream status
as inactive". However, for testing purposes, we use a 1-minute timeout to avoid
excessively long test runs.

## Step 1: Verify Prerequisites

Ensure the test stack is deployed and running:

```bash
# Check stack status
aws cloudformation describe-stacks \
  --stack-name NTNVideoStreamingTest \
  --query "Stacks[0].StackStatus" \
  --output text

# Expected output: CREATE_COMPLETE or UPDATE_COMPLETE
```

## Step 2: Get Required Parameters

Extract the necessary parameters from the CloudFormation stack:

```bash
# Get S3 bucket name
export TEST_BUCKET_NAME=$(aws cloudformation describe-stacks \
  --stack-name NTNVideoStreamingTest \
  --query "Stacks[0].Outputs[?OutputKey=='VideoBucketName'].OutputValue" \
  --output text)

# Get DynamoDB table name
export TEST_TABLE_NAME=$(aws cloudformation describe-stacks \
  --stack-name NTNVideoStreamingTest \
  --query "Stacks[0].Outputs[?OutputKey=='DynamoDBTableName'].OutputValue" \
  --output text)

# Get CloudFront URL
export TEST_CLOUDFRONT_URL=$(aws cloudformation describe-stacks \
  --stack-name NTNVideoStreamingTest \
  --query "Stacks[0].Outputs[?OutputKey=='CloudFrontURL'].OutputValue" \
  --output text)

# Get EC2 instance IP
export TEST_EC2_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

# Verify all parameters are set
echo "Bucket: $TEST_BUCKET_NAME"
echo "Table: $TEST_TABLE_NAME"
echo "CloudFront: $TEST_CLOUDFRONT_URL"
echo "EC2 IP: $TEST_EC2_IP"
```

## Step 3: Run the Integration Test

Execute the offline/online transitions test:

```bash
node --experimental-transform-types test/integration/offline-online-transitions.spec.ts \
  --host $TEST_EC2_IP \
  --bucket-name $TEST_BUCKET_NAME \
  --table-name $TEST_TABLE_NAME \
  --cloudfront-url $TEST_CLOUDFRONT_URL \
  --port 5000 \
  --packet-count 100 \
  --inactivity-timeout 65
```

### Test Parameters

- `--host` - EC2 instance public IP (required)
- `--bucket-name` - S3 bucket name from stack outputs (required)
- `--table-name` - DynamoDB table name from stack outputs (required)
- `--cloudfront-url` - CloudFront distribution URL (required)
- `--port` - UDP port to test (default: 5000)
- `--packet-count` - Number of UDP packets to send per phase (default: 100)
- `--inactivity-timeout` - Seconds to wait for inactivity timeout (default: 65)
- `--timeout` - Maximum test timeout in seconds (default: 300)
- `--region` - AWS region (default: us-east-1)

### Test Flow

The test executes in three phases:

**Phase 1: Establish Active Stream (~ 30 seconds)**

1. Send 100 UDP packets to port 5000
2. Wait 20 seconds for processing
3. Verify stream status is "active" in DynamoDB

**Phase 2: Go Offline (~ 65 seconds)** 4. Stop sending packets 5. Wait 65
seconds for inactivity timeout 6. Verify stream status is "inactive" in
DynamoDB 7. Verify last frame snapshot exists in S3 8. Verify snapshot
accessible via CloudFront URL

**Phase 3: Resume Online (~ 30 seconds)** 9. Resume sending 100 UDP packets 10.
Wait 20 seconds for processing 11. Verify stream status is "active" again in
DynamoDB 12. Verify new HLS segments are generated

**Total Duration:** Approximately 2-3 minutes

### Expected Output

```
Integration Test: Offline/Online Transitions (Task 7.4)
========================================================
Target: 3.123.45.67:5000
S3 Bucket: ntnvideostreamingtest-videobucket-abc123
DynamoDB Table: NTNVideoStreamingTest-StreamMetadata-xyz789
CloudFront URL: d1234567890abc.cloudfront.net
Region: eu-central-1
Packet count: 100
Inactivity timeout: 65 seconds
Test timeout: 300 seconds

Step 1: Sending initial UDP packets to establish active stream...
  Sent 100 packets in 1234ms (0 errors)

Step 2: Waiting 20 seconds for initial processing...
  ✓ Wait complete

Step 3: Verifying stream is active in DynamoDB...
  ✓ Stream metadata found:
    - Status: active
    - Last packet time: 2024-01-27T10:30:45.123Z

Step 4: Stopping packets and waiting 65 seconds for inactivity timeout...
  (System should mark stream as inactive after 1 minute of no packets)
  ✓ Wait complete

Step 5: Verifying stream marked as inactive in DynamoDB...
  ✓ Stream metadata found:
    - Status: inactive
    - Last packet time: 2024-01-27T10:30:45.123Z
    - Last frame path: snapshots/5000/last_frame.jpg

Step 6: Verifying last frame snapshot in S3...
  ✓ Snapshot found in S3:
    - Key: snapshots/5000/last_frame.jpg
    - Size: 45.67 KB
    - Last modified: 2024-01-27T10:31:50.123Z
    - Content type: image/jpeg

Step 7: Verifying snapshot accessible via CloudFront...
  Requesting: https://d1234567890abc.cloudfront.net/snapshots/5000/last_frame.jpg
  ✓ Snapshot accessible via CloudFront:
    - Status code: 200
    - Size: 45.67 KB
    - Content type: image/jpeg

Step 8: Resuming UDP packet transmission...
  Sent 100 packets in 1234ms (0 errors)

Step 9: Waiting 20 seconds for stream resumption processing...
  ✓ Wait complete

Step 10: Verifying stream marked as active again in DynamoDB...
  ✓ Stream metadata found:
    - Status: active
    - Last packet time: 2024-01-27T10:33:15.456Z

Step 11: Verifying new HLS segments are generated...
  ✓ Found 15 total objects in HLS 1080p folder
  ✓ Found 12 .ts segment files
  Most recent segments:
    - hls/5000/1080p/segment_12.ts (234.56 KB, 2024-01-27T10:33:20.123Z)
    - hls/5000/1080p/segment_11.ts (231.45 KB, 2024-01-27T10:33:14.123Z)
    - hls/5000/1080p/segment_10.ts (229.34 KB, 2024-01-27T10:33:08.123Z)

================================================================================
Test Results Summary - Task 7.4: Offline/Online Transitions
================================================================================
✓ PASS Send initial UDP packets (1234ms) [Req: 11.1]
       Sent 100/100 packets (0 errors)
✓ PASS Verify stream active (456ms) [Req: 11.1]
       Status: active
✓ PASS Verify stream inactive (567ms) [Req: 11.4]
       Status: inactive
✓ PASS Verify snapshot in S3 (789ms) [Req: 11.4]
       Found snapshot (45.67 KB)
✓ PASS Verify snapshot via CloudFront (1234ms) [Req: 11.5]
       HTTP 200, 45.67 KB
✓ PASS Resume sending packets (1234ms) [Req: 11.2]
       Sent 100/100 packets (0 errors)
✓ PASS Verify stream active again (456ms) [Req: 11.2]
       Status: active
✓ PASS Verify new HLS segments (789ms) [Req: 11.2]
       Found 12 HLS segments

================================================================================
Total: 8 | Passed: 8 | Failed: 0
Total test duration: 2.15 minutes
================================================================================

✓ All tests passed! Offline/online transitions working correctly.

Requirements validated:
  - 11.1: System treats NTNCam offline as expected behavior
  - 11.2: System automatically resumes processing when NTNCam returns
  - 11.4: System marks stream as inactive after 1 hour offline
  - 11.5: Web frontend displays last frame when offline
```

## Step 4: Manual Verification (Optional)

### Verify Stream State Transitions

Monitor the stream state in real-time:

```bash
# Watch DynamoDB for status changes
watch -n 5 "aws dynamodb get-item \
  --table-name $TEST_TABLE_NAME \
  --key '{\"port\": {\"N\": \"5000\"}}' \
  --query 'Item.{Status:status.S,LastPacket:lastPacketTime.S}' \
  --output table"
```

### Verify Snapshot in S3

```bash
# Check snapshot exists
aws s3 ls s3://$TEST_BUCKET_NAME/snapshots/5000/

# Download snapshot for inspection
aws s3 cp s3://$TEST_BUCKET_NAME/snapshots/5000/last_frame.jpg ./last_frame.jpg

# View image (macOS)
open last_frame.jpg

# View image (Linux with ImageMagick)
display last_frame.jpg
```

### Verify Snapshot via CloudFront

```bash
# Download via CloudFront
curl -o cloudfront_snapshot.jpg \
  https://$TEST_CLOUDFRONT_URL/snapshots/5000/last_frame.jpg

# Check file size
ls -lh cloudfront_snapshot.jpg
```

### Monitor CloudWatch Logs

```bash
# Watch UDP listener logs
aws logs tail /ntn-video-streaming/udp-listener --follow

# Look for messages like:
# - "Stream 5000 started"
# - "Stream 5000 inactive (no packets for 60 seconds)"
# - "Stream 5000 resumed"
# - "Captured snapshot for stream 5000"
```

## Troubleshooting

### Test Fails: "Stream not marked as inactive"

**Cause:** Inactivity timeout not configured or not working

**Solutions:**

1. Check backend code implements inactivity detection
2. Verify timeout is set to 60 seconds (1 minute)
3. Increase `--inactivity-timeout` to 90 seconds to allow more time
4. Check CloudWatch logs for timeout events
5. Verify StreamStateManager is tracking last packet time

### Test Fails: "Snapshot not found in S3"

**Cause:** Snapshot capture not implemented or failing

**Solutions:**

1. Check FFmpeg command includes snapshot capture
2. Verify FFmpeg has write permissions to S3
3. Check CloudWatch logs for FFmpeg errors
4. Manually trigger snapshot: Send packets, wait, check S3
5. Verify S3 bucket policy allows PutObject

### Test Fails: "Snapshot not accessible via CloudFront"

**Cause:** CloudFront distribution not configured or not propagated

**Solutions:**

1. Verify CloudFront distribution is deployed
2. Check CloudFront origin is configured for S3 bucket
3. Wait 5-10 minutes for CloudFront propagation
4. Test direct S3 access first (see manual verification)
5. Check CloudFront cache behaviors include `/snapshots/*`
6. Verify Origin Access Identity has S3 read permissions

### Test Fails: "Stream not marked as active again"

**Cause:** Stream resumption not working

**Solutions:**

1. Check UDP listener is still running
2. Verify packets are reaching EC2 instance
3. Check security group allows UDP traffic
4. Increase wait time: `--inactivity-timeout 90`
5. Check CloudWatch logs for stream resumption events

### Test Fails: "No new HLS segments"

**Cause:** FFmpeg not restarting or transcoding failing

**Solutions:**

1. Check FFmpeg process restarts when stream resumes
2. Verify FFmpeg logs for errors
3. Increase wait time after resumption
4. Check S3 upload permissions
5. Verify disk space on EC2 instance

## Success Criteria for Task 7.4

Task 7.4 is complete when:

- [x] Integration test script created
- [ ] Stream successfully established as active
- [ ] Stream marked as inactive after 1 minute of no packets
- [ ] Last frame snapshot captured and stored in S3
- [ ] Snapshot accessible via CloudFront URL
- [ ] Stream automatically resumes when packets resume
- [ ] Stream marked as active again after resumption
- [ ] New HLS segments generated after resumption
- [ ] All tests pass with no errors

## Test Variations

### Test Different Ports

Test offline/online transitions on multiple ports:

```bash
# Test port 5001
node --experimental-transform-types test/integration/offline-online-transitions.spec.ts \
  --host $TEST_EC2_IP \
  --bucket-name $TEST_BUCKET_NAME \
  --table-name $TEST_TABLE_NAME \
  --cloudfront-url $TEST_CLOUDFRONT_URL \
  --port 5001

# Test port 5002
node --experimental-transform-types test/integration/offline-online-transitions.spec.ts \
  --host $TEST_EC2_IP \
  --bucket-name $TEST_BUCKET_NAME \
  --table-name $TEST_TABLE_NAME \
  --cloudfront-url $TEST_CLOUDFRONT_URL \
  --port 5002
```

### Test Longer Inactivity Period

Test with longer inactivity timeout (closer to 1 hour requirement):

```bash
# Test with 5-minute timeout
node --experimental-transform-types test/integration/offline-online-transitions.spec.ts \
  --host $TEST_EC2_IP \
  --bucket-name $TEST_BUCKET_NAME \
  --table-name $TEST_TABLE_NAME \
  --cloudfront-url $TEST_CLOUDFRONT_URL \
  --port 5000 \
  --inactivity-timeout 300
```

### Test Multiple Offline/Online Cycles

Run the test multiple times to verify consistency:

```bash
# Run 3 cycles
for i in {1..3}; do
  echo "=== Cycle $i ==="
  node --experimental-transform-types test/integration/offline-online-transitions.spec.ts \
    --host $TEST_EC2_IP \
    --bucket-name $TEST_BUCKET_NAME \
    --table-name $TEST_TABLE_NAME \
    --cloudfront-url $TEST_CLOUDFRONT_URL \
    --port 5000
  echo ""
done
```

## Next Steps

After completing Task 7.4:

1. **Task 7.5** - Test error scenarios (malformed packets, S3 failures, FFmpeg
   crashes)
2. **Task 7.6** - Test security configurations (HTTPS, CORS, encryption)
3. **Task 7.7** - Load testing (10 concurrent streams)
4. **Task 7.8** - Verify HLS playback compatibility
5. **Task 7.9** - Create deployment documentation

## References

- Requirements: `.kiro/specs/ntn-video-streaming/requirements.md`
- Design: `.kiro/specs/ntn-video-streaming/design.md`
- Tasks: `.kiro/specs/ntn-video-streaming/tasks.md`
- Task 7.2 Guide: `test/TASK-7.2-EXECUTION-GUIDE.md`
- Test Summary: `test/SUMMARY.md`

## Cleanup

After testing, you can clean up test data:

```bash
# Delete snapshots for specific port
aws s3 rm s3://$TEST_BUCKET_NAME/snapshots/5000/ --recursive

# Delete HLS segments for specific port
aws s3 rm s3://$TEST_BUCKET_NAME/hls/5000/ --recursive

# Delete raw segments for specific port
aws s3 rm s3://$TEST_BUCKET_NAME/raw/5000/ --recursive

# Delete DynamoDB item
aws dynamodb delete-item \
  --table-name $TEST_TABLE_NAME \
  --key '{"port": {"N": "5000"}}'
```

To destroy the entire test stack:

```bash
npm run test:deploy -- --destroy
```
