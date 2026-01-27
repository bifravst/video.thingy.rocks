# Test Environment Setup Guide

This guide walks through setting up the test environment for the NTN Video
Streaming system.

## Prerequisites

1. **AWS Account**: You need an AWS account with appropriate permissions
2. **AWS CLI**: Install and configure AWS CLI with credentials
3. **Node.js**: Version 24 or higher
4. **CDK**: AWS CDK should be bootstrapped in your account

## Step 1: Configure AWS Credentials

Ensure your AWS credentials are configured:

```bash
aws configure
```

Or set environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

## Step 2: Bootstrap CDK (if not already done)

```bash
npm run cdk:bootstrap
```

This creates the necessary CDK resources in your AWS account.

## Step 3: Deploy Test Stack

### Option A: Using the test deployment script

```bash
npm run test:deploy
```

This synthesizes the CDK stack for testing. To actually deploy:

```bash
cdk deploy NTNVideoStreamingTest
```

### Option B: Manual deployment

```bash
cd cdk
cdk deploy --app "node --experimental-transform-types ../test/deploy-test-stack.ts"
```

### Deployment outputs

After deployment, note the following outputs:

- `IdentityPoolId` - Cognito Identity Pool ID
- `DynamoDBTableName` - DynamoDB table name
- `VideoBucketName` - S3 bucket name
- `CloudFrontURL` - CloudFront distribution URL
- `VPCId` - VPC ID
- `AlarmTopicArn` - SNS topic ARN

Save these values for testing.

## Step 4: Get EC2 Instance IP

Find the public IP of the EC2 instance:

```bash
aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
  --query "Reservations[*].Instances[*].[PublicIpAddress,State.Name]" \
  --output table
```

## Step 5: Test UDP Packet Generation

### Local testing (to localhost)

If running the backend locally:

```bash
npm run test:generate-packets -- --port 5000 --duration 30
```

### Remote testing (to EC2 instance)

```bash
npm run test:generate-packets -- \
  --host <EC2_PUBLIC_IP> \
  --port 5000 \
  --duration 60 \
  --bitrate 1000
```

Options:

- `--port` - Target port (5000-5009)
- `--host` - Target host (EC2 public IP)
- `--duration` - Duration in seconds
- `--bitrate` - Bitrate in kbps
- `--packet-size` - Packet size in bytes
- `--pattern` - Test pattern (color-bars, test-card, random)

## Step 6: Run Integration Tests

```bash
npm run test:integration -- \
  --host <EC2_PUBLIC_IP> \
  --bucket-name <BUCKET_NAME> \
  --table-name <TABLE_NAME> \
  --port 5000 \
  --packet-count 100
```

This will:

1. Send UDP packets to the EC2 instance
2. Wait for processing
3. Verify DynamoDB metadata
4. Check S3 raw segments
5. Check HLS segments
6. Verify HLS manifests

## Step 7: Verify Results

### Check DynamoDB

```bash
aws dynamodb get-item \
  --table-name <TABLE_NAME> \
  --key '{"port": {"N": "5000"}}'
```

### Check S3 Bucket

```bash
# List raw segments
aws s3 ls s3://<BUCKET_NAME>/raw/5000/ --recursive

# List HLS segments
aws s3 ls s3://<BUCKET_NAME>/hls/5000/ --recursive

# List snapshots
aws s3 ls s3://<BUCKET_NAME>/snapshots/5000/
```

### Check CloudWatch Logs

```bash
aws logs tail /ntn-video-streaming/udp-listener --follow
```

### Check CloudWatch Metrics

```bash
aws cloudwatch get-metric-statistics \
  --namespace NTN/VideoStreaming \
  --metric-name PacketLossRate \
  --start-time $(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Average
```

## Step 8: Test Multiple Streams

Test concurrent streams on different ports:

```bash
# Terminal 1
npm run test:generate-packets -- --host <EC2_IP> --port 5000 --duration 120 &

# Terminal 2
npm run test:generate-packets -- --host <EC2_IP> --port 5001 --duration 120 &

# Terminal 3
npm run test:generate-packets -- --host <EC2_IP> --port 5002 --duration 120 &
```

## Step 9: Test Stream Offline/Online Transitions

```bash
# Start stream
npm run test:generate-packets -- --host <EC2_IP> --port 5000 --duration 30

# Wait 2 minutes (stream should go inactive)
sleep 120

# Check status
aws dynamodb get-item \
  --table-name <TABLE_NAME> \
  --key '{"port": {"N": "5000"}}'

# Resume stream
npm run test:generate-packets -- --host <EC2_IP> --port 5000 --duration 30
```

## Step 10: Clean Up

When done testing, destroy the test stack:

```bash
cdk destroy NTNVideoStreamingTest
```

Or manually delete resources:

```bash
# Delete S3 buckets (must be empty first)
aws s3 rm s3://<BUCKET_NAME> --recursive
aws s3 rb s3://<BUCKET_NAME>

# Delete DynamoDB table
aws dynamodb delete-table --table-name <TABLE_NAME>

# Terminate EC2 instances
# (Auto Scaling Group will be deleted with the stack)
```

## Troubleshooting

### EC2 instance not receiving packets

1. Check security group rules:

   ```bash
   aws ec2 describe-security-groups \
     --filters "Name=group-name,Values=*UDPSecurityGroup*"
   ```

2. Verify UDP ports are open (5000-5009)

3. Check EC2 instance logs:
   ```bash
   aws ssm start-session --target <INSTANCE_ID>
   sudo journalctl -u ntn-video-streaming -f
   ```

### No segments in S3

1. Check EC2 instance has IAM permissions for S3
2. Check FFmpeg is installed and running
3. Check CloudWatch logs for errors

### DynamoDB not updating

1. Check EC2 instance has IAM permissions for DynamoDB
2. Check network connectivity to DynamoDB
3. Verify table name is correct

### High packet loss

1. Check network bandwidth
2. Increase EC2 instance size
3. Check Auto Scaling Group is scaling appropriately

## Environment Variables

Set these for easier testing:

```bash
export TEST_STACK_NAME=NTNVideoStreamingTest
export AWS_REGION=us-east-1
export TEST_EC2_IP=<EC2_PUBLIC_IP>
export TEST_BUCKET_NAME=<BUCKET_NAME>
export TEST_TABLE_NAME=<TABLE_NAME>
```

Then use in commands:

```bash
npm run test:generate-packets -- --host $TEST_EC2_IP --port 5000
npm run test:integration -- --host $TEST_EC2_IP --bucket-name $TEST_BUCKET_NAME --table-name $TEST_TABLE_NAME
```

## Next Steps

After successful test environment setup:

1. Run integration tests (Task 7.2)
2. Test offline/online transitions (Task 7.4)
3. Test error scenarios (Task 7.5)
4. Test security configurations (Task 7.6)
5. Perform load testing (Task 7.7)
6. Verify HLS playback (Task 7.8)
