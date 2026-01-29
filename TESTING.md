# End-to-End Testing Guide

This guide walks you through testing the complete video streaming solution using
your webcam as the video source and the web application to view the stream.

## Prerequisites

Before starting, ensure you have:

- ✅ AWS CLI configured with credentials
- ✅ CDK stack deployed (`npm run cdk:prod:deploy`)
- ✅ FFmpeg installed on your local machine
- ✅ A working webcam
- ✅ Frontend configuration updated with stack outputs

## Quick Start

Follow these steps to test the complete system:

### 1. Verify Stack Deployment

Check that the infrastructure is deployed and running:

```bash
# Get stack outputs
./scripts/get-stack-outputs.sh
```

You should see outputs for:

- UserPoolURL
- UserPoolClientId
- IdentityPoolId
- DynamoDBTableName
- CloudFrontURL
- VideoBucketName

### 2. Get EC2 Instance IP Address

Find the public IP of a running EC2 instance:

```bash
./scripts/get-instance-ip.sh
```

This will display all running instances and their IP addresses. Copy the IP
address for the next step.

**Example output:**

```
Running Instances:
==================
Instance ID: i-0123456789abcdef0
  Public IP:  3.123.45.67
  Private IP: 10.0.1.123
  Launched:   2024-01-29T10:30:00.000Z

To stream webcam to this instance:
  ./scripts/stream-webcam-to-udp.sh 3.123.45.67 5000
```

### 3. Start Webcam Streaming

Stream your webcam video to the EC2 instance on port 5000:

```bash
./scripts/stream-webcam-to-udp.sh <INSTANCE_IP> 5000
```

Replace `<INSTANCE_IP>` with the IP address from step 2.

**Example:**

```bash
./scripts/stream-webcam-to-udp.sh 3.123.45.67 5000
```

You should see:

```
Streaming Configuration:
  Target: 3.123.45.67:5000
  Video: 1280x720 @ 30fps
  Codec: H.264
  Format: MPEG-TS over UDP

Press Ctrl+C to stop streaming

frame=   45 fps= 30 q=28.0 size=     512kB time=00:00:01.50 bitrate=2793.6kbits/s speed=1.0x
```

**Keep this terminal window open** - the stream will continue as long as this
command is running.

### 4. Verify Stream Processing

In a new terminal, check the CloudWatch logs to verify the stream is being
processed:

```bash
./scripts/view-logs.sh
```

Look for log entries indicating:

- UDP packets received on port 5000
- Stream marked as active in DynamoDB
- FFmpeg transcoding started
- HLS segments uploaded to S3

### 5. Check DynamoDB

Verify the stream metadata was created:

```bash
aws dynamodb scan \
  --table-name $DYNAMODB_TABLE_NAME \
  --query "Items[?port==\`5000\`]" \
  --output json
```

You should see an item with:

- `port`: 5000
- `status`: "active"
- `lastPacketTime`: Recent timestamp
- `hlsManifestPath`: S3 path to master.m3u8
- `lastFramePath`: S3 path to snapshot

### 6. Verify S3 Content

Check that video segments are being uploaded to S3:

```bash
# List HLS segments
aws s3 ls s3://$BUCKET_NAME/hls/5000/ --recursive | head -20

# List raw segments
aws s3 ls s3://$BUCKET_NAME/raw/5000/ --recursive | head -10

# Check snapshot
aws s3 ls s3://$BUCKET_NAME/snapshots/5000/
```

You should see:

- `hls/5000/master.m3u8` - Master playlist
- `hls/5000/1080p/`, `hls/5000/720p/`, etc. - Quality variants
- `hls/5000/*/segment_*.ts` - Video segments
- `snapshots/5000/last_frame.jpg` - Latest frame snapshot

### 7. Test CloudFront Access

Verify the video is accessible via CloudFront:

```bash
# Get CloudFront domain
CLOUDFRONT_DOMAIN=$(aws cloudformation describe-stacks \
  --stack-name video-streaming \
  --query "Stacks[0].Outputs[?OutputKey=='CloudFrontURL'].OutputValue" \
  --output text)

# Test master playlist
curl -I "https://${CLOUDFRONT_DOMAIN}/hls/5000/master.m3u8"
```

You should get a `200 OK` response.

### 8. Open Web Application

Open the web application in your browser:

```bash
# If running locally
cd frontend
npm start
# Then open http://localhost:8080

# Or open production URL
# https://video.thingy.rocks
```

### 9. View Stream in Browser

1. **Sign In** (if required):
   - Click "Sign In" or you'll be redirected to Cognito
   - Create an account or sign in with existing credentials
   - Verify your email if it's a new account

2. **View Stream List**:
   - You should see a grid of available streams
   - Port 5000 should show as "ACTIVE" with a green badge
   - A thumbnail of your webcam feed should be visible

3. **Click on Stream**:
   - Click on the Port 5000 stream card
   - You should be redirected to the stream player

4. **Watch Video**:
   - The video player should load and start playing your webcam feed
   - You should see yourself with a slight delay (5-10 seconds)
   - The "LIVE" indicator should be visible

5. **Test Adaptive Bitrate**:
   - Toggle "Raw Stream Mode" checkbox to switch between raw and adaptive
   - In adaptive mode, use the quality dropdown to manually select quality
   - Watch the "Current Bitrate" and "Resolution" update

### 10. Test Offline/Online Transitions

Test the system's handling of intermittent connectivity:

1. **Stop the webcam stream**:
   - Go back to the terminal running the stream
   - Press `Ctrl+C` to stop

2. **Wait 1 minute**:
   - The system detects inactivity after 1 minute
   - The stream should be marked as inactive

3. **Refresh the web application**:
   - The stream should now show "OFFLINE" badge
   - The last frame snapshot should be displayed
   - The timestamp should show when the last packet was received

4. **Resume streaming**:

   ```bash
   ./scripts/stream-webcam-to-udp.sh <INSTANCE_IP> 5000
   ```

5. **Watch automatic transition**:
   - Within 5 seconds, the web app should detect the stream is active again
   - The video player should automatically switch from snapshot to live video
   - The badge should change from "OFFLINE" to "LIVE"

## Testing Multiple Streams

Test concurrent stream handling:

### Stream to Multiple Ports

Open multiple terminal windows and stream to different ports:

```bash
# Terminal 1
./scripts/stream-webcam-to-udp.sh <INSTANCE_IP> 5000

# Terminal 2 (if you have multiple cameras or use test video)
./scripts/stream-webcam-to-udp.sh <INSTANCE_IP> 5001

# Terminal 3
./scripts/stream-webcam-to-udp.sh <INSTANCE_IP> 5002
```

### Verify in Web Application

- All streams should appear in the stream list
- Each should have its own thumbnail and status
- You should be able to view each stream independently
- Switching between streams should work smoothly

## Testing with Test Video File

If you don't have a webcam or want to test with a video file:

```bash
# Download a test video
wget https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/720/Big_Buck_Bunny_720_10s_1MB.mp4 -O test-video.mp4

# Stream the test video in a loop
ffmpeg -re -stream_loop -1 -i test-video.mp4 \
  -c:v libx264 \
  -preset ultrafast \
  -tune zerolatency \
  -b:v 2000k \
  -maxrate 2000k \
  -bufsize 4000k \
  -pix_fmt yuv420p \
  -g 60 \
  -f mpegts \
  "udp://<INSTANCE_IP>:5000?pkt_size=1316"
```

## Troubleshooting

### No Video Appears in Web App

**Check 1: Is the stream being received?**

```bash
./scripts/view-logs.sh
# Look for "UDP packet received on port 5000"
```

**Check 2: Is DynamoDB updated?**

```bash
aws dynamodb get-item \
  --table-name $DYNAMODB_TABLE_NAME \
  --key '{"port":{"N":"5000"}}'
```

**Check 3: Are segments in S3?**

```bash
aws s3 ls s3://$BUCKET_NAME/hls/5000/ --recursive
```

**Check 4: Is CloudFront accessible?**

```bash
curl -I "https://${CLOUDFRONT_DOMAIN}/hls/5000/master.m3u8"
```

### Webcam Stream Won't Start

**Linux:**

```bash
# Check if webcam is detected
ls -la /dev/video*

# Test webcam with ffplay
ffplay -f v4l2 -i /dev/video0

# Check permissions
sudo usermod -a -G video $USER
# Log out and back in
```

**macOS:**

```bash
# List available devices
ffmpeg -f avfoundation -list_devices true -i ""

# Test with device index
ffplay -f avfoundation -i "0"
```

### High Latency

If you experience high latency (>15 seconds):

1. **Check network connection**:

   ```bash
   ping <INSTANCE_IP>
   ```

2. **Reduce buffer size**: Edit the stream command to use smaller buffers:

   ```bash
   -bufsize 2000k  # Instead of 4000k
   ```

3. **Check EC2 instance CPU**:
   ```bash
   aws cloudwatch get-metric-statistics \
     --namespace AWS/EC2 \
     --metric-name CPUUtilization \
     --dimensions Name=AutoScalingGroupName,Value=video-streaming-UDPListenerASG* \
     --start-time $(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%S) \
     --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
     --period 300 \
     --statistics Average
   ```

### Authentication Issues

**Can't sign in:**

1. Check User Pool exists:

   ```bash
   aws cognito-idp describe-user-pool --user-pool-id <USER_POOL_ID>
   ```

2. Verify callback URLs:

   ```bash
   aws cognito-idp describe-user-pool-client \
     --user-pool-id <USER_POOL_ID> \
     --client-id <CLIENT_ID>
   ```

3. Check browser console for errors

**Can't access DynamoDB:**

1. Verify Identity Pool configuration
2. Check IAM role permissions
3. Ensure frontend has correct Identity Pool ID

### Stream Stops After a While

**Check CloudWatch alarms:**

```bash
aws cloudwatch describe-alarms \
  --alarm-name-prefix video-streaming \
  --state-value ALARM
```

**Check EC2 instance health:**

```bash
aws autoscaling describe-auto-scaling-groups \
  --auto-scaling-group-names video-streaming-UDPListenerASG*
```

**View system logs:**

```bash
./scripts/view-logs.sh
# Look for errors or crashes
```

## Performance Testing

### Measure End-to-End Latency

1. Display a timer on your webcam feed
2. Compare the timer in the web player with real-time
3. Typical latency should be 5-15 seconds

### Test Concurrent Viewers

Open the stream in multiple browser tabs or devices:

- All should play smoothly
- No interference between viewers
- CloudFront should cache segments efficiently

### Monitor Metrics

```bash
# View custom metrics
aws cloudwatch get-metric-statistics \
  --namespace video-streaming \
  --metric-name ActiveStreamCount \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Average
```

## Success Criteria

Your test is successful if:

- ✅ Webcam stream reaches EC2 instance
- ✅ Stream appears as "active" in DynamoDB within 10 seconds
- ✅ HLS segments are created and uploaded to S3
- ✅ Master playlist and variant playlists are generated
- ✅ Video is accessible via CloudFront
- ✅ Web application displays the stream list
- ✅ Video plays in the browser with acceptable latency (<15s)
- ✅ Adaptive bitrate switching works
- ✅ Offline/online transitions work correctly
- ✅ Multiple concurrent streams work without interference

## Next Steps

After successful testing:

1. **Configure DNS** for your custom domain
2. **Set up SSL certificate** in CloudFront
3. **Create user accounts** in Cognito
4. **Configure alarm notifications** via SNS
5. **Set up monitoring dashboard** in CloudWatch
6. **Document operational procedures**
7. **Plan for scaling** based on expected load

## Cleanup

To stop testing and clean up resources:

1. **Stop webcam streaming**: Press `Ctrl+C` in the streaming terminal

2. **Delete test data from S3** (optional):

   ```bash
   aws s3 rm s3://$BUCKET_NAME/hls/5000/ --recursive
   aws s3 rm s3://$BUCKET_NAME/raw/5000/ --recursive
   aws s3 rm s3://$BUCKET_NAME/snapshots/5000/ --recursive
   ```

3. **Delete DynamoDB item** (optional):

   ```bash
   aws dynamodb delete-item \
     --table-name $DYNAMODB_TABLE_NAME \
     --key '{"port":{"N":"5000"}}'
   ```

4. **Destroy stack** (if completely done):
   ```bash
   npm run cdk:prod:deploy -- --destroy
   ```

## Support

If you encounter issues:

1. Check CloudWatch Logs: `./scripts/view-logs.sh`
2. Review CloudWatch Alarms
3. Verify security group rules allow UDP 5000-5009
4. Ensure EC2 instances are running and healthy
5. Check that all environment variables are set correctly
