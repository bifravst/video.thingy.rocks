# Video Streaming System for Cat 1 bis Connected Cameras

A serverless video streaming solution on AWS that receives UDP video streams
from Cat 1 bis connected cameras (Cat1bisCam devices) and delivers them to web
viewers with support for both raw and adaptive bitrate streaming.

## Features

- **UDP Video Ingestion**: Receives video streams on ports 5000-5009
- **Real-time Transcoding**: FFmpeg-based transcoding to multiple bitrates
  (1080p, 720p, 480p, 360p)
- **Adaptive Bitrate Streaming**: HLS-based adaptive streaming with automatic
  quality switching
- **Web Interface**: React-based frontend for viewing streams
- **Offline Handling**: Preserves last frame when cameras go offline
- **Auto-Resume**: Automatically resumes streaming when cameras reconnect
- **Authentication**: Cognito-based user authentication with OIDC
- **Monitoring**: CloudWatch metrics, logs, and alarms
- **Scalable**: Auto Scaling Group handles multiple concurrent streams

## Architecture

- **EC2 Auto Scaling Group**: UDP listeners and FFmpeg transcoding
- **S3**: Video segment storage (raw and HLS)
- **CloudFront**: Global content delivery
- **DynamoDB**: Stream metadata and state management
- **Cognito**: User authentication and AWS credentials
- **CloudWatch**: Metrics, logs, and alarms

## Quick Start

### 1. Deploy Infrastructure

```bash
# Install dependencies
npm install

# Deploy CDK stack
npm run cdk:prod:deploy
```

### 2. Configure Frontend

```bash
# Get stack outputs and update frontend config
./scripts/get-stack-outputs.sh
./scripts/update-frontend-config.sh
```

### 3. Test with Webcam

```bash
# Get EC2 instance IP
./scripts/get-instance-ip.sh

# Stream webcam to instance
./scripts/stream-webcam-to-udp.sh <INSTANCE_IP> 5000

# Start frontend
cd frontend
npm start
# Open http://localhost:8080
```

See [QUICK-TEST.md](QUICK-TEST.md) for a 5-minute test guide.

## Documentation

- **[DEPLOYMENT.md](DEPLOYMENT.md)** - Complete deployment guide with current
  configuration
- **[TESTING.md](TESTING.md)** - Comprehensive end-to-end testing instructions
- **[QUICK-TEST.md](QUICK-TEST.md)** - 5-minute quick test guide
- **[frontend/CONFIG.md](frontend/CONFIG.md)** - Frontend configuration guide
- **[scripts/README.md](scripts/README.md)** - Helper scripts documentation

## Project Structure

```
.
├── cdk/                    # CDK infrastructure code
│   ├── StreamingStack.ts   # Main stack definition
│   ├── user-data.sh        # EC2 instance initialization
│   └── deploy-to-production.ts
├── backend/                # Backend services (UDP listener, transcoding)
│   └── src/
│       ├── UDPListener.ts
│       ├── FFmpegTranscoder.ts
│       ├── S3UploadService.ts
│       └── StreamMetadataService.ts
├── frontend/               # React web application
│   └── src/
│       ├── page/
│       │   ├── StreamList.tsx
│       │   └── StreamPlayer.tsx
│       └── utils/
│           └── StreamDynamoDBClient.ts
├── scripts/                # Helper scripts
│   ├── get-stack-outputs.sh
│   ├── update-frontend-config.sh
│   ├── stream-webcam-to-udp.sh
│   ├── get-instance-ip.sh
│   └── view-logs.sh
└── test/                   # Integration tests
    └── integration/
```

## Requirements

- Node.js 24+
- npm 11+
- AWS CLI configured
- FFmpeg (for testing with webcam)

## Testing

### Quick Test (5 minutes)

```bash
# 1. Get instance IP
./scripts/get-instance-ip.sh

# 2. Stream webcam
./scripts/stream-webcam-to-udp.sh <IP> 5000

# 3. Open web app
cd frontend && npm start
```

### Full Test Suite

```bash
# Run all tests
npm run test:all

# Run integration tests
npm run test:integration

# Run specific test
npm run test:offline-online-transition
```

See [TESTING.md](TESTING.md) for detailed testing instructions.

## Development

### Local Development

```bash
# Set environment variables
direnv allow  # If using direnv

# Start frontend dev server
cd frontend
npm start
```

### Update Backend Code

```bash
# Deploy code changes to running instances
./scripts/dev-update-backend.sh
```

### View Logs

```bash
# View CloudWatch logs
./scripts/view-logs.sh
```

### SSH to Instance

```bash
# Connect via AWS Systems Manager
./scripts/ssh-to-instance.sh
```

## Monitoring

### CloudWatch Alarms

The system includes alarms for:

- High packet loss (>5%)
- FFmpeg process failures
- S3 upload failures
- DynamoDB throttling
- High CPU usage (>80%)

### Metrics

Custom metrics tracked:

- Active stream count
- Packet loss rate per stream
- Bitrate per stream
- FFmpeg process failures
- S3 upload failures

### Logs

CloudWatch log groups:

- `/video-streaming/application` - Application logs
- `/video-streaming/system` - System logs
- `/video-streaming/cloud-init` - Instance initialization logs

## Configuration

### Environment Variables

Set in `.envrc` or export in shell:

```bash
export AWS_PROFILE=thingy.rocks
export AWS_REGION=eu-central-1
export COGNITO_USER_POOL_URL=https://cognito-idp.eu-central-1.amazonaws.com/...
export COGNITO_USER_POOL_CLIENT_ID=...
export COGNITO_IDENTITY_POOL_ID=...
export DYNAMODB_TABLE_NAME=...
export CLOUDFRONT_DOMAIN_NAME=...
export BUCKET_NAME=...
```

Get values with: `./scripts/get-stack-outputs.sh`

## Troubleshooting

### No streams appearing

```bash
# Check if instances are running
./scripts/get-instance-ip.sh

# View logs
./scripts/view-logs.sh

# Check DynamoDB
aws dynamodb scan --table-name $DYNAMODB_TABLE_NAME
```

### Authentication issues

```bash
# Verify Cognito configuration
aws cognito-idp describe-user-pool --user-pool-id <POOL_ID>

# Check Identity Pool
aws cognito-identity describe-identity-pool --identity-pool-id <POOL_ID>
```

### High latency

- Check network connection to EC2 instance
- Monitor EC2 CPU usage
- Review CloudWatch metrics
- Consider scaling up instance type

See [TESTING.md](TESTING.md) for more troubleshooting tips.

## Security

- HTTPS enforced via CloudFront
- S3 encryption at rest
- TLS for all AWS service communication
- IAM roles follow least privilege
- Security groups restrict access to necessary ports
- Cognito authentication for user access
- Email verification required for new users

## Cost Optimization

- S3 lifecycle policy (30-day retention)
- DynamoDB on-demand billing
- EC2 Auto Scaling (2-10 instances)
- CloudFront caching
- Spot instances (optional)

## License

UNLICENSED - Nordic Semiconductor ASA

## Support

For issues or questions:

1. Check [TESTING.md](TESTING.md) troubleshooting section
2. Review CloudWatch logs
3. Verify configuration with `./scripts/get-stack-outputs.sh`
4. Check security group rules and IAM permissions

## Contributing

This is a private project for Nordic Semiconductor ASA.
