# Deployment Guide

This guide explains how to deploy and configure the video streaming
infrastructure.

## Prerequisites

- Node.js 24+ and npm 11+
- AWS CLI configured with appropriate credentials
- AWS account with permissions to create CloudFormation stacks

## Initial Deployment

### 1. Deploy the CDK Stack

The CDK stack creates all necessary AWS resources including:

- Cognito User Pool and Identity Pool for authentication
- DynamoDB table for stream metadata
- S3 buckets for video storage and code
- EC2 Auto Scaling Group for UDP ingestion
- CloudFront distribution for content delivery
- CloudWatch alarms and logging

Deploy the stack:

```bash
npm run cdk:prod:deploy
```

This will create a stack named `video-streaming` (or `${STACK_PREFIX}-streaming`
if you set a custom prefix).

### 2. Get Stack Outputs

After deployment, retrieve the configuration values:

```bash
./scripts/get-stack-outputs.sh
```

This will display:

- **UserPoolURL** - Cognito User Pool URL for OIDC authentication
- **UserPoolClientId** - Client ID for the web application
- **IdentityPoolId** - Identity Pool ID for AWS credentials
- **DynamoDBTableName** - Table name for stream metadata
- **CloudFrontURL** - CloudFront domain for video delivery
- **VideoBucketName** - S3 bucket for video storage

### 3. Update Frontend Configuration

Automatically update the frontend with deployed values:

```bash
./scripts/update-frontend-config.sh
```

Or manually update `frontend/vite.config.ts` with the values from step 2.

### 4. Build and Deploy Frontend

```bash
cd frontend
npm install
npm run build
```

The built files will be in `frontend/build/` and can be deployed to your hosting
service.

## Current Deployed Configuration

The following values are currently deployed and configured in
`frontend/vite.config.ts` and `.envrc`:

- **User Pool URL**:
  `https://cognito-idp.eu-central-1.amazonaws.com/eu-central-1_AIb3Ir43E/`
- **User Pool Client ID**: `5tkfrh5baeh3t0tp51ail9nc8f`
- **Identity Pool ID**: `eu-central-1:945b0d2a-eab5-49d7-8c8e-768e80f1153c`
- **DynamoDB Table**: `video-streaming-StreamMetadataD1FE2960-1IL7LYVPF9K2B`
- **S3 Bucket**: `video-streaming-videobucket6ed8e1af-c7ncbofwunt1`
- **CloudFront URL**: _Get from stack outputs_

## Redeployment

When you redeploy the CDK stack (e.g., after making infrastructure changes):

1. Deploy the updated stack:

   ```bash
   npm run cdk:prod:deploy
   ```

2. Update frontend configuration:

   ```bash
   ./scripts/update-frontend-config.sh
   ```

3. Rebuild and redeploy frontend:
   ```bash
   cd frontend
   npm run build
   # Deploy to hosting
   ```

## Stack Outputs Reference

| Output Key        | Description           | Used By                  |
| ----------------- | --------------------- | ------------------------ |
| UserPoolURL       | Cognito User Pool URL | Frontend authentication  |
| UserPoolClientId  | User Pool Client ID   | Frontend authentication  |
| IdentityPoolId    | Identity Pool ID      | Frontend AWS credentials |
| DynamoDBTableName | DynamoDB table name   | Frontend and backend     |
| CloudFrontURL     | CloudFront domain     | Frontend video playback  |
| VideoBucketName   | S3 bucket name        | Backend video storage    |
| VPCId             | VPC ID                | Infrastructure reference |
| AlarmTopicArn     | SNS topic for alarms  | Monitoring               |

## Authentication Setup

The system uses Cognito User Pool for authentication:

### User Sign-Up

Users can sign up at the application with:

- Email address (required)
- Password (min 8 chars, uppercase, lowercase, digits)
- Email verification required

### OAuth Configuration

The User Pool is configured with OAuth callbacks for:

- Local development: `http://localhost:8080/auth/callback`
- Production: `https://video.thingy.rocks/auth/callback`

### Identity Pool

The Identity Pool provides AWS credentials for:

- **Authenticated users**: Read access to DynamoDB
- **Unauthenticated users**: Read access to DynamoDB (for public viewing)

## Monitoring

CloudWatch alarms are configured for:

- High packet loss (>5%)
- FFmpeg process failures
- S3 upload failures
- DynamoDB throttling
- High CPU usage (>80%)

View logs:

```bash
./scripts/view-logs.sh
```

## Development

### Local Development

1. Set environment variables (already in `.envrc`):

   ```bash
   direnv allow  # If using direnv
   ```

2. Start frontend dev server:

   ```bash
   cd frontend
   npm start
   ```

3. Access at http://localhost:8080

### Update Backend Code

To update backend code on running EC2 instances:

```bash
./scripts/dev-update-backend.sh
```

### SSH to Instance

```bash
./scripts/ssh-to-instance.sh
```

## Troubleshooting

### Stack Deployment Fails

- Check AWS credentials: `aws sts get-caller-identity`
- Verify region is set: `echo $AWS_REGION`
- Check for resource limits in your AWS account

### Frontend Can't Connect

- Verify all environment variables are set correctly
- Check that the User Pool and Identity Pool exist
- Ensure OAuth callback URLs match your domain
- Check browser console for authentication errors

### No Video Streams

- Verify EC2 instances are running
- Check that UDP ports 5000-5009 are accessible
- View CloudWatch logs for errors
- Ensure security groups allow UDP traffic

### Configuration Out of Sync

After redeployment, if configuration seems wrong:

```bash
# Get latest values
./scripts/get-stack-outputs.sh

# Update frontend
./scripts/update-frontend-config.sh

# Rebuild
cd frontend && npm run build
```

## Security Notes

- User Pool requires email verification
- Identity Pool supports both authenticated and unauthenticated access
- All data in transit is encrypted (HTTPS/TLS)
- S3 buckets use server-side encryption
- IAM roles follow least privilege principle
- Security groups restrict access to necessary ports only

## Cost Optimization

- S3 lifecycle policy deletes objects after 30 days
- DynamoDB uses on-demand billing
- EC2 Auto Scaling adjusts capacity based on load
- CloudFront caching reduces origin requests

## Next Steps

1. Configure DNS for your domain
2. Set up SSL certificate in CloudFront
3. Configure SNS topic subscriptions for alarms
4. Set up CI/CD pipeline for automated deployments
5. Create additional Cognito users or configure federation
