# Frontend Configuration

This document explains how to configure the frontend with values from the
deployed AWS stack.

## Quick Start

After deploying the CDK stack, run this script to automatically update the
frontend configuration:

```bash
./scripts/update-frontend-config.sh
```

This will fetch all values from CloudFormation and update
`frontend/vite.config.ts` with the deployed values.

## Configuration Values

The frontend requires the following configuration values from the deployed
stack:

1. **Cognito User Pool URL** - For OIDC authentication
2. **Cognito User Pool Client ID** - For authentication
3. **Cognito Identity Pool ID** - For AWS credentials (authenticated and
   unauthenticated access)
4. **DynamoDB Table Name** - For stream metadata
5. **CloudFront Domain Name** - For video content delivery

## Getting Configuration Values

### Option 1: Use the update script (Recommended)

Automatically update the vite.config.ts:

```bash
./scripts/update-frontend-config.sh
```

### Option 2: View stack outputs

Run the helper script to see all stack outputs:

```bash
./scripts/get-stack-outputs.sh
```

This will display all outputs and provide ready-to-use environment variable
exports.

### Option 3: Query CloudFormation directly

Get specific values:

```bash
# User Pool URL
aws cloudformation describe-stacks \
  --stack-name video-streaming \
  --query "Stacks[0].Outputs[?OutputKey=='UserPoolURL'].OutputValue" \
  --output text

# User Pool Client ID
aws cloudformation describe-stacks \
  --stack-name video-streaming \
  --query "Stacks[0].Outputs[?OutputKey=='UserPoolClientId'].OutputValue" \
  --output text

# Identity Pool ID
aws cloudformation describe-stacks \
  --stack-name video-streaming \
  --query "Stacks[0].Outputs[?OutputKey=='IdentityPoolId'].OutputValue" \
  --output text

# DynamoDB Table Name
aws cloudformation describe-stacks \
  --stack-name video-streaming \
  --query "Stacks[0].Outputs[?OutputKey=='DynamoDBTableName'].OutputValue" \
  --output text

# CloudFront URL
aws cloudformation describe-stacks \
  --stack-name video-streaming \
  --query "Stacks[0].Outputs[?OutputKey=='CloudFrontURL'].OutputValue" \
  --output text
```

## Deployment Workflow

1. **Deploy the CDK stack**:

   ```bash
   npm run cdk:deploy
   ```

2. **Update frontend configuration**:

   ```bash
   ./scripts/update-frontend-config.sh
   ```

3. **Build the frontend**:
   ```bash
   cd frontend
   npm run build
   ```

## Development

For local development, you can use environment variables. Run
`./scripts/get-stack-outputs.sh` to get the export commands, then:

```bash
cd frontend
npm start
```

The dev server will be available at http://localhost:8080

## Authentication

The application uses Cognito User Pool for authentication with OIDC:

- Users can sign up with email
- Email verification is required
- Password requirements: min 8 characters, uppercase, lowercase, digits
- OAuth callback URLs are configured for localhost:8080 and video.thingy.rocks

## Troubleshooting

### "Failed to fetch stream list from DynamoDB"

- Check that `COGNITO_IDENTITY_POOL_ID` is correct
- Verify the Identity Pool allows unauthenticated access
- Ensure the roles have read permissions on the DynamoDB table

### "Authentication failed" or "User pool not found"

- Verify `COGNITO_USER_POOL_URL` is correct
- Check that `COGNITO_USER_POOL_CLIENT_ID` matches the deployed client
- Ensure OAuth callback URLs include your domain

### Configuration not updating

- Make sure you've run `./scripts/update-frontend-config.sh` after deploying
- Check that AWS credentials are configured correctly
- Try clearing browser cache and rebuilding
