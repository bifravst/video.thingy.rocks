# Scripts

This directory contains helper scripts for managing the video streaming
infrastructure.

## Configuration Scripts

### get-stack-outputs.sh

Retrieves all CloudFormation stack outputs and displays them in a readable
format.

```bash
./scripts/get-stack-outputs.sh
```

**Output:**

- Lists all stack outputs (User Pool URL, Client ID, Identity Pool ID, DynamoDB
  table, CloudFront URL, etc.)
- Provides ready-to-use environment variable export commands

**Usage:**

```bash
# View outputs
./scripts/get-stack-outputs.sh

# Copy the export commands to your .envrc or shell
```

### update-frontend-config.sh

Automatically updates `frontend/vite.config.ts` with values from the deployed
stack.

```bash
./scripts/update-frontend-config.sh
```

**What it does:**

1. Fetches all CloudFormation stack outputs
2. Updates the default values in `frontend/vite.config.ts`
3. Creates a backup of the original file

**Usage:**

```bash
# After deploying the CDK stack
npm run cdk:deploy

# Update frontend configuration
./scripts/update-frontend-config.sh

# Build frontend
cd frontend && npm run build
```

## Development Scripts

### dev-update-backend.sh

Updates the backend code on running EC2 instances during development.

```bash
./scripts/dev-update-backend.sh
```

### ssh-to-instance.sh

SSH into a running EC2 instance using AWS Systems Manager Session Manager.

```bash
./scripts/ssh-to-instance.sh
```

### view-logs.sh

View CloudWatch logs from EC2 instances.

```bash
./scripts/view-logs.sh
```

## Workflow

### Initial Deployment

1. Deploy the CDK stack:

   ```bash
   npm run cdk:deploy
   ```

2. Update frontend configuration:

   ```bash
   ./scripts/update-frontend-config.sh
   ```

3. Build and deploy frontend:
   ```bash
   cd frontend
   npm run build
   # Deploy to S3 or hosting service
   ```

### Development Workflow

1. Make changes to backend code

2. Update running instances:

   ```bash
   ./scripts/dev-update-backend.sh
   ```

3. View logs to verify:

   ```bash
   ./scripts/view-logs.sh
   ```

4. SSH to instance if needed:
   ```bash
   ./scripts/ssh-to-instance.sh
   ```

### Configuration Updates

If you need to check or update configuration values:

```bash
# View current stack outputs
./scripts/get-stack-outputs.sh

# Update frontend with latest values
./scripts/update-frontend-config.sh
```

## Environment Variables

These scripts use the following environment variables:

- `AWS_PROFILE` - AWS profile to use (default: from .envrc)
- `AWS_REGION` - AWS region (default: eu-central-1)
- `STACK_PREFIX` - Stack name prefix (default: video)

Set these in your `.envrc` file or export them in your shell.
