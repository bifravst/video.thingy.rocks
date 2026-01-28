#!/bin/bash
# Development Script: Update Backend Code on Running Instances
# 
# WARNING: This is for DEVELOPMENT/DEBUGGING only!
# For production, always use: npm run cdk:deploy + terminate instances
#
# This script:
# 1. Copies local backend code to running EC2 instances
# 2. Installs dependencies
# 3. Restarts the service
#
# Use this for rapid iteration during development.

set -e

echo "=========================================="
echo "Development: Update Backend Code"
echo "=========================================="
echo ""
echo "⚠️  WARNING: This is for development only!"
echo "⚠️  For production, use CDK deployment."
echo ""

# Get stack name
STACK_NAME="${1:-video-streaming}"

# Check if backend directory exists
if [ ! -d "backend" ]; then
  echo "❌ Error: backend directory not found!"
  echo "Run this script from the project root."
  exit 1
fi

echo "1. Finding running EC2 instances..."
INSTANCE_IDS=$(aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[*].Instances[*].InstanceId" \
  --output text)

if [ -z "$INSTANCE_IDS" ]; then
  echo "❌ No running instances found!"
  exit 1
fi

echo "  Found instances: $INSTANCE_IDS"
echo ""

# Create temporary archive of backend code
echo "2. Creating archive of backend code..."
TEMP_DIR=$(mktemp -d)
ARCHIVE="$TEMP_DIR/backend.tar.gz"

tar -czf "$ARCHIVE" \
  --exclude='node_modules' \
  --exclude='*.spec.ts' \
  --exclude='.git' \
  -C backend \
  .

echo "  ✓ Archive created: $ARCHIVE"
echo "  Size: $(du -h "$ARCHIVE" | cut -f1)"
echo ""

# Update each instance
for INSTANCE_ID in $INSTANCE_IDS; do
  echo "3. Updating instance: $INSTANCE_ID"
  echo "-------------------------------------------"
  
  # Get instance IP for display
  INSTANCE_IP=$(aws ec2 describe-instances \
    --instance-ids "$INSTANCE_ID" \
    --query "Reservations[0].Instances[0].PublicIpAddress" \
    --output text)
  
  echo "  Instance IP: $INSTANCE_IP"
  
  # Upload archive to S3 for transfer
  echo "  Uploading code to S3..."
  CODE_BUCKET=$(aws s3 ls | grep "$STACK_NAME" | grep -i code | awk '{print $3}' | head -1)
  
  if [ -z "$CODE_BUCKET" ]; then
    echo "  ❌ Error: Could not find code bucket!"
    echo "  Looking for bucket with '$STACK_NAME' and 'code' in name"
    continue
  fi
  
  TEMP_S3_KEY="dev-updates/backend-$(date +%Y%m%d_%H%M%S).tar.gz"
  aws s3 cp "$ARCHIVE" "s3://$CODE_BUCKET/$TEMP_S3_KEY"
  echo "  ✓ Uploaded to s3://$CODE_BUCKET/$TEMP_S3_KEY"
  
  # Run update on instance via SSM
  echo "  Running update on instance..."
  COMMAND_ID=$(aws ssm send-command \
    --instance-ids "$INSTANCE_ID" \
    --document-name "AWS-RunShellScript" \
    --parameters 'commands=[
      "echo Downloading backend code from S3...",
      "aws s3 cp s3://'"$CODE_BUCKET"'/'"$TEMP_S3_KEY"' /tmp/backend.tar.gz",
      "echo Stopping service...",
      "sudo systemctl stop video-streaming.service",
      "echo Backing up current code...",
      "sudo cp -r /opt/video-streaming /opt/video-streaming.backup.$(date +%Y%m%d_%H%M%S) 2>/dev/null || true",
      "echo Extracting new code...",
      "cd /opt/video-streaming",
      "sudo tar -xzf /tmp/backend.tar.gz",
      "echo Installing dependencies...",
      "sudo npm install --production",
      "echo Starting service...",
      "sudo systemctl start video-streaming.service",
      "sleep 3",
      "echo",
      "echo Service status:",
      "sudo systemctl status video-streaming.service --no-pager || true",
      "echo",
      "echo Recent logs:",
      "sudo tail -20 /var/log/video-streaming/application.log",
      "echo",
      "echo Errors (if any):",
      "sudo tail -10 /var/log/video-streaming/error.log 2>/dev/null || echo No errors"
    ]' \
    --output text \
    --query 'Command.CommandId')
  
  echo "  Command ID: $COMMAND_ID"
  echo "  Waiting for update to complete..."
  sleep 10
  
  # Get command output
  echo ""
  echo "  Update output:"
  aws ssm get-command-invocation \
    --command-id "$COMMAND_ID" \
    --instance-id "$INSTANCE_ID" \
    --query 'StandardOutputContent' \
    --output text 2>/dev/null || echo "  (Command still running or failed)"
  
  echo ""
  echo "  ✓ Instance $INSTANCE_ID updated"
  echo ""
done

# Cleanup
rm -rf "$TEMP_DIR"

echo "=========================================="
echo "Update Complete!"
echo "=========================================="
echo ""
echo "All instances have been updated with local backend code."
echo ""
echo "Next steps:"
echo "1. Test the changes:"
echo "   ./scripts/ssh-to-instance.sh"
echo "   sudo tail -f /var/log/video-streaming/application.log"
echo ""
echo "2. Send test packets and verify behavior"
echo ""
echo "3. If changes work, commit them and deploy via CDK:"
echo "   git add backend/"
echo "   git commit -m 'Fix: ...'"
echo "   npm run cdk:deploy"
echo "   # Then terminate instances to get persistent fix"
echo ""
echo "⚠️  Remember: This update is TEMPORARY!"
echo "⚠️  Instances will revert to CDK-deployed code when replaced."
