#!/bin/bash
# Quick script to connect to the first running EC2 instance via AWS Systems Manager (SSM).
# Uses the same region as the stack (AWS_REGION or eu-central-1).

set -e

REGION="${AWS_REGION:-eu-central-1}"
STACK_NAME="${STACK_PREFIX:-video}-streaming"

echo "Finding running EC2 instances (region: $REGION)..."

# Get first running instance ID
INSTANCE_ID=$(aws ec2 describe-instances \
  --region "$REGION" \
  --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].InstanceId" \
  --output text)

if [ "$INSTANCE_ID" = "None" ] || [ -z "$INSTANCE_ID" ]; then
  echo "❌ No running instances found!"
  echo ""
  echo "Check if instances are running:"
  echo "  aws ec2 describe-instances --region $REGION --filters \"Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*\""
  exit 1
fi

# Get instance details
INSTANCE_IP=$(aws ec2 describe-instances \
  --region "$REGION" \
  --instance-ids "$INSTANCE_ID" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

# Check if instance is connected to SSM (avoids cryptic TargetNotConnected)
SSM_STATUS=$(aws ssm describe-instance-information \
  --region "$REGION" \
  --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
  --query "InstanceInformationList[0].PingStatus" \
  --output text 2>/dev/null || true)

if [ "$SSM_STATUS" != "Online" ]; then
  echo "❌ Instance $INSTANCE_ID is not connected to Systems Manager (status: ${SSM_STATUS:-unknown})"
  echo ""
  echo "Common causes:"
  echo "  • Instance just launched – user-data (yum, npm, Kinesis SDK build) can take 10–30 min. Wait and retry."
  echo "  • Wrong region – ensure AWS_REGION matches your stack (e.g. export AWS_REGION=eu-central-1)."
  echo "  • SSM agent not running – check instance in EC2 → Fleet Manager; reboot if needed."
  echo ""
  echo "Retry with: ./scripts/ssh-to-instance.sh"
  exit 1
fi

echo "✓ Found instance: $INSTANCE_ID (SSM: $SSM_STATUS)"
echo "  Public IP: $INSTANCE_IP"
echo ""
echo "Connecting via AWS Systems Manager..."
echo ""

aws ssm start-session --target "$INSTANCE_ID" --region "$REGION"
