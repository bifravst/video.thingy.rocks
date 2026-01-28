#!/bin/bash
# Quick script to SSH into the first running EC2 instance

set -e

echo "Finding running EC2 instances..."

# Get first running instance ID
INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].InstanceId" \
  --output text)

if [ "$INSTANCE_ID" = "None" ] || [ -z "$INSTANCE_ID" ]; then
  echo "❌ No running instances found!"
  echo ""
  echo "Check if instances are running:"
  echo "  aws ec2 describe-instances --filters \"Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*\""
  exit 1
fi

# Get instance details
INSTANCE_IP=$(aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

echo "✓ Found instance: $INSTANCE_ID"
echo "  Public IP: $INSTANCE_IP"
echo ""
echo "Connecting via AWS Systems Manager..."
echo ""

# Start SSM session
aws ssm start-session --target "$INSTANCE_ID"
