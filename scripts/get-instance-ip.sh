#!/bin/bash
# Get the public IP address of a running EC2 instance

set -e

echo "Finding running EC2 instances..."

# Get all running instances
INSTANCES=$(aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[*].Instances[*].[InstanceId,PublicIpAddress,PrivateIpAddress,LaunchTime]" \
  --output text)

if [ -z "$INSTANCES" ]; then
  echo "❌ No running instances found!"
  echo ""
  echo "Possible reasons:"
  echo "  1. Stack not deployed yet - run: npm run cdk:prod:deploy"
  echo "  2. Instances are starting up - wait a few minutes"
  echo "  3. Auto Scaling Group scaled down to 0"
  echo ""
  echo "Check instance status:"
  echo "  aws ec2 describe-instances --filters \"Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*\""
  exit 1
fi

echo ""
echo "Running Instances:"
echo "=================="
echo "$INSTANCES" | while IFS=$'\t' read -r instance_id public_ip private_ip launch_time; do
  echo "Instance ID: $instance_id"
  echo "  Public IP:  $public_ip"
  echo "  Private IP: $private_ip"
  echo "  Launched:   $launch_time"
  echo ""
done

# Get first instance IP for easy copying
FIRST_IP=$(echo "$INSTANCES" | head -1 | awk '{print $2}')

echo "To stream webcam to this instance:"
echo "  ./scripts/stream-webcam-to-udp.sh $FIRST_IP 5000"
echo ""
echo "Available ports: 5000-5009"
