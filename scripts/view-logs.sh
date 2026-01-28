#!/bin/bash
# Helper script to view CloudWatch Logs for EC2 instances

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}Available log groups:${NC}"
echo "1. /video-streaming/application - Application logs (systemd journal)"
echo "2. /video-streaming/system - System logs (/var/log/messages)"
echo "3. /video-streaming/cloud-init - Cloud-init logs"
echo ""

# Default to application logs
LOG_GROUP="${1:-/video-streaming/application}"

echo -e "${GREEN}Viewing logs from: ${LOG_GROUP}${NC}"
echo ""

# Check if log group exists
if ! aws logs describe-log-groups --log-group-name-prefix "$LOG_GROUP" --query 'logGroups[0].logGroupName' --output text 2>/dev/null | grep -q "$LOG_GROUP"; then
    echo -e "${YELLOW}Warning: Log group '$LOG_GROUP' not found yet.${NC}"
    echo "This is normal if instances just started. Logs will appear once the CloudWatch agent starts."
    echo ""
    echo "Available log groups:"
    aws logs describe-log-groups --log-group-name-prefix "/video-streaming" --query 'logGroups[*].logGroupName' --output table
    exit 1
fi

# Get the most recent log stream
LOG_STREAM=$(aws logs describe-log-streams \
    --log-group-name "$LOG_GROUP" \
    --order-by LastEventTime \
    --descending \
    --max-items 1 \
    --query 'logStreams[0].logStreamName' \
    --output text)

if [ "$LOG_STREAM" = "None" ] || [ -z "$LOG_STREAM" ]; then
    echo -e "${YELLOW}No log streams found in $LOG_GROUP${NC}"
    exit 1
fi

echo -e "${GREEN}Most recent log stream: ${LOG_STREAM}${NC}"
echo ""

# Tail the logs
echo -e "${BLUE}Tailing logs (Ctrl+C to stop)...${NC}"
echo ""

aws logs tail "$LOG_GROUP" --follow --format short

# Alternative: View last 50 lines without following
# aws logs tail "$LOG_GROUP" --since 1h --format short
