#!/bin/bash
# End-to-end test script - automates the complete testing workflow

set -e

echo "=========================================="
echo "Video Streaming End-to-End Test"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Step 1: Check prerequisites
echo "Step 1: Checking prerequisites..."
echo ""

if ! command -v aws &> /dev/null; then
  echo -e "${RED}❌ AWS CLI not found${NC}"
  echo "Install with: https://aws.amazon.com/cli/"
  exit 1
fi
echo -e "${GREEN}✓${NC} AWS CLI installed"

if ! command -v ffmpeg &> /dev/null; then
  echo -e "${RED}❌ FFmpeg not found${NC}"
  echo "Install with: sudo apt install ffmpeg (Linux) or brew install ffmpeg (macOS)"
  exit 1
fi
echo -e "${GREEN}✓${NC} FFmpeg installed"

if ! command -v jq &> /dev/null; then
  echo -e "${YELLOW}⚠${NC} jq not found (optional but recommended)"
  echo "Install with: sudo apt install jq (Linux) or brew install jq (macOS)"
fi

echo ""

# Step 2: Verify stack is deployed
echo "Step 2: Verifying stack deployment..."
echo ""

STACK_NAME="${STACK_PREFIX:-video}-streaming"
STACK_STATUS=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].StackStatus" \
  --output text 2>/dev/null || echo "NOT_FOUND")

if [ "$STACK_STATUS" = "NOT_FOUND" ]; then
  echo -e "${RED}❌ Stack not deployed${NC}"
  echo "Deploy with: npm run cdk:prod:deploy"
  exit 1
fi

if [[ "$STACK_STATUS" != *"COMPLETE"* ]]; then
  echo -e "${RED}❌ Stack status: $STACK_STATUS${NC}"
  echo "Wait for deployment to complete or check for errors"
  exit 1
fi

echo -e "${GREEN}✓${NC} Stack deployed: $STACK_STATUS"
echo ""

# Step 3: Get stack outputs
echo "Step 3: Getting stack outputs..."
echo ""

CLOUDFRONT_DOMAIN=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='CloudFrontURL'].OutputValue" \
  --output text 2>/dev/null)

TABLE_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='DynamoDBTableName'].OutputValue" \
  --output text 2>/dev/null)

BUCKET_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='VideoBucketName'].OutputValue" \
  --output text 2>/dev/null)

echo -e "${GREEN}✓${NC} CloudFront: $CLOUDFRONT_DOMAIN"
echo -e "${GREEN}✓${NC} DynamoDB: $TABLE_NAME"
echo -e "${GREEN}✓${NC} S3 Bucket: $BUCKET_NAME"
echo ""

# Step 4: Find running instance
echo "Step 4: Finding running EC2 instance..."
echo ""

INSTANCE_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text 2>/dev/null)

if [ "$INSTANCE_IP" = "None" ] || [ -z "$INSTANCE_IP" ]; then
  echo -e "${RED}❌ No running instances found${NC}"
  echo "Wait for instances to start or check Auto Scaling Group"
  exit 1
fi

echo -e "${GREEN}✓${NC} Instance IP: $INSTANCE_IP"
echo ""

# Step 5: Test connectivity
echo "Step 5: Testing connectivity to instance..."
echo ""

if ping -c 1 -W 2 "$INSTANCE_IP" &> /dev/null; then
  echo -e "${GREEN}✓${NC} Instance is reachable"
else
  echo -e "${YELLOW}⚠${NC} Instance ping failed (may be blocked by security group)"
fi
echo ""

# Step 6: Instructions for streaming
echo "=========================================="
echo "Ready to Test!"
echo "=========================================="
echo ""
echo "To start streaming your webcam:"
echo ""
echo -e "${YELLOW}./scripts/stream-webcam-to-udp.sh $INSTANCE_IP 5000${NC}"
echo ""
echo "Keep that terminal open while streaming."
echo ""
echo "Then, in another terminal:"
echo ""
echo "  cd frontend"
echo "  npm start"
echo "  # Open http://localhost:8080"
echo ""
echo "Or open the production URL if deployed."
echo ""

# Step 7: Offer to start streaming
read -p "Start streaming now? (y/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
  echo ""
  echo "Starting webcam stream to $INSTANCE_IP:5000..."
  echo "Press Ctrl+C to stop"
  echo ""
  sleep 2
  ./scripts/stream-webcam-to-udp.sh "$INSTANCE_IP" 5000
else
  echo ""
  echo "Test preparation complete!"
  echo ""
  echo "Next steps:"
  echo "  1. Run: ./scripts/stream-webcam-to-udp.sh $INSTANCE_IP 5000"
  echo "  2. Open web app: cd frontend && npm start"
  echo "  3. View stream at http://localhost:8080"
  echo ""
  echo "For detailed testing instructions, see TESTING.md"
fi
