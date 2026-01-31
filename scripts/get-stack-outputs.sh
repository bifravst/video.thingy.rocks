#!/bin/bash
# Get CloudFormation stack outputs for the video streaming stack

STACK_NAME="${STACK_PREFIX:-video}-streaming"
REGION="${AWS_REGION:-eu-central-1}"

echo "Fetching outputs from stack: $STACK_NAME in region: $REGION"
echo ""

# Get all outputs in JSON format
OUTPUTS=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs" \
  --output json 2>/dev/null)

if [ $? -ne 0 ] || [ "$OUTPUTS" == "null" ] || [ "$OUTPUTS" == "[]" ]; then
  echo "Error: Could not fetch stack outputs. Make sure the stack is deployed and you have AWS credentials configured."
  exit 1
fi

# Print in readable format
echo "Stack Outputs:"
echo "=============="
echo "$OUTPUTS" | jq -r '.[] | "\(.OutputKey): \(.OutputValue)"'

echo ""
echo "Environment Variables for .envrc:"
echo "=================================="
echo "export COGNITO_USER_POOL_URL=\"$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="UserPoolURL") | .OutputValue')\""
echo "export COGNITO_USER_POOL_CLIENT_ID=\"$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="UserPoolClientId") | .OutputValue')\""
echo "export COGNITO_IDENTITY_POOL_ID=\"$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="IdentityPoolId") | .OutputValue')\""
echo "export TABLE_NAME=\"$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="DynamoDBTableName") | .OutputValue')\""
echo "