#!/bin/bash
# Update frontend/vite.config.ts with values from deployed stack

STACK_NAME="${STACK_PREFIX:-video}-streaming"
REGION="${AWS_REGION:-eu-central-1}"

echo "Fetching outputs from stack: $STACK_NAME in region: $REGION"

# Get all outputs
OUTPUTS=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs" \
  --output json 2>/dev/null)

if [ $? -ne 0 ] || [ "$OUTPUTS" == "null" ] || [ "$OUTPUTS" == "[]" ]; then
  echo "Error: Could not fetch stack outputs. Make sure the stack is deployed and you have AWS credentials configured."
  exit 1
fi

# Extract values
USER_POOL_URL=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="UserPoolURL") | .OutputValue')
USER_POOL_CLIENT_ID=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="UserPoolClientId") | .OutputValue')
IDENTITY_POOL_ID=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="IdentityPoolId") | .OutputValue')
TABLE_NAME=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="DynamoDBTableName") | .OutputValue')
CLOUDFRONT_URL=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="CloudFrontURL") | .OutputValue')

echo "Retrieved values:"
echo "  User Pool URL: $USER_POOL_URL"
echo "  User Pool Client ID: $USER_POOL_CLIENT_ID"
echo "  Identity Pool ID: $IDENTITY_POOL_ID"
echo "  DynamoDB Table: $TABLE_NAME"
echo "  CloudFront URL: $CLOUDFRONT_URL"
echo ""

# Update vite.config.ts
CONFIG_FILE="frontend/vite.config.ts"

if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: $CONFIG_FILE not found"
  exit 1
fi

# Create backup
cp "$CONFIG_FILE" "$CONFIG_FILE.backup"

# Update the file
sed -i.tmp \
  -e "s|'https://cognito-idp\.[^/]*/[^/]*/|'${USER_POOL_URL}|g" \
  -e "s|PLACEHOLDER_CLIENT_ID|${USER_POOL_CLIENT_ID}|g" \
  -e "s|eu-central-1:PLACEHOLDER-IDENTITY-POOL-ID|${IDENTITY_POOL_ID}|g" \
  -e "s|PLACEHOLDER_TABLE_NAME|${TABLE_NAME}|g" \
  -e "s|PLACEHOLDER\.cloudfront\.net|${CLOUDFRONT_URL}|g" \
  "$CONFIG_FILE"

rm -f "$CONFIG_FILE.tmp"

echo "✓ Updated $CONFIG_FILE"
echo "  Backup saved to $CONFIG_FILE.backup"
echo ""
echo "You can now build the frontend with: cd frontend && npm run build"
