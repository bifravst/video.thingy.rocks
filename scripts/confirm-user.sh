#!/bin/bash
# Manually confirm a Cognito user (workaround for email delivery issues)

set -e

# Check arguments
if [ $# -lt 1 ]; then
  echo "Usage: $0 <email-address> [password]"
  echo ""
  echo "This script manually confirms a user in Cognito User Pool"
  echo "Use this as a workaround when verification emails are not being delivered"
  echo ""
  echo "Example: $0 user@example.com MyPassword123"
  exit 1
fi

EMAIL=$1
PASSWORD=${2:-}

# Get User Pool ID from stack outputs
echo "Getting User Pool ID from CloudFormation..."
STACK_NAME="${STACK_PREFIX:-video}-streaming"
USER_POOL_ID=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='UserPoolId'].OutputValue" \
  --output text 2>/dev/null)

if [ -z "$USER_POOL_ID" ] || [ "$USER_POOL_ID" = "None" ]; then
  echo "Error: Could not find User Pool ID in stack outputs"
  echo "Make sure the stack is deployed with the updated Cognito configuration"
  exit 1
fi

echo "User Pool ID: $USER_POOL_ID"
echo ""

# Confirm the user
echo "Confirming user: $EMAIL"
aws cognito-idp admin-confirm-sign-up \
  --user-pool-id "$USER_POOL_ID" \
  --username "$EMAIL"

if [ $? -eq 0 ]; then
  echo "✓ User confirmed successfully"
else
  echo "✗ Failed to confirm user"
  echo ""
  echo "Possible reasons:"
  echo "  - User doesn't exist (sign up first)"
  echo "  - User is already confirmed"
  echo "  - Invalid User Pool ID"
  exit 1
fi

# Set password if provided
if [ -n "$PASSWORD" ]; then
  echo ""
  echo "Setting permanent password..."
  aws cognito-idp admin-set-user-password \
    --user-pool-id "$USER_POOL_ID" \
    --username "$EMAIL" \
    --password "$PASSWORD" \
    --permanent

  if [ $? -eq 0 ]; then
    echo "✓ Password set successfully"
  else
    echo "✗ Failed to set password"
    exit 1
  fi
fi

echo ""
echo "=========================================="
echo "User Setup Complete!"
echo "=========================================="
echo ""
echo "Email: $EMAIL"
echo "Status: Confirmed"
if [ -n "$PASSWORD" ]; then
  echo "Password: Set"
fi
echo ""
echo "The user can now sign in at:"
echo "  http://localhost:8080 (development)"
echo "  https://video.thingy.rocks (production)"
