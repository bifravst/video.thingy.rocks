#!/bin/bash
# Get CloudFormation stack outputs for the video streaming stack,
# including the NLB IPv6 address (not exposed as a CloudFormation output).

set -e

STACK_NAME="${STACK_NAME:-${STACK_PREFIX:-video}-streaming-2026-05}"
REGION="${AWS_REGION:-eu-central-1}"

echo "Fetching outputs from stack: $STACK_NAME in region: $REGION"
echo ""

OUTPUTS=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs" \
  --output json 2>/dev/null)

if [ $? -ne 0 ] || [ "$OUTPUTS" == "null" ] || [ "$OUTPUTS" == "[]" ]; then
  echo "Error: Could not fetch stack outputs. Make sure the stack is deployed and you have AWS credentials configured." >&2
  exit 1
fi

echo "Stack Outputs:"
echo "=============="
echo "$OUTPUTS" | jq -r '.[] | "\(.OutputKey): \(.OutputValue)"'

# NLB IPv6 isn't a CloudFormation output (CfnLoadBalancer doesn't expose it).
# Look up the NLB ENI to read its IPv6 address(es). CDK appends a hash to
# logical IDs (e.g. VideoStreamingNLBB4AEA49A), so match by type + prefix.
NLB_ARN=$(aws cloudformation describe-stack-resources \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "StackResources[?ResourceType=='AWS::ElasticLoadBalancingV2::LoadBalancer' && starts_with(LogicalResourceId, 'VideoStreamingNLB')] | [0].PhysicalResourceId" \
  --output text 2>/dev/null)

if [ -n "$NLB_ARN" ] && [ "$NLB_ARN" != "None" ]; then
  NLB_ENI_DESC="ELB ${NLB_ARN##*loadbalancer/}"
  NLB_IPV6=$(aws ec2 describe-network-interfaces \
    --region "$REGION" \
    --filters "Name=description,Values=$NLB_ENI_DESC" \
    --query 'NetworkInterfaces[].Ipv6Addresses[].Ipv6Address' \
    --output text | tr '\t' '\n')
  if [ -n "$NLB_IPV6" ]; then
    echo "$NLB_IPV6" | while read -r addr; do
      echo "NLBIPv6Address: $addr"
    done
  fi
fi
