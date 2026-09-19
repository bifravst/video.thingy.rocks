#!/bin/bash
# Provision a static, pre-shared SRTP key for one ingest port as an SSM SecureString
# parameter. Out-of-band, human-run step - key material must never be committed to the
# repo or set by CDK. See docs/TESTING-SRTP-INGESTION.md.
#
# Usage: ./scripts/provision-srtp-key.sh <port> <hexKey> <ssrc> [cipher] [auth]
#
# <port>   SRTP ingest port (6000-6009)
# <hexKey> 60 hex characters (30-byte SRTP master key + salt)
# <ssrc>   Fixed RTP SSRC the device will use on this port (decimal)
# [cipher] Default: aes-128-icm
# [auth]   Default: hmac-sha1-80

set -e

if [ $# -lt 3 ]; then
  echo "Usage: $0 <port> <hexKey> <ssrc> [cipher] [auth]"
  echo ""
  echo "Example: $0 6000 \$(openssl rand -hex 30) 3735928559"
  echo ""
  echo "Available ports: 6000-6009"
  exit 1
fi

PORT=$1
HEX_KEY=$2
SSRC=$3
CIPHER=${4:-aes-128-icm}
AUTH=${5:-hmac-sha1-80}

if [ "$PORT" -lt 6000 ] || [ "$PORT" -gt 6009 ]; then
  echo "Error: Port must be between 6000 and 6009"
  exit 1
fi

if ! [[ "$HEX_KEY" =~ ^[0-9a-fA-F]{60}$ ]]; then
  echo "Error: hexKey must be exactly 60 hex characters (30-byte master key+salt)"
  exit 1
fi

STACK_NAME="${STACK_NAME:-${STACK_PREFIX:-video}-streaming-2026-05}"
REGION="${AWS_REGION:-eu-central-1}"
PARAMETER_NAME="/${STACK_NAME}/srtp/port/${PORT}/key"

VALUE=$(printf '{"key":"%s","ssrc":%s,"cipher":"%s","auth":"%s"}' \
  "$HEX_KEY" "$SSRC" "$CIPHER" "$AUTH")

echo "Provisioning SRTP key for port $PORT at $PARAMETER_NAME (region $REGION)..."

aws ssm put-parameter \
  --region "$REGION" \
  --name "$PARAMETER_NAME" \
  --type SecureString \
  --overwrite \
  --value "$VALUE"

echo "Done. Restart/redeploy the instance(s) so the backend picks up the new key."
