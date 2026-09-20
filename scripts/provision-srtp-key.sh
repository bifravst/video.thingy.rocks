#!/bin/bash
# Provision a static, pre-shared SRTP key for one ingest port as an SSM SecureString
# parameter. Out-of-band, human-run step - key material must never be committed to the
# repo or set by CDK. See docs/TESTING-SRTP-INGESTION.md.
#
# Usage: ./scripts/provision-srtp-key.sh <port> <hexKey> <ssrc>
#
# <port>   SRTP ingest port (6000-6009)
# <hexKey> 60 hex characters (30-byte SRTP master key + salt)
# <ssrc>   Fixed RTP SSRC the device will use on this port (decimal)
#
# Cipher/auth are not configurable here: aes-128-icm/hmac-sha1-80 is the only suite
# validated end-to-end (it's also the only one whose key length - 30 bytes - matches what
# the backend and this script enforce; a different suite, e.g. aes-256-icm, needs a longer
# key and would be rejected by backend/src/SrtpKeyStore.ts).

set -e

if [ $# -lt 3 ]; then
  echo "Usage: $0 <port> <hexKey> <ssrc>"
  echo ""
  echo "Example: $0 6000 \$(openssl rand -hex 30) 3735928559"
  echo ""
  echo "Available ports: 6000-6009"
  exit 1
fi

PORT=$1
HEX_KEY=$2
SSRC=$3
CIPHER=aes-128-icm
AUTH=hmac-sha1-80

# Validate the exact canonical port form - no shell integer arithmetic: values outside
# Bash's integer range make both `-lt`/`-gt` tests fail open ("integer expression
# expected" evaluates false), and non-canonical forms like "06000" pass the numeric tests
# but produce a parameter path (.../port/06000/key) the backend never requests, since it
# only ever asks for .../port/6000/key. The exact-match regex rejects all of those.
if ! [[ "$PORT" =~ ^600[0-9]$ ]]; then
  echo "Error: port must be one of 6000-6009, in canonical decimal form (no leading zeros)"
  exit 1
fi

if ! [[ "$HEX_KEY" =~ ^[0-9a-fA-F]{60}$ ]]; then
  echo "Error: hexKey must be exactly 60 hex characters (30-byte master key+salt)"
  exit 1
fi

# Must be a canonical decimal uint32 (no leading zeros, no sign, no whitespace) - anything
# else either breaks the JSON below (e.g. "abc", or "0123" which JSON forbids as a leading
# zero) or gets silently rejected later by backend/src/SrtpKeyStore.ts's own uint32 check.
# Catch it here so this script doesn't report success for a value that will never work.
# Values longer than 10 decimal digits are rejected by *string length* first: they can
# never be <= 4294967295, and bash's `[` cannot compare them as integers at all (it
# prints "integer expression expected" and exits with an error status, which the `if`
# below then treats as a false condition - letting the invalid value through).
if ! [[ "$SSRC" =~ ^(0|[1-9][0-9]*)$ ]] || [ "${#SSRC}" -gt 10 ] || [ "$SSRC" -gt 4294967295 ]; then
  echo "Error: ssrc must be a canonical decimal uint32 (0-4294967295, no leading zeros)"
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
