#!/bin/bash
# Provision a static, pre-shared SRTP key for one ingest port as an SSM SecureString
# parameter. Out-of-band, human-run step - key material must never be committed to the
# repo or set by CDK. See docs/TESTING-SRTP-INGESTION.md.
#
# Usage: ./scripts/provision-srtp-key.sh <port> <ssrc> [keyFile]
#
# <port>    SRTP ingest port (6000-6009)
# <ssrc>    Fixed RTP SSRC the device will use on this port (decimal)
# [keyFile] File holding the key; read from stdin when omitted
#
# The key is 60 hex characters (a 30-byte SRTP master key + salt) and is never passed as
# an argument, to this script or to the AWS CLI. A process's argument vector is readable
# by other local users through /proc/<pid>/cmdline for as long as it runs, so a key
# passed that way is exposed to every account on the machine - and, via shell history,
# to anything that later reads the history file. It is read from stdin or a file and
# handed to the CLI in a 0600 request file that is deleted on exit. The same rule is
# enforced on the ingest path: see KEY_SHAPED in backend/src/srtp_pipeline.py, which
# refuses to start if anything key-shaped appears in its own argv.
#
# Cipher/auth are not configurable here: aes-128-icm/hmac-sha1-80 is the only suite
# validated end-to-end (it's also the only one whose key length - 30 bytes - matches what
# the backend and this script enforce; a different suite, e.g. aes-256-icm, needs a longer
# key and would be rejected by backend/src/SrtpKeyStore.ts).

set -e

if [ $# -lt 2 ] || [ $# -gt 3 ]; then
  echo "Usage: $0 <port> <ssrc> [keyFile]"
  echo ""
  echo "The key is read from [keyFile], or from stdin when it is omitted."
  echo ""
  echo "Example: openssl rand -hex 30 | $0 6000 3735928559"
  echo ""
  echo "Available ports: 6000-6009"
  exit 1
fi

PORT=$1
SSRC=$2
KEY_FILE=${3:-}
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

# Read the key only once the arguments are known good, so a typo in the port does not
# first prompt for - or consume - key material. `read -r` keeps backslashes literal and
# drops the trailing newline that `openssl rand -hex` and most editors add.
if [ -n "$KEY_FILE" ]; then
  if [ ! -r "$KEY_FILE" ]; then
    echo "Error: cannot read key file: $KEY_FILE"
    exit 1
  fi
  IFS= read -r HEX_KEY <"$KEY_FILE" || true
elif [ -t 0 ]; then
  # Interactive: -s keeps the key off the screen and out of the scrollback.
  IFS= read -rs -p "SRTP key (60 hex characters): " HEX_KEY
  echo ""
else
  IFS= read -r HEX_KEY || true
fi

if ! [[ "$HEX_KEY" =~ ^[0-9a-fA-F]{60}$ ]]; then
  echo "Error: the key must be exactly 60 hex characters (30-byte master key+salt)"
  exit 1
fi

STACK_NAME="${STACK_NAME:-${STACK_PREFIX:-video}-streaming-2026-05}"
REGION="${AWS_REGION:-eu-central-1}"
PARAMETER_NAME="/${STACK_NAME}/srtp/port/${PORT}/key"

# The whole request goes in a file so that neither the key nor the JSON wrapping it
# appears in the AWS CLI's argument vector. mktemp creates it 0600, and the EXIT trap
# removes it however the script ends - normally, on the `set -e` path below, or on one
# of the signals after it.
#
# Cleanup belongs to EXIT alone, and the signal traps only exit. A trap on a signal
# replaces the signal's default action of ending the script, and when the handler
# returns bash carries on from where it was interrupted - so a signal trap that merely
# deleted the file let an interrupted run recreate it by plain redirection, with the
# caller's umask rather than 0600, and then go on to provision the key anyway.
REQUEST_FILE=$(mktemp "${TMPDIR:-/tmp}/srtp-key-request.XXXXXXXXXX")
trap 'rm -f "$REQUEST_FILE"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# --cli-input-json takes the entire request body, so --value never appears. The inner
# value is a JSON string containing JSON, hence the escaped quotes; every part of it is
# either a fixed literal or already validated above as hex or decimal digits, so there is
# nothing here that could need further escaping.
printf '{"Name":"%s","Type":"SecureString","Overwrite":true,"Value":"{\\"key\\":\\"%s\\",\\"ssrc\\":%s,\\"cipher\\":\\"%s\\",\\"auth\\":\\"%s\\"}"}' \
  "$PARAMETER_NAME" "$HEX_KEY" "$SSRC" "$CIPHER" "$AUTH" >"$REQUEST_FILE"

echo "Provisioning SRTP key for port $PORT at $PARAMETER_NAME (region $REGION)..."

aws ssm put-parameter \
  --region "$REGION" \
  --cli-input-json "file://$REQUEST_FILE"

echo "Done. Restart/redeploy the instance(s) so the backend picks up the new key."
