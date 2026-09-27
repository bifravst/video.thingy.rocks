#!/bin/bash
# Streams a local webcam as SRTP-encrypted RTP/H.264 to an SRTP ingest port, so
# the deployed stack can be watched with real video data: authentication, the
# rollover-counter search, media reaching Kinesis, the lock, and the recovery
# behavior on restarts - all under a real camera instead of the synthetic
# source. See docs/TESTING-SRTP-INGESTION.md.
#
# Usage:
#   ./scripts/stream-webcam-to-srtp.sh <host> <port> [--ssrc N] [--device /dev/video0] [--key-file key.txt]
#
#   <host>  the ingest endpoint, the NLB DNS name of the stack
#   <port>  an SRTP ingest port, 6000-6009
#
# The key is the 60 hex characters provisioned for the port, read from
# --key-file, from stdin, or from an interactive prompt - never from the
# command line, where every local user could read it from /proc for as long as
# the stream runs.
#
# No key provisioned for the port yet? This generates one, provisions it, and
# tells you what to do next:
#
#   ./scripts/stream-webcam-to-srtp.sh --provision <port> <ssrc>
#
# Provisioning a fresh key is also what a second streaming session against the
# same port needs: every sender run is a new session at rollover counter zero,
# so streaming twice under one key rewinds the packet index and reuses the
# keystream - the receiver refuses that (and logs it). Rotate between runs.

set -e

cd "$(dirname "$0")/.."

case "${1:-}" in
  --provision)
    if [ $# -ne 3 ]; then
      echo "Usage: $0 --provision <port> <ssrc>"
      exit 1
    fi
    KEY_FILE=$(mktemp "${TMPDIR:-/tmp}/srtp-webcam-key.XXXXXXXXXX")
    chmod 600 "$KEY_FILE"
    openssl rand -hex 30 >"$KEY_FILE"
    # The ops script reads the key from the file; it never passes it anywhere
    # near an argument vector.
    ./scripts/provision-srtp-key.sh "$2" "$3" "$KEY_FILE"
    echo ""
    echo "The key is saved at $KEY_FILE (0600, mktemp of this run)."
    echo "The backend resolves keys at service start, so restart it before"
    echo "streaming: sudo systemctl restart video-streaming.service on the"
    echo "instances, or a rolling deploy."
    echo ""
    echo "Then stream your webcam:"
    echo "  ./scripts/stream-webcam-to-srtp.sh <nlb-dns> $2 --ssrc $3 --key-file $KEY_FILE"
    exit 0
    ;;
  -h|--help)
    sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
esac

if [ $# -lt 2 ]; then
  echo "Usage: $0 <host> <port> [--ssrc N] [--device /dev/video0] [--key-file key.txt]"
  echo "       $0 --provision <port> <ssrc>"
  echo ""
  echo "Run with --help for the full story."
  exit 1
fi

# The streaming itself stays in the GStreamer application, which enforces the
# same two guards the receiver applies (no key-shaped argv, no caps-logging
# debug settings) and stops cleanly on Ctrl+C.
exec python3 scripts/stream-testsrc-to-srtp.py --source webcam "$@"
