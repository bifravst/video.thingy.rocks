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
#   <ssrc>  N is the SSRC provisioned for the port: the plain decimal number
#           (e.g. 42) the camera writes into every RTP header - not the key,
#           which is read from --key-file, stdin, or a prompt, never from the
#           command line, where every local user could read it from /proc.
#
# No key provisioned for the port yet? This generates one (openssl), provisions
# it with scripts/provision-srtp-key.sh, saves it to a 0600 file, and tells you
# the backend restart that has to follow - you only supply the port and the SSRC:
#
#   ./scripts/stream-webcam-to-srtp.sh --provision <port> <ssrc>
#
# Provisioning a fresh key is also what a second streaming session against the
# same port needs: every sender run is a new session at rollover counter zero,
# so streaming twice under one key rewinds the packet index and reuses the
# keystream - the receiver refuses that (and logs it). Rotate between runs.

set -e

cd "$(dirname "$0")/.."

# Nothing key-shaped belongs in an argument vector, where every local user can
# read it from /proc for as long as anything runs. Everything passed to this
# script is checked before any of it can reach another process's argv - the
# receiver and the reference sender apply the same rule to their own.
for arg in "$@"; do
  if [[ "$arg" =~ ^[0-9a-fA-F]{40,}$ ]]; then
    echo "Error: that looks like an SRTP key (${arg:0:4}...), and a key never belongs"
    echo "on the command line - any local user could read it from /proc."
    echo ""
    echo "If it was meant as the SSRC: the SSRC is the plain decimal number the"
    echo "camera writes into every RTP header (e.g. 42), and --provision generates"
    echo "the key itself - there is nothing to paste. To stream with an existing"
    echo "key, pass it on stdin or with --key-file, never as an argument."
    exit 1
  fi
done

case "${1:-}" in
  --provision)
    if [ $# -ne 3 ]; then
      echo "Usage: $0 --provision <port> <ssrc>"
      echo ""
      echo "<ssrc> is the decimal number the camera writes into every RTP header,"
      echo "e.g. 42 - the key is generated for you."
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
    awk 'NR>1 && /^set -e$/ {exit} NR>1 {sub(/^# ?/, ""); print}' "$0"
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
