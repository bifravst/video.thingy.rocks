#!/bin/bash
# Stream a synthetic test video as SRTP-encrypted RTP/H.264 to an SRTP ingest endpoint.
# Usage: ./scripts/stream-testsrc-to-srtp.sh <host> <port> [hexKey] [ssrc]
#
# Uses GStreamer (srtpenc), not ffmpeg - ffmpeg's SRTP support is inconsistent across
# builds, whereas srtpenc is the natural, guaranteed-available counterpart to the
# backend's srtpdec (same gst-plugins-bad package).
#
# TEST KEY ONLY - the default key/SSRC below are for local testing only. Never reuse them
# as a real SSM-provisioned production value; provision real keys with
# scripts/provision-srtp-key.sh using freshly generated key material.

set -e

if [ $# -lt 2 ]; then
  echo "Usage: $0 <host> <port> [hexKey] [ssrc]"
  echo ""
  echo "Example: $0 video.thingy.rocks 6000"
  echo ""
  echo "Available ports: 6000-6009"
  exit 1
fi

HOST=$1
PORT=$2
# TEST KEY ONLY - DO NOT USE IN PRODUCTION. 60 hex chars = 30-byte master key+salt, same
# format the backend requires (backend/src/SrtpKeyStore.ts).
HEX_KEY=${3:-0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab}
SSRC=${4:-3735928559}

if [ "$PORT" -lt 6000 ] || [ "$PORT" -gt 6009 ]; then
  echo "Error: Port must be between 6000 and 6009"
  exit 1
fi

if ! [[ "$HEX_KEY" =~ ^[0-9a-fA-F]{60}$ ]]; then
  echo "Error: hexKey must be exactly 60 hex characters (30-byte master key+salt)"
  exit 1
fi

if ! command -v gst-launch-1.0 &> /dev/null; then
  echo "Error: gst-launch-1.0 is not installed"
  echo "Install with: sudo apt install gstreamer1.0-tools gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly gstreamer1.0-plugins-good (Ubuntu/Debian) or brew install gstreamer gst-plugins-bad gst-plugins-ugly gst-plugins-good (macOS)"
  exit 1
fi

if ! gst-inspect-1.0 srtpenc &> /dev/null; then
  echo "Error: GStreamer element 'srtpenc' not found (ships in gst-plugins-bad; Ubuntu/Debian: gstreamer1.0-plugins-bad)"
  exit 1
fi

if ! gst-inspect-1.0 x264enc &> /dev/null; then
  echo "Error: GStreamer element 'x264enc' not found (ships in gst-plugins-ugly, a separate package from gst-plugins-bad; Ubuntu/Debian: gstreamer1.0-plugins-ugly)"
  exit 1
fi

echo ""
echo "Streaming Configuration:"
echo "  Target: $HOST:$PORT"
echo "  Video: 640x480 @ 30fps (videotestsrc)"
echo "  Codec: H.264 over RTP (RFC 6184), SRTP-encrypted"
echo "  SSRC: $SSRC"
echo ""
echo "Press Ctrl+C to stop streaming"
echo ""

gst-launch-1.0 -e \
  videotestsrc is-live=true \
  ! videoconvert \
  ! x264enc tune=zerolatency speed-preset=ultrafast \
  ! rtph264pay config-interval=1 pt=96 ssrc="$SSRC" \
  ! srtpenc key="${HEX_KEY}" rtp-cipher=aes-128-icm rtp-auth=hmac-sha1-80 rtcp-cipher=aes-128-icm rtcp-auth=hmac-sha1-80 \
  ! udpsink host="$HOST" port="$PORT"
