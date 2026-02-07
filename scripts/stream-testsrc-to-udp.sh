#!/bin/bash
# Stream webcam video to UDP endpoint
# Usage: ./scripts/stream-webcam-to-udp.sh <instance-ip> <port>

set -e

# Check arguments
if [ $# -lt 2 ]; then
  echo "Usage: $0 <instance-ip> <port>"
  echo ""
  echo "Example: $0 3.123.45.67 5000"
  echo ""
  echo "Available ports: 5000-5009"
  exit 1
fi

INSTANCE_IP=$1
PORT=$2

# Validate port range
if [ "$PORT" -lt 5000 ] || [ "$PORT" -gt 5009 ]; then
  echo "Error: Port must be between 5000 and 5009"
  exit 1
fi

# Check if ffmpeg is installed
if ! command -v ffmpeg &> /dev/null; then
  echo "Error: ffmpeg is not installed"
  echo "Install with: sudo apt install ffmpeg (Ubuntu/Debian) or brew install ffmpeg (macOS)"
  exit 1
fi

echo ""
echo "Streaming Configuration:"
echo "  Target: $INSTANCE_IP:$PORT"
echo "  Video: 1280x720 @ 30fps"
echo "  Codec: H.264"
echo "  Format: MPEG-TS over UDP"
echo ""
echo "Press Ctrl+C to stop streaming"
echo ""

# Stream webcam to UDP
# Using H.264 encoding with MPEG-TS container
ffmpeg \
  -f lavfi -i testsrc=size=1280x720:rate=30 \
  -vf "drawtext='text=%{localtime\:%X.%N}:fontsize=32:fontcolor=white'" \
  -c:v libx264 \
  -preset ultrafast \
  -tune zerolatency \
  -b:v 2000k \
  -maxrate 2000k \
  -bufsize 4000k \
  -pix_fmt yuv420p \
  -g 60 \
  -f mpegts \
  "udp://${INSTANCE_IP}:${PORT}?pkt_size=1316"
