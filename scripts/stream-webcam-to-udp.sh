#!/bin/bash
# Stream webcam video to UDP endpoint
# Usage: ./scripts/stream-webcam-to-udp.sh <port>

set -e

# Check arguments
if [ $# -lt 1 ]; then
  echo "Usage: $0 <port>"
  echo ""
  echo "Example: $0 5000"
  echo ""
  echo "Available ports: 5000-5009"
  exit 1
fi

PORT=$1
TARGET=video.thingy.rocks
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

# Detect video device
VIDEO_DEVICE="/dev/video0"
if [ "$(uname)" = "Darwin" ]; then
  # macOS uses AVFoundation
  VIDEO_INPUT="-f avfoundation -i 0"
  echo "Detected macOS - using AVFoundation"
else
  # Linux uses v4l2
  if [ ! -e "$VIDEO_DEVICE" ]; then
    echo "Error: No webcam found at $VIDEO_DEVICE"
    echo "Available video devices:"
    ls -la /dev/video* 2>/dev/null || echo "  None found"
    exit 1
  fi
  VIDEO_INPUT="-f v4l2 -i $VIDEO_DEVICE"
  echo "Detected Linux - using v4l2"
fi

echo ""
echo "Streaming Configuration:"
echo "  Target: $TARGET:$PORT"
echo "  Video: 1280x720 @ 30fps"
echo "  Codec: H.264"
echo "  Format: MPEG-TS over UDP"
echo ""
echo "Press Ctrl+C to stop streaming"
echo ""

# Stream webcam to UDP
# Using H.264 encoding with MPEG-TS container
ffmpeg \
  $VIDEO_INPUT \
  -video_size 1280x720 \
  -framerate 30 \
  -c:v libx264 \
  -preset ultrafast \
  -tune zerolatency \
  -b:v 2000k \
  -maxrate 2000k \
  -bufsize 4000k \
  -pix_fmt yuv420p \
  -g 60 \
  -f mpegts \
  "udp://${TARGET}:${PORT}?pkt_size=1316"
