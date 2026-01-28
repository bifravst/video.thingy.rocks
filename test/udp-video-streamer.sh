#!/bin/bash
# UDP Video Streamer - Sends real MPEG-TS video via UDP
# This replaces the fake packet generator with real video data

set -e

# Default values
PORT=5000
HOST=""
DURATION=60
BITRATE=2000
PATTERN="testsrc"
STACK_NAME="video-streaming"
AUTO_DISCOVER=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --port)
            PORT="$2"
            shift 2
            ;;
        --host)
            HOST="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --bitrate)
            BITRATE="$2"
            shift 2
            ;;
        --pattern)
            PATTERN="$2"
            shift 2
            ;;
        --stack)
            STACK_NAME="$2"
            shift 2
            ;;
        --auto)
            AUTO_DISCOVER=true
            shift
            ;;
        -h|--help)
            echo "UDP Video Streamer"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --port PORT          Target UDP port (default: 5000)"
            echo "  --host HOST          Target host/IP (required unless --auto is used)"
            echo "  --auto               Auto-discover EC2 instance IP from stack"
            echo "  --stack NAME         Stack name for auto-discovery (default: video-streaming)"
            echo "  --duration SECONDS   Stream duration (default: 60)"
            echo "  --bitrate KBPS       Video bitrate (default: 2000)"
            echo "  --pattern PATTERN    Video pattern (default: testsrc)"
            echo "                       Options: testsrc, mandelbrot, life, plasma"
            echo "  -h, --help           Show this help"
            echo ""
            echo "Examples:"
            echo "  # Stream to localhost"
            echo "  $0 --host localhost --port 5000 --duration 30"
            echo ""
            echo "  # Stream to specific IP"
            echo "  $0 --host 54.123.45.67 --port 5000"
            echo ""
            echo "  # Auto-discover instance IP and stream"
            echo "  $0 --auto --port 5000 --duration 30"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Auto-discover instance IP if requested
if [ "$AUTO_DISCOVER" = true ]; then
    echo "Auto-discovering EC2 instance IP..."
    
    # Find running instance from the stack
    INSTANCE_IP=$(aws ec2 describe-instances \
        --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
                  "Name=instance-state-name,Values=running" \
        --query "Reservations[0].Instances[0].PublicIpAddress" \
        --output text 2>/dev/null)
    
    if [ -z "$INSTANCE_IP" ] || [ "$INSTANCE_IP" = "None" ]; then
        echo "❌ Error: No running instances found"
        echo "   Make sure your stack is deployed and instances are running"
        exit 1
    fi
    
    HOST="$INSTANCE_IP"
    echo "✓ Found instance: $HOST"
    echo ""
fi

# Validate host is provided
if [ -z "$HOST" ]; then
    echo "❌ Error: Host is required"
    echo "   Use --host <IP> or --auto to discover instance IP"
    echo "   Use --help for more information"
    exit 1
fi

echo "UDP Video Streamer"
echo "=================="
echo "Target: udp://$HOST:$PORT"
echo "Duration: $DURATION seconds"
echo "Bitrate: $BITRATE kbps"
echo "Pattern: $PATTERN"
echo ""

# Check if ffmpeg is installed
if ! command -v ffmpeg &> /dev/null; then
    echo "❌ Error: ffmpeg is not installed"
    exit 1
fi

echo "Starting video stream..."
echo "Press Ctrl+C to stop early"
echo ""

# Generate and stream video via UDP
ffmpeg -re \
    -f lavfi -i "${PATTERN}=duration=${DURATION}:size=1920x1080:rate=30" \
    -f lavfi -i "sine=frequency=1000:duration=${DURATION}" \
    -c:v libx264 \
    -preset ultrafast \
    -tune zerolatency \
    -b:v ${BITRATE}k \
    -g 30 \
    -keyint_min 30 \
    -c:a aac \
    -b:a 128k \
    -f mpegts \
    "udp://$HOST:$PORT?pkt_size=1316"

echo ""
echo "Stream complete!"
