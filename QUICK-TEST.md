# Quick Test Guide

Fast track to test the video streaming system with your webcam.

## Prerequisites

- CDK stack deployed
- FFmpeg installed
- Webcam connected

## 5-Minute Test

### 1. Get Instance IP

```bash
./scripts/get-instance-ip.sh
```

Copy the IP address shown.

### 2. Start Streaming

```bash
./scripts/stream-webcam-to-udp.sh <INSTANCE_IP> 5000
```

Replace `<INSTANCE_IP>` with the IP from step 1.

Keep this running!

### 3. Open Web App

```bash
cd frontend
npm start
```

Open http://localhost:8080

### 4. Watch Your Stream

- Sign in (or it will use unauthenticated access)
- Click on "Port 5000" stream
- See yourself on screen!

## Test Offline/Online

1. Stop stream: Press `Ctrl+C` in streaming terminal
2. Wait 1 minute
3. Refresh browser - should show "OFFLINE" with last frame
4. Restart stream: Run step 2 again
5. Browser should auto-switch to live video

## Verify It Works

✅ Stream shows as "ACTIVE" in web app ✅ Video plays with 5-15 second delay ✅
Can switch between quality levels ✅ Offline detection works ✅ Auto-resume
works

## Troubleshooting

**No stream appears?**

```bash
./scripts/view-logs.sh
```

Look for "UDP packet received"

**Can't find instance?**

```bash
aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
  --query "Reservations[*].Instances[*].[InstanceId,State.Name,PublicIpAddress]" \
  --output table
```

**Webcam not working?**

```bash
# Linux
ls -la /dev/video*

# macOS
ffmpeg -f avfoundation -list_devices true -i ""
```

## Full Documentation

See [TESTING.md](TESTING.md) for comprehensive testing instructions.
