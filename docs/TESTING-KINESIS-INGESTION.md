# Testing Kinesis Video Ingestion

How to test the UDP → FFmpeg → Kinesis Video Streams pipeline.

## Prerequisites

- Stack deployed (`npm run cdk:prod:deploy`).
- EC2 instances have `KINESIS_STREAM_PREFIX` set (e.g. `{stackName}-video`) so
  ingestion is enabled.
- FFmpeg installed on your machine for sending UDP (and on the server via
  user-data).

## 1. Unit tests

Run backend tests (including KinesisVideoSender):

```bash
cd backend && npm test
```

Run only KinesisVideoSender tests:

```bash
cd backend && node --no-warnings --experimental-transform-types --test src/KinesisVideoSender.spec.ts
```

## 2. Send UDP video to the server

Get an instance IP:

```bash
./scripts/get-instance-ip.sh
```

Stream a test source (synthetic video, H.264/MPEG-TS over UDP) to a port
(5000–5009):

```bash
./scripts/stream-testsrc-to-udp.sh <instance-ip> 5000
```

Or stream from a webcam:

```bash
./scripts/stream-webcam-to-udp.sh <instance-ip> 5000
```

The backend receives UDP on that port, starts the pipeline for that port, and
sends MKV to the Kinesis stream named `{KINESIS_STREAM_PREFIX}-{port}` (e.g.
`video-streaming-video-5000`).

## 3. Verify in AWS

1. **Kinesis Video Streams console**  
   Open the stream for the port you used (e.g. `{stackName}-video-5000`).
   Confirm fragments are ingesting (e.g. “Fragment count” or “Ingestion”
   metrics).

2. **Playback (optional)**  
   Use “Playback” or “Get HLS streaming session URL” in the console to play the
   stream (may take a short time after first fragments).

3. **Application logs**  
   To confirm pipeline start/stop and errors on the instance:

   ```bash
   ./scripts/ssh-to-instance.sh
   sudo tail -f /var/log/video-streaming/application.log
   ```

   Look for lines like “Kinesis ingestion started” and “PutMedia”/“FFmpeg”
   messages.

## 4. End-to-end script

The existing end-to-end script sends UDP and checks DynamoDB; it does not assert
on Kinesis:

```bash
./scripts/test-end-to-end.sh
```

After it runs, use the console (step 3) to confirm fragments for the stream
corresponding to the port used (e.g. 5000).

## Troubleshooting

- **Only “Updated last packet time” in logs, no Kinesis upload**  
  Check startup logs for **“Kinesis ingestion disabled (KINESIS_STREAM_PREFIX
  not set)”**. If you see that, the backend is not sending to Kinesis because
  the env var is missing. On the server, confirm the systemd service has it:  
  `systemctl show video-streaming.service --property=Environment`  
  or inspect `/etc/systemd/system/video-streaming.service`. The CDK replaces
  `__KINESIS_STREAM_PREFIX__` in user-data when the stack is deployed; replace
  instances (or redeploy) if the service was created before that was added.

- **No fragments in Kinesis / no video data received on Kinesis stream**  
  Ensure `KINESIS_STREAM_PREFIX` is set and the stack has Kinesis streams and
  IAM for GetDataEndpoint/PutMedia. In application logs, confirm: **“PutMedia
  connection established”** (HTTP 200), then **“PutMedia first fragment ACK”**
  (Buffering/Received). If you see connection established but no first fragment
  ACK, FFmpeg may not be producing MKV in time (the backend uses
  `-flush_packets 1` to flush promptly). If you see **“PutMedia Error ACK”**
  with e.g. `INVALID_MKV_DATA`, the MKV stream is being rejected. Check for
  “PutMedia error”, “PutMedia HTTP error”, or “FFmpeg error” for failures.

- **Stream not starting**  
  Send UDP for at least a few seconds so the inactivity timer sees activity and
  emits `streamStart`. Port must be in 5000–5009.

- **Invalid MKV / PutMedia errors**  
  Source must be valid MPEG-TS (e.g. H.264). The test scripts produce compatible
  streams; custom encoders must output MPEG-TS over UDP.

- **Repeating “PutMedia Error ACK” or same fragment in logs**  
  Repeated Error ACKs for the same fragment are throttled (logged once per 60s
  per fragment). If Kinesis keeps rejecting the same fragment, the stream is
  likely corrupt (e.g. out-of-order or lost UDP packets). Ensure the path to the
  server preserves packet order and avoids loss where possible.

- **Out-of-order or corrupt video**  
  UDP does not guarantee order. The pipeline buffers packets (default 128) and
  feeds FFmpeg in receive order; it does not reorder by RTP or TS continuity.
  For best results, send from a single source over a stable path. If the source
  uses RTP, consider adding RTP sequence-based reordering in the pipeline.
