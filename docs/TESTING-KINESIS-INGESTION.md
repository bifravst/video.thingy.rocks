# Testing Kinesis Video Ingestion

How to test the UDP → GStreamer (kvssink) → Kinesis Video Streams pipeline.

## Prerequisites

- Stack deployed (`npm run cdk:prod:deploy`).
- EC2 instances have `KINESIS_STREAM_PREFIX` set (e.g. `{stackName}-video`) so
  ingestion is enabled.
- FFmpeg installed on your machine for sending UDP (used by the test scripts).
  The server uses GStreamer and the AWS kvssink plugin (built from the Kinesis
  Video C++ Producer SDK in user-data).

## Pipeline overview

The backend receives MPEG-TS over UDP, feeds it to a GStreamer pipeline that
reads from stdin, demuxes TS, parses H.264, and sends it to Kinesis Video
Streams via the **kvssink** plugin. Ingestion does not use Node’s PutMedia;
kvssink handles the connection to Kinesis. The systemd service sets
`GST_PLUGIN_PATH` and `LD_LIBRARY_PATH` so the Node-spawned `gst-launch-1.0`
finds the kvssink plugin and its dependencies.

## 1. Unit tests

Run backend tests (including KinesisVideoSender, which is retained for possible
future use):

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

The backend receives UDP on that port, starts the GStreamer pipeline for that
port, and kvssink sends H.264 to the Kinesis stream named
`{KINESIS_STREAM_PREFIX}-{port}` (e.g. `video-streaming-video-5000`).

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

   Look for “Kinesis ingestion started” and “GStreamer” messages. There are no
   “PutMedia” lines from the backend; kvssink sends directly to Kinesis.

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

- **No fragments in Kinesis / kvssink not found**  
  Ensure `GST_PLUGIN_PATH` and `LD_LIBRARY_PATH` are set in the systemd unit
  (see user-data). The kvssink plugin is built from the Amazon Kinesis Video
  Streams C++ Producer SDK during instance bootstrap. On the instance, verify:  
  `GST_PLUGIN_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build LD_LIBRARY_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build:/opt/amazon-kinesis-video-streams-producer-sdk-cpp/open-source/local/lib gst-inspect-1.0 kvssink`  
  If this fails, the SDK build in user-data may have failed; check
  `/var/log/cloud-init-output.log`.

- **No fragments in Kinesis (plugin loads)**  
  Ensure `KINESIS_STREAM_PREFIX` is set and the stack has Kinesis streams and
  IAM for the EC2 role (GetDataEndpoint, PutMedia). In application logs, look
  for “Kinesis ingestion started” and any “GStreamer stderr” or “GStreamer
  error” messages. Source must be valid MPEG-TS with H.264; the test scripts
  produce compatible streams.

- **Stream not starting**  
  Send UDP for at least a few seconds so the inactivity timer sees activity and
  emits `streamStart`. Port must be in 5000–5009.

- **Invalid or corrupt video**  
  Source must be valid MPEG-TS (e.g. H.264). The test scripts produce compatible
  streams; custom encoders must output MPEG-TS over UDP.

- **Out-of-order or corrupt video**  
  UDP does not guarantee order. The pipeline buffers packets (default 128) and
  feeds GStreamer stdin in receive order; it does not reorder by RTP or TS
  continuity. For best results, send from a single source over a stable path. If
  the source uses RTP, consider adding RTP sequence-based reordering in the
  pipeline.
