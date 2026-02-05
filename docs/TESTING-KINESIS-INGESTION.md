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
finds the kvssink plugin and its dependencies. The KVS C++ SDK requires a
log4cplus config file; the pipeline passes its path via the `log-config`
property (default on EC2: `/opt/video-streaming/kvs_log_configuration`, created
in user-data). For local runs, set `KVS_LOG_CONFIG_PATH` to the path of
`backend/kvs_log_configuration` (or an absolute path to it).

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

## Verify GStreamer (minimal pipeline)

On the server (or wherever you run GStreamer), confirm a minimal pipeline works.
Use **filesrc** (same as the app); **fdsrc fd=0** can trigger shell history
expansion (`!`) or parser errors on some systems:

```bash
# Recommended: same pattern as the app (stdin via /dev/stdin)
echo -n "" | gst-launch-1.0 -e filesrc location=/dev/stdin ! fakesink
```

If you must use `fdsrc`, avoid history expansion by running in a script or
escaping the bang (e.g. in bash: `gst-launch-1.0 -e fdsrc fd=0 \! fakesink`).

## Troubleshooting

- **Only “Updated last packet time” in logs, no Kinesis upload**  
  Check startup logs for **“Kinesis ingestion disabled (KINESIS_STREAM_PREFIX
  not set)”**. If you see that, the backend is not sending to Kinesis because
  the env var is missing. On the server, confirm the systemd service has it:  
  `systemctl show video-streaming.service --property=Environment`  
  or inspect `/etc/systemd/system/video-streaming.service`. The CDK replaces
  `__KINESIS_STREAM_PREFIX__` in user-data when the stack is deployed; replace
  instances (or redeploy) if the service was created before that was added.

- **"erroneous pipeline: syntax error" when testing GStreamer**  
  See **Verify GStreamer (minimal pipeline)** above. Use
  `filesrc location=/dev/stdin ! fakesink`; avoid `fdsrc fd=0 ! …` in
  interactive shells (the `!` can be expanded by the shell).

- **no element "kvssink"**  
  GStreamer can’t find the kvssink plugin because `GST_PLUGIN_PATH` (and usually
  `LD_LIBRARY_PATH`) are not set for that process. When you run the pipeline
  **manually** on the server, set the same environment as the systemd service,
  for example:  
  `GST_PLUGIN_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build LD_LIBRARY_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build gst-launch-1.0 -e filesrc location=/dev/stdin ! … ! kvssink …`  
  The app process gets these from the service; only manual runs need them set in
  the shell.

- **No fragments in Kinesis / kvssink not found**  
  Ensure `GST_PLUGIN_PATH` and `LD_LIBRARY_PATH` are set in the systemd unit
  (see user-data). The kvssink plugin is built from the Amazon Kinesis Video
  Streams C++ Producer SDK during instance bootstrap. On the instance, verify:  
  `GST_PLUGIN_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build LD_LIBRARY_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build gst-inspect-1.0 kvssink`  
  If this fails, the SDK build in user-data may have failed. Check
  `/var/log/cloud-init-output.log` and **`/var/log/kinesis-sdk-build.log`**
  (full cmake/make output). If `build/` only contains `CMakeCache.txt` and
  `CMakeFiles`, `make` did not complete; install build deps (e.g. `autoconf`,
  `automake`, `libcurl-devel`) and re-run the build or redeploy. **"Each
  download failed" / "Connection timed out"** when building the SDK usually
  means the instance cannot reach the internet on port 80 (e.g. security group
  only allowed 443); the stack allows TCP 80 egress so dependency tarballs (e.g.
  from ftp.gnu.org) can be downloaded during bootstrap. If **`build/` is empty**
  (no `libgstkvssink.so`, not even `CMakeCache.txt`), the clone or cmake step
  likely failed; check `kinesis-sdk-build.log` and ensure the instance has
  outbound HTTP/HTTPS. Redeploying with a clean instance runs the build from
  scratch (user-data removes the SDK dir before cloning). **"Killed signal
  terminated program cc1plus"** means the build ran out of memory (OOM);
  user-data uses `make -j2` to limit memory use. On very small instances, run
  the build manually with `make -j1`.

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
