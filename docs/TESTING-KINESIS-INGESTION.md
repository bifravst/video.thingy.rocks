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
in user-data). The config uses standard log4cplus appenders (e.g. `STDOUT` with
`log4cplus::ConsoleAppender`), not `KvsConsoleAppender`, which can be
unregistered when the GStreamer plugin loads the config. **Credentials:** the
kvssink C++ plugin does not use the same credential chain as Node and often
reports "Could not find any AWS credentials!". The backend resolves credentials
in Node (env vars, EC2 instance profile/IMDS, etc.) and passes
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`
to the GStreamer child so kvssink finds them. For local runs, set
`KVS_LOG_CONFIG_PATH` to the path of `backend/kvs_log_configuration` (or an
absolute path to it). If the Node process is not started with `GST_PLUGIN_PATH`
and `LD_LIBRARY_PATH` (e.g. not from the systemd unit), set
`KINESIS_GST_PLUGIN_PATH` and `KINESIS_LD_LIBRARY_PATH` to the SDK build
directory so the GStreamer child can load kvssink and its dependencies.

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

- **"Could not initialize supporting library" / "Failed to set pipeline to
  PAUSED"**  
  This comes from the Kinesis Video C++ SDK inside kvssink when the underlying
  client fails to initialize (before any network call). Common causes:
  1. **Plugin/library path** – The process that runs `gst-launch-1.0` must have
     `GST_PLUGIN_PATH` and `LD_LIBRARY_PATH` set so the plugin and its .so
     dependencies (e.g. libKinesisVideoProducer, log4cplus) are found. When run
     under the systemd unit, these are set in the service file. If you start the
     app by hand (e.g. from a shell), set them in the same environment, or set
     `KINESIS_GST_PLUGIN_PATH` and `KINESIS_LD_LIBRARY_PATH` to the SDK build
     directory (e.g.
     `/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build`); the app will
     pass them through to the GStreamer child.

  2. **AWS credentials** – The backend resolves credentials in Node (env vars,
     EC2 instance profile/IMDS, `AWS_PROFILE`, etc.) and passes them to the
     GStreamer child so kvssink finds them. If you see "Could not find any AWS
     credentials!" or "Failed to init kvs producer", Node failed to resolve
     credentials (check "Failed to resolve AWS credentials for kvssink" in logs)
     or the child did not receive them. **"Could not load credentials from any
     providers"** means the Node credential chain failed: on EC2, ensure the
     instance has an IAM role attached (launch template / instance profile),
     that IMDS is not disabled (`AWS_EC2_METADATA_DISABLED` must be unset or
     `false`), and that the app has retried (the backend retries with backoff
     and uses a longer IMDS timeout). Locally, set `AWS_ACCESS_KEY_ID`,
     `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION` (or use `AWS_PROFILE`).

  3. **Log config** – The SDK requires a valid log4cplus config file. The
     pipeline uses `log-config`; default on EC2 is
     `/opt/video-streaming/kvs_log_configuration`. Override with
     `KVS_LOG_CONFIG_PATH` if the file is elsewhere. The file must exist and use
     appenders the SDK supports (e.g. `log4cplus::ConsoleAppender`).

  4. **Build incomplete or broken** – If the SDK build failed or was skipped,
     kvssink may load but fail when initializing the producer library. Check
     `/var/log/kinesis-sdk-build.log` on the instance and ensure
     `libgstkvssink.so` and the other built libraries exist in the build
     directory. Re-run the build or redeploy with a new instance if needed.

  5. **CONTINUITY: Mismatch** – tsdemux reports gaps in the MPEG-TS continuity
     counter when packets are lost or reordered on UDP. Reduce loss (better
     network, lower bitrate) or accept occasional warnings; the backend's
     reorder buffer can skip missing packets to avoid stalls, which may cause
     these warnings.

  6. **status 0x30000005 / "Could not write to resource"** – Kinesis Video
     rejects frames with overlapping or out-of-order timestamps. Often caused by
     the same loss/jitter that triggers CONTINUITY warnings. Improve network
     conditions or use a source with more stable timing; the pipeline may still
     upload after an initial burst of errors.

  7. **"Failed to submit ACK" / status 0x52000047** – The producer failed to
     submit a fragment ACK (RECEIVED/PERSISTED) to the service. Can be transient
     (network, service latency) or due to timestamp/stream state. Ingestion may
     continue; if persistent, check network and AWS service health.

```bash
# App uses fdsrc fd=0 (filesrc location=/dev/stdin fails when stdin is a pipe from Node spawn).
# In an interactive shell, escape ! or run from a script to avoid history expansion.
GST_PLUGIN_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build \
LD_LIBRARY_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build \
gst-launch-1.0 fdsrc fd=0 ! tsparse set-timestamps=true ! tsdemux name=d d. ! queue ! h264parse ! capsfilter caps="video/x-h264,stream-format=avc,alignment=au" ! kvssink stream-name="video-streaming-video-5000" aws-region="eu-central-1" storage-size=128 log-config="/opt/video-streaming/kvs_log_configuration"
```
