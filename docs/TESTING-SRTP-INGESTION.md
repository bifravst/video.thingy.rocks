# Testing SRTP Ingestion

How to test the SRTP → GStreamer (srtpdec + kvssink) → Kinesis Video Streams
pipeline. This is additive to (and shares most infrastructure with) the
unencrypted UDP/MPEG-TS pipeline documented in
[TESTING-KINESIS-INGESTION.md](./TESTING-KINESIS-INGESTION.md).

## Prerequisites

- Stack deployed (`npm run cdk:prod:deploy`).
- EC2 instances have `SRTP_INGESTION_ENABLED=true` set (the CDK stack always
  sets this), and a matching SRTP key provisioned for the port you're testing
  (see below).
- Local GStreamer with `gst-plugins-bad` (for `srtpenc`) **and**
  `gst-plugins-ugly` (for `x264enc`) installed - both are used by the test
  sender script, and `x264enc` is a separate package from `srtpenc`'s. Confirm
  with:
  ```bash
  gst-inspect-1.0 srtpenc
  gst-inspect-1.0 x264enc
  ```

## Pipeline overview

Devices send SRTP-encrypted RTP/H.264 (RFC 6184) to ports 6000-6009. The backend
(`backend/src/UDPListener.ts`) receives each UDP datagram and relays it,
unmodified and individually, to a loopback port that GStreamer's own `udpsrc`
binds to - this preserves the per-datagram framing SRTP decryption depends on
(unlike the unencrypted path's FIFO/byte-stream bridge, which is fine for
self-synchronizing MPEG-TS but would destroy RTP/SRTP packet boundaries).
GStreamer then does
`udpsrc ! srtpdec ! rtpjitterbuffer ! rtph264depay ! h264parse ! kvssink`, same
kvssink tail as the unencrypted path. Keys are static and pre-shared, loaded
once at process start from SSM Parameter Store (see
`backend/src/SrtpKeyStore.ts`).

**SRTP and unencrypted ingestion share the same Kinesis Video Streams**, named
`video-streaming-2026-09-video-1` through `-10`. SRTP port 6000+N targets the
same stream as unencrypted port 5000+N (e.g. port 6000 and port 5000 both feed
`video-streaming-2026-09-video-1`). A device is assigned one port range or the
other - never send both transports for the same numbered stream at once.

**Key management is per-port**: each SRTP port (6000-6009) has its own key and a
fixed, provisioned SSRC. The device must use a stable SSRC on a given port -
`srtpdec`'s static-key mode pins the SSRC in the pipeline's caps. **Session
continuity**: a device must never restart its RTP sequence numbering while
keeping the same key and SSRC (the backend persists rollover-counter state under
that identity and would seed the fresh session with the previous session's ROC -
see backend/README.md, "Session continuity"). When re-running the test sender
below after it has exited, either:

1. provision a **fresh key/SSRC pair** for the port and make the backend load
   it - `./scripts/provision-srtp-key.sh <port> <newKeyHex> <newSsrc>`, then
   restart/redeploy the instance(s) (keys and SSRCs are resolved once at process
   start) - and pass that key/SSRC to the sender, **or**
2. keep the same key/SSRC, clear the slot's persisted ROC fields
   (`srtpRoc`/`srtpHighestSeq`/`srtpRocSsrc`/`srtpRocKeyFingerprint`) from the
   `StreamMetadata` DynamoDB item, **and restart the backend on the instance(s)
   receiving the stream** - clearing DynamoDB alone is not enough: the backend
   keeps its per-port in-memory ROC estimate until the process restarts (a
   stream stopping does not clear it, and `KinesisIngestionPipeline.seedSrtpRoc`
   deliberately never replaces a more-current in-memory estimate with a lower
   persisted value), so a sender restarted against the same still-running
   process would still be seeded with the old ROC and fail to decrypt.

Note that passing a fresh SSRC to the sender **alone** does not work: the
backend's `srtpdec` is pinned to the SSRC loaded from SSM at startup, so every
packet would be rejected as an SSRC (or key) mismatch until the parameter is
reprovisioned and the backend has loaded it.

## 1. Unit tests

```bash
cd backend && npm test
```

Run only the SRTP key-validation tests:

```bash
cd backend && node --no-warnings --experimental-transform-types --test src/SrtpKeyStore.spec.ts
```

## 2. Provision a test key

Generate fresh test key material and provision it for a port (e.g. 6000). **Save
the generated key in a shell variable** - it is needed again in step 3, and the
sender will not authenticate against the backend if you let it fall back to its
built-in test key:

```bash
TEST_KEY=$(openssl rand -hex 30)
./scripts/provision-srtp-key.sh 6000 "$TEST_KEY" 3735928559
```

Restart or redeploy the instance(s) so the backend picks up the new parameter
(keys are loaded once at process start, not live-reloaded).

If you're testing locally (not against a deployed stack), set
`SRTP_KEY_PARAMETER_PREFIX` and run the backend against your own AWS credentials
so it can read the parameter you just created, or point
`SRTP_KEY_PARAMETER_PREFIX` at a parameter path you've provisioned by hand.

## 3. Send an SRTP test stream

Get an instance IP (same script as the unencrypted path):

```bash
./scripts/get-instance-ip.sh
```

Stream a synthetic SRTP test source to a port (6000-6009), passing **the same
key and SSRC you provisioned in step 2** (here `"$TEST_KEY"` from that step and
the provisioned SSRC):

```bash
./scripts/stream-testsrc-to-srtp.sh <instance-ip> 6000 "$TEST_KEY" 3735928559
```

The script's built-in default key/SSRC are for local testing only and will
**not** authenticate against a backend provisioned with your own key - always
pass the provisioned values explicitly.

The backend receives UDP on that port, relays it to GStreamer's `udpsrc`, and
kvssink sends decrypted H.264 to the numbered Kinesis Video Stream for that
port: port 6000 → `video-streaming-2026-09-video-1`, port 6001 →
`video-streaming-2026-09-video-2`, ..., port 6009 →
`video-streaming-2026-09-video-10` (`port - 6000 + 1`) - the same stream the
unencrypted path would use for the corresponding 5000+N port.

## 4. Verify in AWS

Same as the unencrypted path (see
[TESTING-KINESIS-INGESTION.md](./TESTING-KINESIS-INGESTION.md#3-verify-in-aws)),
using the stream name for the port you tested (e.g. port 6000 →
`video-streaming-2026-09-video-1`), and looking for "SRTP Kinesis ingestion
started" (not "Kinesis ingestion started") in application logs.

## Troubleshooting

- **`gst-inspect-1.0 srtpdec` / `srtpenc` / `rtph264depay` / `rtpjitterbuffer` /
  `udpsrc` fails on the EC2 instance** - these ship in the
  `gstreamer1-plugins-base`/`good`/`bad-free` packages already installed by
  `cdk/user-data.sh`, which also runs this check (fatally - a missing element
  aborts bootstrap, so the instance never opens its health port and the target
  group stays unhealthy) during bootstrap; check
  `/var/log/cloud-init-output.log` for the warning if ingestion doesn't start.

- **"No SRTP key configured for port; refusing to start ingestion"** - the SSM
  parameter `/{stackName}/srtp/port/{port}/key` is missing, malformed JSON, the
  key isn't 60 hex characters, or it was provisioned without
  `--type SecureString` (plain `String` parameters are rejected on load). Re-run
  `scripts/provision-srtp-key.sh` and restart the instance(s).

- **`srtpdec` authentication/auth-tag failures (no fragments in Kinesis,
  GStreamer stderr shows auth failures)** - almost always a key,
  cipher/auth-suite, or SSRC mismatch between the device and the provisioned SSM
  parameter. Confirm the SSRC the device is actually sending matches what's
  provisioned; a device that picks a new SSRC per session will not work with the
  static-key approach used here.

- **No packets arriving at all** - confirm ports 6000-6009 are open in the
  security group (`cdk/StreamingStack.ts`) and that the NLB has UDP listeners
  for the port you're using (`SrtpUDPListener{port}`).

- **Out-of-order or corrupt video** - unlike the unencrypted path's ad hoc
  receive-order buffer, the SRTP path relays datagrams immediately and relies on
  GStreamer's `rtpjitterbuffer` for real RTP-sequence-aware reordering.
  Persistent issues here point to network loss/jitter upstream of the NLB, not
  the backend.

- **Decryption/auth failures only after a restart, on an otherwise-healthy
  long-running stream** - likely an SRTP rollover counter (ROC) mismatch. The
  backend tracks and persists the ROC _and_ the highest RTP sequence number seen
  under it per stream slot (in DynamoDB, `srtpRoc`/`srtpHighestSeq`) by
  observing the plaintext RTP sequence number of every SRTP datagram - not just
  while a pipeline is actively running, but for the whole lock-held window
  (including while GStreamer has crashed and is waiting to restart) - and seeds
  a fresh GStreamer pipeline with the full state (see
  `KinesisIngestionPipeline.trackSrtpRoc`/`seedSrtpRoc`/`getSrtpRocState`). Both
  fields are persisted together because restoring only the ROC (and inventing a
  highest-sequence of 0) misclassifies the next real packet whenever the
  sender's actual sequence number at restart time isn't near 0. Without any of
  this, a fresh `srtpdec` instance always starts at ROC 0, which breaks
  decryption for any restart (GStreamer crash, redeploy, EC2 reboot) after the
  sender's 16-bit sequence number has ever wrapped. This is a best-effort
  estimate, not a guarantee - if it's ever visibly wrong, check the
  `srtpRoc`/`srtpHighestSeq` values in the `StreamMetadata` DynamoDB item for
  the affected slot (clearing them as a fresh-session reset also requires a
  backend restart to take effect - see "Session continuity" above, which
  explains why). This state is only trusted when both `srtpRocSsrc` and
  `srtpRocKeyFingerprint` (a non-secret hash of the key, see
  `SrtpKeyStore.keyFingerprint`) still match the currently-configured key - a
  key rotated in SSM without changing the SSRC would otherwise seed a fresh
  session with a previous key's stale state. If DynamoDB itself can't be read at
  startup, the backend treats that as "state unavailable" (not a fresh ROC of 0)
  and releases the lock/backs off rather than guessing. The ROC handed to
  `srtpdec` via its caps is the raw persisted/tracked ROC (see
  `srtpdecSeedRoc`): libsrtp

  > = 2.3 (any version with `srtp_set_stream_roc`, which the caps `roc` field
  > requires) applies it directly to the first packet's extended index -
  > `(seededRoc << 16) | seq` - and pins its rollover state from that estimate,
  > so no extra compensation is needed or correct, regardless of which half of
  > the 16-bit sequence space the sender is currently in.

- **Two producers competing for the same stream** - a device must use only one
  of the unencrypted or SRTP transports per assigned stream slot (see
  `backend/README.md`); the backend refuses to start a second producer for a
  slot that already has one active (logged as "paired port for this stream slot
  is already active"), and the DynamoDB lock is keyed by stream slot (not raw
  port) so this is enforced across instances too. If you see this log line, some
  client is sending on both port 5000+N and port 6000+N.

- **Everything else** (credentials, `log-config`, kvssink plugin path, KVS
  timestamp/continuity errors) - see the shared troubleshooting section in
  [TESTING-KINESIS-INGESTION.md](./TESTING-KINESIS-INGESTION.md#troubleshooting);
  it applies to both paths since they share the same kvssink tail and
  credential-resolution logic.
