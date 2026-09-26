# Testing SRTP ingestion

This is the operator manual for the SRTP ingest path (UDP ports 6000-6009): how
it works, how to provision keys, how to send a test stream, and - most
importantly - how to run the e2e suite that decides whether it works.
`SRTP-DESIGN.md` explains why it is built the way it is;
`SRTP-REVIEW-FINDINGS.md` is the complete list of bugs from the two previous
attempts and how this design handles each.

## Prerequisites

- A deployed stack: `npm run cdk:prod:deploy`
- `ffmpeg` locally (the e2e suite's isolation case uses it for the unencrypted
  path)
- The e2e suite also needs credentials allowed to provision SSM parameters,
  query CloudWatch metrics/logs, read DynamoDB, and send SSM commands to the
  fleet

## How it works

- Devices send SRTP (RTP/H.264 encrypted per RFC 3711) to UDP ports 6000-6009 on
  the NLB. One port = one device = one static, pre-shared key.
- Each port is owned by one helper process (`backend/src/srtp_port.py`) for the
  life of the service: it **binds the public port itself** and hands the same
  kernel socket to every GStreamer pipeline it builds, so nothing relays
  datagrams through the Node service and a pipeline rebuild never unbinds the
  port.
- The helper searches for the sender's rollover counter by seeding `srtpdec`
  with candidates and watching the pad that only emits what libsrtp
  authenticated. Only an authenticated packet can produce a report, so traffic
  that does not hold the key cannot make the port do anything but keep
  searching. When the supervisor has acquired the port's Kinesis lock, it grants
  producing
  (`udpsrc ! srtpdec ! rtpjitterbuffer ! rtph264depay ! h264parse ! kvssink`),
  on the same socket.
- The lock row for an SRTP port is only ever taken **after** authentication and
  refreshed only while authenticated traffic continues.

## The rollover counter takes care of itself

- The highest packet index ever accepted under a key and SSRC is persisted as
  the port's **replay floor**. A sender that carries on counting while the
  receiver restarts is re-confirmed within a few datagrams (`trials: 1` in the
  application log) - no operator action.
- A sender that **rewinds** its packet index under the same key is refused:
  every packet at or below the floor is dropped and counted
  (`dropped N authenticated datagrams at or below the highest packet index already accepted`).
  Rewinding reuses the AES-CM keystream, which is not survivable; **rotate the
  key** (`scripts/provision-srtp-key.sh`), restart the service, and the device
  starts a clean index space.
- A wrong or missing floor costs the candidates spent on failed trials - video
  resumes at the sender's next keyframe.
- The floor write itself is best effort by design: production is never stopped
  over it. The honest cost of a write that fails is a bounded replay window -
  after a restart, datagrams already accepted above the stale persisted floor
  authenticate again until the floor catches up (the write is monotonic, so it
  converges on the next successful report).

## Provisioning a port key

```bash
STACK_NAME=<stack> ./scripts/provision-srtp-key.sh 6000 42
# key read from stdin (openssl rand -hex 30), never an argument
```

`42` is the port's SSRC. Keys are SecureStrings under the AWS-managed `aws/ssm`
key (no KMS grant needed; the one supported suite is `aes-128-icm` +
`hmac-sha1-80`, 30 bytes of key+salt). Keys are resolved at **service start**:
after (re)provisioning, restart the service
(`sudo systemctl restart video-streaming.service` on the instances, or a rolling
deploy).

## Sending a test stream

The reference sender (`scripts/stream-testsrc-to-srtp.py`) needs the local
GStreamer stack it drives (checked with its own
`./scripts/stream-testsrc-to-srtp.py --check`, which names anything missing):

```bash
sudo apt install python3-gi gir1.2-gstreamer-1.0 gstreamer1.0-plugins-base gstreamer1.0-plugins-ugly gstreamer1.0-plugins-good gstreamer1.0-plugins-bad
```

Then:

```bash
openssl rand -hex 30 > /tmp/key
STACK_NAME=<stack> ./scripts/provision-srtp-key.sh 6000 42 < /tmp/key
# restart the backend, then:
python3 scripts/stream-testsrc-to-srtp.py <nlb-dns> 6000 --ssrc 42 < /tmp/key
```

Each run of the sender is a new session at rollover 0, so rotate the key between
runs against anything that matters (see above).

## Running the e2e suite

The e2e suite (`backend/e2e/`) is the acceptance test: it runs against the
deployed stack, uses its own real H.264 fixture (a 1-second-GOP x264 stream),
its own RFC 3711 sender with **arbitrary initial rollover counter and sequence
number**, and asserts on the stack's own observables - the KVS `PutMedia`
metric, `GetMedia` fragments read back, the application log, the per-transport
traffic metrics, and the lock table.

The stack it runs against is named, never guessed: pass the deployed stack's
name unless it is the default one.

```bash
cd backend
STREAMING_STACK_NAME=<stack> npm run test:e2e
# one case: npm run test:e2e -- --only wrap
```

The suite provisions a fresh key per port, then **deploys the backend code to
the running fleet itself** — `aws s3 sync` from the stack's code bucket, the
SRTP Python bindings, the GStreamer `srtpdec` element (Amazon Linux 2023 does
not ship it, so the deployed `install-gst-srtp-plugin.sh` builds the two-file
plugin from the matching gst-plugins-bad release, ~2.5 minutes once per
instance), `npm install`, service restart — so it works against instances that
predate the deploy (a plain restart cannot do this: instances receive code only
at boot). It then waits for the service's own log lines — the SRTP transport
started, and a port's helper actually reaching `searching` — and fails fast with
the reason if they do not, before any case runs. It needs the fleet's instances
reachable through SSM (the instance role already has it). Ports and cases:

| Port        | Case               | What it proves                                                                                                                                                                                         |
| ----------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 6000        | happy path         | Real media reaches Kinesis and `GetMedia` reads it back; the lock is taken while producing and released when the sender stops; the transport publishes the serving and traffic metrics the alarms read |
| 6001        | wrong key          | Unauthenticated traffic never acquires the lock and never ingests                                                                                                                                      |
| 6002        | forged flood       | Correct public header + random tags earn nothing, forever                                                                                                                                              |
| 6003        | wrap               | A stream crossing rollover 65536 - the upper end of the 48-bit index space - keeps ingesting and reports it                                                                                            |
| 6004        | key rotation       | A rotated key starts a new index space below the old floor's rollover: the floor is scoped to the key identity, not the port                                                                           |
| 6005        | rewind             | The same key restarting its index is refused and reported as stale drops                                                                                                                               |
| 6006        | restart recovery   | The backend restarts under a live sender and the stream recovers with media, asserted from the restarted process's own boot                                                                            |
| 6007        | fresh key          | Rotation restarts the index space cleanly, confirmed on trial 1                                                                                                                                        |
| 6008 + 5000 | isolation          | SRTP noise on one port while the unencrypted path keeps ingesting undisturbed                                                                                                                          |
| 6009        | walked-past search | Traffic that cannot authenticate walks the counter search past the answer, and the real sender is still found through the re-sweep                                                                     |

## Recovery and troubleshooting

- **`SRTP traffic authenticated`** in `/video-streaming/application` (CloudWatch
  Logs) is the marker that a port's traffic holds the key. `trials: 1` means it
  was confirmed in the floor's own rollover.
- No authentication, `inputs` climbing and `drops` climbing in the stats:
  traffic arrives but fails authentication - wrong key, wrong SSRC, or a rewind
  below the floor.
- `inputs` at zero: nothing is arriving (security group, NLB listener, sender).
- A device must use a **stable SSRC per port** (`srtpdec` with static keys is
  pinned to the provisioned one). Forward jumps are fine; rewinds are refused
  until the key is rotated.
- The 20-second provisional window in previous designs does not exist here: the
  lock is only ever taken after authentication, and a producing port gives its
  lock up when authentication stops (3 s) - the inactivity lease is the outer
  bound only for wedged processes.

## What cannot be tested without a deployment

The e2e suite covers everything from the NLB down: the real dual-stack UDP
forwarding, the lock table, the floor, restarts, `kvssink` to real Kinesis, the
media itself. What it still does not do: multi-instance failover of a flow
mid-stream (stickiness keeps one flow on one instance), and the zero-ingestion
restart automation (which would reboot the fleet under the test).
