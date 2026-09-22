# Testing SRTP Ingestion

How to test the SRTP → GStreamer (`srtpdec` + `kvssink`) → Kinesis Video Streams
pipeline. This path is additive to the unencrypted UDP/MPEG-TS pipeline
documented in [TESTING-KINESIS-INGESTION.md](./TESTING-KINESIS-INGESTION.md) and
shares its fleet, load balancer and lock table.

## Prerequisites

- Stack deployed (`npm run cdk:prod:deploy`). No extra arguments: the SRTP path
  needs no migration, no cutover and no deploy-time context.
- At least one SRTP port provisioned with a key (below). **One is enough** -
  ports are independent, and an unprovisioned port simply drops its traffic.
- Local GStreamer with `gst-plugins-bad` (for `srtpenc`) **and**
  `gst-plugins-ugly` (for `x264enc`), both used by the test sender:

  ```bash
  gst-inspect-1.0 srtpenc
  gst-inspect-1.0 x264enc
  ```

## How it works

Devices send SRTP-encrypted RTP/H.264
([RFC 6184](https://www.rfc-editor.org/rfc/rfc6184), payload type 96, 90000 Hz)
to ports **6000-6009**. The backend relays each datagram, unmodified and
individually, to a loopback port belonging to that port's pipeline helper, which
preserves the one-datagram-per-packet framing SRTP authentication depends on.
(The unencrypted path's FIFO bridge is a byte stream - correct for
self-synchronizing MPEG-TS, and it would destroy RTP packet boundaries.)

The pipeline itself runs as a GStreamer application,
`backend/src/srtp_pipeline.py`:

```
udpsrc ! srtpdec ! rtpjitterbuffer ! rtph264depay ! h264parse ! capsfilter ! kvssink
```

It is an application rather than a `gst-launch-1.0` command line for three
reasons: the master key is delivered to `srtpdec` in process, so it never
appears in `/proc/<pid>/cmdline`; readiness is the port `udpsrc` actually bound,
reported back rather than guessed at; and `srtpdec` tells the backend which
packets it authenticated.

**Each transport has its own streams.** SRTP port 6000+N feeds
`{stackName}-video-{6000+N}`; the unencrypted port 5000+N feeds
`{stackName}-video-{5000+N}`. Nothing is shared between them except the fleet,
so a device can use either transport without coordinating with the other.

**Keys are per port**, static and pre-shared, provisioned out of band into SSM
Parameter Store as `SecureString` and resolved once at process start. Each port
has its own key and a fixed SSRC, which `srtpdec` is pinned to.

### The rollover counter takes care of itself

SRTP authentication covers a 48-bit packet index - the 16-bit RTP sequence
number plus a rollover counter that is never transmitted. A receiver that starts
with the wrong rollover counter authenticates nothing, and the sequence number
wraps every 65536 packets, which is a minute or two of video.

The pipeline resolves this by asking the authenticator instead of guessing. It
seeds a candidate, and if `libsrtp` authenticates packets, that candidate was
right and is stored as a hint for next time; if not, those packets are counted
as dropped and the next candidate is tried. The candidates are the stored hint
first, then zero, then outwards in both directions.

**There is therefore nothing to do when a sender restarts.** Re-running the test
sender below, rebooting a camera, or restarting the backend all recover on their
own, typically within a few tens of datagrams. You do not need to reprovision a
key, pick a new SSRC, clear anything in DynamoDB, or restart the backend.

The one visible cost of a wrong hint is that the datagrams spent on failed
candidates are lost, so video resumes at the sender's **next keyframe**. With a
long keyframe interval that can be several seconds.

## 1. Unit tests

```bash
npm test
```

The tests that exercise the real pipeline (`SrtpPipelineHelper.spec.ts`) need
the Python GStreamer bindings and the `srtp` plugin, and skip themselves where
those are missing - so check they actually ran rather than skipped:

```bash
node --no-warnings --experimental-transform-types --test backend/src/SrtpPipelineHelper.spec.ts
```

## 2. Provision a key

Generate fresh key material and provision it for a port. **Keep the key in a
shell variable** - the sender needs the same one.

```bash
TEST_KEY=$(openssl rand -hex 30)
printf '%s\n' "$TEST_KEY" | ./scripts/provision-srtp-key.sh 6000 3735928559
```

The key goes in on stdin rather than as an argument, because arguments are
readable by any local user through `/proc/<pid>/cmdline` while the process runs.
The script also accepts a file
(`./scripts/provision-srtp-key.sh 6000 3735928559 key.txt`), and prompts without
echoing when stdin is a terminal - which is the one to use for a production key,
since it keeps the key out of shell history too.

Restart or redeploy the instances so the backend picks the parameter up; keys
are resolved once at process start.

Only `aes-128-icm` with `hmac-sha1-80` is accepted, and the key must be exactly
60 hex characters (a 30-byte master key and salt) - that suite is the one whose
key length matches, and a mismatched pair would otherwise fail inside GStreamer
instead of at load time.

## 3. Send a test stream

```bash
./scripts/get-instance-ip.sh
./scripts/stream-testsrc-to-srtp.sh <instance-ip> 6000 "$TEST_KEY" 3735928559
```

The script's built-in default key and SSRC are for local experiments only and
will not authenticate against a backend provisioned with your own key.

## 4. Verify

1. **Kinesis Video Streams console** - open `{stackName}-video-6000` and confirm
   fragments are arriving.
2. **Application log** - `/var/log/video-streaming/application.log` should show
   `SRTP traffic authenticated` with the rollover counter and how many
   candidates were tried. `trials: 1` means the hint was right first time.
3. **Unencrypted path unaffected** - port 5000 keeps ingesting into
   `{stackName}-video-5000` throughout.

## 5. Recovery behaviour worth checking

- **Restart the sender.** Stop the script and run it again. Ingestion recovers
  by itself; the log shows a second `SRTP traffic authenticated`, usually with
  `trials: 2`.
- **Restart the backend.** `systemctl restart video-streaming` on the instance
  while the sender runs. The stored hint is tried first, so this usually
  recovers with `trials: 1`.
- **Send noise.** Anything that is not authentic SRTP is dropped. The port may
  briefly claim its slot while the pipeline starts - that window is bounded at
  twenty seconds, after which it gives the slot back and waits a minute - but it
  never refreshes its lease or stores a rollover counter.

## Troubleshooting

- **No fragments, and no `SRTP traffic authenticated` in the log.** Key, cipher
  suite, or SSRC mismatch between the sender and the provisioned parameter. The
  `stats` lines distinguish the cases: `inputs` climbing with `authenticated` at
  0 and `drops` climbing means datagrams are arriving and failing
  authentication; `inputs` at 0 means nothing is arriving at all.
- **`No SRTP key configured for port`.** The parameter
  `/{stackName}/srtp/port/{port}/key` is missing, is not a `SecureString`, is
  not valid JSON, or its key is not 60 hex characters. Re-run
  `scripts/provision-srtp-key.sh` and restart the instances.
- **Nothing arrives at all.** Check the security group allows UDP 6000-6009 and
  that the load balancer has a listener for the port (`SrtpUDPListener{port}` in
  `cdk/StreamingStack.ts`).
- **SRTP prerequisites missing.** The bootstrap log
  (`/var/log/cloud-init-output.log`) reports this as a warning, deliberately: it
  is the same on every instance, so failing the health check over it would cycle
  the whole fleet through identically broken replacements and take the
  unencrypted path down too. Check with:

  ```bash
  python3 -c "import gi; gi.require_version('Gst','1.0')
  from gi.repository import Gst; Gst.init(None)
  print([n for n in ['udpsrc','srtpdec','rtpjitterbuffer','rtph264depay'] if not Gst.ElementFactory.find(n)])"
  ```

  Note this checks the Python bindings, not `gst-inspect-1.0`: the helper needs
  the GObject typelib, and the command line tools can be absent while the
  typelib is fine.

- **A device that picks a new SSRC per session will not work.** `srtpdec` is
  pinned to the provisioned SSRC. A stable SSRC per port is required; a stable
  _sequence numbering_ is not.

## What cannot be tested locally

Real `srtpdec` authentication and the rollover-counter search are covered by
`SrtpPipelineHelper.spec.ts` against real `libsrtp`. What needs a deployed
stack: `kvssink` fed from the in-process pipeline, the Python bindings being
present on the instance AMI, load balancer UDP forwarding and dual-stack
translation, lock handoff between instances, and rollover-counter recovery
across a real restart.
