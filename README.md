# video.thingy.rocks

Ingest video streams from UDP connected devices (Cat.1 bis cameras, NTN modems)
into
[Amazon Kinesis Video Streams](https://aws.amazon.com/kinesis-video-streams/),
behind one Network Load Balancer, on one fleet of EC2 instances.

Devices can use one of two ingest transports:

|                 | Ports         | Wire format                                            | Encryption               |
| --------------- | ------------- | ------------------------------------------------------ | ------------------------ |
| **Unencrypted** | UDP 5000–5009 | MPEG-TS over raw UDP                                   | none                     |
| **S/RTP**       | UDP 6000–6009 | RTP/H.264 (RFC 3550 + RFC 6184) inside SRTP (RFC 3711) | AES-128-CM, HMAC-SHA1-80 |

One UDP port carries one device's stream, on both transports. Port 5000+N and
port 6000+N are independent: each feeds its own Kinesis Video stream
(`{stack}-video-{port}`), each has its own ownership lock, and a device uses
**exactly one** of the two for a given stream.

This README explains how S/RTP ingest is implemented and — most importantly —
what that means for a client that wants to send video to it. The operator side
is covered in `docs/TESTING-SRTP-INGESTION.md`; the design rationale and the
full history of bugs this design eliminates are in `docs/SRTP-DESIGN.md` and
`docs/SRTP-REVIEW-FINDINGS.md`.

## How S/RTP ingest works

- **Static, pre-shared keys, no handshake.** There is no DTLS, no key exchange,
  no signaling: the key for a port is provisioned out-of-band (SSM Parameter
  Store) and the receiver loads it at service start. A client that knows the key
  can send; nothing else can.
- **One receiver process per port, owning the port.** A helper process
  (`backend/src/srtp_port.py`) binds the public UDP port itself and runs a
  GStreamer pipeline on it:
  `udpsrc ! srtpdec ! rtpjitterbuffer ! rtph264depay ! h264parse ! kvssink`. The
  Node service never touches an S/RTP datagram.
- **Authentication is the only thing that counts.** Everything the receiver does
  — taking the port's Kinesis stream lock, producing, refreshing it — is driven
  exclusively by packets that libsrtp authenticated. Traffic that does not hold
  the key (wrong key, forged tags, other SSRCs) is counted and dropped, and can
  never acquire, hold or disturb anything.
- **The rollover counter takes care of itself.** SRTP's packet index is
  implicit: the receiver must know the 32-bit rollover counter (ROC) to
  authenticate anything. The receiver searches for it by seeding `srtpdec` with
  candidates and watching what authenticates — confirmed by authentication,
  never guessed from packet headers. It searches upwards from the highest packet
  index the port has ever accepted under the current key, and re-offers the most
  likely candidate first, so a sender that simply keeps counting is re-confirmed
  within a few packets of any restart.
- **The replay floor.** The highest packet index ever accepted under a port's
  key and SSRC is persisted. Nothing at or below it is ever accepted again. This
  is what makes a restart safe: a recording of earlier traffic, replayed after a
  restart, does not authenticate into the stream.

What this adds up to, in one sentence: **the receiver can crash, restart,
re-deploy or be taken over by another instance at any time, and a client that
keeps sending what it was sending is picked up again automatically** — at the
cost of at most one keyframe interval of video.

## What a client must do

### Connection

- Send UDP to the load balancer (dual-stack: IPv4 or IPv6 both work; the NLB
  translates everything toward the instances). One flow — one source address and
  port to one ingest port — is hashed to one instance and stays there.
- Keep the source address/port stable for the session; the flow is what
  stickiness and the receiver's state are keyed on.
- Pace a **real-time stream**: roughly `clock rate ÷ fps` between frames. The
  receiver's jitter buffer tolerates ~200 ms of network reorder and jitter; it
  is not a store-and-forward buffer.

### Stream format

- RTP ([RFC 3550](https://www.rfc-editor.org/rfc/rfc3550)) carrying H.264
  ([RFC 6184](https://www.rfc-editor.org/rfc/rfc6184)): payload type 96, 90 kHz
  clock, marker bit on the last packet of each access unit, NAL units fragmented
  with FU-A when larger than the path MTU.
- H.264 must include SPS/PPS and periodic keyframes (IDR), because that is what
  makes a join or a recovery cost one GOP instead of forever. A keyframe
  interval of 1–2 s is recommended.
- SRTP (RFC 3711) with the single supported suite: **AES-128-CM**
  (`aes-128-icm`) encryption and **HMAC-SHA1-80** authentication, using the
  pre-shared 30-byte master key + salt (60 hex characters) provisioned for your
  port. This is the only suite validated end-to-end; do not send SRTCP.
- **SSRC — Synchronization SouRCe — is the 32-bit stream identifier in the fixed
  RTP header** (bytes 8–11, RFC 3550 section 3): the label that says "these
  packets all belong to one RTP stream from one sender". Normally a sender picks
  it at random when a session starts; here it is **provisioned per port together
  with the key** (a plain decimal number, e.g. `42`), and the receiver accepts
  only that value — datagrams carrying any other SSRC are dropped without being
  attempted. Your device must therefore use one fixed SSRC per port, on every
  packet, for the lifetime of the key.

### The one hard rule: never rewind the packet index

The RTP sequence number and the SRTP rollover counter must be **strictly
increasing for the lifetime of a key**:

- **Forward jumps are fine** — skip as many sequence numbers as you like; the
  receiver follows.
- **Wrapping is fine** — 65535 → 0 continues into the next rollover counter,
  exactly per RFC 3711.
- **Restarting your sequence numbering is not.** Re-running the same sender,
  rebooting with your counter reset to zero, or replaying a recording is
  indistinguishable from a keystream-reusing attack, and the receiver refuses
  it: every packet at or below the floor is dropped and counted. The log will
  show
  `dropped N authenticated datagrams at or below the highest packet index already accepted under this key`.

If your device must restart its numbering — after a reboot, a firmware update,
or a session reset — ask for a **fresh key** (`scripts/provision-srtp-key.sh`,
plus a service restart). A new key starts a clean index space at ROC 0 and is
confirmed on the first packet. This is the only recovery from a rewind.

Also: use a **stable SSRC** for your port — the Synchronization Source
identifier from _Stream format_ above. The static-key receiver is pinned to the
SSRC provisioned with the key; datagrams carrying any other SSRC are dropped
(counted, never logged per packet).

### What each client-side event costs

| Event                                       | What the receiver does                                                                      | Gap                         |
| ------------------------------------------- | ------------------------------------------------------------------------------------------- | --------------------------- |
| You keep sending normally                   | Confirmed within a few packets of the floor; `trials: 1`                                    | none                        |
| You start sending after idle                | Same as above — searching resumes automatically                                             | ≤ 1 keyframe                |
| The receiver restarts / redeploys under you | Your index continues where it was; the new process reads the floor, re-confirms immediately | ≤ 1 keyframe + restart time |
| You wrap 65535 → 0                          | Tracked as ROC+1, reported in the log, nothing to do                                        | none                        |
| You reset your numbering under the same key | **Refused.** Everything at or below the floor is dropped                                    | until a fresh key           |
| You send with the wrong key or forged tags  | Nothing — no lock, no stream, no observable effect beyond a counter                         | n/a                         |

### Where your video lands

Your port's Kinesis Video stream `{stack}-video-{port}` (30-day retention,
`video/h264`). Verification, playback and the e2e suite are in
`docs/TESTING-SRTP-INGESTION.md`; the strongest check is `GetMedia` — if
fragments come back, your stream works end to end.

## Provisioning (out-of-band, per port)

```bash
openssl rand -hex 30                       # 30 bytes: key + salt
STACK_NAME=<stack> ./scripts/provision-srtp-key.sh <port> <ssrc>
# key read from stdin or a 0600 file - never an argument, never a log line
#
# <port>  the ingest port this key is for (6000-6009)
# <ssrc>  the SSRC your device writes into every RTP header: a decimal
#         uint32 (0-4294967295), e.g. 42 - see "Stream format" above
```

Keys are stored as SSM `SecureString` parameters and are loaded **at service
start**: rotating a key means reprovisioning and restarting the service. There
is no in-band key change, ever — if your threat model requires frequent
rotation, plan for the restart.

## Testing your client

```bash
python3 scripts/stream-testsrc-to-srtp.py <nlb-host> <port> --ssrc <ssrc> < /tmp/key
```

A GStreamer reference sender with the same requirements as above (see
`scripts/stream-testsrc-to-srtp.py --check` for the element/package
prerequisites). Note that each run of it starts a new session at ROC 0 — rotate
the key between runs, exactly as the hard rule requires.

The repository's e2e suite (`backend/e2e`) exercises every behavior in the table
above against a deployed stack, using a sender with arbitrary initial
ROC/sequence number — the same states your device will be in after weeks of
uptime. If your client passes what that suite tests, it will survive the
receiver's worst days.
