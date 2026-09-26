# SRTP ingest v3 — design

This is the third implementation of SRTP ingest. It exists because the first two
(PRs #46 and #48) could not converge under review, and the agreed way forward is
different: **the deployed system is the arbiter**. This document describes the
architecture; `SRTP-REVIEW-FINDINGS.md` maps every known bug from the previous
attempts to how this design eliminates, fixes, tests, or accepts it; `e2e/`
contains the tests that must pass against a deployed stack before this is
considered done.

## What stays the same (decisions proven in #46/#48)

These are not re-litigated; they are the load-bearing lessons of 120+ review
findings:

1. **SRTP is an additive transport.** Ports 6000–6009, own Kinesis Video Streams
   named `{stackName}-video-{port}`, own NLB listeners/target groups. The
   existing 5000–5009 streams keep their names _and_ construct IDs (pinned by a
   CDK test). No renames, no lock re-keying, no fleet cutover.
2. **Static, pre-shared keys, one per port**, in SSM `SecureString` parameters
   (`/{stackName}/srtp/port/{port}/key`), resolved once at process start, single
   supported suite (`aes-128-icm` + `hmac-sha1-80`), SSRC pinned per port.
   Provisioned out-of-band by `scripts/provision-srtp-key.sh` (key never in
   argv; SecureString under the AWS-managed `aws/ssm` key — no KMS grant needed,
   and this is pinned by a test).
3. **The rollover counter is confirmed by authentication, never estimated.**
   Candidates seed `srtpdec` via its `request-key` callback; the pad that only
   emits what libsrtp authenticated is the sole oracle. The candidate order: the
   floor's ROC first (re-offered periodically), then a near climb from just
   above the floor (restarts each session), alternating with a far climb that
   **carries across sessions** (`searchFrom`). Nothing below the floor is ever
   offered — a stream below the floor is a replay or a keystream reuse, and both
   are refused.
4. **The replay floor.** The highest packet index ever accepted under a
   key+SSRC, persisted in DynamoDB with a monotonic conditional write and read
   strongly consistently, enforced in the helper _before_ the depayloader. An
   unreadable floor aborts the port (it must not look like a missing one).
5. **Session continuity contract:** a sender that resets its RTP numbering must
   be reprovisioned with a fresh key. Rewinds under the same key are detected
   (floor) and rejected with a security log line.
6. **The key never appears in argv, env, or protocol output.** It crosses to the
   helper once, on stdin, in the init frame, and reaches `srtpdec` through
   `request-key` caps in process. Every process that touches it fails closed on
   key-shaped argv, `GST_DEBUG > 3`, and dot-dump env.

## What is structurally new, and why

The fights that never ended were all at **one boundary**: the Node process
relaying datagrams to a GStreamer child and reconciling cryptographic and lock
state across that boundary. v3 removes the boundary.

### 1. The helper binds the public port; Node never touches a datagram

`srtp_port.py` owns the UDP socket for its entire lifetime. It is created once
(`SO_RCVBUF` sized to bridge pipeline rebuilds), bound to the public port
`6000+N`, and handed to every GStreamer pipeline the helper builds via
`udpsrc`'s `socket` property. Consequences, each the fix for a whole category of
findings:

- **No relay socket** (async `ECONNREFUSED` killing Node, closed-socket throws
  in the send path, connect-before-ready ordering).
- **No pre-start buffer, no paced replay, no unsent-datagram handback** — the
  kernel receive buffer retains datagrams across pipeline rebuilds, in order,
  with none of the user-space reordering machinery that every replay bug lived
  in.
- **No per-datagram events in Node** — no unbounded event queue, no coalescing,
  no admission filter (the helper's `request-key` filters SSRCs before any
  crypto; malformed datagrams are libsrtp's problem).
- **Forged traffic earns nothing, by construction**: Node sees no packets at
  all. Lock acquisition and every lease refresh are driven _only_ by the
  helper's authentication reports. Unauthenticated traffic — for any duration —
  cannot acquire a lock, refresh a lease, raise the floor, or log at line rate.

### 2. One helper per port, for the life of the service

The helper starts at boot (once keys are resolved) and runs continuously. It has
two modes:

- **Searching** (default): `udpsrc(socket) ! srtpdec ! fakesink`. The candidate
  search runs exactly as in #48 (trial evidence: ≥4 drops or inputs+ timeout;
  auth credited on the streaming thread to the key `srtpdec` actually held;
  `remove-key` resets the stream; never stepped while idle; the far climb
  carried across sessions). On first confirmed authentication it emits
  `auth ok first` with the ROC — and keeps reporting authenticated activity in
  its periodic stats. Wrap tracking is a faithful port of libsrtp's own
  `srtp_rdbx_estimate_index`/`srtp_index_guess` (including its early-session
  shortcut, without which a forward jump from a low sequence number is read as a
  bogus rollover to `0xffffffff` — a divergence found and pinned by test against
  the real library).
- **Producing**: granted by the supervisor after the port lock is acquired. The
  pipeline is rebuilt on the _same socket_ —
  `udpsrc(socket) ! srtpdec ! rtpjitterbuffer ! rtph264depay ! h264parse ! capsfilter ! kvssink`
  — with `srtpdec` seeded from the just-confirmed candidate. Datagrams that
  arrived during the rebuild are waiting in the kernel buffer.

Transitions: `start` (grant) and `stop` (revoke) arrive on stdin — the only
commands besides the init frame. `stop` sends EOS (so `kvssink` flushes its
fragment), waits for it, sets the pipeline to NULL, and **acks with a `stopped`
frame** carrying the final floor. Auth loss is reported (`auth lost`) and the
helper rebuilds itself back to Searching — same process, same socket — instead
of exiting.

### 3. The Node side is a small supervisor, not a pipeline

Per port, a serialized state machine with five states:

```
Searching ──auth ok first──▶ Acquiring ──lock──▶ Producing
    ▲                          │ (refused/stale:   │ auth lost / stats stalled
    │                          │  retry on timer   ▼
    └──stopped / auth lost─────┘  while alive)   Stopping ──stopped ack──▶ Searching
```

Invariants (asserted by a transition hook in every unit test):

- **The lock is held ⟺ the port is Producing or tearing down.** Acquire after
  `auth ok first`; release only after the helper's `stopped` ack (pipeline
  provably NULL) or its verified exit. A stop that fails keeps the lock and
  retries — the lease going stale is the documented fallback.
- **One operation in flight per port.** All events — protocol frames, child
  exit, a slow 10 s supervision tick — funnel into one serialized queue. The
  frame rate is bounded by design (stats at most once per interval), so the
  queue is bounded by construction; no datagram ever joins it.
- The supervision tick is the only timer, and it exists to observe _absence_: a
  wedged helper (no frames) must still lead to teardown and lock release.

`index` frames from any session (even a dying one) still raise the floor — a
dead helper's last words are exactly the ones worth keeping. Everything else
only acts if it is from the current session.

### 4. Lease, metrics and alarms

- The lease heartbeat is written when the helper's authenticated counter
  advances (stats frame), with the _current_ time.
- `ReceivedBytes{Transport=srtp}` is published from the helper's input-byte
  counters on each stats frame; `TransportServing` when the transport's helpers
  are up. Names and dimension values come from the shared `TrafficMetricNames`
  module (a mismatched case here once made an alarm silently never fire).
- CDK alarms keep #48's final shape: per-transport traffic leg + per-transport
  no-ingestion leg (explicit port list), zero-ingestion restart composite
  `anyOf` across transports, `TransportNotServing` notify-only.

### 5. Isolation

The unencrypted path is `saga`'s code, untouched. SRTP setup happens after the
primary listener serves and the health port is open. A failure in key loading,
helper spawning, or any SRTP runtime path can only ever disable SRTP ports. One
helper crashing affects its port; the supervisor restarts it with backoff. The
helper dying entirely (stdin EOF, since the parent owns the pipe) can never
outlive the service.

## What this costs (accepted)

- **Producing starts one grant round-trip after confirmation** (lock acquire +
  stdin command + pipeline rebuild, sub-second) — the kernel buffer usually
  hides it; a keyframe interval is the worst case.
- **A wrong or missing floor costs the candidates spent on failed trials.** The
  floor is a search accelerator, not correctness.
- See `SRTP-REVIEW-FINDINGS.md` ("Accepted risks") for the pre-existing
  exposures inherited from `saga` (no fencing token on the lease; KVS `PutMedia`
  cannot be fenced; KVS SDK built from source at boot).

## How correctness is judged

Unit/integration tests cover the state machine invariants, the protocol bounds,
the search order, the floor, and the cryptographic sender — ported and re-pinned
from #48 where those were already verified against real libsrtp.

The **e2e suite** (`e2e/`) is the acceptance test this feature never had. It
runs against a deployed stack, uses a real RFC 3711 sender with **arbitrary
initial ROC and sequence number** (a real encoder cannot do this, and it is the
only way to exercise wrap, climb and rewind), carries real H.264 (a committed
x264 fixture with a 1 s GOP, so recovery gaps are measurable in seconds), and
asserts on the deployed system's own observables: KVS `PutMedia.IncomingBytes`,
CloudWatch Logs marker lines, the per-transport traffic metrics, the DynamoDB
lock table, and the unencrypted path's continued health. Every contested
behavior from the review history — wrap, restart, key rotation, the walked-past
search, rewind rejection, forged traffic, isolation — is an executable case.
