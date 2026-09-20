# Video Streaming Backend

UDP listener and stream processing service for Non-Terrestrial Network Connected
Cameras.

## Overview

This service receives UDP video streams from Cat1bisCam devices on ports
5000-5009, buffers the data, tracks stream state, and integrates with AWS
services (DynamoDB, S3) for metadata and storage.

## Sending Video (Client / Device Integration)

Each device is assigned one Kinesis Video Stream (named
`video-streaming-2026-09-video-1` through `-10`) and sends video using **exactly
one** of the two methods below - never both for the same device. Unencrypted
port 5000+N and SRTP port 6000+N both feed the same stream
(`video-streaming-2026-09-video-{N+1}`), so a device's stream number determines
both which port to use for its chosen method and which stream to watch for its
video (e.g. a device using SRTP on port 6003, or unencrypted UDP on port 5003,
both land in `video-streaming-2026-09-video-4`).

### SRTP (encrypted, recommended)

Devices can send **SRTP-encrypted RTP/H.264** video to ports **6000-6009** on
this service's public endpoint (the NLB DNS name/IP from the CDK stack outputs).
This is the preferred, encrypted ingest path. See
[`../docs/TESTING-SRTP-INGESTION.md`](../docs/TESTING-SRTP-INGESTION.md) for a
full walkthrough, and `../scripts/stream-testsrc-to-srtp.sh` for a runnable
example of the GStreamer pipeline shape described below.

What a client/device needs to send:

- **Transport**: SRTP ([RFC 3711](https://www.rfc-editor.org/rfc/rfc3711)) over
  UDP, one UDP datagram per SRTP packet - never fragment or coalesce packets;
  each datagram the backend receives must be exactly one SRTP packet, since
  decryption depends on per-packet framing.
- **Payload**: H.264 video packetized as RTP per
  [RFC 6184](https://www.rfc-editor.org/rfc/rfc6184) (payload type 96, 90000 Hz
  clock rate), SRTP-encrypted on top.
- **Port**: one fixed port per device/session, in the range 6000-6009. Port
  6000+N feeds `video-streaming-2026-09-video-{N+1}` (the same stream
  unencrypted port 5000+N would use) and has its own SRTP key.
- **SSRC**: a fixed, stable RTP SSRC for the lifetime of that port assignment.
  The backend decrypts using a static SSRC configured per port, so a device that
  picks a new SSRC on every (re)connect will not decrypt correctly - always
  reuse the same SSRC on a given port.
- **Session continuity**: a port's (key, SSRC) pair is the device's session
  identity, and the backend persists the RTP rollover-counter (ROC) state under
  it across restarts. This means a device **must never restart its RTP sequence
  numbering** (sequence counter at 0 after a reboot, re-session, or factory
  reset) while keeping the same key and SSRC - the backend would seed the fresh
  session with the previous session's ROC, and `srtpdec` would reject its
  packets until the sender wraps around to that ROC again (effectively forever).
  If a device cannot guarantee monotonic sequence numbers across its restarts
  (e.g. the bundled test sender, which restarts at a fixed sequence offset),
  reprovision it with a **new key or SSRC** (see
  `../scripts/provision-srtp-key.sh`) whenever it restarts its sequence, or ask
  the operator to clear the port's persisted ROC state in DynamoDB
  (`srtpRoc`/`srtpHighestSeq`/`srtpRocSsrc`/`srtpRocKeyFingerprint` on the
  stream slot's `StreamMetadata` item).
- **Encryption**: a static, pre-shared 30-byte SRTP master key + salt (60 hex
  characters), using **aes-128-icm** for encryption and **hmac-sha1-80** for
  authentication - the only suite this service supports. Keys are exchanged
  out-of-band, not negotiated in-band (no DTLS/SDES handshake) - coordinate with
  whoever operates this service to have a key issued for your assigned port (see
  `../scripts/provision-srtp-key.sh`), and configure the device with that same
  key, SSRC, and port.
- Send SPS/PPS periodically (e.g. on every keyframe, as the test sender does
  with `rtph264pay config-interval=1`) rather than relying on a single in-band
  set at stream start, for robustness against reconnects/restarts.

### Unencrypted (legacy)

Devices can also send plain **MPEG-TS/H.264 over UDP** (no RTP framing, no
encryption) to ports **5000-5009**; port 5000+N feeds
`video-streaming-2026-09-video-{N+1}`. This is the original ingest path and is
unauthenticated - prefer SRTP above for anything internet-facing. See
[`../docs/TESTING-KINESIS-INGESTION.md`](../docs/TESTING-KINESIS-INGESTION.md).

## Components Implemented

### UDPListener

- Listens for UDP packets on ports 5000-5009
- Validates incoming packets
- Handles port binding failures with exponential backoff retry
- Emits events for packet reception

### PacketBuffer

- Buffers incoming packets in memory
- Flushes to disk when size or time threshold is reached
- Handles buffer overflow with FIFO packet dropping
- Writes MPEG-TS files to local storage

### StreamStateManager

- Tracks active/inactive state for each stream
- Detects stream start (first packet on a port)
- Detects stream stop (no packets for 1 minute)
- Emits events for state transitions

### StreamMetadataService

- Integrates with DynamoDB to store stream metadata
- Updates stream status (active/inactive)
- Updates last packet timestamps
- Stores S3 paths for HLS manifests and snapshots

## Directory Structure

```
backend/
├── src/
│   ├── UDPListener.ts              # UDP packet reception
│   ├── UDPListener.spec.ts         # UDP listener tests
│   ├── PacketBuffer.ts             # Packet buffering and disk writing
│   ├── PacketBuffer.spec.ts        # Buffer tests
│   ├── StreamStateManager.ts       # Stream state tracking
│   ├── StreamStateManager.spec.ts  # State manager tests
│   ├── StreamMetadataService.ts    # DynamoDB integration
│   └── index.ts                    # Main service entry point
├── package.json
└── README.md
```

## Requirements

- Node.js v24 or higher
- NPM v11 or higher

## Installation

```bash
npm install
```

## Configuration

Environment variables:

- `OUTPUT_DIR` - Directory for buffered video files (default:
  `/tmp/video-streams`)
- `TABLE_NAME` - DynamoDB table name (default: `StreamMetadata`)
- `AWS_REGION` - AWS region (default: `eu-central-1`)

## Running

```bash
node --experimental-transform-types src/index.ts
```

## Testing

```bash
npm test
```

All tests pass successfully:

- UDPListener: Port binding, packet reception, error handling
- PacketBuffer: Buffering, flushing, overflow handling
- StreamStateManager: State transitions, active stream tracking

## Architecture

```
UDP Packets (ports 5000-5009)
    ↓
UDPListener (validates & receives)
    ↓
PacketBuffer (buffers & flushes) → Local Disk (MPEG-TS files)
    ↓
StreamStateManager (tracks state)
    ↓
StreamMetadataService → DynamoDB
```

## Requirements Implemented

- **Requirement 1.1**: UDP ingestion on ports 5000-5009 ✓
- **Requirement 1.2**: Packet buffering ✓
- **Requirement 1.4**: Port-based stream identification ✓
- **Requirement 1.5**: Stream state tracking ✓
- **Requirement 6.2**: Stream metadata in DynamoDB ✓
- **Requirement 6.5**: Active stream count tracking ✓
- **Requirement 8.4**: Stream status API support ✓
- **Requirement 9.1**: Error handling for malformed packets ✓
- **Requirement 11.4**: Inactivity detection (1 minute timeout) ✓

## Dependencies

- `@aws-sdk/client-s3` - S3 operations for video storage
- `@aws-sdk/client-dynamodb` - DynamoDB operations for stream metadata
- `@aws-sdk/lib-dynamodb` - DynamoDB document client
- `@aws-sdk/client-cloudwatch` - CloudWatch metrics emission

## Development Dependencies

- `typescript` - TypeScript compiler
- `@types/node` - Node.js type definitions
