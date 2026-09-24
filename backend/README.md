# Video Streaming Backend

UDP listener and stream processing service for Non-Terrestrial Network Connected
Cameras.

## Overview

This service receives UDP video streams from Cat1bisCam devices on ports
5000-5009, buffers the data, tracks stream state, and integrates with AWS
services (DynamoDB, S3) for metadata and storage.

## Sending video (device integration)

A device sends to **one** port and uses **one** transport. Each port has its own
Kinesis Video Stream, `{stackName}-video-{port}`, so the choice of transport is
local to the device and needs no coordination with anything else.

### SRTP (encrypted, preferred)

Ports **6000-6009** on the load balancer's address.

- **Transport**: SRTP ([RFC 3711](https://www.rfc-editor.org/rfc/rfc3711)) over
  UDP, **one SRTP packet per datagram**. Never coalesce or fragment packets:
  authentication is per packet, so the framing is load-bearing.
- **Payload**: H.264 packetized as RTP
  ([RFC 6184](https://www.rfc-editor.org/rfc/rfc6184)), payload type 96, clock
  rate 90000.
- **Cipher suite**: `aes-128-icm` with `hmac-sha1-80`, the only suite accepted.
- **Key**: a 30-byte master key and salt, provisioned out of band per port by an
  operator (`scripts/provision-srtp-key.sh`). Devices never negotiate one: there
  is no DTLS or WebRTC handshake on this path.
- **SSRC**: fixed per port, provisioned alongside the key. A device that picks a
  new SSRC per session will not be decrypted, because the receiver is pinned to
  the provisioned one.
- **Sequence numbering**: a device may have wrapped any number of times while
  ingestion was down, and need not tell the receiver - it establishes the
  rollover counter by authentication rather than assuming it. The cost is time:
  usually just the wait for the device's next keyframe, and for a device that
  wrapped hundreds of times while nobody was listening, a few provisional
  windows, because each pipeline session carries on the search where the last
  one stopped. Keep the keyframe interval short if fast recovery matters.

  **A device must never restart its sequence numbering while keeping its key,
  and the receiver will not accept it if it does.** SRTP derives its keystream
  from the master key, the SSRC and the packet index
  ([RFC 3711 §9.1](https://www.rfc-editor.org/rfc/rfc3711#section-9.1)), and on
  this path the key is static and the SSRC fixed per port - so rewinding the
  index encrypts new payloads under a keystream that has already been used, and
  XORing two such packets cancels the keystream and leaves the XOR of the two
  plaintexts.

  The receiver keeps a replay floor for each port: the highest packet index it
  has ever accepted under the port's key and SSRC, stored in DynamoDB and only
  ever raised. Nothing at or below it is accepted, whichever instance receives
  it and however often the pipeline restarts. That is what stops a recording of
  earlier traffic from authenticating again - `libsrtp`'s own replay window
  lives in the pipeline process and starts empty every time - and it means a
  device that rewinds its index under the same key is refused too, because from
  the receiver the two are indistinguishable. Such a device's traffic is counted
  and dropped
  (`dropped N authenticated datagrams at or below the highest packet index already accepted under this key`),
  or, if it rewound by a whole rollover or more, simply never authenticates.

  So whenever a device restarts its numbering - on reboot, re-session or factory
  reset - provision a fresh key for its port first
  (`scripts/provision-srtp-key.sh`), and restart the instances so the new key is
  read. A new key starts a new floor. A device that cannot guarantee a monotonic
  index across reboots needs a new key on every boot.

  The floor is written from the pipeline's reports every few seconds and when
  the pipeline ends, so what a replay can still reach is the traffic since the
  last write before an instance died without warning - seconds of it, not the
  whole history. A start that cannot read the floor does not start.

- **Keyframes**: send SPS/PPS regularly (for example `config-interval=1` in
  GStreamer), so a receiver that joins mid-stream can start decoding.

`scripts/stream-testsrc-to-srtp.py` is a runnable example of the sender side. It
takes the key on stdin or from a file and sets it on `srtpenc` in process, so
the key never appears in a command line - the same rule the receiver keeps.

### Unencrypted MPEG-TS

Ports **5000-5009**, MPEG-TS over UDP. This is the original path and is
unchanged. It offers no confidentiality, integrity or authenticity: anything
that reaches the port is ingested, so prefer SRTP for anything real.

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
