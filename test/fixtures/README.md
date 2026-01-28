# Test Fixtures

This directory contains test data and fixtures for the Video Streaming system.

## Contents

- `sample-packets.ts` - Pre-generated MPEG-TS packets for unit tests
- `expected-manifests/` - Expected HLS manifest files
- `test-metadata.json` - Test stream metadata
- `video-samples/` - Sample video files (not included in repo)

## Usage

### Sample Packets

The `sample-packets.ts` file exports pre-generated MPEG-TS packets for use in
unit tests:

```typescript
import {
  validPacket,
  malformedPacket,
  largePacket,
} from "./fixtures/sample-packets.js";

// Use in tests
test("should handle valid packet", () => {
  const result = processPacket(validPacket);
  assert.ok(result);
});
```

### Expected Manifests

The `expected-manifests/` directory contains expected HLS manifest files for
validation:

- `master.m3u8` - Expected master playlist
- `1080p-playlist.m3u8` - Expected 1080p variant playlist
- `720p-playlist.m3u8` - Expected 720p variant playlist
- `480p-playlist.m3u8` - Expected 480p variant playlist
- `360p-playlist.m3u8` - Expected 360p variant playlist

### Test Metadata

The `test-metadata.json` file contains sample stream metadata for testing
DynamoDB operations:

```json
{
  "port": 5000,
  "status": "active",
  "lastPacketTime": "2024-01-15T10:30:00.000Z",
  "lastFramePath": "s3://bucket/snapshots/5000/last_frame.jpg",
  "hlsManifestPath": "s3://bucket/hls/5000/master.m3u8",
  "rawStreamPath": "s3://bucket/raw/5000/"
}
```

## Generating Test Data

To generate new test data:

1. Run the UDP packet generator to create sample packets
2. Process the packets through the system
3. Extract the generated HLS manifests
4. Save as expected output for validation

## Video Samples

Sample video files are not included in the repository due to size constraints.
To generate test video files:

```bash
# Generate a 10-second test video with color bars
ffmpeg -f lavfi -i testsrc=duration=10:size=1920x1080:rate=30 \
  -c:v libx264 -preset fast -pix_fmt yuv420p \
  test/fixtures/video-samples/test-video-1080p.mp4

# Convert to MPEG-TS format
ffmpeg -i test/fixtures/video-samples/test-video-1080p.mp4 \
  -c copy -f mpegts \
  test/fixtures/video-samples/test-video-1080p.ts
```
