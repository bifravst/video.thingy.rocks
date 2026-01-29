# Design Document: Continuous HLS Playback Fix

## Overview

This design addresses the bug where the web application only plays the first
6-second segment of a live HLS stream, despite continuous video data being
transcoded and uploaded to S3. The root cause is a combination of:

1. **FFmpeg configuration**: Missing live streaming flags in HLS playlist
   generation
2. **Missing master playlist**: No master.m3u8 file to coordinate multi-bitrate
   streaming
3. **Cache headers**: Incorrect cache headers causing browsers to cache stale
   playlists
4. **HLS.js configuration**: Player not configured for live streaming mode

The fix involves updating the FFmpeg command generation, implementing master
playlist creation, configuring proper cache headers, and updating the HLS.js
player configuration for live streaming.

## Architecture

### Current Architecture

```
UDP Stream → FFmpeg Transcoder → Local Files → File Watcher → S3 Upload
                                                                    ↓
                                                              CloudFront
                                                                    ↓
                                                              HLS.js Player
```

### Issues in Current Architecture

1. **FFmpeg generates VOD-style playlists**: Uses default HLS flags that create
   Video-On-Demand playlists instead of live/event playlists
2. **No master playlist**: Each quality profile has its own playlist, but no
   master.m3u8 to coordinate them
3. **Playlist caching**: S3 uploads don't set cache headers, causing browsers to
   cache playlists indefinitely
4. **Player configuration**: HLS.js is not configured for live streaming, so it
   doesn't refresh playlists

### Fixed Architecture

```
UDP Stream → FFmpeg Transcoder (with live flags) → Local Files → File Watcher → S3 Upload (with cache headers)
                     ↓                                                                    ↓
              Master Playlist Generator                                            CloudFront (no-cache for playlists)
                                                                                          ↓
                                                                                  HLS.js Player (live mode)
```

## Components and Interfaces

### 1. FFmpegTranscoder Updates

**File**: `backend/src/FFmpegTranscoder.ts`

**Changes Required**:

1. Update `buildFFmpegCommand()` to add live streaming flags
2. Generate master playlist after starting transcoding
3. Update file watcher to detect master playlist changes

**New FFmpeg Flags**:

```typescript
// Add to each HLS output:
args.push("-hls_playlist_type", "event"); // Mark as live/event stream
args.push("-hls_flags", "delete_segments+append_list+omit_endlist"); // Live streaming flags
args.push("-hls_list_size", "10"); // Keep 10 segments in playlist
args.push("-hls_time", this.config.segmentDuration.toString());
```

**Flag Explanations**:

- `hls_playlist_type: event`: Marks the playlist as an event stream (live but
  seekable)
- `delete_segments`: Removes old segment files from disk
- `append_list`: Appends new segments to existing playlist
- `omit_endlist`: Doesn't add `#EXT-X-ENDLIST` tag (signals ongoing stream)

### 2. Master Playlist Generator

**New Component**: `MasterPlaylistGenerator`

**Purpose**: Generate and maintain the master.m3u8 file that references all
quality profiles

**Interface**:

```typescript
export class MasterPlaylistGenerator {
  constructor(config: {
    port: number;
    profiles: BitrateProfile[];
    localOutputDir: string;
  });

  // Generate master playlist content
  generateMasterPlaylist(): string;

  // Write master playlist to disk
  async writeMasterPlaylist(): Promise<void>;

  // Get the local path to master playlist
  getMasterPlaylistPath(): string;
}
```

**Master Playlist Format**:

```m3u8
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080
1080p/playlist.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720
720p/playlist.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=1500000,RESOLUTION=854x480
480p/playlist.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360
360p/playlist.m3u8
```

### 3. S3UploadService Updates

**File**: `backend/src/S3UploadService.ts`

**Changes Required**:

1. Add cache header configuration for different file types
2. Update `uploadData()` to set cache headers based on file type
3. Ensure playlists get no-cache headers
4. Ensure segments get immutable cache headers

**Cache Header Strategy**:

```typescript
// For playlist files (.m3u8):
CacheControl: "no-cache, no-store, must-revalidate";
Expires: "0";

// For segment files (.ts):
CacheControl: "max-age=31536000, immutable"; // 1 year, segments never change

// For snapshots (.jpg):
CacheControl: "no-cache"; // Allow caching but revalidate
```

**Updated Interface**:

```typescript
async uploadData(
  data: Buffer,
  s3Key: string,
  options: UploadOptions & {
    cacheControl?: string  // New: explicit cache control
  }
): Promise<void>
```

### 4. FFmpegTranscoder File Watcher Updates

**File**: `backend/src/FFmpegTranscoder.ts`

**Changes Required**:

1. Add watcher for master.m3u8 file
2. Ensure playlist files are uploaded immediately when modified
3. Add debouncing to prevent duplicate uploads

**Implementation**:

```typescript
private async setupFileWatchers(): Promise<void> {
  // Watch master playlist
  const masterPlaylistPath = join(
    this.config.localOutputDir,
    'hls',
    this.config.port.toString(),
    'master.m3u8'
  )

  const masterWatcher = watch(dirname(masterPlaylistPath), (eventType, filename) => {
    if (filename === 'master.m3u8') {
      void this.uploadMasterPlaylist()
    }
  })

  this.fileWatchers.set('master', masterWatcher)

  // Existing profile watchers...
  // Update to use proper cache headers
}
```

### 5. HLS.js Player Configuration Updates

**File**: `frontend/src/page/StreamPlayer.tsx`

**Changes Required**:

1. Update Hls configuration for live streaming
2. Add live sync configuration
3. Add latency management configuration

**Updated Configuration**:

```typescript
const hls = new Hls({
  enableWorker: true,
  lowLatencyMode: true,

  // Live streaming configuration
  liveSyncDurationCount: 3, // Stay 3 segments behind live edge
  liveMaxLatencyDurationCount: 10, // Max 10 segments behind
  liveDurationInfinity: true, // Handle infinite duration streams

  // Playlist refresh configuration
  manifestLoadingTimeOut: 10000, // 10 second timeout
  manifestLoadingMaxRetry: 5, // Retry up to 5 times
  manifestLoadingRetryDelay: 1000, // Start with 1 second delay
  manifestLoadingMaxRetryTimeout: 64000, // Max 64 seconds between retries

  // Adaptive bitrate streaming
  startLevel: -1, // Auto quality selection
  capLevelToPlayerSize: true,
  maxBufferLength: 30,
  maxMaxBufferLength: 60,
});
```

**Configuration Explanations**:

- `liveSyncDurationCount: 3`: Player stays 3 segments behind the live edge for
  smooth playback
- `liveMaxLatencyDurationCount: 10`: If player falls more than 10 segments
  behind, it will skip forward
- `liveDurationInfinity: true`: Handles streams with unknown/infinite duration
- `manifestLoadingMaxRetry: 5`: Retries playlist loading up to 5 times before
  giving up

### 6. StreamDynamoDBClient Updates

**File**: `frontend/src/utils/StreamDynamoDBClient.ts`

**Changes Required**:

1. Update `getStreamDetail()` to return master playlist URL instead of
   profile-specific URL
2. Add fallback to specific profile if master playlist doesn't exist (backward
   compatibility)

**Updated Interface**:

```typescript
export type StreamDetailResponse = {
  port: number;
  status: "active" | "inactive";
  lastPacketTime: string;
  hlsManifestUrl: string; // Now points to master.m3u8
  rawStreamUrl: string;
  lastFrameUrl: string;
  metadata: {
    createdAt: string;
    updatedAt: string;
  };
};
```

**URL Construction**:

```typescript
// Before: https://cloudfront.domain/hls/5000/1080p/playlist.m3u8
// After:  https://cloudfront.domain/hls/5000/master.m3u8
```

## Data Models

### Master Playlist Structure

```typescript
type MasterPlaylist = {
  version: number; // HLS version (3 or higher)
  streams: StreamVariant[];
};

type StreamVariant = {
  bandwidth: number; // Bits per second
  resolution: {
    width: number;
    height: number;
  };
  playlistPath: string; // Relative path to variant playlist
};
```

### Playlist Metadata

```typescript
type PlaylistMetadata = {
  type: "EVENT" | "VOD"; // Playlist type
  targetDuration: number; // Maximum segment duration
  mediaSequence: number; // Starting sequence number
  segments: SegmentInfo[];
  hasEndList: boolean; // Whether stream has ended
};

type SegmentInfo = {
  duration: number; // Segment duration in seconds
  filename: string; // Segment filename
  sequenceNumber: number; // Segment sequence number
};
```

### Cache Configuration

```typescript
type CacheConfig = {
  fileType: "playlist" | "segment" | "snapshot";
  cacheControl: string;
  expires?: string;
};

const CACHE_CONFIGS: Record<string, CacheConfig> = {
  ".m3u8": {
    fileType: "playlist",
    cacheControl: "no-cache, no-store, must-revalidate",
    expires: "0",
  },
  ".ts": {
    fileType: "segment",
    cacheControl: "max-age=31536000, immutable",
  },
  ".jpg": {
    fileType: "snapshot",
    cacheControl: "no-cache",
  },
};
```

## Correctness Properties

_A property is a characteristic or behavior that should hold true across all
valid executions of a system—essentially, a formal statement about what the
system should do. Properties serve as the bridge between human-readable
specifications and machine-verifiable correctness guarantees._

### Property 1: Live Playlist Tags

_For any_ active HLS stream, the generated playlist files should contain the
`#EXT-X-PLAYLIST-TYPE:EVENT` tag and should NOT contain the `#EXT-X-ENDLIST`
tag.

**Validates: Requirements 1.1, 1.3**

### Property 2: Playlist Updates on Segment Creation

_For any_ new segment created by FFmpeg, the corresponding playlist file should
be updated to include the new segment entry within 2 seconds.

**Validates: Requirements 1.2, 2.1, 2.2**

### Property 3: Playlist Cache Headers

_For any_ playlist file (.m3u8) uploaded to S3, the Cache-Control header should
be set to `no-cache, no-store, must-revalidate` and the content type should be
`application/vnd.apple.mpegurl`.

**Validates: Requirements 2.3, 2.4, 2.5**

### Property 4: Segment Cache Headers

_For any_ segment file (.ts) uploaded to S3, the Cache-Control header should be
set to `max-age=31536000, immutable` and the content type should be
`video/mp2t`.

**Validates: Requirements 7.4, 7.5**

### Property 5: Master Playlist Content

_For any_ generated master.m3u8 file, it should reference all configured quality
profiles (1080p, 720p, 480p, 360p) with correct bandwidth and resolution
information for each profile.

**Validates: Requirements 4.2, 4.4**

### Property 6: Master Playlist Upload

_For any_ transcoding session that starts, a master.m3u8 file should be created
and uploaded to S3 within 5 seconds of the first segment being generated.

**Validates: Requirements 4.1, 4.3**

### Property 7: Sequence Number Continuity

_For any_ sequence of playlist updates, the `#EXT-X-MEDIA-SEQUENCE` values
should increment monotonically, and segment deletions should not cause sequence
number gaps or resets.

**Validates: Requirements 5.1, 5.2, 5.4**

### Property 8: Minimum Playlist Size

_For any_ active stream that has generated more than 10 segments, the playlist
should contain at least 10 segment entries.

**Validates: Requirements 5.3**

### Property 9: HLS Player Live Configuration

_For any_ HLS player instance initialized for a live stream, the configuration
should have `liveSyncDurationCount >= 3` and `liveMaxLatencyDurationCount`
should be defined.

**Validates: Requirements 3.3, 3.4**

### Property 10: Automatic Segment Playback

_For any_ new segment added to a playlist while the player is active, the player
should automatically load and begin playing the segment without manual
intervention.

**Validates: Requirements 3.5**

### Property 11: Playlist Refresh Retry

_For any_ playlist refresh failure, the player should retry with exponential
backoff, and after 5 consecutive failures, should emit an error event.

**Validates: Requirements 6.1, 6.3**

### Property 12: Graceful Degradation

_For any_ sequence of playlist refresh failures, the player should continue
playing buffered segments and should resume normal operation when the playlist
becomes available again.

**Validates: Requirements 6.2, 6.4**

### Property 13: Parallel Upload

_For any_ transcoding session with multiple segments and playlists ready for
upload, the S3 upload service should upload them in parallel (multiple
concurrent uploads).

**Validates: Requirements 7.2**

### Property 14: Upload Retry

_For any_ failed S3 upload, the service should retry up to 3 times with
exponential backoff before marking the upload as failed.

**Validates: Requirements 7.3**

## Error Handling

### FFmpeg Transcoder Errors

1. **FFmpeg Process Crash**
   - Current: Retries with exponential backoff (up to 3 attempts)
   - Addition: Validate configuration before starting to catch errors early
   - Log complete FFmpeg command for debugging

2. **Invalid Configuration**
   - New: Validate HLS flags before starting FFmpeg
   - Check for required flags: `append_list`, positive `hls_list_size`
   - Emit error and refuse to start if configuration is invalid

3. **File System Errors**
   - Current: Handled by file watcher error events
   - Addition: Ensure directories exist before starting FFmpeg
   - Retry file operations with backoff

### S3 Upload Errors

1. **Network Failures**
   - Current: Buffers uploads and retries with exponential backoff
   - Keep: Existing retry logic (up to 3 attempts)
   - Addition: Emit events for monitoring

2. **Permission Errors**
   - Current: Logs error and continues
   - Addition: Emit critical error event for monitoring
   - Consider: Fallback to local storage if S3 is unavailable

3. **Rate Limiting**
   - Current: Retries with backoff
   - Addition: Implement adaptive rate limiting
   - Consider: Batch uploads to reduce request count

### HLS Player Errors

1. **Playlist Load Failures**
   - New: Retry with exponential backoff (up to 5 attempts)
   - Continue playing buffered segments during retries
   - Emit error event after max retries exceeded

2. **Segment Load Failures**
   - Current: HLS.js handles with built-in retry logic
   - Addition: Monitor and log segment load failures
   - Consider: Fallback to lower quality on repeated failures

3. **Media Errors**
   - Current: Attempts recovery with `recoverMediaError()`
   - Keep: Existing recovery logic
   - Addition: Fallback to lower quality if recovery fails

### Master Playlist Errors

1. **Generation Failures**
   - New: Log error and continue with profile-specific playlists
   - Emit warning event for monitoring
   - Frontend should fallback to specific profile if master not available

2. **Upload Failures**
   - New: Retry with same logic as other S3 uploads
   - Log error but don't block transcoding
   - Frontend can fallback to profile-specific URLs

## Testing Strategy

### Unit Tests

Unit tests will focus on specific examples, edge cases, and error conditions:

1. **FFmpeg Command Generation**
   - Test that live streaming flags are included in command
   - Test that configuration validation catches invalid settings
   - Test command generation for different profile configurations

2. **Master Playlist Generation**
   - Test master playlist format with all profiles
   - Test master playlist with missing profiles
   - Test bandwidth and resolution formatting

3. **Cache Header Configuration**
   - Test playlist files get no-cache headers
   - Test segment files get immutable headers
   - Test snapshot files get appropriate headers

4. **File Watcher Behavior**
   - Test watcher detects playlist changes
   - Test watcher detects master playlist changes
   - Test debouncing prevents duplicate uploads

5. **Error Handling**
   - Test configuration validation rejects invalid settings
   - Test retry logic for failed uploads
   - Test player error recovery

### Property-Based Tests

Property-based tests will verify universal properties across all inputs. Each
test should run a minimum of 100 iterations.

1. **Property 1: Live Playlist Tags**
   - Generate random active streams
   - Verify playlists contain EVENT tag and no ENDLIST tag
   - Tag: **Feature: continuous-hls-playback-fix, Property 1: Live Playlist
     Tags**

2. **Property 2: Playlist Updates on Segment Creation**
   - Generate random segments
   - Verify playlist updates occur within 2 seconds
   - Tag: **Feature: continuous-hls-playback-fix, Property 2: Playlist Updates
     on Segment Creation**

3. **Property 3: Playlist Cache Headers**
   - Upload random playlist files
   - Verify Cache-Control and Content-Type headers
   - Tag: **Feature: continuous-hls-playback-fix, Property 3: Playlist Cache
     Headers**

4. **Property 4: Segment Cache Headers**
   - Upload random segment files
   - Verify Cache-Control and Content-Type headers
   - Tag: **Feature: continuous-hls-playback-fix, Property 4: Segment Cache
     Headers**

5. **Property 5: Master Playlist Content**
   - Generate random profile configurations
   - Verify master playlist references all profiles with correct metadata
   - Tag: **Feature: continuous-hls-playback-fix, Property 5: Master Playlist
     Content**

6. **Property 6: Master Playlist Upload**
   - Start random transcoding sessions
   - Verify master playlist is uploaded within 5 seconds
   - Tag: **Feature: continuous-hls-playback-fix, Property 6: Master Playlist
     Upload**

7. **Property 7: Sequence Number Continuity**
   - Generate random sequences of playlist updates with deletions
   - Verify sequence numbers increment monotonically without gaps
   - Tag: **Feature: continuous-hls-playback-fix, Property 7: Sequence Number
     Continuity**

8. **Property 8: Minimum Playlist Size**
   - Generate streams with varying segment counts
   - Verify playlists with >10 segments contain at least 10 entries
   - Tag: **Feature: continuous-hls-playback-fix, Property 8: Minimum Playlist
     Size**

9. **Property 9: HLS Player Live Configuration**
   - Initialize players with random configurations
   - Verify live streaming parameters are set correctly
   - Tag: **Feature: continuous-hls-playback-fix, Property 9: HLS Player Live
     Configuration**

10. **Property 10: Automatic Segment Playback**
    - Add random new segments to active playlists
    - Verify player loads and plays them automatically
    - Tag: **Feature: continuous-hls-playback-fix, Property 10: Automatic
      Segment Playback**

11. **Property 11: Playlist Refresh Retry**
    - Simulate random playlist refresh failures
    - Verify exponential backoff and error emission after 5 failures
    - Tag: **Feature: continuous-hls-playback-fix, Property 11: Playlist Refresh
      Retry**

12. **Property 12: Graceful Degradation**
    - Simulate random failure and recovery sequences
    - Verify playback continues and resumes correctly
    - Tag: **Feature: continuous-hls-playback-fix, Property 12: Graceful
      Degradation**

13. **Property 13: Parallel Upload**
    - Generate random sets of files for upload
    - Verify multiple concurrent uploads occur
    - Tag: **Feature: continuous-hls-playback-fix, Property 13: Parallel
      Upload**

14. **Property 14: Upload Retry**
    - Simulate random upload failures
    - Verify retry count and exponential backoff timing
    - Tag: **Feature: continuous-hls-playback-fix, Property 14: Upload Retry**

### Integration Tests

1. **End-to-End Streaming**
   - Start UDP stream → Verify continuous playback in browser
   - Verify playlist updates occur automatically
   - Verify player switches between quality levels

2. **Stream Interruption Recovery**
   - Start stream → Stop stream → Restart stream
   - Verify player recovers and continues playback
   - Verify playlists are updated correctly

3. **Multi-Quality Streaming**
   - Start stream with all quality profiles
   - Verify master playlist is generated and uploaded
   - Verify player can switch between qualities

### Testing Tools

- **Property-Based Testing**: Use `fast-check` (TypeScript) for property-based
  tests
- **Unit Testing**: Use Node.js built-in test runner for backend, Vitest for
  frontend
- **Integration Testing**: Use existing test infrastructure with real UDP
  streams
- **Manual Testing**: Use browser developer tools to verify playlist refresh
  behavior
