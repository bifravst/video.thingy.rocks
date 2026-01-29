# Implementation Plan: Continuous HLS Playback Fix

## Overview

This implementation plan fixes the bug where only the first 6-second segment
plays by:

1. Updating FFmpeg configuration to generate live HLS playlists
2. Creating and uploading master playlists for multi-bitrate streaming
3. Configuring proper cache headers for playlists and segments
4. Updating the HLS.js player for live streaming mode

## Tasks

- [x] 1. Update FFmpeg command generation for live streaming
  - Modify `buildFFmpegCommand()` in `FFmpegTranscoder.ts` to add live streaming
    flags
  - Add `hls_playlist_type: event` flag to mark playlists as live/event streams
  - Update `hls_flags` to include `omit_endlist` flag (prevents adding
    #EXT-X-ENDLIST tag)
  - Ensure `append_list` flag is present for continuous playlist updates
  - Log the complete FFmpeg command for debugging
  - _Requirements: 1.1, 1.2, 1.3, 8.1_

- [x] 1.1 Write unit tests for FFmpeg command generation
  - Test that live streaming flags are included in generated command
  - Test command format with different profile configurations
  - Test that required flags (append_list, omit_endlist) are present
  - _Requirements: 1.1, 1.3_

- [x] 2. Implement master playlist generation
  - [x] 2.1 Create `MasterPlaylistGenerator` class
    - Implement constructor accepting port, profiles, and output directory
    - Implement `generateMasterPlaylist()` method to create master.m3u8 content
    - Include #EXT-X-STREAM-INF tags with bandwidth and resolution for each
      profile
    - Implement `writeMasterPlaylist()` method to write file to disk
    - Implement `getMasterPlaylistPath()` helper method
    - _Requirements: 4.1, 4.2, 4.4_

  - [x] 2.2 Write property test for master playlist content
    - **Property 5: Master Playlist Content**
    - **Validates: Requirements 4.2, 4.4**
    - Generate random profile configurations
    - Verify master playlist references all profiles with correct bandwidth and
      resolution
    - _Requirements: 4.2, 4.4_

  - [x] 2.3 Integrate master playlist generation into FFmpegTranscoder
    - Create MasterPlaylistGenerator instance in FFmpegTranscoder constructor
    - Call `writeMasterPlaylist()` after FFmpeg process starts
    - Add file watcher for master.m3u8 file
    - Upload master playlist to S3 when created or modified
    - _Requirements: 4.1, 4.3_

  - [ ]\* 2.4 Write property test for master playlist upload timing
    - **Property 6: Master Playlist Upload**
    - **Validates: Requirements 4.1, 4.3**
    - Start random transcoding sessions
    - Verify master.m3u8 is uploaded within 5 seconds of first segment
    - _Requirements: 4.1, 4.3_

- [x] 3. Update S3 upload service with cache headers
  - [x] 3.1 Add cache header configuration
    - Define cache configuration constants for different file types
    - Playlist files (.m3u8):
      `Cache-Control: no-cache, no-store, must-revalidate`
    - Segment files (.ts): `Cache-Control: max-age=31536000, immutable`
    - Snapshot files (.jpg): `Cache-Control: no-cache`
    - _Requirements: 2.3, 2.4, 7.5_

  - [x] 3.2 Update `uploadData()` method to set cache headers
    - Add `cacheControl` parameter to UploadOptions type
    - Determine cache headers based on file extension if not explicitly provided
    - Set Cache-Control header in PutObjectCommand
    - Set Expires header for playlist files
    - _Requirements: 2.3, 2.4, 2.5, 7.4, 7.5_

  - [x] 3.3 Write property tests for cache headers
    - **Property 3: Playlist Cache Headers**
    - **Validates: Requirements 2.3, 2.4, 2.5**
    - Upload random playlist files and verify Cache-Control and Content-Type
      headers
    - **Property 4: Segment Cache Headers**
    - **Validates: Requirements 7.4, 7.5**
    - Upload random segment files and verify Cache-Control and Content-Type
      headers
    - _Requirements: 2.3, 2.4, 2.5, 7.4, 7.5_

  - [x] 3.4 Update all upload methods to use proper cache headers
    - Update `uploadHLSPlaylist()` to use no-cache headers
    - Update `uploadMasterPlaylist()` to use no-cache headers
    - Update `uploadHLSSegment()` to use immutable headers
    - Update `uploadSnapshot()` to use no-cache headers
    - _Requirements: 2.3, 2.4, 2.5, 7.4, 7.5_

- [x] 4. Checkpoint - Verify backend changes
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Update HLS.js player configuration for live streaming
  - [x] 5.1 Update player initialization in StreamPlayer.tsx
    - Add `liveSyncDurationCount: 3` to Hls configuration
    - Add `liveMaxLatencyDurationCount: 10` to configuration
    - Add `liveDurationInfinity: true` for infinite duration streams
    - Configure manifest loading retry parameters (maxRetry: 5,
      retryDelay: 1000)
    - Add exponential backoff for manifest loading retries
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 6.1_

  - [x] 5.2 Write unit tests for player configuration
    - Test that live streaming parameters are set correctly
    - Test that retry parameters are configured
    - Test player initialization with live stream URL
    - _Requirements: 3.1, 3.3, 3.4_

  - [x] 5.3 Add error handling for playlist refresh failures
    - Listen for HLS.Events.ERROR with type NETWORK_ERROR
    - Implement exponential backoff retry logic
    - Continue playing buffered segments during retries
    - Emit error event after 5 consecutive failures
    - Resume normal operation when playlist becomes available
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [ ]\* 5.4 Write property tests for error handling
    - **Property 11: Playlist Refresh Retry**
    - **Validates: Requirements 6.1, 6.3**
    - Simulate random playlist refresh failures and verify retry behavior
    - **Property 12: Graceful Degradation**
    - **Validates: Requirements 6.2, 6.4**
    - Simulate failure and recovery sequences, verify playback continues
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [ ] 6. Update frontend to use master playlist URL
  - [ ] 6.1 Update StreamDynamoDBClient
    - Modify `getStreamDetail()` to construct master.m3u8 URL
    - Change URL format from `hls/{port}/{profile}/playlist.m3u8` to
      `hls/{port}/master.m3u8`
    - Add fallback to 1080p profile playlist if master doesn't exist (backward
      compatibility)
    - _Requirements: 4.5_

  - [ ] 6.2 Write unit tests for URL construction
    - Test master playlist URL format
    - Test fallback to profile-specific URL
    - Test URL construction for different ports
    - _Requirements: 4.5_

- [ ] 7. Add configuration validation to FFmpegTranscoder
  - [ ] 7.1 Implement configuration validation method
    - Create `validateConfiguration()` method
    - Verify `hls_flags` includes `append_list`
    - Verify `hls_list_size` is a positive integer
    - Verify `segmentDuration` is positive
    - Throw error if configuration is invalid
    - _Requirements: 8.2, 8.3, 8.4_

  - [ ] 7.2 Call validation before starting FFmpeg
    - Call `validateConfiguration()` in `start()` method before spawning process
    - Log validation errors with details
    - Emit error event if validation fails
    - Refuse to start if configuration is invalid
    - _Requirements: 8.2, 8.3, 8.4_

  - [ ] 7.3 Write unit tests for configuration validation
    - Test validation accepts valid configuration
    - Test validation rejects missing append_list flag
    - Test validation rejects invalid hls_list_size
    - Test validation rejects negative segment duration
    - Test that invalid configuration prevents startup
    - _Requirements: 8.2, 8.3, 8.4_

- [ ] 8. Checkpoint - Verify all changes
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Update file watcher to handle playlist updates
  - [ ] 9.1 Add debouncing to file watcher
    - Implement debounce logic to prevent duplicate uploads
    - Wait 500ms after file change before uploading
    - Cancel pending uploads if file changes again
    - _Requirements: 2.1, 2.2_

  - [ ] 9.2 Ensure playlist uploads happen immediately
    - Prioritize playlist uploads over segment uploads
    - Upload playlists within 1 second of modification
    - Log upload timing for monitoring
    - _Requirements: 2.1, 2.2_

  - [ ]\* 9.3 Write property test for playlist update timing
    - **Property 2: Playlist Updates on Segment Creation**
    - **Validates: Requirements 1.2, 2.1, 2.2**
    - Generate random segments and verify playlist updates occur within 2
      seconds
    - _Requirements: 1.2, 2.1, 2.2_

- [ ] 10. Add stream end handling
  - [ ] 10.1 Update FFmpeg command for stream end
    - Remove `omit_endlist` flag when stream is stopping
    - Allow FFmpeg to add #EXT-X-ENDLIST tag on graceful shutdown
    - Ensure final playlist is uploaded to S3
    - _Requirements: 1.4_

  - [ ] 10.2 Update stop() method in FFmpegTranscoder
    - Send SIGTERM to FFmpeg for graceful shutdown
    - Wait for FFmpeg to finish writing final playlist
    - Upload final playlist with #EXT-X-ENDLIST tag
    - _Requirements: 1.4_

  - [ ] 10.3 Write unit test for stream end behavior
    - Test that stopped streams have #EXT-X-ENDLIST tag
    - Test that final playlist is uploaded
    - Test graceful shutdown timing
    - _Requirements: 1.4_

- [ ] 11. Final checkpoint and integration testing
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- Unit tests validate specific examples and edge cases
- The fix requires changes to both backend (FFmpeg, S3) and frontend (HLS.js
  player)
- Cache headers are critical for preventing stale playlist caching
- Master playlist enables adaptive bitrate streaming across quality profiles
