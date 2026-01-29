# Requirements Document: Continuous HLS Playback Fix

## Introduction

This specification addresses a critical bug in the video streaming system where
the web application only plays the first 6-second segment of a live HLS stream,
despite continuous video data being transcoded and uploaded to S3. The system
currently generates HLS segments correctly but fails to provide continuous
playback in the browser.

## Glossary

- **HLS_Player**: The hls.js-based video player component in the web frontend
- **FFmpeg_Transcoder**: The backend service that transcodes UDP video streams
  to HLS format
- **Playlist_File**: The .m3u8 manifest file that lists available video segments
- **Master_Playlist**: The top-level playlist.m3u8 file for each quality profile
- **Video_Segment**: A .ts (MPEG-TS) file containing 6 seconds of video data
- **S3_Upload_Service**: The service responsible for uploading HLS files to S3
- **Live_Stream**: A continuous video stream that generates new segments
  indefinitely
- **VOD_Stream**: A Video-On-Demand stream with a fixed number of segments

## Requirements

### Requirement 1: Generate Live HLS Playlists

**User Story:** As a video streaming system, I want to generate HLS playlists
with live streaming tags, so that players know to continuously refresh the
playlist for new segments.

#### Acceptance Criteria

1. WHEN FFmpeg generates HLS playlists, THE FFmpeg_Transcoder SHALL include the
   `#EXT-X-PLAYLIST-TYPE:EVENT` tag in the playlist
2. WHEN a new segment is created, THE FFmpeg_Transcoder SHALL update the
   playlist file with the new segment entry
3. THE FFmpeg_Transcoder SHALL NOT include the `#EXT-X-ENDLIST` tag in active
   stream playlists
4. WHEN the stream stops, THE FFmpeg_Transcoder SHALL append the
   `#EXT-X-ENDLIST` tag to signal completion

### Requirement 2: Upload Updated Playlists to S3

**User Story:** As a streaming backend, I want to upload updated playlist files
to S3 whenever they change, so that the frontend can fetch the latest segment
list.

#### Acceptance Criteria

1. WHEN a playlist file is modified by FFmpeg, THE S3_Upload_Service SHALL
   detect the change within 1 second
2. WHEN a playlist change is detected, THE S3_Upload_Service SHALL upload the
   updated playlist to S3
3. THE S3_Upload_Service SHALL set appropriate cache headers to prevent stale
   playlist caching
4. WHEN uploading playlists, THE S3_Upload_Service SHALL set
   `Cache-Control: no-cache, no-store, must-revalidate` headers
5. WHEN uploading playlists, THE S3_Upload_Service SHALL set the content type to
   `application/vnd.apple.mpegurl`

### Requirement 3: Configure HLS Player for Live Streaming

**User Story:** As a web frontend, I want to configure the HLS player to
continuously refresh playlists, so that new segments are discovered and played
automatically.

#### Acceptance Criteria

1. WHEN initializing the HLS player, THE HLS_Player SHALL enable live streaming
   mode
2. WHEN in live streaming mode, THE HLS_Player SHALL periodically refresh the
   playlist to check for new segments
3. THE HLS_Player SHALL set `liveSyncDurationCount` to at least 3 segments for
   live playback
4. THE HLS_Player SHALL set `liveMaxLatencyDurationCount` to limit maximum
   latency
5. WHEN new segments are discovered, THE HLS_Player SHALL automatically load and
   play them

### Requirement 4: Generate Master Playlist for Multi-Bitrate Streaming

**User Story:** As a streaming system, I want to generate a master playlist that
references all quality profiles, so that the player can perform adaptive bitrate
streaming.

#### Acceptance Criteria

1. WHEN transcoding starts, THE FFmpeg_Transcoder SHALL generate a master.m3u8
   file
2. THE master.m3u8 file SHALL reference all available quality profile playlists
   (1080p, 720p, 480p, 360p)
3. WHEN the master playlist is created, THE S3_Upload_Service SHALL upload it to
   S3
4. THE master.m3u8 file SHALL include bandwidth and resolution information for
   each profile
5. WHEN the HLS_Player loads a stream, THE HLS_Player SHALL load the master.m3u8
   file as the source

### Requirement 5: Maintain Playlist Continuity

**User Story:** As a streaming system, I want to maintain proper segment
sequencing in playlists, so that players can seamlessly transition between
segments.

#### Acceptance Criteria

1. WHEN generating playlists, THE FFmpeg_Transcoder SHALL include
   `#EXT-X-MEDIA-SEQUENCE` tags with incrementing sequence numbers
2. WHEN a segment is deleted from disk, THE FFmpeg_Transcoder SHALL maintain
   correct sequence numbers in the playlist
3. THE FFmpeg_Transcoder SHALL keep at least 10 segments in the playlist at any
   time
4. WHEN the playlist reaches maximum size, THE FFmpeg_Transcoder SHALL remove
   the oldest segment entry while maintaining sequence continuity

### Requirement 6: Handle Playlist Refresh Errors

**User Story:** As an HLS player, I want to gracefully handle playlist refresh
failures, so that temporary network issues don't stop playback.

#### Acceptance Criteria

1. WHEN a playlist refresh fails, THE HLS_Player SHALL retry with exponential
   backoff
2. WHEN multiple refresh attempts fail, THE HLS_Player SHALL continue playing
   buffered segments
3. IF playlist refresh fails after 5 attempts, THEN THE HLS_Player SHALL emit an
   error event
4. WHEN the playlist becomes available again, THE HLS_Player SHALL resume normal
   operation

### Requirement 7: Optimize S3 Upload Performance

**User Story:** As a backend service, I want to upload segments and playlists
efficiently, so that they are available to players with minimal delay.

#### Acceptance Criteria

1. WHEN a segment file is fully written, THE S3_Upload_Service SHALL upload it
   within 2 seconds
2. THE S3_Upload_Service SHALL upload segments and playlists in parallel
3. WHEN upload fails, THE S3_Upload_Service SHALL retry up to 3 times with
   exponential backoff
4. THE S3_Upload_Service SHALL set appropriate content types for all uploaded
   files
5. WHEN uploading segments, THE S3_Upload_Service SHALL set
   `Cache-Control: max-age=31536000, immutable` headers

### Requirement 8: Validate HLS Configuration

**User Story:** As a developer, I want to validate that FFmpeg is configured
correctly for live streaming, so that I can catch configuration errors early.

#### Acceptance Criteria

1. WHEN the transcoder starts, THE FFmpeg_Transcoder SHALL log the complete
   FFmpeg command
2. THE FFmpeg_Transcoder SHALL verify that the `hls_flags` parameter includes
   `append_list`
3. THE FFmpeg_Transcoder SHALL verify that `hls_list_size` is set to a positive
   integer
4. IF the configuration is invalid, THEN THE FFmpeg_Transcoder SHALL emit an
   error and refuse to start
