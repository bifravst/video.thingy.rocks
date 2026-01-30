# Requirements Document: Offline Video Playback

## Introduction

This specification addresses the enhancement of the video streaming system to
enable playback of recorded video content when a stream goes offline. Currently,
when a stream becomes inactive, the system displays only a static snapshot image
with an "OFFLINE" badge. However, the HLS segments and playlists remain
available in S3 after the stream stops (event-type playlist), allowing for
Video-On-Demand (VOD) playback of the last recorded session. This feature will
improve user experience by allowing viewers to watch the most recent recording
instead of seeing only a static image.

## Glossary

- **HLS_Player**: The hls.js-based video player component in the web frontend
- **VOD_Mode**: Video-On-Demand playback mode where the entire recording is
  available for seeking
- **Live_Mode**: Live streaming playback mode where the player stays near the
  live edge
- **Event_Playlist**: An HLS playlist with `#EXT-X-PLAYLIST-TYPE:EVENT` that
  contains all segments from a recording session
- **Stream_Status**: The current state of a stream, either 'active' (live) or
  'inactive' (offline)
- **Recording_Session**: A continuous period of streaming that produces a
  complete set of HLS segments
- **Playback_Badge**: The UI indicator showing stream state (LIVE, RECORDED,
  OFFLINE)
- **Stream_Transition**: The change in stream status from active to inactive or
  vice versa

## Requirements

### Requirement 1: Detect Stream Status and Recording Availability

**User Story:** As a video player, I want to detect when a stream goes offline
and whether a recording is available, so that I can switch to VOD playback mode.

#### Acceptance Criteria

1. WHEN the stream status changes from 'active' to 'inactive', THE HLS_Player
   SHALL detect the transition
2. WHEN a stream is 'inactive', THE HLS_Player SHALL check if the HLS manifest
   URL is accessible
3. IF the HLS manifest returns a valid playlist with segments, THEN THE
   HLS_Player SHALL consider a recording available
4. IF the HLS manifest is not accessible or contains no segments, THEN THE
   HLS_Player SHALL consider no recording available

### Requirement 2: Initialize VOD Playback for Offline Streams

**User Story:** As a viewer, I want to watch the recorded video when a stream is
offline, so that I can see the most recent content.

#### Acceptance Criteria

1. WHEN a stream is 'inactive' and a recording is available, THE HLS_Player
   SHALL initialize in VOD_Mode
2. WHEN initializing in VOD_Mode, THE HLS_Player SHALL load the HLS manifest URL
3. WHEN in VOD_Mode, THE HLS_Player SHALL disable live sync parameters
   (`liveSyncDurationCount`, `liveMaxLatencyDurationCount`)
4. WHEN in VOD_Mode, THE HLS_Player SHALL enable seeking through the entire
   recording
5. WHEN the VOD player is ready, THE HLS_Player SHALL attempt to autoplay the
   recording

### Requirement 3: Configure HLS Player for VOD Playback

**User Story:** As a video player, I want to use appropriate HLS.js
configuration for VOD playback, so that the recorded video plays correctly.

#### Acceptance Criteria

1. WHEN creating an HLS instance for VOD, THE HLS_Player SHALL NOT set
   `liveSyncDurationCount`
2. WHEN creating an HLS instance for VOD, THE HLS_Player SHALL NOT set
   `liveMaxLatencyDurationCount`
3. WHEN creating an HLS instance for VOD, THE HLS_Player SHALL NOT set
   `liveDurationInfinity`
4. WHEN creating an HLS instance for VOD, THE HLS_Player SHALL set
   `startPosition` to 0 for playback from the beginning
5. THE HLS_Player SHALL maintain standard buffer settings for VOD playback

### Requirement 4: Update UI for Recorded Playback

**User Story:** As a viewer, I want to see a clear indicator when watching a
recording versus a live stream, so that I understand the content state.

#### Acceptance Criteria

1. WHEN playing a recording, THE Playback_Badge SHALL display "RECORDED" instead
   of "LIVE"
2. WHEN no recording is available and stream is offline, THE Playback_Badge
   SHALL display "OFFLINE"
3. THE "RECORDED" badge SHALL use a distinct visual style from the "LIVE" badge
4. WHEN displaying a recording, THE HLS_Player SHALL show the video player with
   standard controls
5. WHEN no recording is available, THE HLS_Player SHALL display the last frame
   image

### Requirement 5: Handle Transition from Live to Recorded

**User Story:** As a viewer, I want a smooth transition when a stream goes
offline, so that I can continue watching the recorded content.

#### Acceptance Criteria

1. WHEN a stream transitions from 'active' to 'inactive', THE HLS_Player SHALL
   destroy the existing live HLS instance
2. WHEN transitioning to offline with recording available, THE HLS_Player SHALL
   initialize a new VOD HLS instance
3. WHEN transitioning to VOD playback, THE HLS_Player SHALL start playback from
   the beginning of the recording
4. THE Stream_Transition SHALL complete within 2 seconds of status change
   detection
5. WHEN the transition fails, THE HLS_Player SHALL fall back to displaying the
   last frame image

### Requirement 6: Handle Transition from Recorded to Live

**User Story:** As a viewer, I want to automatically switch to live playback
when a stream resumes, so that I can watch the current broadcast.

#### Acceptance Criteria

1. WHEN a stream transitions from 'inactive' to 'active', THE HLS_Player SHALL
   destroy the existing VOD HLS instance
2. WHEN transitioning to live, THE HLS_Player SHALL initialize a new live HLS
   instance with live streaming configuration
3. WHEN transitioning to live, THE HLS_Player SHALL update the Playback_Badge to
   "LIVE"
4. THE Stream_Transition SHALL complete within 2 seconds of status change
   detection
5. WHEN transitioning to live, THE HLS_Player SHALL attempt to autoplay the live
   stream

### Requirement 7: Preserve Player Controls for VOD

**User Story:** As a viewer, I want full video controls when watching a
recording, so that I can seek, pause, and control playback.

#### Acceptance Criteria

1. WHEN playing a recording, THE HLS_Player SHALL display the standard HTML5
   video controls
2. WHEN playing a recording, THE HLS_Player SHALL enable the seek bar for
   navigation
3. WHEN a viewer seeks in a recording, THE HLS_Player SHALL jump to the
   requested position
4. WHEN playing a recording, THE HLS_Player SHALL display the total duration of
   the recording
5. WHEN playing a recording, THE HLS_Player SHALL allow pause and resume
   operations

### Requirement 8: Handle Recording Playback Errors

**User Story:** As a video player, I want to gracefully handle errors during
recording playback, so that viewers see appropriate feedback.

#### Acceptance Criteria

1. WHEN the recording manifest fails to load, THE HLS_Player SHALL fall back to
   displaying the last frame image
2. WHEN recording playback encounters a fatal error, THE HLS_Player SHALL
   display an error message
3. IF recording segments are missing or corrupted, THEN THE HLS_Player SHALL
   attempt to skip to the next available segment
4. WHEN recording playback fails, THE HLS_Player SHALL log the error details for
   debugging
5. WHEN falling back to last frame display, THE Playback_Badge SHALL show
   "OFFLINE"

### Requirement 9: Maintain Mode Toggle Compatibility

**User Story:** As a viewer, I want the raw stream mode toggle to work correctly
for both live and recorded playback, so that I can choose my preferred quality.

#### Acceptance Criteria

1. WHEN a stream is 'active', THE HLS_Player SHALL display the mode toggle
   control
2. WHEN a stream is 'inactive' with recording, THE HLS_Player SHALL hide the
   mode toggle control
3. WHEN playing a recording, THE HLS_Player SHALL use the adaptive HLS manifest
   URL
4. WHEN transitioning from live to recorded, THE HLS_Player SHALL reset the mode
   toggle to adaptive mode
5. WHEN transitioning from recorded to live, THE HLS_Player SHALL restore the
   previous mode toggle state

### Requirement 10: Optimize Initial Load for Offline Streams

**User Story:** As a viewer, I want recordings to load quickly when I visit an
offline stream, so that I can start watching without delay.

#### Acceptance Criteria

1. WHEN loading a stream page with 'inactive' status, THE HLS_Player SHALL
   immediately check for recording availability
2. WHEN a recording is available, THE HLS_Player SHALL initialize the VOD player
   without waiting for status polling
3. THE HLS_Player SHALL load the recording manifest within 1 second of page load
4. WHEN the recording is ready, THE HLS_Player SHALL display the first frame
   within 2 seconds
5. THE HLS_Player SHALL continue polling for status changes to detect when the
   stream goes live
