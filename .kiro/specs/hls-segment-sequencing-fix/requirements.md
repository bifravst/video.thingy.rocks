# Requirements Document

## Introduction

This document specifies the requirements for fixing the HLS segment sequencing
issue where FFmpeg continuously overwrites segment_00000.ts instead of creating
sequential segments. The system must generate properly numbered sequential
segments (segment_00000.ts, segment_00001.ts, segment_00002.ts, etc.) that are
uploaded to S3 and referenced in the HLS playlist.

## Glossary

- **FFmpeg**: The multimedia framework used for transcoding video streams into
  HLS format
- **HLS_Segment**: A time-bounded chunk of video data in MPEG-TS format,
  typically 6 seconds in duration
- **Segment_Counter**: The internal state tracking the current segment number
  for sequential naming
- **Playlist**: The M3U8 file that references all available video segments
- **Transcoder**: The FFmpeg process that converts incoming UDP video stream to
  HLS segments
- **S3_Bucket**: AWS S3 storage location where segments and playlists are
  uploaded
- **Segment_Filename_Pattern**: The template used to generate segment filenames
  with sequential numbering

## Requirements

### Requirement 1: Sequential Segment Creation

**User Story:** As a streaming system, I want to create sequential HLS segments
with incrementing numbers, so that each segment is a unique file rather than
overwriting previous segments.

#### Acceptance Criteria

1. WHEN the Transcoder processes incoming video data, THE Transcoder SHALL
   create segments with sequential filenames starting from segment_00000.ts
2. WHEN a new segment is created, THE Segment_Counter SHALL increment by one
3. WHEN multiple segments are created, THE Transcoder SHALL NOT overwrite
   existing segment files
4. THE Transcoder SHALL maintain the Segment_Counter state across segment
   boundaries
5. WHEN the Transcoder creates segment_00000.ts, THE next segment SHALL be named
   segment_00001.ts, followed by segment_00002.ts, and so on

### Requirement 2: Segment Counter Persistence

**User Story:** As a streaming system, I want the segment counter to persist
correctly, so that segment numbering continues sequentially without resetting.

#### Acceptance Criteria

1. WHEN the Transcoder starts processing a stream, THE Transcoder SHALL
   initialize the Segment_Counter to zero
2. WHILE the Transcoder is running, THE Segment_Counter SHALL persist across
   segment writes
3. THE Transcoder SHALL NOT reset the Segment_Counter to zero after creating the
   first segment
4. WHEN FFmpeg writes a segment, THE Segment_Counter SHALL be incremented before
   the next segment is created

### Requirement 3: FFmpeg Configuration Correctness

**User Story:** As a system administrator, I want FFmpeg configured with correct
HLS parameters, so that segment sequencing works as expected.

#### Acceptance Criteria

1. THE Transcoder SHALL use the segment filename pattern with %05d format for
   five-digit zero-padded numbering
2. THE Transcoder SHALL configure hls_start_number_source to maintain segment
   counter state
3. THE Transcoder SHALL use hls_flags that support sequential segment creation
4. THE Transcoder SHALL NOT use flags that cause segment overwriting behavior
5. WHEN configuring segment duration, THE Transcoder SHALL set appropriate
   timing parameters that trigger new segment creation

### Requirement 4: S3 Upload Verification

**User Story:** As a streaming system, I want all created segments uploaded to
S3, so that the complete video stream is available for playback.

#### Acceptance Criteria

1. WHEN a new segment file is created, THE Transcoder SHALL upload it to the
   S3_Bucket
2. THE S3_Bucket SHALL contain all sequential segments (segment_00000.ts,
   segment_00001.ts, segment_00002.ts, etc.)
3. THE Transcoder SHALL NOT overwrite existing segments in the S3_Bucket
4. WHEN listing S3_Bucket contents, THE system SHALL show multiple segment files
   with sequential numbering

### Requirement 5: Playlist Accuracy

**User Story:** As a video player, I want the HLS playlist to reference all
available segments, so that I can play the complete video stream.

#### Acceptance Criteria

1. WHEN segments are created, THE Playlist SHALL reference all available segment
   files
2. THE Playlist SHALL list segments in sequential order
3. WHEN a new segment is created and uploaded, THE Playlist SHALL be updated to
   include the new segment
4. THE Playlist SHALL NOT reference only segment_00000.ts when multiple segments
   exist

### Requirement 6: Process Stability

**User Story:** As a streaming system, I want the FFmpeg process to run
continuously without restarts, so that segment counter state is maintained.

#### Acceptance Criteria

1. THE Transcoder SHALL run continuously while receiving video data
2. IF the Transcoder process restarts, THEN THE system SHALL detect and log the
   restart event
3. THE Transcoder SHALL NOT restart between segment writes under normal
   operation
4. WHEN video data is continuously received, THE Transcoder SHALL maintain a
   single long-running process

### Requirement 7: Diagnostic Logging

**User Story:** As a system administrator, I want detailed logging of segment
creation, so that I can verify sequential numbering is working correctly.

#### Acceptance Criteria

1. WHEN a segment is created, THE Transcoder SHALL log the segment filename
2. WHEN a segment is uploaded to S3, THE system SHALL log the upload event with
   the segment name
3. THE system SHALL log the current Segment_Counter value periodically
4. WHEN FFmpeg configuration is applied, THE system SHALL log the HLS parameters
   being used
5. IF segment overwriting is detected, THEN THE system SHALL log a warning with
   diagnostic information

### Requirement 8: Cache Control Configuration

**User Story:** As a streaming system, I want segments and playlists to have
appropriate cache headers, so that clients receive fresh content without
excessive caching.

#### Acceptance Criteria

1. WHEN a segment is uploaded to S3, THE system SHALL set Cache-Control header
   with max-age of 60 seconds
2. WHEN the playlist is uploaded to S3, THE system SHALL set Cache-Control
   header with max-age of 60 seconds
3. THE Cache-Control headers SHALL ensure clients check for updates at least
   every 60 seconds
4. THE system SHALL NOT allow segments or playlists to be cached beyond 60
   seconds
