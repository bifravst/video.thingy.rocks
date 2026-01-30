# Design Document: HLS Segment Sequencing Fix

## Overview

This design addresses the critical issue where FFmpeg continuously overwrites
segment_00000.ts instead of creating sequential segments. The root cause is a
combination of incorrect FFmpeg HLS configuration parameters and missing segment
counter initialization. The fix involves updating the FFmpeg command-line
parameters in the user-data script to properly configure HLS segment sequencing,
ensuring that segments are created with incrementing numbers (segment_00000.ts,
segment_00001.ts, segment_00002.ts, etc.) and uploaded to S3 without
overwriting.

The solution focuses on three key areas:

1. Correcting FFmpeg HLS flags to enable proper segment sequencing
2. Configuring segment counter initialization and persistence
3. Adding diagnostic logging to verify correct behavior

## Architecture

The streaming architecture consists of:

```
UDP Video Stream → FFmpeg Transcoder → HLS Segments → S3 Upload → CloudFront Distribution
                                    ↓
                                 Playlist (M3U8)
```

The fix targets the FFmpeg Transcoder configuration, specifically the HLS output
parameters that control segment naming and sequencing. The transcoder runs as a
long-lived process on EC2 instances managed by an Auto Scaling Group.

### Current Problem

The current FFmpeg command uses:

```bash
-hls_segment_filename ${profileDir}/segment_%05d.ts
-hls_playlist_type event
-hls_flags delete_segments+append_list
```

The issue is that the `delete_segments` flag, combined with missing segment
counter configuration, causes FFmpeg to reset the segment counter and overwrite
segment_00000.ts repeatedly.

### Solution Approach

Update the FFmpeg configuration to:

1. Remove the `delete_segments` flag (segments should persist for S3 upload)
2. Add `hls_start_number_source` configuration to maintain counter state
3. Configure proper segment initialization with `hls_init_time`
4. Add diagnostic logging to track segment creation

## Components and Interfaces

### 1. FFmpeg Transcoder Configuration

**Location:** `cdk/user-data.sh` (embedded in CDK stack)

**Modified FFmpeg Parameters:**

```bash
# Updated HLS output configuration
ffmpeg -i udp://0.0.0.0:${UDP_PORT}?overrun_nonfatal=1&fifo_size=50000000 \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -c:a aac -b:a 128k \
  -f hls \
  -hls_time 6 \
  -hls_list_size 0 \
  -hls_segment_filename "${profileDir}/segment_%05d.ts" \
  -hls_segment_type mpegts \
  -hls_flags append_list+program_date_time \
  -hls_start_number_source datetime \
  -hls_init_time 6 \
  -hls_segment_options "Cache-Control:max-age=60" \
  -method PUT \
  "${profileDir}/playlist.m3u8"
```

**Key Changes:**

- **Removed:** `delete_segments` flag (was causing segment deletion/overwriting)
- **Added:** `hls_start_number_source datetime` (maintains segment counter based
  on timestamp)
- **Added:** `hls_init_time 6` (ensures proper segment initialization timing)
- **Added:** `program_date_time` flag (adds timestamps to playlist for better
  tracking)
- **Added:** `hls_segment_options "Cache-Control:max-age=60"` (sets 60-second
  cache TTL for segments and playlist)
- **Kept:** `append_list` flag (appends new segments to playlist)

### 2. S3 Upload Integration

**Current Implementation:** FFmpeg's built-in S3 upload via `-method PUT`

The FFmpeg command already includes S3 upload functionality through the output
URL format:

```bash
s3://${BUCKET_NAME}/${STREAM_KEY}/${PROFILE}/
```

**Cache Control Configuration:**

To ensure clients don't cache stale segments or playlists, we must set
appropriate cache headers:

```bash
# Add cache control headers to FFmpeg S3 upload
-hls_segment_options "Cache-Control:max-age=60" \
```

**Cache Requirements:**

- **Segments (.ts files)**: `Cache-Control: max-age=60` (60 seconds)
- **Playlist (.m3u8 file)**: `Cache-Control: max-age=60` (60 seconds)

This ensures:

- Clients check for updates at least every 60 seconds
- Stale segments/playlists are not served beyond 60 seconds
- Balance between caching efficiency and freshness

**Implementation Note:** FFmpeg's `-hls_segment_options` flag passes options to
the S3 PUT request for both segments and the playlist.

**Verification:** The upload mechanism is working (segment_00000.ts is being
uploaded), so no changes needed to the upload logic itself. The fix ensures
multiple segments are created for upload rather than overwriting the same file.

### 3. Diagnostic Logging

**Location:** `cdk/user-data.sh` (streaming service script)

**Added Logging:**

```bash
# Log segment creation events
echo "[$(date)] Starting FFmpeg transcoder for profile: ${profile}" >> /var/log/streaming.log

# Monitor segment creation in background
(
  while true; do
    segment_count=$(ls -1 ${profileDir}/segment_*.ts 2>/dev/null | wc -l)
    latest_segment=$(ls -1t ${profileDir}/segment_*.ts 2>/dev/null | head -1)
    echo "[$(date)] Segment count: ${segment_count}, Latest: ${latest_segment}" >> /var/log/streaming.log
    sleep 30
  done
) &

# Log FFmpeg output for debugging
ffmpeg ... 2>&1 | tee -a /var/log/ffmpeg-${profile}.log
```

This logging allows administrators to:

- Track segment creation over time
- Verify sequential numbering is working
- Debug any issues with segment generation

## Data Models

### Segment File Naming

**Pattern:** `segment_%05d.ts`

**Format:**

- `%05d`: Five-digit zero-padded decimal number
- Range: 00000 to 99999
- Examples: segment_00000.ts, segment_00001.ts, segment_00002.ts

### Segment Counter State

**Type:** Integer counter maintained by FFmpeg

**Initialization:**

- With `hls_start_number_source datetime`: Counter derived from current
  timestamp
- Ensures unique starting point even if process restarts

**Persistence:**

- Counter maintained in FFmpeg process memory
- Increments with each segment write
- Does not reset during normal operation

### HLS Playlist Structure

**Format:** M3U8 (Extended M3U8 playlist)

**Content:**

```m3u8
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:EVENT
#EXT-X-PROGRAM-DATE-TIME:2024-01-15T10:30:00.000Z
#EXTINF:6.000000,
segment_00000.ts
#EXT-X-PROGRAM-DATE-TIME:2024-01-15T10:30:06.000Z
#EXTINF:6.000000,
segment_00001.ts
#EXT-X-PROGRAM-DATE-TIME:2024-01-15T10:30:12.000Z
#EXTINF:6.000000,
segment_00002.ts
```

**Key Fields:**

- `EXT-X-MEDIA-SEQUENCE`: Starting segment number
- `EXT-X-PROGRAM-DATE-TIME`: Timestamp for each segment (added by our fix)
- `EXTINF`: Segment duration
- Segment filenames: Listed sequentially

## Correctness Properties

A property is a characteristic or behavior that should hold true across all
valid executions of a system—essentially, a formal statement about what the
system should do. Properties serve as the bridge between human-readable
specifications and machine-verifiable correctness guarantees.

### Property 1: Sequential Segment Numbering

_For any_ sequence of created segment files, the segment numbers should form a
consecutive sequence starting from 00000, where each segment number is exactly
one more than the previous segment number.

**Validates: Requirements 1.1, 1.2, 1.4, 1.5, 2.2, 2.3, 2.4**

**Rationale:** This property consolidates all the requirements about counter
increment and sequential naming. If segment numbers are consecutive (0, 1, 2,
3...), then the counter is incrementing correctly, maintaining state, and not
resetting. This single property validates the core sequencing behavior.

### Property 2: Segment Filename Uniqueness

_For any_ set of segment files created during a transcoding session, all segment
filenames should be unique with no duplicates.

**Validates: Requirements 1.3, 4.3**

**Rationale:** This property ensures segments are never overwritten. If all
filenames are unique, then no file can overwrite another, whether on local disk
or in S3. This is a fundamental invariant that must hold.

### Property 3: S3 Upload Completeness

_For any_ set of N segment files created locally, the S3 bucket should contain
all N segments with matching filenames.

**Validates: Requirements 4.1, 4.2, 4.4**

**Rationale:** This property verifies that the upload mechanism works correctly
and that all created segments make it to S3. It's a completeness property
ensuring no segments are lost during upload.

### Property 4: Playlist Completeness

_For any_ set of segment files in S3, the HLS playlist should contain entries
for all segments in sequential order.

**Validates: Requirements 5.1, 5.2, 5.3, 5.4**

**Rationale:** This property ensures the playlist accurately reflects available
segments. It combines both completeness (all segments referenced) and ordering
(sequential order). If this holds, players can access all segments in the
correct sequence.

### Property 5: Segment Filename Pattern Conformance

_For any_ created segment file, the filename should match the pattern
`segment_\d{5}\.ts` where the digits form a five-digit zero-padded number.

**Validates: Requirements 3.1**

**Rationale:** This property validates that the filename format is correct. It
ensures the %05d pattern is working and producing properly formatted names.

### Property 6: Cache Control Header Presence

_For any_ segment or playlist uploaded to S3, the Cache-Control header should be
set to "max-age=60".

**Validates: Requirements 8.1, 8.2, 8.3, 8.4**

**Rationale:** This property ensures that all uploaded files have appropriate
cache headers to prevent stale content from being served. The 60-second max-age
ensures clients check for updates frequently enough for live streaming while
still benefiting from some caching.

## Error Handling

### FFmpeg Process Failures

**Scenario:** FFmpeg process crashes or terminates unexpectedly

**Handling:**

- The systemd service is configured with `Restart=always` to automatically
  restart FFmpeg
- On restart, FFmpeg will reinitialize with a new segment counter based on
  `hls_start_number_source datetime`
- This ensures segment numbers remain unique even across restarts
  (timestamp-based initialization)
- Logging captures restart events for monitoring

**Code Location:** `cdk/user-data.sh` systemd service definition

### S3 Upload Failures

**Scenario:** Network issues or S3 unavailability prevents segment upload

**Handling:**

- FFmpeg's S3 output includes built-in retry logic
- Segments remain on local disk until successfully uploaded
- If upload fails repeatedly, FFmpeg will log errors to
  `/var/log/ffmpeg-${profile}.log`
- CloudWatch alarms monitor FFmpeg error logs for upload failures

**Mitigation:** Ensure sufficient local disk space to buffer segments during
temporary S3 outages

### Segment Counter Overflow

**Scenario:** Segment counter exceeds 99999 (five-digit limit)

**Handling:**

- With 6-second segments, 99999 segments = ~7 days of continuous streaming
- For event-type playlists (our use case), streams typically don't exceed this
  duration
- If overflow occurs, FFmpeg will wrap to 00000, potentially overwriting old
  segments
- This is acceptable for event streams where old segments are no longer needed

**Mitigation:** For longer streams, consider using `hls_list_size` to limit
playlist length and clean up old segments

### Invalid Video Data

**Scenario:** Corrupted or invalid video data received via UDP

**Handling:**

- FFmpeg UDP input configured with `overrun_nonfatal=1` to handle buffer
  overruns gracefully
- Invalid frames are skipped rather than causing process termination
- Transcoding continues with next valid frame
- Errors logged to FFmpeg log file

## Testing Strategy

### Dual Testing Approach

This feature requires both unit tests and property-based tests to ensure
comprehensive coverage:

- **Unit tests**: Verify specific configuration examples, edge cases, and error
  conditions
- **Property tests**: Verify universal properties across all possible segment
  sequences

Both testing approaches are complementary and necessary for complete validation.

### Unit Testing

**Focus Areas:**

1. **FFmpeg Configuration Validation**
   - Verify FFmpeg command includes correct HLS parameters
   - Verify `delete_segments` flag is NOT present
   - Verify `hls_start_number_source datetime` is present
   - Verify `append_list` flag is present
   - Verify segment filename pattern is `segment_%05d.ts`

2. **Edge Cases**
   - First segment initialization (segment_00000.ts)
   - Segment counter at boundaries (00009 → 00010, 00099 → 00100)
   - Empty or zero-length segments

3. **Error Conditions**
   - FFmpeg process restart behavior
   - S3 upload failure handling
   - Invalid video data handling

4. **Logging Verification**
   - Verify logging code is present for segment creation
   - Verify logging code is present for S3 uploads
   - Verify periodic counter logging is configured

**Test Framework:** Bash unit tests using `bats` (Bash Automated Testing System)
or similar

**Example Unit Test:**

```bash
@test "FFmpeg command includes hls_start_number_source datetime" {
  source cdk/user-data.sh
  # Extract FFmpeg command from script
  ffmpeg_cmd=$(grep -A 20 "ffmpeg -i udp" cdk/user-data.sh)
  # Verify parameter is present
  echo "$ffmpeg_cmd" | grep -q "hls_start_number_source datetime"
}

@test "FFmpeg command does not include delete_segments flag" {
  source cdk/user-data.sh
  ffmpeg_cmd=$(grep -A 20 "ffmpeg -i udp" cdk/user-data.sh)
  # Verify delete_segments is NOT present
  ! echo "$ffmpeg_cmd" | grep -q "delete_segments"
}
```

### Property-Based Testing

**Testing Library:** Use `quickcheck` for bash or integration tests in Python
using `hypothesis`

**Configuration:**

- Minimum 100 iterations per property test
- Each test tagged with feature name and property number

**Property Test Implementation:**

Each correctness property will be implemented as a property-based test:

**Property 1 Test: Sequential Segment Numbering**

```python
# Feature: hls-segment-sequencing-fix, Property 1: Sequential Segment Numbering
@given(st.integers(min_value=1, max_value=100))
def test_sequential_segment_numbering(num_segments):
    """For any sequence of created segments, numbers should be consecutive."""
    # Simulate segment creation
    segments = create_test_segments(num_segments)
    segment_numbers = extract_segment_numbers(segments)

    # Verify consecutive sequence
    expected = list(range(num_segments))
    assert segment_numbers == expected
```

**Property 2 Test: Segment Filename Uniqueness**

```python
# Feature: hls-segment-sequencing-fix, Property 2: Segment Filename Uniqueness
@given(st.integers(min_value=1, max_value=1000))
def test_segment_filename_uniqueness(num_segments):
    """For any set of segments, all filenames should be unique."""
    segments = create_test_segments(num_segments)
    filenames = [seg.filename for seg in segments]

    # Verify no duplicates
    assert len(filenames) == len(set(filenames))
```

**Property 3 Test: S3 Upload Completeness**

```python
# Feature: hls-segment-sequencing-fix, Property 3: S3 Upload Completeness
@given(st.lists(st.text(min_size=1), min_size=1, max_size=50))
def test_s3_upload_completeness(segment_names):
    """For any set of N segments created, S3 should contain all N."""
    # Create segments locally
    local_segments = create_local_segments(segment_names)

    # Simulate upload
    upload_segments_to_s3(local_segments)

    # Verify S3 contains all
    s3_segments = list_s3_segments()
    assert set(local_segments) == set(s3_segments)
```

**Property 4 Test: Playlist Completeness**

```python
# Feature: hls-segment-sequencing-fix, Property 4: Playlist Completeness
@given(st.integers(min_value=1, max_value=100))
def test_playlist_completeness(num_segments):
    """For any set of segments, playlist should reference all in order."""
    segments = create_test_segments(num_segments)
    playlist = generate_playlist(segments)

    # Verify all segments in playlist
    playlist_segments = extract_playlist_segments(playlist)
    assert len(playlist_segments) == num_segments

    # Verify sequential order
    segment_numbers = [extract_number(seg) for seg in playlist_segments]
    assert segment_numbers == sorted(segment_numbers)
```

**Property 5 Test: Segment Filename Pattern Conformance**

```python
# Feature: hls-segment-sequencing-fix, Property 5: Filename Pattern Conformance
@given(st.integers(min_value=0, max_value=99999))
def test_segment_filename_pattern(segment_number):
    """For any segment number, filename should match pattern."""
    filename = generate_segment_filename(segment_number)

    # Verify pattern: segment_XXXXX.ts (5 digits, zero-padded)
    pattern = r'^segment_\d{5}\.ts$'
    assert re.match(pattern, filename)

    # Verify zero-padding
    expected = f"segment_{segment_number:05d}.ts"
    assert filename == expected
```

**Property 6 Test: Cache Control Header Presence**

```python
# Feature: hls-segment-sequencing-fix, Property 6: Cache Control Header Presence
@given(st.lists(st.text(min_size=1), min_size=1, max_size=50))
def test_cache_control_headers(file_list):
    """For any uploaded file, Cache-Control should be max-age=60."""
    # Upload files to S3
    upload_files_to_s3(file_list)

    # Check each file's metadata
    for filename in file_list:
        metadata = get_s3_object_metadata(filename)
        assert 'CacheControl' in metadata
        assert metadata['CacheControl'] == 'max-age=60'
```

### Integration Testing

**Scope:** End-to-end testing with actual FFmpeg process

**Test Scenarios:**

1. Start FFmpeg with test video stream
2. Let it run for 60 seconds (should create ~10 segments)
3. Verify segments are numbered sequentially (segment_00000.ts through
   segment_00009.ts)
4. Verify all segments uploaded to S3
5. Verify playlist references all segments
6. Verify no segment_00000.ts overwriting occurred

**Test Environment:** Use localstack for S3 simulation or test against actual
AWS S3 bucket

### Test Execution

**Unit Tests:**

```bash
# Run bash unit tests
bats tests/unit/ffmpeg-config.bats
```

**Property Tests:**

```bash
# Run property-based tests (100+ iterations each)
pytest tests/property/test_segment_sequencing.py -v --hypothesis-show-statistics
```

**Integration Tests:**

```bash
# Run integration tests
pytest tests/integration/test_hls_streaming.py -v
```

### Success Criteria

All tests must pass:

- ✅ All unit tests pass (FFmpeg configuration correct)
- ✅ All property tests pass with 100+ iterations each
- ✅ Integration test shows sequential segments created
- ✅ No segment overwriting detected in any test
- ✅ S3 contains all expected segments
- ✅ Playlist references all segments in order
