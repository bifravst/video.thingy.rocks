# Implementation Plan: HLS Segment Sequencing Fix

## Overview

This implementation fixes the HLS segment sequencing issue by updating the
FFmpeg configuration in the user-data script. The fix removes the problematic
`delete_segments` flag, adds proper segment counter initialization with
`hls_start_number_source datetime`, and configures cache control headers for S3
uploads. All changes are made to infrastructure code following the
Infrastructure as Code principle - no manual fixes on running instances.

## Tasks

- [x] 1. Update FFmpeg HLS configuration in user-data script
  - Locate the FFmpeg command in `cdk/user-data.sh`
  - Remove `delete_segments` from `hls_flags` parameter
  - Add `hls_start_number_source datetime` parameter
  - Add `hls_init_time 6` parameter
  - Add `program_date_time` to `hls_flags` parameter
  - Add `hls_segment_options "Cache-Control:max-age=60"` parameter
  - Verify the complete FFmpeg command matches the design specification
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3,
    3.5, 8.1, 8.2, 8.3, 8.4_

- [x] 1.1 Write unit test for FFmpeg configuration
  - Verify `delete_segments` flag is NOT present in FFmpeg command
  - Verify `hls_start_number_source datetime` is present
  - Verify `append_list` flag is present
  - Verify `hls_segment_options "Cache-Control:max-age=60"` is present
  - Verify segment filename pattern is `segment_%05d.ts`
  - _Requirements: 3.1, 3.2, 3.3, 8.1, 8.2_

- [ ] 2. Add diagnostic logging for segment creation
  - [x] 2.1 Add logging for FFmpeg process startup
    - Log FFmpeg command being executed with all parameters
    - Log profile name and output directory
    - Write logs to `/var/log/streaming.log`
    - _Requirements: 7.4_
  - [x] 2.2 Add background monitoring script for segment tracking
    - Create background process that runs every 30 seconds
    - Count segment files in profile directory
    - Log segment count and latest segment filename
    - Write logs to `/var/log/streaming.log`
    - _Requirements: 7.1, 7.2, 7.3_
  - [x] 2.3 Redirect FFmpeg output to dedicated log file
    - Pipe FFmpeg stdout and stderr to `/var/log/ffmpeg-${profile}.log`
    - Use `tee` to maintain console output while logging
    - Ensure log rotation is configured for FFmpeg logs
    - _Requirements: 7.5_

- [x] 2.4 Write unit tests for logging configuration
  - Verify logging statements are present in user-data script
  - Verify log file paths are correct
  - Verify background monitoring script is started
  - _Requirements: 7.1, 7.2, 7.3, 7.4_

- [x] 3. Checkpoint - Verify configuration changes
  - Review all changes to `cdk/user-data.sh`
  - Ensure FFmpeg command is correctly formatted (no syntax errors)
  - Ensure logging is properly configured
  - Ensure all changes follow Infrastructure as Code principles (no manual
    fixes)
  - Ask the user if questions arise

- [ ]\* 4. Write property-based tests for segment sequencing
  - [ ]\* 4.1 Property test for sequential segment numbering
    - **Property 1: Sequential Segment Numbering**
    - **Validates: Requirements 1.1, 1.2, 1.4, 1.5, 2.2, 2.3, 2.4**
    - Generate random number of segments (1-100)
    - Verify segment numbers form consecutive sequence starting from 0
    - Run with minimum 100 iterations
  - [ ]\* 4.2 Property test for segment filename uniqueness
    - **Property 2: Segment Filename Uniqueness**
    - **Validates: Requirements 1.3, 4.3**
    - Generate random number of segments (1-1000)
    - Verify all filenames are unique (no duplicates)
    - Run with minimum 100 iterations
  - [ ]\* 4.3 Property test for S3 upload completeness
    - **Property 3: S3 Upload Completeness**
    - **Validates: Requirements 4.1, 4.2, 4.4**
    - Generate random list of segment names
    - Simulate upload to S3
    - Verify S3 contains all uploaded segments
    - Run with minimum 100 iterations
  - [ ]\* 4.4 Property test for playlist completeness
    - **Property 4: Playlist Completeness**
    - **Validates: Requirements 5.1, 5.2, 5.3, 5.4**
    - Generate random number of segments (1-100)
    - Generate playlist from segments
    - Verify playlist references all segments in sequential order
    - Run with minimum 100 iterations
  - [ ]\* 4.5 Property test for segment filename pattern
    - **Property 5: Segment Filename Pattern Conformance**
    - **Validates: Requirements 3.1**
    - Generate random segment numbers (0-99999)
    - Verify filenames match pattern `segment_\d{5}\.ts`
    - Verify zero-padding is correct
    - Run with minimum 100 iterations
  - [ ]\* 4.6 Property test for cache control headers
    - **Property 6: Cache Control Header Presence**
    - **Validates: Requirements 8.1, 8.2, 8.3, 8.4**
    - Generate random list of files to upload
    - Simulate S3 upload with metadata
    - Verify Cache-Control header is set to "max-age=60" for all files
    - Run with minimum 100 iterations

- [x] 5. Update CDK stack if needed
  - Check if CDK stack needs updates for user-data changes
  - If user-data is embedded in CDK TypeScript code, update it there
  - If user-data is in separate shell script, ensure CDK references it correctly
  - Verify launch template will use updated user-data
  - _Requirements: All (infrastructure deployment)_

- [ ]\* 5.1 Write unit tests for CDK stack configuration
  - Verify launch template includes updated user-data
  - Verify Auto Scaling Group configuration is correct
  - _Requirements: All (infrastructure deployment)_

- [x] 6. Final checkpoint - Deployment readiness
  - Ensure all code changes are complete
  - Ensure all non-optional tests pass
  - Review deployment plan with user
  - Document deployment steps: `npm run cdk:deploy` then terminate instances
  - Ask the user if questions arise

## Deployment Instructions

After all tasks are complete, deploy the fix using Infrastructure as Code
principles:

1. **Deploy updated infrastructure:**

   ```bash
   npm run cdk:deploy
   ```

2. **Replace running instances to apply the fix:**

   ```bash
   # Get instance IDs from Auto Scaling Group
   INSTANCES=$(aws ec2 describe-instances \
     --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
               "Name=instance-state-name,Values=running" \
     --query "Reservations[*].Instances[*].InstanceId" \
     --output text)

   # Terminate instances (ASG will create new ones with the fix)
   aws ec2 terminate-instances --instance-ids $INSTANCES
   ```

3. **Wait for new instances to launch (5-10 minutes)**

4. **Verify the fix:**

   ```bash
   # SSH to new instance
   ./scripts/ssh-to-instance.sh

   # Check segment files are being created sequentially
   ls -la /tmp/hls/*/segment_*.ts

   # Check logs for segment creation
   tail -f /var/log/streaming.log

   # Verify S3 has multiple segments
   aws s3 ls s3://YOUR_BUCKET/YOUR_STREAM_KEY/YOUR_PROFILE/ | grep segment_
   ```

## Notes

- All tasks follow Infrastructure as Code principles - no manual fixes on
  running instances
- Tasks marked with `*` are optional and can be skipped for faster deployment
- Property tests validate universal correctness properties with 100+ iterations
  each
- Unit tests validate specific configuration examples and edge cases
- The fix is persistent and will apply to all future instances automatically
- Cache control headers ensure clients don't cache stale content beyond 60
  seconds
