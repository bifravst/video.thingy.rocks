# Implementation Plan: NTN Video Streaming

## Overview

This implementation plan breaks down the NTN video streaming system into
discrete coding tasks. The system will be built incrementally, starting with
core infrastructure, then UDP ingestion, stream processing, storage, and finally
the web frontend. Each task builds on previous work to ensure continuous
integration.

## Tasks

- [x] 1. Initialize project structure
  - [x] 1.1 Create CDK TypeScript project
    - Run `mkdir ntn-video-streaming && cd ntn-video-streaming`
    - Use `fnm use 24` to switch to Node.js 24.
    - Update the `package.json`, set the `type` to `module`
    - Run `npx cdk init app --language typescript`
    - Update the `package.json` engines to required Node.js v24 and NPM v11.
    - Add test script to package.json:
      `"test": "node --no-warnings --experimental-transform-types --test \"!(node_modules)/**/*.spec.ts\""`
    - _Requirements: 7.1_

  - [x] 1.2 Create backend service directory structure
    - Create `backend/` directory for UDP listener and transcoding services
    - Create `backend/src/` for source code
    - Create `backend/tests/` for tests
    - Initialize Node.js project: `cd backend && npm init -y`
    - Update the `package.json`, set the `type` to `module`
    - Update the `package.json` engines to required Node.js v24 and NPM v11.
    - Install dependencies:
      `npm install @aws-sdk/client-s3 @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb @aws-sdk/client-cloudwatch`
    - Install dev dependencies: `npm install --save-dev typescript @types/node`
    - Add test script to package.json:
      `"test": "node --no-warnings --experimental-transform-types --test \"!(node_modules)/**/*.spec.ts\""`
    - _Requirements: 1.1, 2.1_

  - [x] 1.3 Create frontend directory structure
    - Create `frontend/` directory
    - Initialize React app:
      `npx create-react-app frontend --template typescript`
    - Update the `package.json`, set the `type` to `module`
    - Update the `package.json` engines to required Node.js v24 and NPM v11.
    - Update all dependencies to their latest version.
    - Install dependencies:
      `npm install @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb @aws-sdk/credential-providers hls.js @types/hls.js react-router-dom`
    - _Requirements: 4.1_

- [x] 2. Implement CDK infrastructure stack
  - [x] 2.1 Define VPC and networking
    - Create VPC with 2 availability zones
    - Configure public and private subnets
    - Set up NAT gateway for private subnet internet access
    - _Requirements: 7.1, 7.2_

  - [x] 2.2 Define S3 buckets
    - Create video storage bucket with folder structure (raw/, hls/, snapshots/)
    - Configure lifecycle policy to delete objects after 30 days
    - Set up CORS configuration for web access
    - Enable encryption at rest
    - _Requirements: 3.1, 3.4, 10.4_

  - [x] 2.3 Define DynamoDB table
    - Create StreamMetadata table with port as partition key
    - Configure on-demand billing mode
    - Define attributes: status, lastPacketTime, lastFramePath, hlsManifestPath,
      rawStreamPath
    - _Requirements: 6.2, 8.4_

  - [x] 2.4 Configure Cognito Identity Pool
    - Create identity pool allowing unauthenticated access
    - Create IAM role for unauthenticated users with read-only DynamoDB access
    - Grant read access to StreamMetadata table
    - _Requirements: 4.1, 10.2_

  - [x] 2.5 Define security groups
    - Create security group allowing UDP ingress on ports 5000-5009
    - Allow HTTPS egress for AWS service communication
    - _Requirements: 7.2, 10.3_

  - [ ]\* 2.6 Write CDK infrastructure tests
    - Test VPC configuration
    - Test security group rules
    - Test S3 bucket policies and lifecycle rules
    - Test DynamoDB table schema
    - _Requirements: 7.1_

- [x] 3. Implement UDP listener service
  - [x] 3.1 Create UDPListener class
    - Implement class with port range configuration (5000-5009)
    - Create UDP sockets using dgram module for each port
    - Implement packet reception handler
    - Add basic logging for received packets
    - _Requirements: 1.1, 1.2, 1.4_

  - [x] 3.2 Implement packet buffering
    - Create buffer with configurable size and flush interval
    - Buffer incoming packets in memory
    - Flush buffer to local disk as MPEG-TS files when size or time threshold
      reached
    - _Requirements: 1.2, 1.4_

  - [ ]\* 3.3 Write property test for packet buffering
    - **Property 1: Packet Buffering and Forwarding**
    - **Validates: Requirements 1.2, 1.4, 1.5**

  - [x] 3.4 Implement stream state tracking
    - Track active streams by port number in memory
    - Detect stream start (first packet on a port)
    - Detect stream stop (no packets for 1 minute using timeout)
    - Create StreamStateManager class
    - _Requirements: 1.5, 6.2, 6.5, 11.4_

  - [x] 3.5 Integrate DynamoDB for stream metadata
    - Create StreamMetadataService class
    - Implement updateStreamStatus() to write to DynamoDB
    - Implement updateLastPacketTime() to update timestamps
    - Call from stream state tracking on start/stop events
    - _Requirements: 6.2, 8.4_

  - [ ]\* 3.6 Write property test for stream auto-detection
    - **Property 11: Stream Auto-Detection**
    - **Validates: Requirements 6.2**

  - [ ]\* 3.7 Write property test for concurrent stream handling
    - **Property 2: Concurrent Stream Handling**
    - **Validates: Requirements 1.3, 6.1**

  - [ ]\* 3.8 Write property test for active stream count
    - **Property 14: Active Stream Count Accuracy**
    - **Validates: Requirements 6.5**

  - [x] 3.9 Implement error handling for UDP ingestion
    - Handle malformed packets (validate and discard)
    - Handle buffer overflow (drop oldest packets with FIFO)
    - Handle port binding failures (retry with exponential backoff)
    - Log warnings appropriately (not errors for expected conditions)
    - _Requirements: 9.1_

  - [ ]\* 3.10 Write property test for malformed packet handling
    - **Property 17: Malformed Packet Handling**
    - **Validates: Requirements 9.1**

  - [x] 3.11 Write unit tests for UDP listener
    - Test port binding on startup
    - Test packet reception and buffering
    - Test stream state transitions
    - Test error handling paths
    - _Requirements: 1.1, 1.2_

- [x] 4. Implement FFmpeg transcoding service
  - [x] 4.1 Create FFmpegTranscoder class
    - Define bitrate profiles (1080p, 720p, 480p, 360p) with resolutions and
      bitrates
    - Implement FFmpeg command builder for multiple outputs
    - Create method to spawn FFmpeg process with stdin pipe
    - Implement process lifecycle management (start, stop, restart)
    - _Requirements: 2.1, 2.2, 2.3_

  - [x] 4.2 Implement HLS transcoding pipeline
    - Configure FFmpeg for HLS output with 6-second segments
    - Generate master.m3u8 manifest
    - Generate variant playlists for each bitrate profile
    - Pipe buffered UDP data to FFmpeg stdin
    - _Requirements: 2.3, 2.4_

  - [ ]\* 4.3 Write property test for dual stream output
    - **Property 3: Dual Stream Output**
    - **Validates: Requirements 2.2, 2.3**

  - [ ]\* 4.4 Write property test for HLS format validity
    - **Property 4: HLS Format Validity**
    - **Validates: Requirements 2.4**

  - [x] 4.5 Implement S3 upload for video segments
    - Create S3UploadService class
    - Upload raw segments to s3://bucket/raw/{port}/
    - Upload HLS segments to s3://bucket/hls/{port}/{profile}/
    - Upload master.m3u8 and variant playlists
    - Add reception timestamp to S3 object metadata
    - _Requirements: 3.1, 3.5_

  - [ ]\* 4.6 Write property test for segment storage
    - **Property 6: Segment Storage and Accessibility**
    - **Validates: Requirements 3.1**

  - [ ]\* 4.7 Write property test for timestamp embedding
    - **Property 8: Reception Timestamp Embedding**
    - **Validates: Requirements 3.5**

  - [x] 4.8 Implement snapshot capture
    - Extract I-frames from video stream using FFmpeg
    - Save last frame as JPEG to s3://bucket/snapshots/{port}/last_frame.jpg
    - Update DynamoDB with snapshot S3 path
    - Overwrite previous snapshot on each capture
    - _Requirements: 6.3, 11.3_

  - [ ]\* 4.9 Write property test for last frame preservation
    - **Property 12: Last Frame Preservation**
    - **Validates: Requirements 6.3, 11.3**

  - [x] 4.10 Implement transcoding error handling
    - Handle FFmpeg process crashes (detect and restart)
    - Implement retry logic with exponential backoff (max 3 retries)
    - Handle invalid input formats (fallback to raw-only mode)
    - Handle S3 upload failures (buffer in memory up to 60 seconds)
    - Ensure errors in one stream don't affect others
    - _Requirements: 2.5, 9.2, 9.3_

  - [ ]\* 4.11 Write property test for error isolation
    - **Property 5: Error Isolation**
    - **Validates: Requirements 2.5, 6.4**

  - [x] 4.12 Write unit tests for FFmpeg transcoding
    - Test FFmpeg command generation
    - Test process lifecycle management
    - Test S3 upload integration
    - Test error handling and retries
    - _Requirements: 2.1, 2.5_

- [x] 5. Implement CloudWatch metrics and monitoring
  - [x] 5.1 Add metrics emission to UDP listener
    - Create MetricsService class using CloudWatch SDK
    - Emit packet loss rate per stream (calculate from sequence numbers)
    - Emit bitrate per stream (calculate from bytes received)
    - Emit active stream count
    - Emit metrics every 60 seconds
    - _Requirements: 8.1, 8.2_

  - [ ]\* 5.2 Write property test for metrics emission
    - **Property 15: Stream Metrics Emission**
    - **Validates: Requirements 8.1**

  - [x] 5.3 Implement structured logging
    - Create Logger utility with log levels (INFO, WARN, ERROR)
    - Log stream start/stop as INFO with port and timestamp
    - Log packet loss and retries as WARN with context
    - Log crashes and failures as ERROR with stack traces
    - Include port, timestamp, and stream identifier in all logs
    - _Requirements: 9.5_

  - [x] 5.4 Write unit tests for monitoring
    - Test metrics calculation and emission
    - Test logging output format
    - Test CloudWatch integration
    - _Requirements: 8.1, 8.2_

- [ ] 6. Complete EC2 deployment configuration
  - [ ] 6.1 Write EC2 user data script
    - Install Node.js v24 and NPM v11
    - Install FFmpeg with required codecs
    - Install AWS CLI
    - Clone/copy application code to instance
    - Configure environment variables (AWS region, S3 bucket, DynamoDB table)
    - Create systemd service for UDP listener
    - Start service on boot
    - _Requirements: 7.3_

  - [ ] 6.2 Add EC2 Auto Scaling Group to CDK stack
    - Define launch template with user data script
    - Configure instance type (c5.xlarge for compute-intensive transcoding)
    - Set up Auto Scaling Group (min 2, max 10 instances)
    - Configure target tracking based on network throughput
    - Add health checks
    - _Requirements: 7.1, 7.3, 7.5_

  - [ ] 6.3 Configure IAM role for EC2 instances
    - Grant S3 PutObject and GetObject permissions on video bucket
    - Grant DynamoDB PutItem, UpdateItem, GetItem on StreamMetadata table
    - Grant CloudWatch PutMetricData and PutLogEvents permissions
    - Follow least privilege principle
    - _Requirements: 7.5, 10.5_

  - [ ] 6.4 Add CloudFront distribution to CDK stack
    - Create distribution with S3 origin for video bucket
    - Configure HTTPS redirect (enforce HTTPS)
    - Set up Origin Access Identity for S3 access
    - Add cache behaviors for HLS segments (.m3u8, .ts files)
    - Configure appropriate TTLs for live streaming
    - _Requirements: 10.1_

  - [ ] 6.5 Configure CloudWatch alarms in CDK
    - Alarm for high packet loss (>5%)
    - Alarm for FFmpeg process failures
    - Alarm for S3 upload failures
    - Alarm for DynamoDB throttling
    - Alarm for EC2 CPU usage >80%
    - _Requirements: 8.3_

  - [ ] 6.6 Write CDK deployment tests
    - Test EC2 launch template configuration
    - Test Auto Scaling Group settings
    - Test IAM role permissions
    - Test CloudFront distribution setup
    - _Requirements: 7.1_

- [ ] 7. Implement web frontend for stream viewing
  - [ ] 7.1 Install required frontend dependencies
    - Install AWS SDK packages:
      `npm install @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb`
    - Install HLS player: `npm install hls.js`
    - Install types: `npm install --save-dev @types/hls.js`
    - _Requirements: 4.1_

  - [ ] 7.2 Create TypeScript interfaces for data models
    - Define StreamMetadata interface matching DynamoDB schema
    - Define StreamSummary interface for list view
    - Define StreamDetailResponse interface for player view
    - Create types.d.ts file in frontend/src/
    - _Requirements: 4.1_

  - [ ] 7.3 Create DynamoDB client service
    - Implement StreamDynamoDBClient class in frontend/src/utils/
    - Use existing AWS credentials from Auth context
    - Implement listStreams() method using DynamoDB scan
    - Implement getStreamDetail() method using DynamoDB getItem
    - Implement getStreamStatus() method with projection expression
    - Add error handling for DynamoDB operations
    - _Requirements: 4.1, 8.5_

  - [ ] 7.4 Implement StreamList component
    - Create StreamList component in frontend/src/page/
    - Display grid layout of all streams
    - Show stream port number, status badge (active/inactive), and thumbnail
    - Implement polling mechanism (query DynamoDB every 5 seconds)
    - Handle loading state with skeleton UI
    - Handle error state with retry button
    - Add click handler to navigate to stream player
    - Replace placeholder in Cameras.tsx with StreamList
    - _Requirements: 4.1, 4.5_

  - [ ]\* 7.5 Write property test for stream list display
    - **Property 9: Stream List Display**
    - **Validates: Requirements 4.1, 4.5**

  - [ ] 7.6 Implement StreamPlayer component
    - Create StreamPlayer component in frontend/src/page/
    - Create video player container with hls.js integration
    - Add toggle switch for raw vs adaptive bitrate mode
    - Display stream metadata panel (port, status, timestamp, current bitrate)
    - Implement video element with HLS source
    - Handle stream URL construction from CloudFront domain
    - Add route for /stream/:port in App.tsx
    - _Requirements: 4.2, 4.3, 4.4, 4.6_

  - [ ] 7.7 Implement adaptive bitrate functionality
    - Configure hls.js for automatic quality switching
    - Add manual quality selection dropdown
    - Display current active bitrate and resolution
    - Handle bandwidth estimation and quality transitions
    - Maintain playback continuity during switches
    - _Requirements: 5.1, 5.2, 5.3, 5.5_

  - [ ]\* 7.8 Write property test for adaptive bitrate switching
    - **Property 10: Adaptive Bitrate Quality Switching**
    - **Validates: Requirements 4.4, 5.2, 5.3**

  - [ ] 7.9 Implement offline stream handling
    - Detect inactive stream status from DynamoDB
    - Display last frame snapshot image when stream is offline
    - Show "Offline" indicator badge
    - Display reception timestamp of last frame
    - Implement automatic transition to live video when stream resumes
    - Poll stream status every 5 seconds to detect resumption
    - _Requirements: 11.5, 11.6, 11.7_

  - [ ]\* 7.10 Write property test for stream resumption
    - **Property 13: Stream Resumption**
    - **Validates: Requirements 11.2, 11.7**

  - [ ] 7.11 Implement frontend error handling
    - Handle stream not found errors (show user-friendly message)
    - Handle playback errors (retry, fallback to lower quality)
    - Handle network interruptions (auto-reconnect with exponential backoff)
    - Handle DynamoDB query failures (retry with backoff)
    - Display error messages with retry actions
    - _Requirements: 9.4_

  - [ ] 7.12 Write unit tests for frontend components
    - Test StreamList rendering and polling
    - Test StreamPlayer initialization
    - Test adaptive bitrate controls
    - Test offline/online transitions
    - Test error handling
    - _Requirements: 4.1, 4.2_

- [ ] 8. Deploy frontend and configure CDK outputs
  - [ ] 8.1 Create frontend S3 bucket in CDK
    - Add S3 bucket for frontend static hosting to StreamingStack
    - Configure bucket for static website hosting
    - Enable encryption at rest
    - _Requirements: 7.7, 10.4_

  - [ ] 8.2 Create CloudFront distribution for frontend
    - Create CloudFront distribution for frontend with S3 origin
    - Set up Origin Access Control for frontend bucket
    - Configure HTTPS enforcement
    - Add cache behaviors for static assets
    - _Requirements: 7.7, 10.1_

  - [ ] 8.3 Add S3 bucket deployment to CDK
    - Add BucketDeployment construct to upload frontend build files
    - Configure deployment to invalidate CloudFront cache
    - Set up build script to run before deployment
    - _Requirements: 7.7_

  - [ ] 8.4 Create build configuration for frontend
    - Configure environment variables for production build
    - Create script to inject CDK outputs (Identity Pool ID, DynamoDB table,
      CloudFront URL)
    - Optimize bundle size (code splitting, tree shaking)
    - Add build command to frontend package.json
    - _Requirements: 4.1_

  - [ ] 8.5 Add CDK outputs for frontend configuration
    - Output Cognito Identity Pool ID
    - Output DynamoDB table name
    - Output CloudFront video distribution URL
    - Output CloudFront frontend distribution URL
    - Create script to inject outputs into frontend build
    - _Requirements: 4.1, 7.7_

  - [ ] 8.6 Configure CORS and security policies
    - Update CORS headers on video S3 bucket for frontend access
    - Configure Content Security Policy headers in CloudFront
    - Verify S3 encryption at rest for both buckets
    - Verify TLS for all AWS service communication
    - _Requirements: 10.2, 10.3, 10.4, 10.5_

- [ ] 9. Integration and end-to-end testing
  - [ ] 9.1 Set up test environment
    - Deploy CDK stack to test AWS account
    - Configure test UDP packet generator
    - Set up test data and fixtures
    - _Requirements: 1.1, 7.1_

  - [ ] 9.2 Test UDP to playback flow
    - Send test UDP packets to port 5000
    - Verify HLS segments appear in S3 within expected time
    - Verify DynamoDB metadata is created and updated
    - Verify stream appears in web frontend list
    - Verify video playback works in browser
    - Measure end-to-end latency
    - _Requirements: 1.1, 2.1, 3.1, 4.1_

  - [ ]\* 9.3 Write property test for concurrent viewer support
    - **Property 7: Concurrent Viewer Support**
    - **Validates: Requirements 3.3**

  - [ ] 9.4 Test offline/online transitions
    - Send UDP packets to establish active stream
    - Stop sending packets and wait for timeout (1 minute)
    - Verify stream marked as inactive in DynamoDB
    - Verify last frame snapshot is captured and stored in S3
    - Verify frontend displays snapshot with "Offline" indicator
    - Resume sending UDP packets
    - Verify stream marked as active in DynamoDB
    - Verify frontend automatically transitions to live video
    - _Requirements: 11.1, 11.2, 11.4, 11.5, 11.7_

  - [ ] 9.5 Test error scenarios
    - Send malformed UDP packets and verify graceful handling
    - Simulate S3 failures (using IAM policy changes) and verify buffering
    - Kill FFmpeg process and verify automatic restart
    - Verify other streams continue working during failures
    - Check error logs for appropriate severity levels
    - _Requirements: 9.1, 9.2, 9.3_

  - [ ] 9.6 Test security configurations
    - Verify HTTPS enforcement on frontend (HTTP redirects to HTTPS)
    - Verify CORS policies allow only authorized origins
    - Verify security group rules (only UDP 5000-5009 allowed)
    - Verify S3 encryption at rest is enabled
    - Verify IAM roles follow least privilege
    - Test unauthenticated access to DynamoDB (should only allow read)
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_

  - [ ] 9.7 Load testing
    - Test 10 concurrent streams for 15 minutes
    - Measure packet loss rate per stream
    - Measure end-to-end latency
    - Monitor CPU and memory usage on EC2 instances
    - Verify Auto Scaling Group scales appropriately
    - Verify no stream interference
    - _Requirements: 6.1, 6.4_

  - [ ] 9.8 Create deployment documentation
    - Document CDK deployment steps
    - Document required AWS permissions
    - Document environment variables and configuration
    - Document testing procedures
    - Document troubleshooting common issues
    - _Requirements: 7.1_

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- Unit tests validate specific examples and edge cases. Unit tests are placed in
  files with the extension `.spec.ts`
- The implementation follows a bottom-up approach: infrastructure → backend →
  frontend
- Each component is tested before moving to the next
