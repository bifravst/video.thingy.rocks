# Implementation Plan: Offline Video Playback

## Overview

This implementation adds Video-On-Demand (VOD) playback capability to the
StreamPlayer component, allowing viewers to watch recorded content when streams
are offline. The implementation modifies the existing player to support three
distinct modes (live, VOD, offline) with appropriate HLS configurations and UI
states for each mode.

## Tasks

- [x] 1. Add playback mode state management
  - Add new state variables: `playbackMode` ('live' | 'vod' | 'offline') and
    `recordingAvailable` (boolean)
  - Initialize `playbackMode` to 'offline' by default
  - Initialize `recordingAvailable` to false
  - _Requirements: 1.1, 2.1_

- [ ] 2. Implement recording availability check function
  - [x] 2.1 Create `checkRecordingAvailability` async function
    - Accept `manifestUrl` parameter
    - Perform HEAD request to manifest URL using fetch API
    - Return true if response status is 200, false otherwise
    - Handle network errors and timeouts (5 second timeout)
    - Log check results for debugging
    - _Requirements: 1.2, 1.3, 1.4_
  - [ ]\* 2.2 Write property test for recording availability check
    - **Property 5: Recording Availability Determines Mode Selection**
    - **Validates: Requirements 1.3, 1.4, 2.1**

- [ ] 3. Create HLS configuration factory functions
  - [x] 3.1 Create `createLiveHLSConfig` function
    - Return HLS configuration object with live streaming parameters
    - Include: `liveSyncDurationCount: 3`, `liveMaxLatencyDurationCount: 10`,
      `liveDurationInfinity: true`
    - Include common parameters: `enableWorker`, `lowLatencyMode`, buffer
      settings, manifest loading config
    - _Requirements: 3.1, 3.2, 3.3_
  - [x] 3.2 Create `createVODHLSConfig` function
    - Return HLS configuration object without live parameters
    - Include: `startPosition: 0`, `lowLatencyMode: false`
    - Include common parameters: `enableWorker`, buffer settings, manifest
      loading config
    - Explicitly exclude: `liveSyncDurationCount`,
      `liveMaxLatencyDurationCount`, `liveDurationInfinity`
    - _Requirements: 2.3, 3.1, 3.2, 3.3, 3.4_
  - [ ]\* 3.3 Write property test for VOD configuration
    - **Property 2: VOD Configuration Excludes Live Parameters**
    - **Validates: Requirements 2.3, 3.1, 3.2, 3.3, 3.4**
  - [ ]\* 3.4 Write property test for live configuration
    - **Property 3: Live Configuration Includes Required Live Parameters**
    - **Validates: Requirements 3.1, 3.2, 3.3**

- [ ] 4. Modify status transition effect to support VOD mode
  - [x] 4.1 Update the status transition effect (currently lines 144-153)
    - When transitioning from 'active' to 'inactive', destroy existing HLS
      instance
    - After destroying HLS instance, call `checkRecordingAvailability` with
      `streamDetail.hlsManifestUrl`
    - If recording available, set `recordingAvailable` to true and
      `playbackMode` to 'vod'
    - If recording not available, set `recordingAvailable` to false and
      `playbackMode` to 'offline'
    - When transitioning from 'inactive' to 'active', set `playbackMode` to
      'live'
    - Log all mode transitions for debugging
    - _Requirements: 1.1, 5.1, 5.2, 6.1, 6.2_
  - [ ]\* 4.2 Write property test for status transitions
    - **Property 1: Status Transition Triggers Correct Mode Change**
    - **Validates: Requirements 1.1, 5.1, 5.2, 6.1, 6.2**
  - [ ]\* 4.3 Write property test for HLS cleanup
    - **Property 8: HLS Instance Cleanup on Mode Transition**
    - **Validates: Requirements 5.1, 6.1**

- [x] 5. Checkpoint - Ensure state management works correctly
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 6. Modify HLS initialization effect to support both live and VOD modes
  - [x] 6.1 Update HLS initialization effect (currently lines 156-350)
    - Change condition from `streamDetail.status !== 'active'` to check
      `playbackMode` instead
    - Return early if `playbackMode` is 'offline'
    - For `playbackMode === 'live'`, use `createLiveHLSConfig()` and
      `streamMode` logic (adaptive vs raw)
    - For `playbackMode === 'vod'`, use `createVODHLSConfig()` and always use
      adaptive manifest URL
    - Initialize HLS instance with appropriate config based on mode
    - Attach media and load source for both modes
    - Handle MANIFEST_PARSED event for both modes (autoplay)
    - Maintain existing error handling and level switching logic
    - _Requirements: 2.1, 2.2, 2.3, 2.5, 3.1, 3.2, 3.3, 3.4_
  - [x] 6.2 Write unit tests for HLS initialization
    - Test live mode initialization with live config
    - Test VOD mode initialization with VOD config
    - Test offline mode skips initialization
    - _Requirements: 2.1, 2.2, 2.3_

- [ ] 7. Update UI rendering to support three playback modes
  - [x] 7.1 Modify badge rendering logic
    - When `playbackMode === 'live'`, display "LIVE" badge with green background
    - When `playbackMode === 'vod'`, display "RECORDED" badge with blue/gray
      background
    - When `playbackMode === 'offline'`, display "OFFLINE" badge with gray
      background
    - Update badge styling to distinguish between states
    - _Requirements: 4.1, 4.2, 4.3, 6.3_
  - [x] 7.2 Modify video player vs image rendering logic
    - When `playbackMode === 'live'` or `playbackMode === 'vod'`, render video
      player with controls
    - When `playbackMode === 'offline'`, render static image with last frame
    - Ensure video element has `controls` attribute for both live and VOD
    - _Requirements: 4.4, 4.5, 7.1_
  - [x] 7.3 Update mode toggle visibility
    - Show mode toggle only when `playbackMode === 'live'`
    - Hide mode toggle when `playbackMode === 'vod'` or
      `playbackMode === 'offline'`
    - Reset `streamMode` to 'adaptive' when transitioning to VOD mode
    - _Requirements: 9.1, 9.2, 9.3, 9.4_
  - [ ]\* 7.4 Write property test for badge display
    - **Property 4: Badge Display Matches Playback Mode**
    - **Validates: Requirements 4.1, 4.2, 6.3**
  - [ ]\* 7.5 Write property test for UI element visibility
    - **Property 6: UI Element Visibility Matches Playback Mode**
    - **Validates: Requirements 2.1, 4.4, 4.5**
  - [ ]\* 7.6 Write property test for mode toggle visibility
    - **Property 7: Mode Toggle Visibility Based on Playback Mode**
    - **Validates: Requirements 9.1, 9.2**

- [ ] 8. Implement error handling and fallback logic
  - [x] 8.1 Add error handling for recording availability check
    - Wrap `checkRecordingAvailability` calls in try-catch
    - On error, log to console and set `recordingAvailable` to false
    - Fall back to offline mode on any error
    - _Requirements: 8.1, 8.4_
  - [x] 8.2 Add error handling for VOD playback initialization
    - In HLS error handler, detect VOD-specific errors
    - On fatal VOD errors, set `playbackMode` to 'offline'
    - Display error message to user
    - Ensure "OFFLINE" badge is shown after fallback
    - _Requirements: 8.1, 8.2, 8.5_
  - [x] 8.3 Handle missing segments in VOD playback
    - Allow HLS.js to attempt automatic recovery for missing segments
    - Log segment errors for debugging
    - Continue playback with available segments
    - _Requirements: 8.3, 8.4_
  - [ ]\* 8.4 Write property test for error fallback
    - **Property 10: Error Fallback to Offline Mode**
    - **Validates: Requirements 5.5, 8.1, 8.2, 8.5**

- [ ] 9. Optimize initial load for offline streams
  - [x] 9.1 Add immediate recording check on component mount
    - In the initial `fetchStreamDetail` effect, check if initial status is
      'inactive'
    - If inactive on mount, immediately call `checkRecordingAvailability`
    - Set `playbackMode` based on availability without waiting for polling
    - Ensure polling continues to detect when stream goes live
    - _Requirements: 10.1, 10.2, 10.5_
  - [x] 9.2 Write unit tests for initial load optimization
    - Test immediate check occurs for inactive streams on mount
    - Test polling continues after initial check
    - _Requirements: 10.1, 10.2, 10.5_

- [ ] 10. Add seek capability verification for VOD mode
  - [x] 10.1 Verify seek functionality in VOD mode
    - Ensure video element's `duration` property is finite (not Infinity) in VOD
      mode
    - Ensure video element's seek bar is enabled
    - Verify seeking updates `currentTime` property
    - _Requirements: 2.4, 7.2, 7.3, 7.4, 7.5_
  - [ ]\* 10.2 Write property test for seek capabilities
    - **Property 9: VOD Mode Enables Full Seek Capabilities**
    - **Validates: Requirements 2.4, 7.2, 7.3, 7.4**

- [x] 11. Final checkpoint - Integration testing
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- The implementation maintains backward compatibility with existing live
  streaming functionality
- HLS.js handles most of the heavy lifting for both live and VOD playback
- The main changes are in state management, configuration selection, and UI
  rendering
- Property tests use `fast-check` library with minimum 100 iterations per test
