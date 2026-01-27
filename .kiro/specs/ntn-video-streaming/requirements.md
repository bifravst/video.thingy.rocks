# Requirements Document

## Introduction

This document specifies the requirements for a video streaming application that
receives UDP video streams from multiple Non-Terrestrial Network Connected
Cameras (NTNCam devices) and delivers them to web viewers with support for both
raw and adaptive bitrate streaming. The system will be deployed on AWS using
serverless components and CDK infrastructure written in TypeScript.

## Glossary

- **NTNCam**: Non-Terrestrial Network Connected Camera - a device that sends
  video data via UDP with intermittent connectivity
- **Video_Ingestion_Service**: The AWS service that receives UDP video streams
  from NTNCam devices
- **Stream_Processing_Service**: The service that processes and transcodes video
  streams for delivery
- **Web_Frontend**: The browser-based application that allows users to view
  video streams
- **Adaptive_Bitrate_Streaming**: A technique that dynamically adjusts video
  quality based on viewer's available bandwidth
- **Raw_Stream**: The original video stream at its native bitrate without
  transcoding
- **CDK_Infrastructure**: AWS Cloud Development Kit infrastructure code written
  in TypeScript
- **Stream_Identifier**: The UDP port number (5000-5009) used by an NTNCam
  device to send its video stream
- **Last_Frame**: The most recent video frame received from an NTNCam device
  before it went offline
- **Reception_Timestamp**: The timestamp indicating when video data was received
  by the system

## Requirements

### Requirement 1: UDP Video Ingestion

**User Story:** As a system operator, I want the system to receive UDP video
streams from multiple NTNCam devices, so that video data can be ingested for
processing and delivery.

#### Acceptance Criteria

1. THE Video_Ingestion_Service SHALL listen for incoming UDP video packets on
   ports 5000 through 5009 inclusive
2. WHEN a UDP video packet is received on any port in the range, THE
   Video_Ingestion_Service SHALL accept and buffer the packet
3. THE Video_Ingestion_Service SHALL handle concurrent UDP streams from multiple
   NTNCam devices simultaneously
4. THE Video_Ingestion_Service SHALL use the destination UDP port number as the
   Stream_Identifier for each stream
5. THE Video_Ingestion_Service SHALL forward buffered video data to the
   Stream_Processing_Service with the associated Stream_Identifier

### Requirement 2: Stream Processing and Transcoding

**User Story:** As a system architect, I want video streams to be processed and
transcoded into multiple bitrates, so that viewers can access both raw and
adaptive bitrate streams.

#### Acceptance Criteria

1. WHEN video data is received from the Video_Ingestion_Service, THE
   Stream_Processing_Service SHALL process the stream in real-time
2. THE Stream_Processing_Service SHALL maintain the original raw stream without
   transcoding
3. THE Stream_Processing_Service SHALL transcode each stream into multiple
   bitrate variants for adaptive streaming
4. THE Stream_Processing_Service SHALL package transcoded streams in a format
   compatible with adaptive bitrate protocols
5. WHEN transcoding fails for a stream, THE Stream_Processing_Service SHALL log
   the error and continue processing other streams

### Requirement 3: Stream Storage and Delivery

**User Story:** As a viewer, I want processed video streams to be available for
playback, so that I can watch live video from NTNCam devices.

#### Acceptance Criteria

1. THE Stream_Processing_Service SHALL store processed video segments in a
   storage service accessible to viewers
2. WHEN a viewer requests a stream, THE System SHALL deliver the requested
   stream with latency under 10 seconds
3. THE System SHALL support concurrent delivery of streams to multiple viewers
4. THE System SHALL automatically delete video segments and still images older
   than 30 days
5. THE Stream_Processing_Service SHALL embed Reception_Timestamp metadata in all
   transcoded video segments

### Requirement 4: Web Frontend for Stream Viewing

**User Story:** As a viewer, I want to access a web interface to view available
streams, so that I can select and watch video from NTNCam devices.

#### Acceptance Criteria

1. THE Web_Frontend SHALL display a list of all known streams with their
   Stream_Identifiers and online status
2. WHEN a viewer selects a stream, THE Web_Frontend SHALL present options for
   raw stream or adaptive bitrate streaming
3. WHEN a viewer chooses raw stream, THE Web_Frontend SHALL play the original
   bitrate stream
4. WHEN a viewer chooses adaptive bitrate, THE Web_Frontend SHALL dynamically
   adjust quality based on available bandwidth
5. THE Web_Frontend SHALL display stream metadata including Stream_Identifier,
   current bitrate, and Reception_Timestamp
6. WHEN an NTNCam device is offline, THE Web_Frontend SHALL display the
   Last_Frame as a still image with its Reception_Timestamp

### Requirement 5: Adaptive Bitrate Streaming

**User Story:** As a viewer with variable bandwidth, I want the video player to
automatically adjust quality, so that I experience smooth playback without
buffering.

#### Acceptance Criteria

1. WHEN adaptive bitrate mode is selected, THE Web_Frontend SHALL monitor the
   viewer's available bandwidth
2. WHEN bandwidth decreases, THE Web_Frontend SHALL switch to a lower bitrate
   variant within 3 seconds
3. WHEN bandwidth increases, THE Web_Frontend SHALL switch to a higher bitrate
   variant within 5 seconds
4. THE Web_Frontend SHALL maintain playback continuity during bitrate
   transitions
5. THE Web_Frontend SHALL display the current active bitrate to the viewer

### Requirement 6: Multiple Concurrent Streams

**User Story:** As a system operator, I want the system to handle multiple
NTNCam devices streaming simultaneously, so that all devices can be monitored
concurrently.

#### Acceptance Criteria

1. THE System SHALL support at least 10 concurrent NTNCam device streams across
   the port range 5000-5009
2. WHEN a new NTNCam device begins streaming on any port in the range, THE
   System SHALL automatically detect and process the new stream
3. WHEN an NTNCam device stops streaming, THE System SHALL preserve the
   Last_Frame for display
4. THE System SHALL isolate stream processing so that failure in one stream does
   not affect others
5. THE System SHALL track and report the number of active streams by port number

### Requirement 11: Intermittent Connectivity Handling

**User Story:** As a system operator, I want the system to handle intermittent
NTNCam connectivity gracefully, so that temporary network disruptions are
treated as normal operation rather than errors.

#### Acceptance Criteria

1. WHEN an NTNCam device stops sending data, THE System SHALL treat this as
   expected behavior and not log it as an error
2. WHEN an NTNCam device resumes streaming after being offline, THE System SHALL
   automatically resume processing without manual intervention
3. THE System SHALL preserve the Last_Frame from each NTNCam device for up to 30
   days
4. WHEN an NTNCam device has been offline for more than 1 hour, THE System SHALL
   mark the stream status as inactive but retain the Last_Frame
5. THE Web_Frontend SHALL display the Last_Frame as a still image when an NTNCam
   device is offline
6. THE Web_Frontend SHALL display the Reception_Timestamp of the Last_Frame when
   showing a still image
7. WHEN an NTNCam device resumes streaming, THE Web_Frontend SHALL automatically
   transition from still image to live video

### Requirement 7: CDK Infrastructure Deployment

**User Story:** As a DevOps engineer, I want infrastructure defined as
TypeScript CDK code using serverless AWS components, so that the system can be
deployed and managed programmatically with minimal operational overhead.

#### Acceptance Criteria

1. THE CDK_Infrastructure SHALL define all AWS resources required for the video
   streaming system using serverless services
2. THE CDK_Infrastructure SHALL configure networking components including VPC
   and security groups to allow UDP traffic on ports 5000-5009
3. THE CDK_Infrastructure SHALL provision serverless compute resources for video
   processing
4. THE CDK_Infrastructure SHALL configure S3 for video segment storage
5. THE CDK_Infrastructure SHALL define IAM roles and policies following least
   privilege principles
6. WHEN the CDK stack is deployed, THE System SHALL create all resources in a
   single AWS region
7. THE CDK_Infrastructure SHALL output endpoint URLs for the
   Video_Ingestion_Service and Web_Frontend
8. THE CDK_Infrastructure SHALL utilize AWS Lambda, AWS DynamoDB, S3, and
   MediaLive or MediaConvert for serverless operation

### Requirement 8: Stream Monitoring and Health

**User Story:** As a system operator, I want to monitor stream health and system
status, so that I can identify and resolve issues quickly.

#### Acceptance Criteria

1. THE System SHALL emit metrics for each active stream including packet loss
   rate and bitrate
2. THE System SHALL emit metrics for system health including CPU utilization and
   memory usage
3. WHEN packet loss exceeds 5 percent for a stream, THE System SHALL log a
   warning
4. THE System SHALL provide an API endpoint that returns the status of all
   active streams
5. THE Web_Frontend SHALL display connection status for the selected stream

### Requirement 9: Error Handling and Resilience

**User Story:** As a system architect, I want the system to handle errors
gracefully, so that temporary failures do not cause complete system outages.

#### Acceptance Criteria

1. WHEN the Video_Ingestion_Service encounters a malformed packet, THE System
   SHALL discard the packet and continue processing
2. WHEN the Stream_Processing_Service fails to transcode a stream, THE System
   SHALL retry transcoding up to 3 times
3. WHEN storage operations fail, THE System SHALL buffer video data in memory
   for up to 60 seconds
4. WHEN a viewer's connection is interrupted, THE Web_Frontend SHALL attempt to
   reconnect automatically
5. THE System SHALL log all errors with sufficient context for debugging

### Requirement 10: Security and Access Control

**User Story:** As a security engineer, I want the system to implement basic
security controls, so that unauthorized access is prevented.

#### Acceptance Criteria

1. THE Web_Frontend SHALL be served over HTTPS
2. THE System SHALL implement CORS policies to restrict Web_Frontend access to
   authorized domains
3. THE CDK_Infrastructure SHALL configure security groups to allow UDP traffic
   only on the ingestion port
4. THE System SHALL encrypt video segments at rest in storage
5. THE System SHALL encrypt data in transit between AWS services
