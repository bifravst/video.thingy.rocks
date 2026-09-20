#!/bin/bash
set -e

# EC2 User Data Script for Video Streaming Service
# This script installs dependencies and configures the UDP listener service

# Update system
yum update -y

# Install CloudWatch Logs agent
yum install -y amazon-cloudwatch-agent

# Install Node.js v24 using NodeSource repository
curl -fsSL https://rpm.nodesource.com/setup_24.x | bash -
yum install -y nodejs

# Verify Node.js and NPM versions
node --version
npm --version

# Install AWS CLI v2 (unzip first; use -f so curl fails on HTTP errors; use /tmp for predictable path)
yum install -y unzip
yum remove awscli -y

curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o /tmp/awscliv2.zip
unzip -q -o /tmp/awscliv2.zip -d /tmp
/tmp/aws/install
rm -rf /tmp/aws /tmp/awscliv2.zip

# Verify AWS CLI installation
aws --version

# Install GStreamer and build deps before the app starts so gst-launch-1.0 is on PATH
# autoconf/automake needed by SDK when BUILD_DEPENDENCIES=ON (log4cplus); libcurl-devel for SDK
yum install -y cmake gcc-c++ make git pkg-config m4 autoconf automake libcurl-devel
yum install -y gstreamer1 gstreamer1-devel gstreamer1-plugins-base gstreamer1-plugins-base-devel gstreamer1-plugins-good gstreamer1-plugins-bad-free

# Create application directory and deploy code so the service always gets installed and started
mkdir -p /opt/video-streaming

# Create output directory for video streams
mkdir -p /var/video-streams
chmod 755 /var/video-streams

# Download application code from S3 (placeholders replaced by CDK)
aws s3 sync s3://__CODE_BUCKET__/backend/ /opt/video-streaming/ --region __AWS_REGION__

# Install dependencies from deployed package.json
cd /opt/video-streaming
npm install --production

# Create log directory for application
mkdir -p /var/log/video-streaming
chmod 755 /var/log/video-streaming

# Kinesis Video SDK (kvssink) requires a log4cplus config file; default path "../kvs_log_configuration"
# fails when CWD is /opt/video-streaming. Create it so we can pass an absolute path via log-config.
# Use standard log4cplus appenders (STDOUT); KvsConsoleAppender is not always registered by the plugin.
cat > /opt/video-streaming/kvs_log_configuration << 'KVSCONF'
log4cplus.rootLogger=INFO, STDOUT
log4cplus.appender.STDOUT=log4cplus::ConsoleAppender
log4cplus.appender.STDOUT.layout=log4cplus::PatternLayout
log4cplus.appender.STDOUT.layout.ConversionPattern=%d{%H:%M:%S} [%t] - %m%n
KVSCONF
chmod 644 /opt/video-streaming/kvs_log_configuration

# Create systemd service file (service must exist even if kvssink build fails later)
cat > /etc/systemd/system/video-streaming.service << 'EOF'
[Unit]
Description=Video Streaming UDP Listener Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/video-streaming
Environment="NODE_ENV=production"
Environment="AWS_REGION=__AWS_REGION__"
Environment="TABLE_NAME=__TABLE_NAME__"
Environment="OUTPUT_DIR=/var/video-streams"
Environment="TRANSCODING_OUTPUT_DIR=/tmp/video-streams/transcoding"
Environment="KINESIS_INGESTION_ENABLED=true"
Environment="SRTP_INGESTION_ENABLED=true"
Environment="SRTP_KEY_PARAMETER_PREFIX=__SRTP_KEY_PARAMETER_PREFIX__"
Environment="SRTP_PORT_RANGE_START=__SRTP_PORT_RANGE_START__"
Environment="SRTP_PORT_RANGE_END=__SRTP_PORT_RANGE_END__"
Environment="GST_PLUGIN_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build"
Environment="LD_LIBRARY_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build"
ExecStart=/usr/bin/node --max-old-space-size=16384 --experimental-transform-types --no-warnings src/index.ts
Restart=always
RestartSec=10
StandardOutput=append:/var/log/video-streaming/application.log
StandardError=append:/var/log/video-streaming/error.log
SyslogIdentifier=video-streaming

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd to recognize the new service
systemctl daemon-reload

# Enable service to start on boot
systemctl enable video-streaming.service

# Build the kvssink plugin and verify the GStreamer elements the ingest pipelines need
# BEFORE starting the application service (which opens the TCP:9999 health port).
# FATAL on purpose: the NLB target groups' health checks (and, on a fleet cutover, the
# FleetCutoverReadiness gate) treat "health port open" as "this instance can ingest".
# Starting the service before these prerequisites would make the instance look healthy
# while no Kinesis pipeline can run - and a cutover could move traffic to a fleet that
# drops ingestion. With `set -e`, a failed build or missing element aborts user data:
# the service never starts, the health port stays closed, the target group stays
# unhealthy, and the ASG replaces the instance / the readiness gate blocks the switch.
# Log full build output so failures can be diagnosed (e.g. build/ empty or only CMake files)
KINESIS_SDK_DIR=/opt/amazon-kinesis-video-streams-producer-sdk-cpp
KINESIS_SDK_TAG=v3.5.0
KINESIS_BUILD_LOG=/var/log/kinesis-sdk-build.log
yum install -y perl
rm -rf "$KINESIS_SDK_DIR"
git clone --depth 1 --branch "$KINESIS_SDK_TAG" https://github.com/awslabs/amazon-kinesis-video-streams-producer-sdk-cpp.git "$KINESIS_SDK_DIR"
[ -f "$KINESIS_SDK_DIR/CMakeLists.txt" ] || { echo "Clone failed: CMakeLists.txt missing"; exit 1; }
mkdir -p "$KINESIS_SDK_DIR/build"
cd "$KINESIS_SDK_DIR/build"
# Limit make to 2 jobs to avoid OOM during log4cplus/SDK build (compiler can use ~1–2 GB per job)
{ cmake .. -DBUILD_GSTREAMER_PLUGIN=ON -DBUILD_DEPENDENCIES=ON 2>&1
  make -j2 2>&1
} | tee "$KINESIS_BUILD_LOG"
echo "--- build dir contents after make ---" >> "$KINESIS_BUILD_LOG"
ls -la "$KINESIS_SDK_DIR/build" >> "$KINESIS_BUILD_LOG" 2>&1
cd /
# Verify the built plugin loads - the same environment the service uses
export GST_PLUGIN_PATH="$KINESIS_SDK_DIR/build"
export LD_LIBRARY_PATH="$KINESIS_SDK_DIR/build"
gst-inspect-1.0 kvssink

# Verify the GStreamer elements both ingest pipelines need are present. These ship in
# the gstreamer1-plugins-base/good/bad-free packages installed above; a missing one
# aborts user data (see the rationale above: a healthy instance must be able to ingest).
for element in udpsrc filesrc srtpdec rtpjitterbuffer rtph264depay h264parse tsdemux; do
  if ! gst-inspect-1.0 "$element" > /dev/null 2>&1; then
    echo "FATAL: GStreamer element '$element' not found; this instance cannot ingest."
    exit 1
  fi
done

# All ingest prerequisites are verified: start the application service, which opens
# the health port and thereby marks this instance ingest-ready for the target groups.
systemctl start video-streaming.service

# Configure CloudWatch Logs agent
cat > /opt/aws/amazon-cloudwatch-agent/etc/cloudwatch-config.json << 'EOF'
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/video-streaming/application.log",
            "log_group_name": "/video-streaming/application",
            "log_stream_name": "{instance_id}/application",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/messages",
            "log_group_name": "/video-streaming/system",
            "log_stream_name": "{instance_id}/messages",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/cloud-init.log",
            "log_group_name": "/video-streaming/cloud-init",
            "log_stream_name": "{instance_id}/cloud-init",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/cloud-init-output.log",
            "log_group_name": "/video-streaming/cloud-init",
            "log_stream_name": "{instance_id}/cloud-init-output",
            "timezone": "UTC"
          }
        ]
      }
    }
  }
}
EOF

# Start CloudWatch Logs agent (do not fail user-data if agent has issues)
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -s \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/cloudwatch-config.json || true

echo "EC2 user data script completed successfully"
