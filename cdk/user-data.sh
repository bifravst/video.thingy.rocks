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

# Install AWS CLI v2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
yum install -y unzip
unzip awscliv2.zip
./aws/install
rm -rf aws awscliv2.zip

# Verify AWS CLI installation
aws --version

# Install GStreamer and build dependencies for kvssink
yum install -y cmake gcc-c++ make git pkg-config m4
yum install -y gstreamer1 gstreamer1-devel gstreamer1-plugins-base gstreamer1-plugins-base-devel gstreamer1-plugins-good gstreamer1-plugins-bad

# Build Amazon Kinesis Video Streams C++ Producer SDK with GStreamer plugin (kvssink)
KINESIS_SDK_DIR=/opt/amazon-kinesis-video-streams-producer-sdk-cpp
KINESIS_SDK_TAG=v3.5.0
git clone --depth 1 --branch "$KINESIS_SDK_TAG" https://github.com/awslabs/amazon-kinesis-video-streams-producer-sdk-cpp.git "$KINESIS_SDK_DIR"
mkdir -p "$KINESIS_SDK_DIR/build"
cd "$KINESIS_SDK_DIR/build"
cmake .. -DBUILD_GSTREAMER_PLUGIN=ON -DBUILD_DEPENDENCIES=ON
make -j"$(nproc)"
# Verify kvssink is available
export GST_PLUGIN_PATH="$KINESIS_SDK_DIR/build"
export LD_LIBRARY_PATH="$KINESIS_SDK_DIR/build:$KINESIS_SDK_DIR/open-source/local/lib"
gst-inspect-1.0 kvssink || { echo "kvssink plugin not found"; exit 1; }

# Create application directory
mkdir -p /opt/video-streaming

# Create output directory for video streams
mkdir -p /var/video-streams
chmod 755 /var/video-streams

# Download application code from S3 (placeholders replaced by CDK)
aws s3 sync s3://__CODE_BUCKET__/backend/ /opt/video-streaming/ --region __AWS_REGION__

# Install dependencies from deployed package.json
cd /opt/video-streaming
npm install --production

# Environment variables will be set by CDK
# AWS_REGION - AWS region
# TABLE_NAME - DynamoDB table name
# OUTPUT_DIR - Output directory for video streams

# Create log directory for application
mkdir -p /var/log/video-streaming
chmod 755 /var/log/video-streaming

# Create systemd service file
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
Environment="KINESIS_STREAM_PREFIX=__KINESIS_STREAM_PREFIX__"
Environment="GST_PLUGIN_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build"
Environment="LD_LIBRARY_PATH=/opt/amazon-kinesis-video-streams-producer-sdk-cpp/build:/opt/amazon-kinesis-video-streams-producer-sdk-cpp/open-source/local/lib"
ExecStart=/usr/bin/node --experimental-transform-types --no-warnings src/index.ts
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

# Start the application service immediately (before CloudWatch config so a later failure does not prevent start)
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
