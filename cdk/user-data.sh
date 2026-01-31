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

# Create application directory
mkdir -p /opt/video-streaming
cd /opt/video-streaming

# Create output directory for video streams
mkdir -p /var/video-streams
chmod 755 /var/video-streams

# Copy application code (will be replaced with actual deployment mechanism)
# For now, we'll use AWS Systems Manager Parameter Store or S3 to fetch the code
# This is a placeholder that will be replaced in the CDK stack

# Install application dependencies
# Note: The actual code deployment will happen via CDK BucketDeployment or similar
cat > package.json << 'EOF'
{
  "name": "video-streaming-backend",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "start": "node --experimental-transform-types --no-warnings src/index.ts"
  },
  "dependencies": {
    "@aws-sdk/client-s3": "^3.976.0",
    "@aws-sdk/client-dynamodb": "^3.976.0",
    "@aws-sdk/lib-dynamodb": "^3.976.0",
    "@aws-sdk/client-cloudwatch": "^3.976.0"
  }
}
EOF

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

# Start CloudWatch Logs agent
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -s \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/cloudwatch-config.json

# Note: Service will be started after code deployment
# systemctl start video-streaming.service

echo "EC2 user data script completed successfully"
