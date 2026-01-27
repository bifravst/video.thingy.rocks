#!/bin/bash
set -e

# EC2 User Data Script for NTN Video Streaming Service
# This script installs dependencies and configures the UDP listener service

# Update system
yum update -y

# Install Node.js v24 using NodeSource repository
curl -fsSL https://rpm.nodesource.com/setup_24.x | bash -
yum install -y nodejs

# Verify Node.js and NPM versions
node --version
npm --version

# Install FFmpeg with required codecs
amazon-linux-extras install -y epel
yum install -y ffmpeg

# Verify FFmpeg installation
ffmpeg -version

# Install AWS CLI v2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
yum install -y unzip
unzip awscliv2.zip
./aws/install
rm -rf aws awscliv2.zip

# Verify AWS CLI installation
aws --version

# Create application directory
mkdir -p /opt/ntn-video-streaming
cd /opt/ntn-video-streaming

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
  "name": "ntn-video-streaming-backend",
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
# S3_BUCKET - S3 bucket name for video storage
# DYNAMODB_TABLE_NAME - DynamoDB table name
# OUTPUT_DIR - Output directory for video streams

# Create systemd service file
cat > /etc/systemd/system/ntn-video-streaming.service << 'EOF'
[Unit]
Description=NTN Video Streaming UDP Listener Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/ntn-video-streaming
Environment="NODE_ENV=production"
Environment="AWS_REGION=__AWS_REGION__"
Environment="S3_BUCKET=__S3_BUCKET__"
Environment="DYNAMODB_TABLE_NAME=__DYNAMODB_TABLE_NAME__"
Environment="OUTPUT_DIR=/var/video-streams"
ExecStart=/usr/bin/node --experimental-transform-types --no-warnings src/index.ts
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ntn-video-streaming

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd to recognize the new service
systemctl daemon-reload

# Enable service to start on boot
systemctl enable ntn-video-streaming.service

# Note: Service will be started after code deployment
# systemctl start ntn-video-streaming.service

echo "EC2 user data script completed successfully"
