# CloudWatch Logs Configuration

The EC2 instances are configured to send logs to CloudWatch Logs for debugging
and monitoring.

## Log Groups

### `/video-streaming/application`

- **Source**: systemd journal (filtered for `video-streaming` service)
- **Contains**: Application logs from the Node.js UDP listener service
- **Use for**: Debugging application logic, UDP packet processing, FFmpeg
  transcoding, S3 uploads, DynamoDB writes

### `/video-streaming/system`

- **Source**: `/var/log/messages`
- **Contains**: System-level logs
- **Use for**: System errors, kernel messages, service management

### `/video-streaming/cloud-init`

- **Source**: `/var/log/cloud-init.log` and `/var/log/cloud-init-output.log`
- **Contains**: EC2 instance initialization logs
- **Use for**: Debugging startup issues, dependency installation, service
  configuration

## Viewing Logs

### Using the Helper Script

```bash
# View application logs (default)
./scripts/view-logs.sh

# View specific log group
./scripts/view-logs.sh /video-streaming/system
./scripts/view-logs.sh /video-streaming/cloud-init
```

### Using AWS CLI

```bash
# List all log groups
aws logs describe-log-groups --log-group-name-prefix "/video-streaming"

# Tail logs in real-time
aws logs tail /video-streaming/application --follow --format short

# View last hour
aws logs tail /video-streaming/application --since 1h --format short

# View specific time range
aws logs tail /video-streaming/application \
  --since "2024-01-28T10:00:00" \
  --until "2024-01-28T11:00:00" \
  --format short

# Filter logs by pattern
aws logs filter-log-events \
  --log-group-name /video-streaming/application \
  --filter-pattern "ERROR" \
  --start-time $(date -d '1 hour ago' +%s)000
```

### Using AWS Console

1. Go to CloudWatch Console
2. Navigate to Logs > Log groups
3. Search for `/video-streaming`
4. Click on a log group to view log streams
5. Click on a log stream to view logs

## Log Stream Naming

Log streams are named using the EC2 instance ID:

- Format: `{instance_id}/log-name`
- Example: `i-0123456789abcdef0/video-streaming`

## Troubleshooting

### No logs appearing

1. **Check if log groups exist**:

   ```bash
   aws logs describe-log-groups --log-group-name-prefix "/video-streaming"
   ```

2. **Check CloudWatch agent status on EC2**:

   ```bash
   # Connect via SSM
   aws ssm start-session --target <instance-id>

   # Check agent status
   sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
     -a query -m ec2 -c default -s
   ```

3. **Check IAM permissions**: Ensure the EC2 role has CloudWatch Logs
   permissions (already configured in the stack)

4. **Check cloud-init logs**: If the agent isn't starting, check cloud-init logs
   for errors during setup

### Application not starting

Check the cloud-init logs first:

```bash
./scripts/view-logs.sh /video-streaming/cloud-init
```

Look for errors in:

- Node.js installation
- FFmpeg installation
- npm install
- Code download from S3

### Application crashes

Check application logs:

```bash
./scripts/view-logs.sh /video-streaming/application
```

Look for:

- Uncaught exceptions
- FFmpeg errors
- S3 upload failures
- DynamoDB errors
- UDP socket errors

## Log Retention

By default, CloudWatch Logs are retained for 7 days. To change retention:

```bash
# Set retention to 30 days
aws logs put-retention-policy \
  --log-group-name /video-streaming/application \
  --retention-in-days 30
```

Common retention periods: 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400,
545, 731, 1827, 3653 days
