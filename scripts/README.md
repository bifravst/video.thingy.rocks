## SSH into an instance

List instances with

```bash
./scripts/get-instance-ip.sh
```

Then

```bash
aws ssm start-session --region "${REGION:-eu-central-1}" --target "$INSTANCE_ID"
```
