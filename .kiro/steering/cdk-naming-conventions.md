# CDK Resource Naming Conventions

## Rule: Stack-Prefixed Named Resources

When creating AWS resources in CDK that require explicit physical names (e.g.,
`alarmName`, `topicName`, `bucketName`, etc.), you MUST prefix them with the
stack name to avoid naming conflicts across deployments.

### Why This Matters

- AWS resource names are globally or regionally unique
- Hardcoded names cause deployment failures when resources already exist
- Stack-prefixed names enable multiple stack deployments without conflicts
- Makes it easier to identify which stack owns which resource

### Implementation Pattern

```typescript
// ❌ BAD - Hardcoded name without stack prefix
const alarm = new cloudwatch.Alarm(this, "MyAlarm", {
  alarmName: "HighCPUUsage",
  // ...
});

// ✅ GOOD - Stack-prefixed name
const alarm = new cloudwatch.Alarm(this, "MyAlarm", {
  alarmName: `${Stack.of(this).stackName}-HighCPUUsage`,
  // ...
});

// ✅ BEST - Let CDK auto-generate names (no explicit name)
const alarm = new cloudwatch.Alarm(this, "MyAlarm", {
  // CDK will generate: stackName-MyAlarmABC123
  // ...
});
```

### When to Apply

Apply stack prefixing to these resource properties:

- CloudWatch Alarms: `alarmName`
- SNS Topics: `topicName`
- SQS Queues: `queueName`
- Lambda Functions: `functionName`
- DynamoDB Tables: `tableName` (when explicitly set)
- S3 Buckets: `bucketName` (when explicitly set)
- IAM Roles/Policies: `roleName`, `policyName` (when explicitly set)
- Any other resource with an explicit physical name property

### Best Practice

**Prefer auto-generated names**: Unless there's a specific business requirement
for a human-readable name, omit the name property entirely and let CDK generate
unique names automatically. This is the safest approach and prevents all naming
conflicts.

### Access Stack Name

```typescript
// Inside a Stack class
const stackName = Stack.of(this).stackName;

// Or use this.stackName directly
const stackName = this.stackName;
```
