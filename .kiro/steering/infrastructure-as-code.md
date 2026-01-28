---
inclusion: always
---

# Infrastructure as Code - No Manual Fixes

## Rule: Always Fix Infrastructure in Code, Never Manually

When working with infrastructure (CDK, Terraform, CloudFormation, etc.), you
MUST fix issues by updating the infrastructure code, NOT by providing manual fix
instructions.

### Why This Matters

- **Persistence**: Manual fixes are lost when instances are replaced or
  recreated
- **Reproducibility**: Infrastructure code ensures consistent deployments
- **Documentation**: Code is the source of truth, not scattered manual
  instructions
- **Automation**: Infrastructure as Code enables automated deployments
- **Drift Prevention**: Manual changes cause configuration drift

### What NOT to Do

❌ **BAD - Manual fix instructions:**

```bash
# SSH into instance and run:
sudo sed -i 's/old/new/' /etc/config
sudo systemctl restart service
```

❌ **BAD - Creating fix scripts for running instances:**

```bash
# scripts/fix-running-instance.sh
# This fixes the issue on existing instances
```

❌ **BAD - Providing workarounds:**

```markdown
## Quick Fix

Run this on the instance to fix the issue temporarily...
```

### What TO Do

✅ **GOOD - Fix in infrastructure code:**

```typescript
// cdk/user-data.sh or CDK construct
// Update the configuration in the source
```

✅ **GOOD - Terminate and recreate:**

```bash
# After fixing CDK code:
npm run cdk:deploy
# Terminate instances to get new ones with fix
aws ec2 terminate-instances --instance-ids <ids>
```

✅ **GOOD - Update Launch Template:**

```typescript
// Update CDK stack
const launchTemplate = new ec2.LaunchTemplate(this, "Template", {
  userData: ec2.UserData.custom(updatedUserDataScript),
});
```

## Implementation Pattern

### When a Bug is Found in Running Infrastructure

1. **Identify the root cause** in the infrastructure code
2. **Fix the infrastructure code** (CDK, user-data, etc.)
3. **Deploy the updated infrastructure**: `npm run cdk:deploy`
4. **Replace instances** to get the fix:
   - Terminate old instances
   - Let Auto Scaling Group create new ones
   - Or trigger a rolling update

### Example Workflow

```bash
# 1. Fix the issue in CDK code
# Edit cdk/user-data.sh or cdk/SomeStack.ts

# 2. Deploy the fix
npm run cdk:deploy

# 3. Replace instances (choose one method)

# Method A: Terminate instances (ASG creates new ones)
INSTANCES=$(aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=*MyASG*" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[*].Instances[*].InstanceId" \
  --output text)
aws ec2 terminate-instances --instance-ids $INSTANCES

# Method B: Trigger rolling update (if using ASG)
aws autoscaling start-instance-refresh \
  --auto-scaling-group-name MyASG

# 4. Verify new instances have the fix
./scripts/ssh-to-instance.sh
# Check configuration
```

## Exceptions

The ONLY acceptable manual interventions are:

1. **Debugging**: Temporarily checking logs or testing on a running instance to
   understand the issue
2. **Emergency hotfix**: Critical production issue requiring immediate fix (but
   MUST be followed by proper infrastructure code update)
3. **One-time migration**: Migrating data or state that cannot be automated

Even in these cases:

- Document what was done manually
- Update infrastructure code immediately after
- Plan to replace instances with code-based configuration

## Response Template

When a user reports an infrastructure issue:

````markdown
## Fix Applied

I've updated the infrastructure code to fix this issue:

**Files Modified:**

- `cdk/user-data.sh` - Fixed environment variable configuration
- `cdk/StreamingStack.ts` - Updated launch template

**To Apply the Fix:**

1. Deploy the updated infrastructure:
   ```bash
   npm run cdk:deploy
   ```
````

2. Replace instances to get the fix:

   ```bash
   # Get instance IDs
   INSTANCES=$(aws ec2 describe-instances \
     --filters "Name=tag:aws:autoscaling:groupName,Values=*UDPListenerASG*" \
               "Name=instance-state-name,Values=running" \
     --query "Reservations[*].Instances[*].InstanceId" \
     --output text)

   # Terminate them (Auto Scaling Group will create new ones)
   aws ec2 terminate-instances --instance-ids $INSTANCES
   ```

3. Wait 5-10 minutes for new instances to launch

4. Verify the fix:
   ```bash
   ./scripts/ssh-to-instance.sh
   # Check that the issue is resolved
   ```

The fix is now persistent and will apply to all future instances automatically.

````

## Anti-Patterns to Avoid

### ❌ Creating "Fix Scripts"
Don't create scripts like:
- `scripts/fix-environment-variables.sh`
- `scripts/update-service-logging.sh`
- `scripts/deploy-code-to-instance.sh`

These encourage manual fixes and create maintenance burden.

### ❌ "Quick Fix" Documentation
Don't write documentation sections like:
- "Quick Fix (for existing instances)"
- "Manual Deployment Steps"
- "Workaround until next deployment"

### ❌ SSH Instructions
Don't provide step-by-step SSH instructions to fix issues:
- "SSH into the instance and run..."
- "Edit /etc/config and change..."
- "Restart the service with..."

## Correct Approach

### ✅ Fix in Code
```typescript
// cdk/StreamingStack.ts
const userData = ec2.UserData.custom(`
#!/bin/bash
# Fixed: Added missing environment variable
export TABLE_NAME="${this.streamTable.tableName}"
...
`)
````

### ✅ Document the Fix

```markdown
## Issue Fixed

**Problem:** TABLE_NAME environment variable was empty

**Solution:** Updated `cdk/user-data.sh` to include the correct table name

**To apply:** Deploy CDK and replace instances
```

### ✅ Provide Deployment Instructions

```markdown
## Deployment

1. Deploy: `npm run cdk:deploy`
2. Replace instances: `aws ec2 terminate-instances --instance-ids <ids>`
3. Verify: Check new instances have the fix
```

## Summary

- ✅ **DO**: Fix infrastructure in code (CDK, user-data, etc.)
- ✅ **DO**: Deploy and replace instances
- ✅ **DO**: Document what was fixed in the code
- ❌ **DON'T**: Create manual fix scripts
- ❌ **DON'T**: Provide SSH-based workarounds
- ❌ **DON'T**: Encourage manual configuration changes

**Remember:** Infrastructure as Code means the code is the source of truth.
Manual fixes are technical debt.
