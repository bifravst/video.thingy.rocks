# Bug Fixes

## Issue 1: DynamoDB Scan Permission Denied

### Problem

Users were getting an `AccessDeniedException` when trying to list streams:

```
User: arn:aws:sts::...:assumed-role/.../CognitoIdentityCredentials
is not authorized to perform: dynamodb:Scan on resource:
arn:aws:dynamodb:...:table/video-streaming-StreamMetadataD1FE2960-1IL7LYVPF9K2B
because no identity-based policy allows the dynamodb:Scan action
```

### Root Cause

The CDK `grantReadData()` method only grants these permissions:

- `dynamodb:GetItem`
- `dynamodb:BatchGetItem`
- `dynamodb:Query`

It does NOT grant `dynamodb:Scan`, which is required by the
`StreamDynamoDBClient.listStreams()` method.

### Fix

Added explicit `Scan` permission to both authenticated and unauthenticated IAM
roles in `cdk/StreamingStack.ts`:

```typescript
// For unauthenticated users
this.streamTable.grantReadData(this.unauthRole);
this.unauthRole.addToPolicy(
  new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ["dynamodb:Scan"],
    resources: [this.streamTable.tableArn],
  }),
);

// For authenticated users
this.streamTable.grantReadData(this.authRole);
this.authRole.addToPolicy(
  new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ["dynamodb:Scan"],
    resources: [this.streamTable.tableArn],
  }),
);
```

### Deployment

To apply this fix:

```bash
npm run cdk:prod:deploy
```

The IAM roles will be updated with the new permissions.

---

## Issue 2: Infinite Retry Loop

### Problem

When the frontend failed to fetch streams (e.g., due to permission errors), it
would retry indefinitely instead of stopping after 5 attempts as intended.

### Root Cause

Two issues in the retry logic:

1. **Polling continued after max retries**: The `setInterval` polling kept
   calling `fetchStreams()` every 5 seconds, which would trigger new retry
   attempts even after reaching the 5-retry limit.

2. **No state tracking for max retries**: There was no flag to prevent the
   polling interval from triggering new fetches after max retries were reached.

### Fix

Added `maxRetriesReached` state and logic to both `StreamList.tsx` and
`StreamPlayer.tsx`:

**Changes:**

1. Added `maxRetriesReached` state variable
2. Set `maxRetriesReached = true` after 5 failed attempts
3. Modified polling to skip fetches when `maxRetriesReached` is true
4. Added `isManualRetry` parameter to allow manual retries to bypass the limit
5. Reset `maxRetriesReached` on successful fetch or manual retry

**StreamList.tsx:**

```typescript
const [maxRetriesReached, setMaxRetriesReached] = useState(false);

const fetchStreams = async (isManualRetry = false) => {
  // Don't retry automatically if max retries reached
  if (maxRetriesReached && !isManualRetry) {
    return;
  }

  // ... fetch logic ...

  if (retryCount < 5 && !maxRetriesReached) {
    // Retry logic
  } else {
    setMaxRetriesReached(true);
    console.log("[StreamList] Max retries reached, stopping automatic retries");
  }
};

// Polling respects maxRetriesReached
useEffect(() => {
  const intervalId = setInterval(() => {
    if (!maxRetriesReached) {
      void fetchStreams();
    }
  }, POLL_INTERVAL);

  return () => clearInterval(intervalId);
}, [awsConfig, maxRetriesReached]);

// Manual retry resets the flag
const handleRetry = () => {
  setMaxRetriesReached(false);
  void fetchStreams(true); // Manual retry
};
```

**StreamPlayer.tsx:** Same pattern applied to the stream player component.

### Behavior After Fix

**Automatic Retries:**

- Attempt 1: Immediate
- Attempt 2: After 1 second
- Attempt 3: After 2 seconds
- Attempt 4: After 4 seconds
- Attempt 5: After 8 seconds
- After 5 attempts: Stops retrying, shows error with "Retry Now" button

**Manual Retry:**

- User clicks "Retry Now" button
- Resets retry count and max retries flag
- Starts fresh retry sequence

**Polling:**

- Continues every 5 seconds when successful
- Stops when max retries reached
- Resumes after successful manual retry

---

## Testing the Fixes

### Test Permission Fix

1. Deploy the updated stack:

   ```bash
   npm run cdk:prod:deploy
   ```

2. Open the web application:

   ```bash
   cd frontend
   npm start
   ```

3. Navigate to http://localhost:8080

4. You should now see the stream list without permission errors

### Test Retry Fix

1. Temporarily break the DynamoDB connection (e.g., use wrong table name in
   `.envrc`)

2. Open the web application

3. Observe the retry behavior:
   - Should see "Retrying automatically... (Attempt X/5)"
   - Should stop after 5 attempts
   - Should show "Retry Now" button
   - Clicking "Retry Now" should start fresh retry sequence

4. Restore correct configuration and click "Retry Now"

5. Should successfully load streams

---

## Related Files Modified

### CDK Infrastructure

- `cdk/StreamingStack.ts` - Added Scan permissions to IAM roles

### Frontend Components

- `frontend/src/page/StreamList.tsx` - Fixed infinite retry loop
- `frontend/src/page/StreamPlayer.tsx` - Fixed infinite retry loop

---

## Prevention

### For Future Development

**When adding DynamoDB operations:**

1. Check what permissions are needed (GetItem, Query, Scan, etc.)
2. Use `grantReadData()` for basic read operations
3. Add explicit permissions for Scan if needed
4. Test with actual Cognito credentials, not admin credentials

**When implementing retry logic:**

1. Always have a maximum retry count
2. Track retry state separately from error state
3. Prevent polling/intervals from triggering retries after max reached
4. Provide manual retry option for users
5. Log retry attempts for debugging

**Testing checklist:**

- [ ] Test with unauthenticated access
- [ ] Test with authenticated access
- [ ] Test with permission errors
- [ ] Test retry behavior (should stop after max attempts)
- [ ] Test manual retry button
- [ ] Test that polling resumes after successful retry

---

## Rollback

If issues occur after deployment:

1. **Rollback CDK changes:**

   ```bash
   git revert <commit-hash>
   npm run cdk:prod:deploy
   ```

2. **Rollback frontend changes:**

   ```bash
   git revert <commit-hash>
   cd frontend
   npm run build
   ```

3. **Emergency fix** (if needed):
   - Manually add Scan permission via AWS Console:
     - Go to IAM → Roles
     - Find the Cognito Identity Pool roles
     - Add inline policy with `dynamodb:Scan` permission

---

## Monitoring

After deployment, monitor:

1. **CloudWatch Logs** for DynamoDB errors:

   ```bash
   ./scripts/view-logs.sh
   ```

2. **Browser Console** for retry behavior:
   - Should see retry attempts logged
   - Should see "Max retries reached" after 5 attempts

3. **DynamoDB Metrics** for Scan operations:
   ```bash
   aws cloudwatch get-metric-statistics \
     --namespace AWS/DynamoDB \
     --metric-name ConsumedReadCapacityUnits \
     --dimensions Name=TableName,Value=$DYNAMODB_TABLE_NAME \
     --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
     --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
     --period 300 \
     --statistics Sum
   ```
