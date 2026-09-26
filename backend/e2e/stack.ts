import {
	CloudWatchClient,
	GetMetricStatisticsCommand,
} from '@aws-sdk/client-cloudwatch'
import {
	CloudWatchLogsClient,
	GetQueryResultsCommand,
	StartQueryCommand,
} from '@aws-sdk/client-cloudwatch-logs'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DescribeInstancesCommand, EC2Client } from '@aws-sdk/client-ec2'
import {
	GetDataEndpointCommand,
	KinesisVideoClient,
} from '@aws-sdk/client-kinesis-video'
import {
	GetMediaCommand,
	KinesisVideoMediaClient,
} from '@aws-sdk/client-kinesis-video-media'
import {
	GetCommandInvocationCommand,
	PutParameterCommand,
	SendCommandCommand,
	SSMClient,
} from '@aws-sdk/client-ssm'
import {
	DeleteCommand,
	DynamoDBDocumentClient,
	GetCommand,
	PutCommand,
} from '@aws-sdk/lib-dynamodb'
import assert from 'node:assert/strict'

/** A synthetic lock-table key no ingest port can ever collide with. */
const SUITE_LOCK_PORT = 424242
/** A lock older than this is a crashed run's leftover and may be taken over. */
const SUITE_LOCK_STALE_MS = 30 * 60_000

/**
 * Serializes e2e runs against one stack.
 *
 * Two suites running against the same stack corrupt each other completely:
 * every port's key gets provisioned twice, the fleet is restarted mid-case,
 * and one run's unauthenticated traffic walks the other's rollover-counter
 * search past the answer - each failure mode looks exactly like a product bug.
 * The lock is a row in the stack's own lock table, so it is visible in the
 * same place as every other lock, and it is written conditionally: a run that
 * crashed without releasing does not block forever.
 */
export const acquireSuiteLock = async (
	region: string,
	tableName: string,
	runId: string,
): Promise<void> => {
	const doc = DynamoDBDocumentClient.from(new DynamoDBClient({ region }))
	const staleIso = new Date(Date.now() - SUITE_LOCK_STALE_MS).toISOString()
	try {
		await doc.send(
			new PutCommand({
				TableName: tableName,
				Item: {
					port: SUITE_LOCK_PORT,
					status: 'active',
					e2eRunId: runId,
					createdAt: new Date().toISOString(),
					updatedAt: new Date().toISOString(),
				},
				// Take over only when nobody holds it, or when the holder has
				// clearly crashed: a live suite refreshes this row while
				// provisioning.
				ConditionExpression: 'attribute_not_exists(port) OR updatedAt < :stale',
				ExpressionAttributeValues: { ':stale': staleIso },
			}),
		)
	} catch (error) {
		if (
			error instanceof Error &&
			error.name === 'ConditionalCheckFailedException'
		) {
			const held = await doc.send(
				new GetCommand({
					TableName: tableName,
					Key: { port: SUITE_LOCK_PORT },
				}),
			)
			throw new Error(
				`another e2e run holds this stack's lock (row ${String(SUITE_LOCK_PORT)}, run ${String(held.Item?.e2eRunId ?? '?')}, last refreshed ${String(held.Item?.updatedAt ?? '?')}). Wait for it to finish, or delete that row if it is a crashed run's leftover.`,
				{ cause: error },
			)
		}
		throw error
	}
}

/** Refreshes the lock so a concurrent acquirer sees it is still held. */
export const refreshSuiteLock = async (
	region: string,
	tableName: string,
	runId: string,
): Promise<void> => {
	const doc = DynamoDBDocumentClient.from(new DynamoDBClient({ region }))
	await doc
		.send(
			new PutCommand({
				TableName: tableName,
				Item: {
					port: SUITE_LOCK_PORT,
					status: 'active',
					e2eRunId: runId,
					updatedAt: new Date().toISOString(),
				},
				ConditionExpression: 'attribute_not_exists(port) OR e2eRunId = :run',
				ExpressionAttributeValues: { ':run': runId },
			}),
		)
		.catch((error: unknown) => {
			throw new Error(
				`this run's hold on the stack's e2e lock was taken over by another run: ${String(error)}`,
			)
		})
}

/** Releases the lock. Safe to call more than once. */
export const releaseSuiteLock = async (
	region: string,
	tableName: string,
	runId: string,
): Promise<void> => {
	const doc = DynamoDBDocumentClient.from(new DynamoDBClient({ region }))
	await doc
		.send(
			new DeleteCommand({
				TableName: tableName,
				Key: { port: SUITE_LOCK_PORT },
				ConditionExpression: 'e2eRunId = :run',
				ExpressionAttributeValues: { ':run': runId },
			}),
		)
		.catch((error: unknown) => {
			if (
				error instanceof Error &&
				error.name === 'ConditionalCheckFailedException'
			) {
				return // Taken over or already gone - not ours to delete.
			}
			console.warn('[e2e] could not release the suite lock:', error)
		})
}

/**
 * The e2e suite's connection to the deployed stack it tests.
 *
 * Everything here talks to the real AWS APIs of the real resources: the assertions
 * read the stack's own observables - CloudWatch metrics, the application log, the
 * lock table, Kinesis Video fragments - because the point of this suite is to judge
 * the deployed system, not a model of it.
 */

export type E2eConfig = {
	stackName: string
	region: string
	/** Where the NLB forwards ingest traffic from the internet. */
	nlbHost: string
	/** The DynamoDB lock table (StreamMetadata). */
	tableName: string
	/** The Auto Scaling Group, for finding the fleet's instances. */
	autoScalingGroupName: string
	/** The CloudWatch Logs group the application log ships to. */
	logGroup: string
	/** The prefix of the KVS stream names, `{stackName}-video`. */
	streamPrefix: string
}

const output = (outputs: Record<string, string>, key: string): string => {
	const value = outputs[key]
	assert.ok(value !== undefined, `the stack has no ${key} output`)
	return value
}

export const discoverStack = async (
	region: string,
	stackName: string,
): Promise<E2eConfig> => {
	const { CloudFormationClient, DescribeStacksCommand } =
		await import('@aws-sdk/client-cloudformation')
	const cfn = new CloudFormationClient({ region })
	const stacks = await cfn.send(
		new DescribeStacksCommand({ StackName: stackName }),
	)
	const stack = stacks.Stacks?.[0]
	assert.ok(stack !== undefined, `stack ${stackName} not found`)
	const outputs: Record<string, string> = {}
	for (const o of stack.Outputs ?? []) {
		if (o.OutputKey !== undefined && o.OutputValue !== undefined) {
			outputs[o.OutputKey] = o.OutputValue
		}
	}
	return {
		stackName,
		region,
		nlbHost: output(outputs, 'NLBDnsName'),
		tableName: output(outputs, 'StreamMetadataTableName'),
		autoScalingGroupName: output(outputs, 'AutoScalingGroupName'),
		// The CloudWatch agent ships the application log to /video-streaming/*
		// regardless of stack (see cdk/user-data.sh).
		logGroup: process.env.E2E_LOG_GROUP ?? '/video-streaming/application',
		streamPrefix: `${stackName}-video`,
	}
}

/** The fleet's EC2 instances, found the way the restart automation names them. */
export const fleetInstanceIds = async (
	region: string,
	autoScalingGroupName: string,
): Promise<string[]> => {
	const ec2 = new EC2Client({ region })
	const reservations = await ec2.send(
		new DescribeInstancesCommand({
			Filters: [
				{
					Name: 'tag:aws:autoscaling:groupName',
					Values: [autoScalingGroupName],
				},
				{ Name: 'instance-state-name', Values: ['running', 'pending'] },
			],
		}),
	)
	const ids = (reservations.Reservations ?? [])
		.flatMap((r) => r.Instances ?? [])
		.map((i) => i.InstanceId)
	return ids.filter((id): id is string => id !== undefined)
}

/**
 * The bucket the CDK deployment puts the backend code in.
 *
 * Found through the stack's resources rather than an output so this works against
 * stacks deployed before one existed - the bucket's physical name is generated,
 * and its logical ID (`CodeBucket`) is the stable handle.
 */
export const codeBucketFor = async (
	region: string,
	stackName: string,
): Promise<string> => {
	const { CloudFormationClient, DescribeStackResourcesCommand } =
		await import('@aws-sdk/client-cloudformation')
	const cfn = new CloudFormationClient({ region })
	const resources = await cfn.send(
		new DescribeStackResourcesCommand({ StackName: stackName }),
	)
	// The bucket's logical ID is not a stable handle (CDK appends a hash to S3
	// bucket IDs), but the stack has exactly one AWS::S3::Bucket - the code
	// bucket - so the type is.
	const buckets = (resources.StackResources ?? []).filter(
		(r) =>
			r.ResourceType === 'AWS::S3::Bucket' &&
			r.PhysicalResourceId !== undefined,
	)
	assert.ok(
		buckets.length > 0,
		'the stack has no S3 bucket - was it deployed from the srtp-ingest-v3 branch?',
	)
	assert.ok(
		buckets.length === 1,
		`the stack has ${String(buckets.length)} S3 buckets; cannot tell which holds the backend code`,
	)
	return buckets[0]?.PhysicalResourceId as string
}

/**
 * Provisions one port's key as a SecureString, exactly as the ops script does:
 * no KeyId, so the parameter is encrypted under the AWS-managed aws/ssm key,
 * which the instance role can read without a KMS grant. The value shape is the
 * one SrtpKeyStore validates.
 *
 * No generation is allocated here: the key parameter's own SSM version is it,
 * incremented atomically by SSM on every overwrite - the property the replay
 * floor's rotation fence depends on, and the reason no read-modify-write
 * counter of ours is involved at all (both earlier schemes were review
 * findings for exactly that).
 */
export const provisionKey = async (
	region: string,
	stackName: string,
	port: number,
	options: { ssrc: number; keyHex?: string },
): Promise<string> => {
	const keyHex =
		options.keyHex ??
		(await import('node:crypto')).randomBytes(30).toString('hex')
	const ssm = new SSMClient({ region })
	await ssm.send(
		new PutParameterCommand({
			Name: `/${stackName}/srtp/port/${String(port)}/key`,
			Type: 'SecureString',
			Overwrite: true,
			Value: JSON.stringify({
				key: keyHex,
				ssrc: options.ssrc,
				cipher: 'aes-128-icm',
				auth: 'hmac-sha1-80',
			}),
		}),
	)
	return keyHex
}

/**
 * Seeds a port's replay-floor row directly, as if traffic under that key had
 * already been accepted up to the given rollover counter.
 *
 * The floor is the receiver's only accelerator, and some states a real sender
 * cannot reach from zero in bounded time - a rollover counter in the upper
 * 48-bit index space the search would need hours to climb to, or a floor the
 * search has been walked past - are only reachable by injecting the row the
 * way the receiver itself would have persisted it. The identity fields are
 * derived from the provisioned key, exactly as the receiver computes them, so
 * the row is indistinguishable from one it wrote itself.
 *
 * The whole item is replaced, which is what the receiver's own writes do
 * modulo the lock fields: lock acquisition and floor raising are both
 * conditional updates on this row, so a seeded item without lock fields simply
 * reads as "no floor was persisted and nobody holds the lock".
 */
export const seedSrtpIndexFloor = async (
	region: string,
	tableName: string,
	port: number,
	options: { keyHex: string; ssrc: number; roc: number },
): Promise<void> => {
	const { keyFingerprint } = await import('../src/SrtpKeyStore.ts')
	const doc = DynamoDBDocumentClient.from(new DynamoDBClient({ region }))
	await doc.send(
		new PutCommand({
			TableName: tableName,
			Item: {
				port,
				// Sequence zero of that rollover: the base the receiver's own
				// floor math extracts from it is the roc itself.
				srtpIndex: options.roc * 65_536,
				srtpIndexSsrc: options.ssrc,
				srtpIndexKeyFingerprint: keyFingerprint(options.keyHex),
			},
		}),
	)
}

/**
 * The metric period everything below is aligned to, in milliseconds.
 * PutMedia.IncomingBytes is published per minute.
 */
const METRIC_PERIOD_MS = 60_000

/**
 * The next period boundary strictly after `since`.
 *
 * CloudWatch returns a period-aligned datapoint whenever the query window
 * overlaps its bucket, with the Sum of the WHOLE bucket - so a window that
 * starts mid-minute counts the minute's earlier bytes too. On 2026-09-25 that
 * counted a case's own legitimate first phase as "ingestion since the rewind
 * began", failing a rewind the receiver had in fact refused; on a re-run it
 * can equally pass a wait on the previous run's stale bytes. Every NEGATIVE
 * ingestion query here (assertNoStreamIngestion) starts on this boundary, so
 * only bytes uploaded in full minutes after `since` are ever counted. The
 * positive wait floors instead - see containingPeriodBoundary for why the
 * two cannot share a direction.
 *
 * CloudWatch also rejects `StartTime >= EndTime` with a 400 rather than
 * returning an empty result, so the callers must not query until the
 * boundary is strictly in the past; see `periodBoundaryElapsed`.
 */
const nextPeriodBoundary = (since: Date): Date =>
	new Date(
		Math.ceil((since.getTime() + 1) / METRIC_PERIOD_MS) * METRIC_PERIOD_MS,
	)

/**
 * The period boundary `since` falls inside - the minute the phase being
 * waited on began streaming into.
 *
 * The positive wait must include this bucket: a short stream uploads all of
 * its bytes - the stream itself plus the several-second tail of fragment
 * completion and teardown that follows the sender's stop - inside the one
 * minute bucket it started in. On 2026-09-25 the hint-climb and
 * restart-recovery cases each streamed ~750 KB that reached Kinesis whole
 * (the metric datapoint shows it), every byte of it in the minute before the
 * ceiling-aligned window began, and both healthy cases failed on the 240s
 * timeout. The price is that bytes uploaded earlier in that same minute
 * than `since` count too; that is acceptable because every positive wait is
 * paired with an exact log-line assertion over its own window, and because
 * no phase can share a minute with the phase before it - the previous
 * phase's wait already ran into the next minute by then.
 */
const containingPeriodBoundary = (since: Date): Date =>
	new Date(Math.floor(since.getTime() / METRIC_PERIOD_MS) * METRIC_PERIOD_MS)

/** True once the aligned window has at least one full period before `now`. */
const periodBoundaryElapsed = (from: Date): boolean =>
	Date.now() - from.getTime() >= METRIC_PERIOD_MS

/**
 * Waits until the KVS stream received media after `since`, using the metric the
 * alarms use. This is the e2e definition of "it works": bytes reached Kinesis.
 *
 * The window is floored to the minute `since` falls in by default (see
 * containingPeriodBoundary): the stream's own minute bucket is the one its
 * bytes are most likely all in. `align: 'next'` ceilings instead, for the one
 * wait that must not count bytes uploaded before `since` - the restart
 * recovery's, whose whole point is that only the restarted process's media
 * counts as proof.
 */
export const waitForStreamIngestion = async (
	region: string,
	streamName: string,
	since: Date,
	timeoutMs = 240_000,
	align: 'containing' | 'next' = 'containing',
): Promise<number> => {
	const cw = new CloudWatchClient({ region })
	const from =
		align === 'next'
			? nextPeriodBoundary(since)
			: containingPeriodBoundary(since)
	const deadline = Date.now() + timeoutMs
	for (;;) {
		// CloudWatch rejects StartTime >= EndTime with a 400, so a poll is only
		// valid once a full period has elapsed past the boundary; until then it
		// would have nothing to sum anyway.
		if (!periodBoundaryElapsed(from)) {
			if (Date.now() > deadline) {
				throw noIngestionError(streamName, since, from)
			}
			await sleep(5_000)
			continue
		}
		const end = new Date()
		const stats = await cw.send(
			new GetMetricStatisticsCommand({
				Namespace: 'AWS/KinesisVideo',
				MetricName: 'PutMedia.IncomingBytes',
				Dimensions: [{ Name: 'StreamName', Value: streamName }],
				StartTime: from,
				EndTime: end,
				Period: 60,
				Statistics: ['Sum'],
			}),
		)
		const total = (stats.Datapoints ?? []).reduce(
			(sum, d) => sum + (d.Sum ?? 0),
			0,
		)
		if (total > 0) return total
		if (Date.now() > deadline) {
			throw noIngestionError(streamName, since, from)
		}
		await sleep(15_000)
	}
}

/** The positive wait's failure: says which minute bucket was watched. */
const noIngestionError = (streamName: string, since: Date, from: Date): Error =>
	new Error(
		`no PutMedia.IncomingBytes on ${streamName} since ${from.toISOString()} (the minute ${since.toISOString()} falls in)`,
	)

/**
 * Asserts the opposite: no media reached the stream in the window. Missing
 * datapoints count as zero - a stream nobody writes to publishes nothing at
 * all.
 *
 * The window is the next-boundary-aligned one (see nextPeriodBoundary for
 * why only this direction is safe here): bytes counted are those in full
 * minutes after `since`, never the earlier bytes of the minute `since` falls
 * in - which the phase before this one may legitimately have uploaded. The
 * positive wait above floors for the opposite reason; a short stream's own
 * bytes must not be excluded from the wait that exists to see them.
 *
 * If the aligned window has not yet elapsed a full period, it waits until it
 * has, so the assertion is never vacuously true over an empty window.
 */
export const assertNoStreamIngestion = async (
	region: string,
	streamName: string,
	since: Date,
): Promise<void> => {
	const from = nextPeriodBoundary(since)
	// At least one full period must have elapsed for the assertion to mean
	// anything; the rewind case's own waits usually cover this already.
	const earliestAssert = from.getTime() + METRIC_PERIOD_MS
	if (Date.now() < earliestAssert) {
		await sleep(earliestAssert - Date.now())
	}
	const cw = new CloudWatchClient({ region })
	const stats = await cw.send(
		new GetMetricStatisticsCommand({
			Namespace: 'AWS/KinesisVideo',
			MetricName: 'PutMedia.IncomingBytes',
			Dimensions: [{ Name: 'StreamName', Value: streamName }],
			StartTime: from,
			EndTime: new Date(),
			Period: 60,
			Statistics: ['Sum'],
		}),
	)
	const total = (stats.Datapoints ?? []).reduce(
		(sum, d) => sum + (d.Sum ?? 0),
		0,
	)
	assert.equal(
		total,
		0,
		`${streamName} received media after ${from.toISOString()} (aligned up from ${since.toISOString()}) that it must not have`,
	)
}

/**
 * Queries the application log for lines matching a filter, from a point in time,
 * and waits for CloudWatch Logs' indexing to catch up.
 */
export const waitForLogLines = async (
	region: string,
	logGroup: string,
	filter: string,
	since: Date,
	timeoutMs = 120_000,
): Promise<string[]> => {
	const events = await waitForLogEvents(
		region,
		logGroup,
		filter,
		since,
		timeoutMs,
	)
	return events.map((event) => event.message)
}

/**
 * waitForLogLines, but returning each event's own timestamp.
 *
 * A window that starts "now" can contain activity from a process that is about
 * to be replaced: the only boundary that is the new process's alone is the
 * timestamp of a line only it writes (its boot line), so a case that must prove
 * recovery anchors its windows to that timestamp instead of to a wall clock
 * taken before the restart.
 */
export const waitForLogEvents = async (
	region: string,
	logGroup: string,
	filter: string,
	since: Date,
	timeoutMs = 120_000,
): Promise<{ message: string; timestamp: Date }[]> => {
	const logs = new CloudWatchLogsClient({ region })
	const deadline = Date.now() + timeoutMs
	for (;;) {
		const started = await logs.send(
			new StartQueryCommand({
				logGroupName: logGroup,
				startTime: Math.floor(since.getTime() / 1000),
				endTime: Math.floor(Date.now() / 1000) + 1,
				queryString: `fields @message, @timestamp | filter @message like /${filter}/ | sort @timestamp asc`,
			}),
		)
		assert.ok(started.queryId !== undefined, 'no query id')
		for (;;) {
			await sleep(3_000)
			const results = await logs.send(
				new GetQueryResultsCommand({ queryId: started.queryId }),
			)
			if (results.status === 'Running') continue
			// Only @message and @timestamp: @ptr is a base64 event pointer, not a
			// log line, and would make the result garbage.
			const events: { message: string; timestamp: Date }[] = []
			for (const row of results.results ?? []) {
				const message = row.find((f) => f.field === '@message')?.value ?? ''
				const stamp = row.find((f) => f.field === '@timestamp')?.value
				if (message.length === 0 || stamp === undefined) continue
				const timestamp = new Date(stamp)
				assert.ok(
					!Number.isNaN(timestamp.getTime()),
					`CloudWatch returned a timestamp it cannot parse: ${stamp}`,
				)
				events.push({ message, timestamp })
			}
			if (events.length > 0) return events
			// The query completed with nothing: either the events have not been
			// indexed yet, or they truly are not there. Re-query until the deadline,
			// with a fresh end time so events that arrived meanwhile are in scope.
			if (Date.now() > deadline) {
				throw new Error(
					`no log line matching /${filter}/ since ${since.toISOString()}`,
				)
			}
			break
		}
	}
}

/** Reads the port's lock row, if there is one. */
export const lockRow = async (
	region: string,
	tableName: string,
	port: number,
): Promise<Record<string, unknown> | undefined> => {
	const doc = DynamoDBDocumentClient.from(new DynamoDBClient({ region }))
	const result = await doc.send(
		new GetCommand({
			TableName: tableName,
			Key: { port },
			ConsistentRead: true,
		}),
	)
	return result.Item
}

/**
 * Waits until a port's floor row names the given key's fingerprint: the proof a
 * rotation actually persists its floor.
 *
 * The rotation fence's failure mode is silent otherwise - a rotated key that
 * cannot write its floor still ingests (a floor it cannot read means the same
 * as no floor: the search starts from zero) and confirms on trial 1, so every
 * other assertion in the rotation cases passes with the fence broken. The row
 * itself is the only observable of the fence.
 */
export const waitForSrtpIndexFloorFor = async (
	region: string,
	tableName: string,
	port: number,
	keyHex: string,
	timeoutMs = 60_000,
): Promise<Record<string, unknown>> => {
	const { keyFingerprint } = await import('../src/SrtpKeyStore.ts')
	const fingerprint = keyFingerprint(keyHex)
	const deadline = Date.now() + timeoutMs
	for (;;) {
		const row = await lockRow(region, tableName, port)
		if (row?.srtpIndexKeyFingerprint === fingerprint) return row
		if (Date.now() > deadline) {
			const rowGeneration = row?.srtpKeyGeneration
			const skewHint =
				typeof rowGeneration === 'number' &&
				rowGeneration > 1_500_000_000 &&
				rowGeneration < 2_000_000_000
					? ` The row's srtpKeyGeneration (${String(rowGeneration)}) is a unix-seconds value from an earlier scheme, not a ${'2,000,000,003'}-style parameter-version one: the deployed backend predates the version-derived generation, so it loads the freshly provisioned keys without their old JSON field at generation 0, which this row fences out entirely - the rotated key genuinely has no persisted floor. Refresh the code the suite deploys from (npm run cdk:prod:deploy) and re-run.`
					: ''
			throw new Error(
				`the replay floor of port ${String(port)} was never persisted under the rotated key (row: ${JSON.stringify(row)}).${skewHint}`,
			)
		}
		await sleep(5_000)
	}
}

/**
 * The per-transport traffic metrics the backend publishes and the stack's
 * zero-ingestion alarms read - asserted here against the deployed system, in
 * the namespace the stack itself derives (the same module both sides import).
 *
 * This is the e2e half of the contract the CDK spec checks at synth time: it
 * proves the deployed publisher emits what the alarms actually query, not only
 * that the two sides' constants agree on paper.
 */
export const waitForTransportMetrics = async (
	region: string,
	stackName: string,
	options: {
		/** The transport dimension value, e.g. 'srtp'. */
		transport: string
		/** Waits for `TransportServing` to report this (default 1). */
		serving?: number
		/** Waits for `ReceivedBytes` to sum to at least this (default 1). */
		bytesAtLeast?: number
		since: Date
		timeoutMs?: number
	},
): Promise<void> => {
	const {
		SERVING_METRIC,
		RECEIVED_BYTES_METRIC,
		TRANSPORT_DIMENSION,
		trafficMetricNamespace,
	} = await import('../src/TrafficMetricNames.ts')
	const cw = new CloudWatchClient({ region })
	const namespace = trafficMetricNamespace(stackName)
	const dimension = [{ Name: TRANSPORT_DIMENSION, Value: options.transport }]
	const deadline = Date.now() + (options.timeoutMs ?? 240_000)
	const wantedServing = options.serving ?? 1
	const wantedBytes = options.bytesAtLeast ?? 1
	// Both metrics are published per minute per instance; the first full minute
	// of data needs the same patience the KVS metric does.
	for (;;) {
		const from = nextPeriodBoundary(options.since)
		const end = new Date()
		if (periodBoundaryElapsed(from) && end > from) {
			const query = async (
				metricName: string,
				statistic: 'Sum' | 'Minimum',
			): Promise<number> => {
				const stats = await cw.send(
					new GetMetricStatisticsCommand({
						Namespace: namespace,
						MetricName: metricName,
						Dimensions: dimension,
						StartTime: from,
						EndTime: end,
						Period: 60,
						Statistics: [statistic],
					}),
				)
				return (stats.Datapoints ?? []).reduce(
					(sum, d) =>
						statistic === 'Sum'
							? sum + (d.Sum ?? 0)
							: Math.min(sum, d.Minimum ?? 1),
					statistic === 'Sum' ? 0 : Number.POSITIVE_INFINITY,
				)
			}
			const [serving, bytes] = await Promise.all([
				query(SERVING_METRIC, 'Minimum'),
				query(RECEIVED_BYTES_METRIC, 'Sum'),
			])
			if (serving === wantedServing && bytes >= wantedBytes) return
			if (Date.now() > deadline) {
				throw new Error(
					`the traffic metrics for ${options.transport} never showed serving=${String(wantedServing)} with >=${String(wantedBytes)} bytes since ${from.toISOString()} (last saw serving=${String(serving)}, bytes=${String(bytes)}) - either the publisher is not publishing what the alarms read, or the transport is not serving`,
				)
			}
		}
		await sleep(15_000)
	}
}

/**
 * Puts the deployed backend code on the instances and restarts the service.
 *
 * This is the step a plain `systemctl restart` cannot stand in for: instances
 * receive code only at boot (the user-data `aws s3 sync`), so a fleet that
 * predates a deploy keeps running whatever it booted with - which is how an e2e
 * run against a freshly deployed stack can find no SRTP listener at all. So
 * this does what boot does, on every instance, idempotently:
 *
 * 1. `aws s3 sync` the deployed `backend/` from the CDK code bucket - the same
 *    command user-data runs at boot.
 * 2. Install the SRTP helper's Python bindings, the way the new user-data does -
 *    non-fatally, because on instances that already have them it is a no-op.
 * 3. `npm install --production` for any new dependencies.
 * 4. `systemctl restart` the service.
 *
 * The runner then waits for the service's own startup log line before running
 * any case (see run.ts), so a fleet that still cannot run the backend fails
 * fast with the reason instead of three minutes into a streaming case.
 */
export const restartBackend = async (
	region: string,
	instanceIds: string[],
	codeBucket: string,
): Promise<void> => {
	const ssm = new SSMClient({ region })
	const command = await ssm.send(
		new SendCommandCommand({
			InstanceIds: instanceIds,
			DocumentName: 'AWS-RunShellScript',
			Comment:
				'video-streaming e2e: deploy backend code and restart the service',
			Parameters: {
				commands: [
					'set -e',
					// The same sync boot performs; idempotent, so instances that
					// already run this code just confirm it.
					`aws s3 sync s3://${codeBucket}/backend/ /opt/video-streaming/ --region ${region}`,
					// The SRTP helper's GObject bindings, installed non-fatally like
					// the user-data installs them: an instance that has them
					// already skips this in seconds, an instance from before the
					// SRTP deploy gains them now.
					'yum install -y python3-gobject-base || echo "WARNING: python3-gobject-base not installed; SRTP ingestion will not work"',
					// The GStreamer srtpdec element, built from the deployed
					// script - Amazon Linux 2023 does not ship it. Idempotent,
					// non-fatal, about 2.5 minutes the first time.
					'bash /opt/video-streaming/install-gst-srtp-plugin.sh || echo "WARNING: the GStreamer srtp plugin could not be installed; SRTP ingestion will not work"',
					'cd /opt/video-streaming',
					'npm install --production',
					'systemctl restart video-streaming.service',
				],
			},
		}),
	)
	const commandId = command.Command?.CommandId
	assert.ok(commandId !== undefined, 'no SSM command id')
	const deadline = Date.now() + 15 * 60_000
	for (const instanceId of instanceIds) {
		for (;;) {
			await sleep(5_000)
			const invocation = await ssm.send(
				new GetCommandInvocationCommand({
					CommandId: commandId,
					InstanceId: instanceId,
				}),
			)
			if (invocation.Status === 'Success') break
			if (
				invocation.Status === 'Failed' ||
				invocation.Status === 'Cancelled' ||
				invocation.Status === 'TimedOut'
			) {
				throw new Error(
					`backend deploy/restart on ${instanceId}: ${String(invocation.Status)}\n${String(invocation.StandardOutputContent)}\n${String(invocation.StandardErrorContent)}`,
				)
			}
			if (Date.now() > deadline) {
				throw new Error(
					`backend deploy/restart on ${instanceId} did not complete in time`,
				)
			}
		}
	}
}

/**
 * Proves media came back, not just a metric: KVS GetMedia returns the fragments
 * themselves.
 *
 * The start selector is tied to the case's start, not to the beginning of the
 * stream's retention: EARLIEST replays everything the stream ever retained, so
 * on a reused stream a previous run's fragments would satisfy this read and
 * the assertion would prove nothing about this run's upload. The server
 * timestamp is the moment KVS received the fragment - it does not depend on
 * the producer's timestamp discipline - and the margin covers the skew
 * between this clock and KVS's (the previous suite run on the same streams is
 * minutes away, far beyond it).
 */
export const readMedia = async (
	region: string,
	streamName: string,
	since: Date,
): Promise<number> => {
	const kv = new KinesisVideoClient({ region })
	const endpoint = await kv.send(
		new GetDataEndpointCommand({
			StreamName: streamName,
			APIName: 'GET_MEDIA',
		}),
	)
	assert.ok(endpoint.DataEndpoint !== undefined, 'no data endpoint')
	const media = new KinesisVideoMediaClient({
		region,
		endpoint: endpoint.DataEndpoint,
	})
	const result = await media.send(
		new GetMediaCommand({
			StreamName: streamName,
			StartSelector: {
				StartSelectorType: 'SERVER_TIMESTAMP',
				StartTimestamp: new Date(since.getTime() - 30_000),
			},
		}),
	)
	const payload = result.Payload
	if (payload === undefined) return 0
	// Bounded on purpose: GetMedia streams continuously (it follows the live
	// stream, not just the backog), so an unbounded read never ends. The
	// assertion only needs some bytes back, not the whole archive.
	const chunks: Buffer[] = []
	let total = 0
	const deadline = Date.now() + 30_000
	for await (const chunk of payload as AsyncIterable<Uint8Array>) {
		chunks.push(Buffer.from(chunk))
		total += chunk.byteLength
		if (total > 1_000_000 || Date.now() > deadline) break
	}
	if (typeof (payload as { destroy?: () => void }).destroy === 'function') {
		;(payload as { destroy: () => void }).destroy()
	}
	return Buffer.concat(chunks).length
}

const sleep = async (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms))
