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
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb'
import assert from 'node:assert/strict'

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
 * Waits until the KVS stream received media after `since`, using the metric the
 * alarms use. This is the e2e definition of "it works": bytes reached Kinesis.
 */
export const waitForStreamIngestion = async (
	region: string,
	streamName: string,
	since: Date,
	timeoutMs = 180_000,
): Promise<number> => {
	const cw = new CloudWatchClient({ region })
	const deadline = Date.now() + timeoutMs
	for (;;) {
		const end = new Date()
		const stats = await cw.send(
			new GetMetricStatisticsCommand({
				Namespace: 'AWS/KinesisVideo',
				MetricName: 'PutMedia.IncomingBytes',
				Dimensions: [{ Name: 'StreamName', Value: streamName }],
				StartTime: since,
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
			throw new Error(
				`no PutMedia.IncomingBytes on ${streamName} since ${since.toISOString()}`,
			)
		}
		await sleep(15_000)
	}
}

/**
 * Asserts the opposite: no media reached the stream in the window. Missing
 * datapoints count as zero - a stream nobody writes to publishes nothing at all.
 */
export const assertNoStreamIngestion = async (
	region: string,
	streamName: string,
	since: Date,
): Promise<void> => {
	const cw = new CloudWatchClient({ region })
	const stats = await cw.send(
		new GetMetricStatisticsCommand({
			Namespace: 'AWS/KinesisVideo',
			MetricName: 'PutMedia.IncomingBytes',
			Dimensions: [{ Name: 'StreamName', Value: streamName }],
			StartTime: since,
			EndTime: new Date(),
			Period: 60,
			Statistics: ['Sum'],
		}),
	)
	const total = (stats.Datapoints ?? []).reduce(
		(sum, d) => sum + (d.Sum ?? 0),
		0,
	)
	assert.equal(total, 0, `${streamName} received media it must not have`)
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
	const logs = new CloudWatchLogsClient({ region })
	const deadline = Date.now() + timeoutMs
	for (;;) {
		const started = await logs.send(
			new StartQueryCommand({
				logGroupName: logGroup,
				startTime: Math.floor(since.getTime() / 1000),
				endTime: Math.floor(Date.now() / 1000),
				queryString: `fields @message | filter @message like /${filter}/ | sort @timestamp asc`,
			}),
		)
		assert.ok(started.queryId !== undefined, 'no query id')
		for (;;) {
			await sleep(3_000)
			const results = await logs.send(
				new GetQueryResultsCommand({ queryId: started.queryId }),
			)
			if (results.status === 'Running') continue
			const lines = (results.results ?? [])
				.flatMap((row) => row.map((f) => f.value ?? ''))
				.filter((v) => v.length > 0)
			if (lines.length > 0) return lines
			// The query completed with nothing: either the events have not been
			// indexed yet, or they truly are not there. Re-query until the deadline.
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
 */
export const readMedia = async (
	region: string,
	streamName: string,
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
			StartSelector: { StartSelectorType: 'EARLIEST' },
		}),
	)
	const payload = result.Payload
	if (payload === undefined) return 0
	const chunks: Buffer[] = []
	for await (const chunk of payload as AsyncIterable<Uint8Array>) {
		chunks.push(Buffer.from(chunk))
	}
	return Buffer.concat(chunks).length
}

const sleep = async (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms))
