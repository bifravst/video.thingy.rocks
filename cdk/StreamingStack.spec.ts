import { App } from 'aws-cdk-lib'
import { Template } from 'aws-cdk-lib/assertions'
import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { StreamingStack } from './StreamingStack.ts'

const STACK_NAME = 'video-streaming-2026-05'
const UNENCRYPTED_PORTS = Array.from({ length: 10 }, (_, i) => 5000 + i)
const SRTP_PORTS = Array.from({ length: 10 }, (_, i) => 6000 + i)

/**
 * Synthesises the stack the way a deployment does.
 *
 * No context beyond what cdk.context.json holds: a stack that needs extra context to
 * synthesise cannot be diffed or destroyed by the repository's own scripts, which pass
 * none.
 */
const synthesise = (): Template => {
	const app = new App({ context: { version: '0.0.0-test', isTest: false } })
	const stack = new StreamingStack(app, STACK_NAME, {
		env: { account: '123456789012', region: 'eu-central-1' },
		availabilityZones: new Set(['eu-central-1a', 'eu-central-1b']),
	})
	return Template.fromStack(stack)
}

/** Synthesising takes about a second, so it happens once for the whole suite. */
let cached: Template | undefined
const template = (): Template => {
	cached ??= synthesise()
	return cached
}

/**
 * Ingress rules added to a security group this stack owns are inlined onto the group
 * rather than emitted as separate resources.
 */
const ingressRules = (): {
	IpProtocol?: string
	FromPort?: number
	ToPort?: number
	CidrIp?: unknown
	CidrIpv6?: unknown
}[] => {
	const groups = template().findResources('AWS::EC2::SecurityGroup')
	return Object.values(groups).flatMap(
		(group) =>
			(group.Properties?.SecurityGroupIngress ?? []) as ReturnType<
				typeof ingressRules
			>,
	)
}

void describe('StreamingStack', () => {
	void it('synthesises with no extra context', () => {
		assert.doesNotThrow(() => synthesise())
	})

	void describe('Kinesis Video Streams', () => {
		void it('creates one stream per ingest port on both transports', () => {
			template().resourceCountIs('AWS::KinesisVideo::Stream', 20)
		})

		/**
		 * The guard against a destructive rename.
		 *
		 * Kinesis Video has no rename operation, so changing a stream's name replaces
		 * the stream and drops up to thirty days of retained media. Sharing one set of
		 * streams between the two transports would have required exactly that rename,
		 * and then a whole fleet cutover to keep old and new code from writing to
		 * different names at once. Per-transport streams avoid all of it - as long as
		 * these names and logical IDs never move.
		 */
		void it('keeps the existing streams at their existing names and logical IDs', () => {
			const synthesised = template()
			const streams = synthesised.findResources('AWS::KinesisVideo::Stream')
			for (const port of UNENCRYPTED_PORTS) {
				const logicalId = `KinesisVideoStream${String(port)}`
				assert.ok(
					Object.keys(streams).includes(logicalId),
					`${logicalId} must keep its logical ID, or CloudFormation replaces the stream`,
				)
				synthesised.hasResourceProperties('AWS::KinesisVideo::Stream', {
					Name: `${STACK_NAME}-video-${String(port)}`,
				})
			}
		})

		void it('adds the SRTP streams under their own names', () => {
			const synthesised = template()
			for (const port of SRTP_PORTS) {
				synthesised.hasResourceProperties('AWS::KinesisVideo::Stream', {
					Name: `${STACK_NAME}-video-${String(port)}`,
				})
			}
		})
	})

	void describe('load balancing', () => {
		void it('creates a target group and a listener per ingest port', () => {
			const synthesised = template()
			synthesised.resourceCountIs(
				'AWS::ElasticLoadBalancingV2::TargetGroup',
				20,
			)
			synthesised.resourceCountIs('AWS::ElasticLoadBalancingV2::Listener', 20)
		})

		// Every target group health-checks 9999, SRTP included: with ELB health checks
		// the Auto Scaling group replaces an instance that any attached target group
		// reports unhealthy, so a per-transport health port would let an SRTP-only
		// fault churn the whole fleet.
		void it('health-checks every target group on the same backend port', () => {
			const groups = template().findResources(
				'AWS::ElasticLoadBalancingV2::TargetGroup',
			)
			const ports = Object.values(groups).map(
				(group) => group.Properties?.HealthCheckPort,
			)
			assert.strictEqual(ports.length, 20)
			assert.deepStrictEqual([...new Set(ports)], ['9999'])
		})

		void it('keeps the Auto Scaling group on ELB health checks', () => {
			// Not EC2 health checks: the ASG must not terminate old instances before
			// replacements can actually serve traffic.
			template().hasResourceProperties('AWS::AutoScaling::AutoScalingGroup', {
				HealthCheckType: 'ELB',
				MinSize: '2',
			})
		})

		void it('attaches every target group to the fleet', () => {
			const groups = template().findResources(
				'AWS::AutoScaling::AutoScalingGroup',
			)
			const arns = Object.values(groups)[0]?.Properties?.TargetGroupARNs
			assert.strictEqual((arns as unknown[]).length, 20)
		})
	})

	void describe('security group', () => {
		void it('opens both UDP ranges to v4 and v6', () => {
			const rules = ingressRules()
			for (const [from, to] of [
				[5000, 5009],
				[6000, 6009],
			]) {
				for (const family of ['CidrIp', 'CidrIpv6'] as const) {
					assert.ok(
						rules.some(
							(rule) =>
								rule.IpProtocol === 'udp' &&
								rule.FromPort === from &&
								rule.ToPort === to &&
								rule[family] !== undefined,
						),
						`missing ${family} rule for udp ${String(from)}-${String(to)}`,
					)
				}
			}
		})

		// One health port, so there is nothing else to open.
		void it('opens only the one TCP health port', () => {
			const tcpPorts = ingressRules()
				.filter((rule) => rule.IpProtocol === 'tcp')
				.map((rule) => rule.FromPort)
			assert.deepStrictEqual([...new Set(tcpPorts)], [9999])
		})
	})

	void describe('SRTP key access', () => {
		// Hardcoding "arn:aws:" would be wrong in any other partition.
		void it('scopes the parameter read to this stack, using the stack partition', () => {
			const policies = template().findResources('AWS::IAM::Policy')
			const statements = Object.values(policies).flatMap(
				(policy) =>
					(policy.Properties?.PolicyDocument?.Statement ?? []) as {
						Action?: unknown
						Resource?: unknown
					}[],
			)
			const ssm = statements.filter((statement) =>
				JSON.stringify(statement.Action ?? '').includes('ssm:GetParameter'),
			)
			assert.strictEqual(ssm.length, 1, 'exactly one SSM statement')
			const resource = JSON.stringify(ssm[0]?.Resource)
			assert.ok(
				resource.includes('AWS::Partition'),
				`partition must come from the stack: ${resource}`,
			)
			assert.ok(
				resource.includes(`${STACK_NAME}/srtp/`),
				`must be scoped to this stack's parameters: ${resource}`,
			)
		})
	})

	void describe('alarms', () => {
		void it('watches each transport separately', () => {
			const alarms = template().findResources('AWS::CloudWatch::Alarm')
			const names = Object.values(alarms).map(
				(alarm) => alarm.Properties?.AlarmName as string | undefined,
			)
			assert.ok(names.includes(`${STACK_NAME}-KVS-PutMedia-Incoming-Zero`))
			assert.ok(names.includes(`${STACK_NAME}-KVS-PutMedia-Incoming-Zero-SRTP`))
		})

		/**
		 * No SRTP ingestion is the normal state until devices are provisioned, so this
		 * alarm notifies and nothing more. Wiring it to the restart topic - as the
		 * unencrypted composite alarm is - would reboot the fleet for as long as no
		 * SRTP device happened to be sending.
		 */
		void it('keeps the SRTP alarm notify-only and not breaching on missing data', () => {
			const alarms = template().findResources('AWS::CloudWatch::Alarm')
			const srtp = Object.values(alarms).find(
				(alarm) =>
					alarm.Properties?.AlarmName ===
					`${STACK_NAME}-KVS-PutMedia-Incoming-Zero-SRTP`,
			)
			assert.strictEqual(
				(srtp?.Properties?.AlarmActions as unknown[]).length,
				1,
				'one action: notify',
			)
			assert.strictEqual(srtp?.Properties?.TreatMissingData, 'notBreaching')
		})

		void it('gives each zero-ingestion alarm its own transport metrics', () => {
			const alarms = template().findResources('AWS::CloudWatch::Alarm')
			for (const [name, port] of [
				[`${STACK_NAME}-KVS-PutMedia-Incoming-Zero`, 5000],
				[`${STACK_NAME}-KVS-PutMedia-Incoming-Zero-SRTP`, 6000],
			] as const) {
				const alarm = Object.values(alarms).find(
					(candidate) => candidate.Properties?.AlarmName === name,
				)
				const metrics = JSON.stringify(alarm?.Properties?.Metrics)
				assert.ok(
					metrics.includes(`${STACK_NAME}-video-${String(port)}`),
					`${name} must watch port ${String(port)}`,
				)
			}
		})
	})

	void describe('the fleet cutover machinery is absent', () => {
		// The rename these existed to survive is gone, so none of it should come back.
		void it('has no readiness custom resource and no generation-suffixed resources', () => {
			const synthesised = template()
			synthesised.resourceCountIs('Custom::FleetCutoverReadiness', 0)
			const logicalIds = Object.keys(
				synthesised.toJSON().Resources as Record<string, unknown>,
			)
			const suspicious = logicalIds.filter((id) =>
				/gen2|Legacy|FleetReadiness|FleetCutover/i.test(id),
			)
			assert.deepStrictEqual(suspicious, [])
		})
	})

	void describe('instance bootstrap', () => {
		/**
		 * An unsubstituted placeholder would reach the instance verbatim, and the
		 * backend would read it as a literal value - disabling the ingest path it
		 * configures rather than failing loudly.
		 */
		void it('leaves no placeholder unsubstituted in the user data', () => {
			const templates = template().findResources('AWS::EC2::LaunchTemplate')
			const userData = JSON.stringify(
				Object.values(templates).map(
					(launchTemplate) =>
						launchTemplate.Properties?.LaunchTemplateData?.UserData,
				),
			)
			const leftover = userData.match(/__[A-Z_]+__/g)
			assert.deepStrictEqual(leftover, null)
		})

		void it('configures the SRTP key parameter path and port range', () => {
			const templates = template().findResources('AWS::EC2::LaunchTemplate')
			const userData = JSON.stringify(
				Object.values(templates).map(
					(launchTemplate) =>
						launchTemplate.Properties?.LaunchTemplateData?.UserData,
				),
			)
			assert.ok(userData.includes('SRTP_PORT_RANGE_START=6000'))
			assert.ok(userData.includes('SRTP_PORT_RANGE_END=6009'))
			// SRTP is enabled by this being set, so the path has to be the real one.
			assert.ok(userData.includes('/srtp/port'))
		})
	})

	void describe('stream metadata table', () => {
		// The lock row is keyed by the raw ingest port, unchanged, so there is no data
		// migration and the existing rows stay meaningful.
		void it('keeps the port-keyed schema', () => {
			template().hasResourceProperties('AWS::DynamoDB::Table', {
				KeySchema: [{ AttributeName: 'port', KeyType: 'HASH' }],
				AttributeDefinitions: [{ AttributeName: 'port', AttributeType: 'N' }],
			})
		})
	})
})
