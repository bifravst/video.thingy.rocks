import { App } from 'aws-cdk-lib'
import { Template } from 'aws-cdk-lib/assertions'
import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, it } from 'node:test'

import {
	SRTP_TRANSPORT,
	UNENCRYPTED_TRANSPORT,
} from '../backend/src/TrafficMetricNames.ts'
import { trafficMetricRequest } from '../backend/src/TrafficMetricRequest.ts'
import { StreamingStack } from './StreamingStack.ts'

const STACK_NAME = 'video-streaming-2026-05'
const UNENCRYPTED_PORTS = Array.from({ length: 10 }, (_, i) => 5000 + i)
const SRTP_PORTS = Array.from({ length: 10 }, (_, i) => 6000 + i)

/** One entry of a synthesised template's Resources, as findResources returns it. */
type TemplateResource = { Properties?: Record<string, unknown> }

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
		const resourcesOfType = (type: string): [string, TemplateResource][] =>
			Object.entries(
				template().findResources(type) as Record<string, TemplateResource>,
			)

		const named = (type: string, name: string): [string, TemplateResource] => {
			const entry = resourcesOfType(type).find(
				([, resource]) => resource.Properties?.AlarmName === name,
			)
			assert.ok(entry !== undefined, `no ${type} named ${name}`)
			return entry
		}

		const alarmNamed = (name: string): TemplateResource =>
			named('AWS::CloudWatch::Alarm', name)[1]

		const compositeNamed = (name: string): TemplateResource =>
			named('AWS::CloudWatch::CompositeAlarm', name)[1]

		/** Logical ID of the alarm with this name, as an alarm rule refers to it. */
		const logicalIdOf = (type: string, name: string): string =>
			named(type, name)[0]

		// The dimension values come from the backend's own constants, never from a
		// literal written here: a literal in the spec is authored from the same
		// assumption as the code it checks, so it agrees with a wrong value as readily
		// as a right one. That is how the SRTP leg came to query `SRTP` against a
		// dimension published as `srtp`, with a passing test asserting it.
		const transports = [
			{ label: 'Unencrypted', transport: UNENCRYPTED_TRANSPORT, port: 5000 },
			{ label: 'SRTP', transport: SRTP_TRANSPORT, port: 6000 },
		] as const

		void it('watches each transport separately', () => {
			for (const { label } of transports) {
				compositeNamed(`${STACK_NAME}-UDP-Traffic-No-KVS-Ingestion-${label}`)
			}
		})

		/**
		 * Every alarm on a backend metric must name something the backend publishes.
		 *
		 * The two sides have to agree on a namespace, a metric name and a dimension
		 * *value*, and disagreeing on any of them is invisible: CloudWatch does not
		 * complain about an alarm watching a metric nobody publishes, it simply reports
		 * no data, which reads as "no traffic" and never fires. The SRTP traffic leg
		 * shipped querying `SRTP` for a dimension published as `srtp` - dimension
		 * values are case-sensitive, so that whole fault composite was dead.
		 *
		 * Both sides are therefore compared against each other rather than against
		 * literals: this builds the request the backend would actually send and checks
		 * every alarm resolves to something in it. A fixture would only restate
		 * whichever value was written first.
		 */
		void it('alarms only on metrics the backend actually publishes', () => {
			const at = new Date()
			const request = trafficMetricRequest(
				transports.map(({ transport }) => ({
					transport,
					bytes: 0,
					serving: 1,
					at,
				})),
				STACK_NAME,
			)
			const publishes = new Set(
				(request.MetricData ?? []).map((datum) =>
					JSON.stringify([
						request.Namespace,
						datum.MetricName,
						datum.Dimensions,
					]),
				),
			)

			const watched = resourcesOfType('AWS::CloudWatch::Alarm')
				.map(([, alarm]) => alarm.Properties ?? {})
				.filter((props) => props.Namespace === request.Namespace)
				.map((props) =>
					JSON.stringify([props.Namespace, props.MetricName, props.Dimensions]),
				)

			assert.ok(
				watched.length > 0,
				'no alarm reads the backend namespace at all',
			)
			for (const metric of watched) {
				assert.ok(
					publishes.has(metric),
					`an alarm watches ${metric}, which the backend never publishes. It publishes ${[...publishes].join(', ')}`,
				)
			}
		})

		void it('gives each zero-ingestion alarm its own transport metrics', () => {
			for (const { label, port } of transports) {
				const alarm = alarmNamed(
					`${STACK_NAME}-KVS-PutMedia-Incoming-Zero-${label}`,
				)
				assert.ok(
					JSON.stringify(alarm.Properties?.Metrics ?? null).includes(
						`${STACK_NAME}-video-${String(port)}`,
					),
					`the ${label} alarm must watch port ${String(port)}`,
				)
			}
		})

		/**
		 * The traffic leg has to be scoped to the same transport as the ingestion leg.
		 *
		 * The load balancer's ProcessedBytes_UDP counts both transports, because
		 * CloudWatch publishes no per-listener or per-target-group version of it. Built
		 * on that, the pairing compares "is anything arriving anywhere" against "is
		 * this transport reaching Kinesis", so traffic on the other transport decides
		 * the answer. The backend publishes a per-transport byte count instead.
		 */
		void it('scopes each traffic leg to its own transport', () => {
			for (const { label, transport } of transports) {
				const alarm = alarmNamed(`${STACK_NAME}-UDP-Traffic-${label}`)
				assert.strictEqual(
					alarm.Properties?.Namespace,
					`${STACK_NAME}/ingest`,
					'the traffic leg must read the backend metric, not the load balancer',
				)
				assert.strictEqual(alarm.Properties?.MetricName, 'ReceivedBytes')
				assert.deepStrictEqual(alarm.Properties?.Dimensions, [
					{ Name: 'Transport', Value: transport },
				])
			}
		})

		void it('reads no load-balancer-wide byte count anywhere', () => {
			const alarms = JSON.stringify(
				template().findResources('AWS::CloudWatch::Alarm'),
			)
			assert.ok(
				!alarms.includes('ProcessedBytes_UDP'),
				'a load-balancer-wide metric cannot describe one transport',
			)
		})

		/**
		 * Each per-transport fault is traffic arriving AND nothing being ingested.
		 *
		 * Both conditions, in the positive sense: the rule used to negate a
		 * traffic-is-high alarm, which made it read "traffic is *below* 1 MB/s and
		 * ingestion is zero" - the opposite of its own description, and true of an idle
		 * fleet.
		 */
		void it('requires traffic and no ingestion on the same transport', () => {
			for (const { label } of transports) {
				const rule = JSON.stringify(
					compositeNamed(`${STACK_NAME}-UDP-Traffic-No-KVS-Ingestion-${label}`)
						.Properties?.AlarmRule,
				)
				assert.ok(
					!rule.includes('NOT'),
					`the ${label} rule must not negate a leg: ${rule}`,
				)
				for (const legName of [
					`${STACK_NAME}-UDP-Traffic-${label}`,
					`${STACK_NAME}-KVS-PutMedia-Incoming-Zero-${label}`,
				]) {
					assert.ok(
						rule.includes(logicalIdOf('AWS::CloudWatch::Alarm', legName)),
						`the ${label} rule must include ${legName}: ${rule}`,
					)
				}
			}
		})

		/**
		 * A transport whose devices are not provisioned yet must stay silent, and one
		 * that breaks after they are must not.
		 *
		 * Those two used to be in conflict: the SRTP alarm read the Kinesis metric
		 * alone, and a producer that never starts publishes no samples at all. Treating
		 * that gap as healthy was the only way to stay quiet before devices existed -
		 * and it also suppressed the alarm when a working SRTP path stopped, which is
		 * the failure the alarm is for. Gating on traffic separates them, so the gap
		 * can now be read as the fault it is.
		 */
		void it('treats absent ingestion data as a fault, gated on traffic', () => {
			for (const { label } of transports) {
				const ingestion = alarmNamed(
					`${STACK_NAME}-KVS-PutMedia-Incoming-Zero-${label}`,
				)
				assert.strictEqual(
					ingestion.Properties?.TreatMissingData,
					'breaching',
					'no PutMedia samples is exactly the condition',
				)
				assert.strictEqual(
					ingestion.Properties?.AlarmActions,
					undefined,
					'alone it would fire on an idle transport, so it must not notify',
				)

				const traffic = alarmNamed(`${STACK_NAME}-UDP-Traffic-${label}`)
				assert.strictEqual(
					traffic.Properties?.TreatMissingData,
					'notBreaching',
					'without a sample there is no evidence traffic is arriving',
				)
			}
		})

		void it('notifies on each transport fault', () => {
			for (const { label } of transports) {
				const actions = compositeNamed(
					`${STACK_NAME}-UDP-Traffic-No-KVS-Ingestion-${label}`,
				).Properties?.AlarmActions as unknown[] | undefined
				assert.strictEqual(actions?.length, 1, `${label} must notify`)
			}
		})

		/**
		 * The restart fires when *either* transport is faulty.
		 *
		 * The transports fail independently - a missing GStreamer plugin or an
		 * unreachable key store takes SRTP down by itself - so requiring both would let
		 * a healthy SRTP path suppress the restart an unencrypted failure needs.
		 */
		void it('restarts the fleet when either transport is losing its traffic', () => {
			const composite = compositeNamed(
				`${STACK_NAME}-UDP-Traffic-No-KVS-Ingestion`,
			)
			const rule = JSON.stringify(composite.Properties?.AlarmRule)
			assert.ok(
				rule.includes('OR'),
				`either transport must trigger it: ${rule}`,
			)
			for (const { label } of transports) {
				assert.ok(
					rule.includes(
						logicalIdOf(
							'AWS::CloudWatch::CompositeAlarm',
							`${STACK_NAME}-UDP-Traffic-No-KVS-Ingestion-${label}`,
						),
					),
					`the restart composite must include ${label}: ${rule}`,
				)
			}
			// Notify and restart, in that order of severity.
			assert.strictEqual(
				(composite.Properties?.AlarmActions as unknown[]).length,
				2,
			)
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

	/**
	 * The CDK is installed from the repository root, which carries none of the
	 * backend's AWS SDK clients - so every backend module the CDK code reads has to
	 * load without them.
	 *
	 * This spec once imported the metric request builder from the module that also
	 * publishes it, and so pulled in @aws-sdk/client-cloudwatch. It passed only because
	 * backend/node_modules happened to be installed alongside; a clean root install
	 * would have failed before the first CDK test. Checked by actually loading each
	 * module with the SDK made unresolvable, rather than by reading import lines, so a
	 * dependency that arrives transitively fails too.
	 */
	void it('reads only backend modules that load without the AWS SDK', () => {
		const imported = new Set<string>()
		for (const file of readdirSync('cdk').filter((f) => f.endsWith('.ts'))) {
			const source = readFileSync(join('cdk', file), 'utf8')
			for (const [, specifier] of source.matchAll(
				/from '\.\.\/(backend\/[^']+)'/g,
			)) {
				if (specifier !== undefined) imported.add(`./${specifier}`)
			}
		}
		assert.ok(imported.size > 0, 'expected the CDK to read some backend module')

		const loader = `
			const { registerHooks } = require('node:module')
			registerHooks({
				resolve(specifier, context, next) {
					if (specifier.startsWith('@aws-sdk/')) throw new Error('needs ' + specifier)
					return next(specifier, context)
				},
			})
			const modules = ${JSON.stringify([...imported])}
			Promise.allSettled(modules.map((m) => import(m))).then((results) => {
				const failed = results
					.map((r, i) => r.status === 'rejected' ? modules[i] + ': ' + r.reason.message : '')
					.filter(Boolean)
				console.log(failed.join('\\n'))
				process.exitCode = failed.length === 0 ? 0 : 1
			})
		`
		const result = spawnSync(
			process.execPath,
			['--no-warnings', '--experimental-transform-types', '-e', loader],
			{ encoding: 'utf8', timeout: 60_000 },
		)
		assert.strictEqual(
			result.status,
			0,
			`the CDK reads backend modules that need the backend's AWS SDK:\n${result.stdout}${result.stderr}`,
		)
	})
})
