import {
	CfnOutput,
	Duration,
	Fn,
	RemovalPolicy,
	Size,
	Stack,
	type Environment,
} from 'aws-cdk-lib'
import * as autoscaling from 'aws-cdk-lib/aws-autoscaling'
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch'
import * as cloudwatch_actions from 'aws-cdk-lib/aws-cloudwatch-actions'
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb'
import * as ec2 from 'aws-cdk-lib/aws-ec2'
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2'
import * as events from 'aws-cdk-lib/aws-events'
import * as target from 'aws-cdk-lib/aws-events-targets'
import * as iam from 'aws-cdk-lib/aws-iam'
import * as kinesisvideo from 'aws-cdk-lib/aws-kinesisvideo'
import * as lambda from 'aws-cdk-lib/aws-lambda'
import * as lambdanode from 'aws-cdk-lib/aws-lambda-nodejs'
import * as logs from 'aws-cdk-lib/aws-logs'
import * as s3 from 'aws-cdk-lib/aws-s3'
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment'
import * as sns from 'aws-cdk-lib/aws-sns'
import * as sns_subscriptions from 'aws-cdk-lib/aws-sns-subscriptions'
import type { Construct } from 'constructs'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

export class StreamingStack extends Stack {
	public readonly vpc: ec2.Vpc
	public readonly streamTable: dynamodb.Table
	public readonly udpSecurityGroup: ec2.SecurityGroup
	public readonly ec2Role: iam.Role
	public readonly autoScalingGroup: autoscaling.AutoScalingGroup
	public readonly codeBucket: s3.Bucket
	public readonly kinesisVideoStreams: kinesisvideo.CfnStream[] = []
	public readonly networkLoadBalancer: elbv2.NetworkLoadBalancer

	constructor(
		scope: Construct,
		id: string,
		props?: {
			env?: Environment
			availabilityZones: Set<string>
		},
	) {
		super(scope, id, {
			env: props?.env,
		})

		this.vpc = new ec2.Vpc(this, 'StreamingVPC', {
			ipProtocol: ec2.IpProtocol.DUAL_STACK,
			natGateways: 0,
			subnetConfiguration: [
				{
					cidrMask: 24,
					name: 'Public',
					subnetType: ec2.SubnetType.PUBLIC,
					ipv6AssignAddressOnCreation: true,
				},
			],
			availabilityZones: Array.from(props?.availabilityZones ?? []).slice(0, 2),
		})

		// Create CloudWatch Log Groups for EC2 instances
		new logs.LogGroup(this, 'ApplicationLogGroup', {
			logGroupName: `${this.stackName}/application`,
			retention: logs.RetentionDays.ONE_WEEK,
			removalPolicy: RemovalPolicy.DESTROY,
		})

		new logs.LogGroup(this, 'SystemLogGroup', {
			logGroupName: `${this.stackName}/system`,
			retention: logs.RetentionDays.ONE_WEEK,
			removalPolicy: RemovalPolicy.DESTROY,
		})

		new logs.LogGroup(this, 'CloudInitLogGroup', {
			logGroupName: `${this.stackName}/cloud-init`,
			retention: logs.RetentionDays.ONE_WEEK,
			removalPolicy: RemovalPolicy.DESTROY,
		})

		this.streamTable = new dynamodb.Table(this, 'StreamMetadata', {
			partitionKey: { name: 'port', type: dynamodb.AttributeType.NUMBER },
			billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
			removalPolicy: RemovalPolicy.DESTROY,
		})

		// Ingest ports. The unencrypted MPEG-TS transport is the original path; SRTP is
		// additive and uses its own ports, keys and streams.
		const portRange = (start: number, end: number): number[] =>
			Array.from({ length: end - start + 1 }, (_, i) => start + i)
		const unencryptedPorts = portRange(5000, 5009)
		const srtpPorts = portRange(6000, 6009)

		// Kinesis Video Streams: one per ingest port, on both transports.
		//
		// Each transport gets its own streams rather than sharing one set, which keeps
		// this deployment purely additive: the existing 5000-5009 streams keep both
		// their construct IDs and their names, so nothing is renamed or replaced.
		// Kinesis Video has no rename operation, so a changed name replaces the stream
		// and drops its retained media - and sharing a stream between two transports
		// would also mean two producers arbitrating over one destination.
		const kinesisStreamPrefix = `${this.stackName}-video`
		for (const port of [
			...unencryptedPorts,
			// SRTP ports, whose streams are additions.
			...srtpPorts,
		]) {
			const stream = new kinesisvideo.CfnStream(
				this,
				`KinesisVideoStream${port}`,
				{
					name: `${kinesisStreamPrefix}-${port}`,
					dataRetentionInHours: Duration.days(30).toHours(),
					mediaType: 'video/h264',
				},
			)
			this.kinesisVideoStreams.push(stream)
		}

		this.udpSecurityGroup = new ec2.SecurityGroup(this, 'UDPSecurityGroup', {
			vpc: this.vpc,
			description: 'Security group for UDP video ingestion',
			allowAllOutbound: false,
		})

		// Allow UDP ingress on ports 5000-5009 (NLB forwards as IPv6 to instances since
		// the target group is IPv6, so IPv6 ingress is required even for IPv4 clients)
		this.udpSecurityGroup.addIngressRule(
			ec2.Peer.anyIpv4(),
			ec2.Port.udpRange(5000, 5009),
			'Allow UDP video ingestion on ports 5000-5009',
		)
		this.udpSecurityGroup.addIngressRule(
			ec2.Peer.anyIpv6(),
			ec2.Port.udpRange(5000, 5009),
			'Allow UDP video ingestion on ports 5000-5009 (IPv6)',
		)
		// Allow UDP ingress on the SRTP ports (6000-6009), same dual-stack reasoning
		// as the unencrypted range above.
		this.udpSecurityGroup.addIngressRule(
			ec2.Peer.anyIpv4(),
			ec2.Port.udpRange(6000, 6009),
			'Allow SRTP video ingestion on ports 6000-6009',
		)
		this.udpSecurityGroup.addIngressRule(
			ec2.Peer.anyIpv6(),
			ec2.Port.udpRange(6000, 6009),
			'Allow SRTP video ingestion on ports 6000-6009 (IPv6)',
		)
		// Allow TCP health checks from NLB (originates within VPC)
		this.udpSecurityGroup.addIngressRule(
			ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
			ec2.Port.tcp(9999),
			'Allow NLB TCP health checks on port 9999',
		)
		this.udpSecurityGroup.addIngressRule(
			ec2.Peer.anyIpv6(),
			ec2.Port.tcp(9999),
			'Allow NLB TCP health checks on port 9999 (IPv6)',
		)

		// Allow HTTPS egress for AWS service communication
		this.udpSecurityGroup.addEgressRule(
			ec2.Peer.anyIpv4(),
			ec2.Port.tcp(443),
			'Allow HTTPS egress for AWS service communication',
		)
		// Allow HTTP egress so user-data can download kvssink build deps (autoconf, automake, log4cplus from ftp.gnu.org etc.)
		this.udpSecurityGroup.addEgressRule(
			ec2.Peer.anyIpv4(),
			ec2.Port.tcp(80),
			'Allow HTTP egress for Kinesis SDK dependency downloads during bootstrap',
		)

		this.ec2Role = new iam.Role(this, 'EC2InstanceRole', {
			assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
			description: 'IAM role for EC2 instances running UDP listener service',
			managedPolicies: [
				iam.ManagedPolicy.fromAwsManagedPolicyName(
					'AmazonSSMManagedInstanceCore',
				),
			],
		})

		// Grant DynamoDB permissions for stream metadata
		this.streamTable.grantReadWriteData(this.ec2Role)

		// Grant Kinesis Video Streams: GetDataEndpoint (control plane) and PutMedia (data plane)
		this.ec2Role.addToPolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: [
					'kinesisvideo:GetDataEndpoint',
					'kinesisvideo:DescribeStream',
					'kinesisvideo:PutMedia',
				],
				resources: ['*'],
			}),
		)

		// Grant read access to the SRTP keys, scoped to this stack's parameters.
		// The partition comes from the stack rather than being hardcoded as "aws", so
		// this is still correct in other partitions.
		this.ec2Role.addToPolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: ['ssm:GetParameter', 'ssm:GetParameters'],
				resources: [
					`arn:${this.partition}:ssm:${this.region}:${this.account}:parameter/${this.stackName}/srtp/*`,
				],
			}),
		)

		// Grant CloudWatch permissions for metrics and logs
		this.ec2Role.addToPolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: ['cloudwatch:PutMetricData'],
				resources: ['*'],
			}),
		)

		this.ec2Role.addToPolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: [
					'logs:CreateLogGroup',
					'logs:CreateLogStream',
					'logs:PutLogEvents',
					'logs:DescribeLogStreams',
				],
				resources: [
					`arn:aws:logs:${this.region}:${this.account}:log-group:/video-streaming/*`,
				],
			}),
		)

		// Create S3 bucket for application code
		this.codeBucket = new s3.Bucket(this, 'CodeBucket', {
			encryption: s3.BucketEncryption.S3_MANAGED,
			blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
			removalPolicy: RemovalPolicy.DESTROY,
			autoDeleteObjects: true,
		})

		// Grant EC2 role read access to code bucket
		this.codeBucket.grantRead(this.ec2Role)

		// Deploy backend code to S3
		const __filename = fileURLToPath(import.meta.url)
		const __dirname = dirname(__filename)
		const backendPath = join(__dirname, '..', 'backend')

		new s3deploy.BucketDeployment(this, 'DeployBackendCode', {
			sources: [
				s3deploy.Source.asset(backendPath, {
					exclude: ['node_modules', 'node_modules/**/*'],
				}),
			],
			destinationBucket: this.codeBucket,
			destinationKeyPrefix: 'backend/',
			memoryLimit: 512,
			ephemeralStorageSize: Size.mebibytes(1024),
		})

		// Read user data script
		const userDataScriptPath = join(__dirname, 'user-data.sh')
		let userDataScript = readFileSync(userDataScriptPath, 'utf-8')

		// Replace placeholders in user data script
		userDataScript = userDataScript
			.replace(/__AWS_REGION__/g, this.region)
			.replace(/__TABLE_NAME__/g, this.streamTable.tableName)
			.replace(/__KINESIS_STREAM_PREFIX__/g, kinesisStreamPrefix)
			.replace(/__SRTP_KEY_PARAMETER_PREFIX__/g, `/${this.stackName}/srtp/port`)
			.replace(/__SRTP_PORT_RANGE_START__/g, String(srtpPorts[0]))
			.replace(/__SRTP_PORT_RANGE_END__/g, String(srtpPorts.at(-1)))
			.replace(/__CODE_BUCKET__/g, this.codeBucket.bucketName)

		const userData = ec2.UserData.custom(userDataScript)

		const publicSubnets = this.vpc.selectSubnets({
			subnetType: ec2.SubnetType.PUBLIC,
		}).subnets
		if (publicSubnets.length === 0) {
			throw new Error('No public subnets available')
		}
		const primarySubnet = publicSubnets[0]!

		// Create Launch Template explicitly (AWS is deprecating Launch Configurations)
		const launchTemplate = new ec2.LaunchTemplate(this, 'LaunchTemplate', {
			instanceType: ec2.InstanceType.of(
				ec2.InstanceClass.R8G,
				ec2.InstanceSize.XLARGE,
			),
			machineImage: ec2.MachineImage.latestAmazonLinux2023({
				cpuType: ec2.AmazonLinuxCpuType.ARM_64,
			}),
			role: this.ec2Role,
			securityGroup: this.udpSecurityGroup,
			userData,
			requireImdsv2: true,
			associatePublicIpAddress: true,
		})

		// NLB IPv6 target groups with instance targets require the instance's primary
		// network interface to have a primary IPv6 address.
		const cfnLaunchTemplate = launchTemplate.node
			.defaultChild as ec2.CfnLaunchTemplate
		cfnLaunchTemplate.addPropertyOverride(
			'LaunchTemplateData.NetworkInterfaces.0.PrimaryIpv6',
			true,
		)
		cfnLaunchTemplate.addPropertyOverride(
			'LaunchTemplateData.NetworkInterfaces.0.Ipv6AddressCount',
			1,
		)

		// Create Auto Scaling Group with Launch Template
		this.autoScalingGroup = new autoscaling.AutoScalingGroup(
			this,
			'UDPListenerASG',
			{
				vpc: this.vpc,
				vpcSubnets: { subnets: [primarySubnet] },
				launchTemplate,
				minCapacity: 2,
				maxCapacity: 10,
				updatePolicy: autoscaling.UpdatePolicy.rollingUpdate({
					maxBatchSize: 1,
					minInstancesInService: 1,
					pauseTime: Duration.minutes(5),
				}),
			},
		)

		const cfnAsg = this.autoScalingGroup.node
			.defaultChild as autoscaling.CfnAutoScalingGroup

		const eip = new ec2.CfnEIP(this, 'NLB-EIP', {
			domain: 'vpc',
		})

		// Create Network Load Balancer with fixed IPv4 address and IPv6 (dual-stack).
		// UDP listeners on a dual-stack NLB require source-NAT IPv6 prefixes so
		// IPv6 client traffic can be translated to IPv4 toward instance targets.
		this.networkLoadBalancer = new elbv2.NetworkLoadBalancer(
			this,
			'VideoStreamingNLB',
			{
				vpc: this.vpc,
				internetFacing: true,
				ipAddressType: elbv2.IpAddressType.DUAL_STACK,
				enablePrefixForIpv6SourceNat: true,
				crossZoneEnabled: false,
			},
		)

		const cfnNlb = this.networkLoadBalancer.node
			.defaultChild as elbv2.CfnLoadBalancer
		cfnNlb.subnets = undefined
		cfnNlb.subnetMappings = [
			{
				subnetId: primarySubnet.subnetId,
				allocationId: eip.attrAllocationId,
				sourceNatIpv6Prefix: 'auto_assigned',
			},
		]

		/**
		 * Creates the UDP target group for one ingest port.
		 *
		 * Every target group health-checks port 9999, including the SRTP ones. That
		 * port means "this instance's backend is up", not "this transport can ingest":
		 * the Auto Scaling group uses ELB health checks, and with those, any attached
		 * target group reporting an instance unhealthy gets the instance replaced. A
		 * per-transport health port would therefore let an SRTP-only condition that is
		 * identical on every instance - an unreachable parameter store, a missing
		 * plugin, one unprovisioned key - churn the entire fleet and take the
		 * unencrypted path down with it. The cost of this choice is that an instance
		 * whose SRTP listener never bound still receives SRTP traffic and drops it;
		 * that shows up in the SRTP zero-ingestion alarm rather than in a fleet-wide
		 * outage.
		 */
		const createTargetGroup = (
			id: string,
			port: number,
		): elbv2.NetworkTargetGroup => {
			const targetGroup = new elbv2.NetworkTargetGroup(this, id, {
				vpc: this.vpc,
				port,
				protocol: elbv2.Protocol.UDP,
				targetType: elbv2.TargetType.INSTANCE,
				ipAddressType: elbv2.TargetGroupIpAddressType.IPV6,
				healthCheck: {
					protocol: elbv2.Protocol.TCP,
					port: '9999',
					healthyThresholdCount: 2,
					unhealthyThresholdCount: 2,
					interval: Duration.seconds(10),
					timeout: Duration.seconds(10),
				},
				deregistrationDelay: Duration.seconds(30),
				preserveClientIp: true,
			})

			// Enable stickiness for single active instance pattern
			targetGroup.setAttribute('stickiness.enabled', 'true')
			targetGroup.setAttribute('stickiness.type', 'source_ip')
			return targetGroup
		}

		// Existing construct IDs are kept for the unencrypted ports so this deployment
		// does not replace their target groups; the SRTP ones are new.
		const targetGroups = [
			...unencryptedPorts.map((port) => ({
				port,
				targetGroup: createTargetGroup(`TargetGroup${port}`, port),
				listenerId: `UDPListener${port}`,
			})),
			...srtpPorts.map((port) => ({
				port,
				targetGroup: createTargetGroup(`SrtpTargetGroup${port}`, port),
				listenerId: `SrtpUDPListener${port}`,
			})),
		]

		for (const { port, targetGroup, listenerId } of targetGroups) {
			this.networkLoadBalancer.addListener(listenerId, {
				port,
				protocol: elbv2.Protocol.UDP,
				defaultAction: elbv2.NetworkListenerAction.forward([targetGroup]),
			})
			// Attaching to the ASG is what registers and deregisters instances.
			this.autoScalingGroup.attachToNetworkTargetGroup(targetGroup)
		}

		// Use ELB health check so ASG only considers instances ready when they pass NLB
		// target group health checks. Prevents terminating old instances before new ones
		// can receive traffic.
		cfnAsg.healthCheckType = 'ELB'
		cfnAsg.healthCheckGracePeriod = Duration.minutes(5).toSeconds()

		// Lambda: set streams to inactive when marked active but no frame in 5 minutes
		const streamCleanupLambda = new lambdanode.NodejsFunction(
			this,
			'StreamInactivityCleanup',
			{
				entry: join(
					__dirname,
					'..',
					'lambda',
					'stream-inactivity-cleanup',
					'index.ts',
				),
				runtime: lambda.Runtime.NODEJS_24_X,
				handler: 'handler',
				environment: {
					TABLE_NAME: this.streamTable.tableName,
				},
				timeout: Duration.seconds(30),
				bundling: {
					format: lambdanode.OutputFormat.ESM,
				},
			},
		)
		this.streamTable.grantReadWriteData(streamCleanupLambda)

		new events.Rule(this, 'StreamCleanupSchedule', {
			schedule: events.Schedule.rate(Duration.minutes(1)),
			targets: [new target.LambdaFunction(streamCleanupLambda)],
		})

		// Create SNS topic for alarm notifications
		const alarmTopic = new sns.Topic(this, 'AlarmTopic', {
			displayName: 'Video Streaming Alarms',
		})

		// Topic and Lambda for restarting ingestion when UDPTrafficNoKinesisIngestionAlarm fires (after 10 min)
		const restartIngestionTopic = new sns.Topic(this, 'RestartIngestionTopic', {
			displayName: 'Video Streaming Restart Ingestion',
		})
		const restartIngestionLambda = new lambdanode.NodejsFunction(
			this,
			'RestartIngestionOnAlarm',
			{
				entry: join(
					__dirname,
					'..',
					'lambda',
					'restart-ingestion-on-alarm',
					'index.ts',
				),
				runtime: lambda.Runtime.NODEJS_24_X,
				handler: 'handler',
				environment: {
					AUTO_SCALING_GROUP_NAME: this.autoScalingGroup.autoScalingGroupName,
				},
				timeout: Duration.seconds(60),
				bundling: {
					format: lambdanode.OutputFormat.ESM,
				},
			},
		)
		restartIngestionLambda.addToRolePolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: ['autoscaling:DescribeAutoScalingGroups'],
				resources: ['*'],
			}),
		)
		restartIngestionLambda.addToRolePolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: ['ec2:RebootInstances'],
				resources: ['*'],
			}),
		)
		restartIngestionTopic.addSubscription(
			new sns_subscriptions.LambdaSubscription(restartIngestionLambda),
		)

		// Alarm for high packet loss (>5%)
		const packetLossAlarm = new cloudwatch.Alarm(this, 'PacketLossAlarm', {
			alarmName: `${Stack.of(this).stackName}-HighPacketLoss`,
			alarmDescription: 'Alarm when packet loss exceeds 5% for any stream',
			metric: new cloudwatch.Metric({
				namespace: Stack.of(this).stackName,
				metricName: 'PacketLossRate',
				statistic: 'Average',
				period: Duration.minutes(5),
			}),
			threshold: 5,
			evaluationPeriods: 2,
			comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
			treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
		})
		packetLossAlarm.addAlarmAction(new cloudwatch_actions.SnsAction(alarmTopic))

		// Alarm for EC2 CPU usage >80%
		const cpuMetric = new cloudwatch.Metric({
			namespace: 'AWS/EC2',
			metricName: 'CPUUtilization',
			dimensionsMap: {
				AutoScalingGroupName: this.autoScalingGroup.autoScalingGroupName,
			},
			statistic: 'Average',
			period: Duration.minutes(5),
		})

		const cpuAlarm = new cloudwatch.Alarm(this, 'CPUAlarm', {
			alarmName: `${Stack.of(this).stackName}-HighCPUUsage`,
			alarmDescription: 'Alarm when EC2 CPU usage exceeds 80%',
			metric: cpuMetric,
			threshold: 80,
			evaluationPeriods: 2,
			comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
			treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
		})
		cpuAlarm.addAlarmAction(new cloudwatch_actions.SnsAction(alarmTopic))

		// Composite alarm: NLB UDP traffic > 1 MB/s but no PutMedia ingestion on any Kinesis stream
		const oneMebibytePerSecondBytesPerMinute = 1024 * 1024 * 60 // 1 MiB/s * 60s
		const nlbUdpBytesAlarm = new cloudwatch.Alarm(
			this,
			'NLBUDPBytesHighAlarm',
			{
				alarmName: `${Stack.of(this).stackName}-NLB-UDP-Bytes-Gt-1MBps`,
				alarmDescription:
					'NLB ProcessedBytes_UDP exceeds 1 MB/s (bytes per minute threshold)',
				metric: new cloudwatch.Metric({
					namespace: 'AWS/NetworkELB',
					metricName: 'ProcessedBytes_UDP',
					dimensionsMap: {
						LoadBalancer: Fn.select(
							1,
							Fn.split(
								'loadbalancer/',
								this.networkLoadBalancer.loadBalancerArn,
							),
						),
					},
					statistic: 'Sum',
					period: Duration.minutes(1),
				}),
				threshold: oneMebibytePerSecondBytesPerMinute,
				evaluationPeriods: 5,
				comparisonOperator:
					cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
				treatMissingData: cloudwatch.TreatMissingData.BREACHING,
			},
		)

		/**
		 * Sums PutMedia.IncomingBytes over an explicit set of ports.
		 *
		 * Built from a port list rather than from this.kinesisVideoStreams, which now
		 * holds both transports: iterating it would silently change what the existing
		 * alarm watches every time a transport is added. A metric-math alarm is also
		 * limited in how many metrics it can carry, so one alarm per transport is both
		 * clearer and necessary.
		 */
		const kvsIncomingSumFor = (ports: number[]): cloudwatch.MathExpression => {
			const metrics: Record<string, cloudwatch.IMetric> = {}
			ports.forEach((port, i) => {
				metrics[`s${i}`] = new cloudwatch.Metric({
					namespace: 'AWS/KinesisVideo',
					metricName: 'PutMedia.IncomingBytes',
					dimensionsMap: { StreamName: `${kinesisStreamPrefix}-${port}` },
					statistic: 'Sum',
					period: Duration.minutes(1),
				})
			})
			return new cloudwatch.MathExpression({
				expression: ports.map((_, i) => `s${i}`).join('+'),
				usingMetrics: metrics,
				period: Duration.minutes(1),
				label: 'PutMedia Incoming Bytes',
			})
		}

		const kvsNoIngestionAlarm = new cloudwatch.Alarm(
			this,
			'KVSNoPutMediaIngestionAlarm',
			{
				alarmName: `${Stack.of(this).stackName}-KVS-PutMedia-Incoming-Zero`,
				alarmDescription:
					'Sum of PutMedia.IncomingBytes across the unencrypted Kinesis Video Streams is 0',
				metric: kvsIncomingSumFor(unencryptedPorts),
				threshold: 0,
				evaluationPeriods: 5,
				comparisonOperator:
					cloudwatch.ComparisonOperator.LESS_THAN_OR_EQUAL_TO_THRESHOLD,
				treatMissingData: cloudwatch.TreatMissingData.BREACHING,
			},
		)

		/**
		 * The same signal for the SRTP streams - but notify only.
		 *
		 * No SRTP ingestion is the *normal* state until devices are provisioned, so
		 * this must not be wired to the restart topic the way the composite alarm
		 * below is: that would reboot the fleet forever over a fleet doing nothing
		 * wrong. Missing data is likewise not a breach, for the same reason. It is also
		 * not combined with the load-balancer UDP byte metric, which counts both
		 * transports - unencrypted traffic would satisfy the "there is traffic" leg and
		 * make the composite fire whenever no SRTP device happened to be sending.
		 */
		const srtpNoIngestionAlarm = new cloudwatch.Alarm(
			this,
			'KVSNoPutMediaIngestionAlarmSrtp',
			{
				alarmName: `${Stack.of(this).stackName}-KVS-PutMedia-Incoming-Zero-SRTP`,
				alarmDescription:
					'Sum of PutMedia.IncomingBytes across the SRTP Kinesis Video Streams is 0',
				metric: kvsIncomingSumFor(srtpPorts),
				threshold: 0,
				evaluationPeriods: 5,
				comparisonOperator:
					cloudwatch.ComparisonOperator.LESS_THAN_OR_EQUAL_TO_THRESHOLD,
				treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
			},
		)
		srtpNoIngestionAlarm.addAlarmAction(
			new cloudwatch_actions.SnsAction(alarmTopic),
		)

		const udpTrafficNoIngestionAlarm = new cloudwatch.CompositeAlarm(
			this,
			'UDPTrafficNoKinesisIngestionAlarm',
			{
				alarmRule: cloudwatch.AlarmRule.allOf(
					cloudwatch.AlarmRule.not(nlbUdpBytesAlarm),
					kvsNoIngestionAlarm,
				),
				alarmDescription:
					'NLB UDP processed bytes > 1 MB/s but PutMedia incoming data across all Kinesis Video Streams is 0',
				compositeAlarmName: `${Stack.of(this).stackName}-UDP-Traffic-No-KVS-Ingestion`,
			},
		)
		udpTrafficNoIngestionAlarm.addAlarmAction(
			new cloudwatch_actions.SnsAction(alarmTopic),
		)
		udpTrafficNoIngestionAlarm.addAlarmAction(
			new cloudwatch_actions.SnsAction(restartIngestionTopic),
		)

		// CDK Outputs
		new CfnOutput(this, 'StreamMetadataTableName', {
			value: this.streamTable.tableName,
			description: 'DynamoDB table name for stream metadata',
			exportName: `${this.stackName}:StreamMetadataTableName`,
		})

		new CfnOutput(this, 'StreamMetadataTableArn', {
			value: this.streamTable.tableArn,
			description: 'DynamoDB table ARN for stream metadata',
			exportName: `${this.stackName}:StreamMetadataTableArn`,
		})

		new CfnOutput(this, 'VPCId', {
			value: this.vpc.vpcId,
			description: 'VPC ID for EC2 instances',
		})

		new CfnOutput(this, 'AlarmTopicArn', {
			value: alarmTopic.topicArn,
			description: 'SNS topic ARN for CloudWatch alarms',
		})

		new CfnOutput(this, 'LogGroups', {
			value: `${this.stackName}/*`,
			description: 'CloudWatch Logs log groups for EC2 instances',
		})

		// NLB Outputs
		new CfnOutput(this, 'NLBDnsName', {
			value: this.networkLoadBalancer.loadBalancerDnsName,
			description: `Network Load Balancer DNS name for UDP video streaming (ports 5000-5009). Dual-stack: resolves to both A (IPv4) and AAAA (IPv6) records.`,
		})

		new CfnOutput(this, 'NLBIPv4Address', {
			value: eip.ref,
			description: `NLB fixed IPv4 address (Elastic IP) in ${primarySubnet.availabilityZone}`,
		})
	}
}
