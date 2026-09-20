import {
	CfnOutput,
	CfnResource,
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
import * as custom_resources from 'aws-cdk-lib/custom-resources'
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

		// Fleet-generation contexts, read early because several resource families below
		// depend on them (the streams here, the fleets/target groups later). See the full
		// cutover rationale and procedure at createIngestFleet below.
		const servingGeneration =
			(this.node.tryGetContext('fleetGeneration') as string | undefined) ??
			'gen2'
		let retainedGeneration = this.node.tryGetContext(
			'retainFleetGeneration',
		) as string | undefined

		// Kinesis Video Streams: one per device, numbered 1-10, shared by both ingestion
		// methods. Port 5000+n (unencrypted MPEG-TS) and port 6000+n (SRTP) both target the
		// same stream (n+1) - a device is assigned one port range or the other, never both,
		// so nothing needs to arbitrate between them beyond the existing per-port Kinesis
		// lock (see StreamMetadataService). Stream name prefix must match
		// backend/src/KinesisIngestionPipeline.ts's KINESIS_STREAM_NAME_PREFIX (duplicated,
		// not shared, since backend/ and cdk/ are deployed separately).
		//
		// IMPORTANT: Kinesis Video Stream names cannot be changed in place (no rename API), so
		// changing this prefix (or the stream count) is a genuinely destructive migration for
		// any already-deployed stack: deploying such a change replaces every existing stream
		// (old logical IDs/names deleted, new ones created), discarding up to 30 days of
		// retained media in the old streams and breaking anything that referenced the old
		// names. There is no way to preserve history across a rename - export anything that
		// matters before deploying one.
		const KINESIS_STREAM_NAME_PREFIX = 'video-streaming-2026-09-video'
		const STREAM_COUNT = 10
		const srtpPortRangeStart = 6000
		const srtpPortRangeEnd = srtpPortRangeStart + STREAM_COUNT - 1
		for (let i = 0; i < STREAM_COUNT; i++) {
			const streamName = `${KINESIS_STREAM_NAME_PREFIX}-${i + 1}`
			const stream = new kinesisvideo.CfnStream(
				this,
				`KinesisVideoStream${i + 1}`,
				{
					name: streamName,
					dataRetentionInHours: Duration.days(30).toHours(),
					mediaType: 'video/h264',
				},
			)
			this.kinesisVideoStreams.push(stream)
		}

		// During the legacy migration cutover (retainFleetGeneration set to the empty
		// legacy generation), also keep the currently deployed, unsuffixed Kinesis Video
		// Streams in the template: without this, the same update that renames every
		// stream deletes the old ones while the retained legacy fleet - still the sole
		// NLB receiver until the readiness-gated listener flip - keeps ingesting into the
		// old names. Old code writing to deleted streams is an outage. Recreating them
		// here with their original logical IDs and unchanged properties is a no-op
		// update for CloudFormation (the resources already exist), so they survive the
		// cutover deploy; the cleanup deploy (dropping the retainFleetGeneration context)
		// removes this block and deletes them once traffic has moved to the new streams.
		if (retainedGeneration === '') {
			for (let port = 5000; port <= 5009; port++) {
				new kinesisvideo.CfnStream(this, `KinesisVideoStream${port}`, {
					name: `${this.stackName}-video-${port}`,
					dataRetentionInHours: Duration.days(30).toHours(),
					mediaType: 'video/h264',
				})
			}
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
		// Allow UDP ingress on ports 6000-6009 (SRTP-encrypted RTP/H.264)
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
		// Allow TCP health checks from NLB (originates within VPC). Port 9999 is the
		// unencrypted transport's readiness signal (the backend opens it only once the
		// unencrypted listener is bound); port 9998 is the SRTP transport's (opened only
		// after the SRTP keys are loaded AND the SRTP listener is bound - see
		// backend/src/HealthServer.ts), so the two transports' target groups can gate
		// their health checks separately.
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
		this.udpSecurityGroup.addIngressRule(
			ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
			ec2.Port.tcp(9998),
			'Allow NLB TCP health checks on port 9998 (SRTP readiness)',
		)
		this.udpSecurityGroup.addIngressRule(
			ec2.Peer.anyIpv6(),
			ec2.Port.tcp(9998),
			'Allow NLB TCP health checks on port 9998 (SRTP readiness, IPv6)',
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

		// Grant read access to SRTP static pre-shared keys (SecureString parameters,
		// provisioned out-of-band via scripts/provision-srtp-key.sh, never by CDK)
		this.ec2Role.addToPolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: ['ssm:GetParameter', 'ssm:GetParameters'],
				resources: [
					`arn:aws:ssm:${this.region}:${this.account}:parameter/${this.stackName}/srtp/*`,
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
			.replace(/__CODE_BUCKET__/g, this.codeBucket.bucketName)
			.replace(/__SRTP_KEY_PARAMETER_PREFIX__/g, `/${this.stackName}/srtp/port`)
			.replace(/__SRTP_PORT_RANGE_START__/g, String(srtpPortRangeStart))
			.replace(/__SRTP_PORT_RANGE_END__/g, String(srtpPortRangeEnd))

		// The *legacy* user data, frozen byte-for-byte from the currently deployed
		// (pre-SRTP) stack: the retained legacy fleet's launch template (see
		// createIngestFleet below) must keep rendering EXACTLY the resource that is
		// deployed, so CloudFormation sees no change to it - any change would bump its
		// version number and roll the retained fleet onto the new code mid-cutover,
		// mixing the old (raw-port lock, old stream names) and new (slot lock, new
		// stream names) schemes on instances that still receive listener traffic.
		// Placeholders match the legacy set (the legacy backend had no SRTP
		// configuration).
		const legacyUserDataScript = readFileSync(
			join(__dirname, 'user-data-legacy.sh'),
			'utf-8',
		)
			.replace(/__AWS_REGION__/g, this.region)
			.replace(/__TABLE_NAME__/g, this.streamTable.tableName)
			.replace(/__CODE_BUCKET__/g, this.codeBucket.bucketName)
			.replace(/__KINESIS_STREAM_PREFIX__/g, `${this.stackName}-video`)

		/**
		 * Builds a fleet's launch template (same shape for every fleet: instance type,
		 * AMI, role, security group, IMDSv2, public IP, primary IPv6 - only the user
		 * data and construct ID differ). The generation-scoped ID keeps each fleet's
		 * template independent: a user-data change rolls only the generation whose
		 * script changed.
		 */
		const createFleetLaunchTemplate = (
			id: string,
			script: string,
		): ec2.LaunchTemplate => {
			const template = new ec2.LaunchTemplate(this, id, {
				instanceType: ec2.InstanceType.of(
					ec2.InstanceClass.R8G,
					ec2.InstanceSize.XLARGE,
				),
				machineImage: ec2.MachineImage.latestAmazonLinux2023({
					cpuType: ec2.AmazonLinuxCpuType.ARM_64,
				}),
				role: this.ec2Role,
				securityGroup: this.udpSecurityGroup,
				userData: ec2.UserData.custom(script),
				requireImdsv2: true,
				associatePublicIpAddress: true,
			})
			// NLB IPv6 target groups with instance targets require the instance's primary
			// network interface to have a primary IPv6 address.
			const cfnTemplate = template.node.defaultChild as ec2.CfnLaunchTemplate
			cfnTemplate.addPropertyOverride(
				'LaunchTemplateData.NetworkInterfaces.0.PrimaryIpv6',
				true,
			)
			cfnTemplate.addPropertyOverride(
				'LaunchTemplateData.NetworkInterfaces.0.Ipv6AddressCount',
				1,
			)
			return template
		}

		const publicSubnets = this.vpc.selectSubnets({
			subnetType: ec2.SubnetType.PUBLIC,
		}).subnets
		if (publicSubnets.length === 0) {
			throw new Error('No public subnets available')
		}
		const primarySubnet = publicSubnets[0]!

		// Fleet generation: scopes the Auto Scaling Groups and their NLB target groups so
		// a breaking rollout can be deployed as a *full-fleet cutover* instead of a rolling
		// update (see the servingGeneration/retainedGeneration contexts read near the top
		// of this constructor). A rolling update replaces instances one batch at a time
		// while the whole fleet keeps serving the same NLB target groups - fine for compatible changes,
		// but this update renames every Kinesis Video Stream and moves the DynamoDB lock
		// key from the raw port (5000-5009) to the stream slot (1-10): during a rolling
		// overlap, old instances would write to the old {stackName}-video-5000 stream
		// under lock row 5000 while new instances write to
		// video-streaming-2026-09-video-1 under lock row 1 - the two schemes do not
		// fence each other, so a device's traffic would be split between (or duplicated
		// into) both streams for the duration of the rollout.
		//
		// Cutover procedure (two deploys, keeping the old fleet alive through the
		// verified listener switch - old and new code never both receive traffic, and
		// existing UDP flows pinned to old targets by stickiness keep being served until
		// they idle out):
		//
		//   1. Cutover deploy: `--fleetGeneration <new> --retainFleetGeneration <old>`
		//      creates the new fleet alongside the retained old one. The readiness gates
		//      below block the stable listeners' re-pointing until the new fleet is
		//      *ingesting-ready* (instances only pass the target-group health checks after
		//      user data has verified the GStreamer elements and built kvssink, and the
		//      backend has loaded every SRTP key and bound both listeners - see
		//      cdk/user-data.sh and backend/src/HealthServer.ts). The retained fleet keeps
		//      its (now listener-less) target groups, its prior launch-template version,
		//      and registered instances, so old flows continue to drain through the
		//      switch instead of being cut off or refreshed mid-check.
		//   2. Cleanup deploy: `--fleetGeneration <new> --retainFleetGeneration skip`,
		//      once the retained fleet's remaining flows have drained past the NLB UDP
		//      flow idle timeout - deletes the retained fleet.
		//
		// For THIS update the old fleet is the currently deployed, unsuffixed legacy
		// resources: deploy the cutover with `--fleetGeneration gen2
		// --retainFleetGeneration ""` (empty value = the legacy unsuffixed IDs). The
		// retained legacy fleet is byte-identical to what is deployed (its launch
		// template renders the frozen cdk/user-data-legacy.sh, so CloudFormation sees no
		// change and never rolls it onto the new code mid-cutover - a refresh would mix
		// the raw-port-lock/old-stream-name scheme with the slot-lock/new-stream-name
		// scheme on instances still receiving listener traffic). Within an unchanged
		// generation, ordinary stack updates are normal rolling updates again.
		//
		// The retention context is REQUIRED: without it, deploying this update onto the
		// legacy stack would remove the unsuffixed ASG, target groups, and old Kinesis
		// streams from the template in the same update that flips the listeners, and
		// CloudFormation can delete those still-serving resources before the
		// readiness-gated listener update completes. 'skip' (no previous generation -
		// fresh stacks, or the cleanup deploy) is the only way to deploy without
		// retention.
		if (retainedGeneration === undefined) {
			throw new Error(
				`Missing required context 'retainFleetGeneration': this update migrates the deployed ingest fleet, so every deploy must state what to retain - pass '' (the legacy unsuffixed generation) for the cutover, a previous generation name (e.g. 'gen2') for a later cutover, or 'skip' when there is no previous generation to keep (fresh stacks, or the cleanup deploy after a completed cutover)`,
			)
		}
		if (retainedGeneration === 'skip') {
			// No retention: 'skip' is the explicit "nothing to retain" choice.
			retainedGeneration = undefined as string | undefined
		}
		if (
			retainedGeneration !== undefined &&
			retainedGeneration === servingGeneration
		) {
			throw new Error(
				`Invalid configuration: retainFleetGeneration (${retainedGeneration}) must differ from fleetGeneration (${servingGeneration})`,
			)
		}
		/** Minimum number of instances each fleet keeps - also the number of healthy
		 * targets each target group must report before the NLB listeners cut over to it
		 * (see the FleetCutoverReadiness gates below). */
		const ASG_MIN_CAPACITY = 2
		/** How long the readiness gates may wait for a freshly launched instance to
		 * pass its target groups' health checks: cdk/user-data.sh only starts the
		 * service (thereby opening the health ports) after the kvssink build and
		 * GStreamer verification, so bootstrap takes tens of minutes on a fresh
		 * instance. */
		const FLEET_READINESS_TIMEOUT = Duration.minutes(60)

		/**
		 * Builds one ingest fleet (an ASG and its NLB target groups, with the fleet's
		 * instances registered in them) for a generation.
		 *
		 * An EMPTY generation produces the *legacy, unsuffixed* fleet
		 * (`UDPListenerASG`, `TargetGroup5000`, ...): byte-identical to what the
		 * pre-SRTP stack deployed (launch template rendering the frozen legacy user
		 * data, ELB health checks with the original 5-minute grace, the ten unencrypted
		 * target groups only), so a retention deploy renders a no-op for it - no
		 * launch-template version bump, no rolling refresh, no property drift that
		 * could trigger replacement mid-cutover. The legacy fleet has no SRTP target
		 * groups at all (the pre-SRTP stack had none).
		 *
		 * A non-empty generation scopes all of a fleet's resources (bumping it creates
		 * a replacement fleet) and uses EC2 health checks: with ELB checks, ANY
		 * attached target group - including a transport-specific failure such as SRTP
		 * keys unavailable keeping port 9998 closed - would mark the instance unhealthy
		 * and put the fleet into replacement churn, defeating the intended isolation
		 * of SRTP failures from the unencrypted path. EC2 checks still replace
		 * instances on real instance-level failures; transport readiness is the NLB's
		 * and the cutover gates' job, not the ASG's.
		 */
		const createIngestFleet = (
			generation: string,
			launchTemplate: ec2.LaunchTemplate,
		): {
			asg: autoscaling.AutoScalingGroup
			unencryptedTargetGroups: elbv2.NetworkTargetGroup[]
			srtpTargetGroups: elbv2.NetworkTargetGroup[]
		} => {
			const isLegacy = generation === ''
			const suffix = isLegacy ? '' : `-${generation}`
			const asg = new autoscaling.AutoScalingGroup(
				this,
				`UDPListenerASG${suffix}`,
				{
					vpc: this.vpc,
					vpcSubnets: { subnets: [primarySubnet] },
					launchTemplate,
					minCapacity: ASG_MIN_CAPACITY,
					maxCapacity: 10,
					updatePolicy: autoscaling.UpdatePolicy.rollingUpdate({
						maxBatchSize: 1,
						minInstancesInService: 1,
						pauseTime: Duration.minutes(5),
					}),
				},
			)
			const cfnAsg = asg.node.defaultChild as autoscaling.CfnAutoScalingGroup
			if (isLegacy) {
				// Byte-compatible with the deployed legacy ASG: any change here would
				// be an update to the retained fleet in the cutover deploy.
				cfnAsg.healthCheckType = 'ELB'
				cfnAsg.healthCheckGracePeriod = Duration.minutes(5).toSeconds()
			} else {
				// See the doc comment above: EC2 health keeps a transport-specific
				// readiness failure (e.g. SRTP keys unavailable) from churning the
				// whole fleet.
				cfnAsg.healthCheckType = 'EC2'
			}

			const createTargetGroup = (
				id: string,
				port: number,
				// Readiness signal this target group gates on: 9999 = unencrypted path
				// (bound listener), 9998 = SRTP path (keys + bound listener) - see
				// backend/src/HealthServer.ts and the security-group rules above. A
				// transport whose prerequisites are unavailable therefore reports its
				// own targets unhealthy, without affecting the other transport's.
				healthPort: number,
			): elbv2.NetworkTargetGroup => {
				const targetGroup = new elbv2.NetworkTargetGroup(this, id, {
					vpc: this.vpc,
					port,
					protocol: elbv2.Protocol.UDP,
					targetType: elbv2.TargetType.INSTANCE,
					ipAddressType: elbv2.TargetGroupIpAddressType.IPV6,
					healthCheck: {
						protocol: elbv2.Protocol.TCP,
						port: String(healthPort),
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
			const unencryptedTargetGroups: elbv2.NetworkTargetGroup[] = []
			for (let port = 5000; port <= 5009; port++) {
				unencryptedTargetGroups.push(
					createTargetGroup(`TargetGroup${port}${suffix}`, port, 9999),
				)
			}
			const srtpTargetGroups: elbv2.NetworkTargetGroup[] = []
			if (!isLegacy) {
				// The legacy (pre-SRTP) stack had no SRTP target groups - the retained
				// legacy fleet must keep exactly its original ten, or the cutover deploy
				// would change (and refresh) it.
				for (let port = 6000; port <= 6009; port++) {
					srtpTargetGroups.push(
						createTargetGroup(`SrtpTargetGroup${port}${suffix}`, port, 9998),
					)
				}
			}
			// Register the fleet's instances in all of its target groups
			for (const targetGroup of [
				...unencryptedTargetGroups,
				...srtpTargetGroups,
			]) {
				asg.attachToNetworkTargetGroup(targetGroup)
			}
			return { asg, unencryptedTargetGroups, srtpTargetGroups }
		}

		// The serving fleet receives all listener traffic; the (optional) retained fleet
		// stays alive through a cutover without any listeners pointing at it (created for
		// its side effects: its ASG keeps its instances registered in its target groups
		// so draining flows are served until the cleanup deploy removes it). The legacy
		// retained fleet uses the frozen legacy launch template (construct ID
		// 'LaunchTemplate', byte-identical to the deployed one); every other fleet uses
		// a generation-scoped template with the current user data.
		const servingFleet = createIngestFleet(
			servingGeneration,
			createFleetLaunchTemplate(
				`LaunchTemplate-${servingGeneration}`,
				userDataScript,
			),
		)
		this.autoScalingGroup = servingFleet.asg
		if (retainedGeneration !== undefined) {
			createIngestFleet(
				retainedGeneration,
				retainedGeneration === ''
					? createFleetLaunchTemplate('LaunchTemplate', legacyUserDataScript)
					: createFleetLaunchTemplate(
							`LaunchTemplate-${retainedGeneration}`,
							userDataScript,
						),
			)
		}

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

		// Create UDP listeners for ports 5000-5009, forwarding to the *serving* fleet's
		// target groups (the listener IDs are stable across generations - a cutover
		// updates the listeners' default actions, never replaces them). The listener
		// sets are kept separate by transport so each can be gated on its own
		// transport's readiness below.
		const unencryptedListeners: elbv2.NetworkListener[] = []
		for (const [
			i,
			targetGroup,
		] of servingFleet.unencryptedTargetGroups.entries()) {
			const port = 5000 + i
			unencryptedListeners.push(
				this.networkLoadBalancer.addListener(`UDPListener${port}`, {
					port,
					protocol: elbv2.Protocol.UDP,
					defaultAction: elbv2.NetworkListenerAction.forward([targetGroup]),
				}),
			)
		}

		// Same for the SRTP ports 6000-6009
		const srtpListeners: elbv2.NetworkListener[] = []
		for (const [i, targetGroup] of servingFleet.srtpTargetGroups.entries()) {
			const port = 6000 + i
			srtpListeners.push(
				this.networkLoadBalancer.addListener(`SrtpUDPListener${port}`, {
					port,
					protocol: elbv2.Protocol.UDP,
					defaultAction: elbv2.NetworkListenerAction.forward([targetGroup]),
				}),
			)
		}

		// Deployment-time readiness gates for the fleet cutover, one per transport:
		// the ASG resource alone reaching CREATE_COMPLETE proves nothing about the
		// *instances* - they may still be bootstrapping, so re-pointing the stable
		// listeners at the new target groups at that point can leave every listener with
		// zero healthy targets (a full ingest outage). Each gate blocks its own
		// transport's listener updates until every target group of the serving
		// generation for THAT transport reports at least ASG_MIN_CAPACITY healthy
		// targets - the same per-transport TCP health checks (9999 unencrypted, 9998
		// SRTP) the NLB itself uses, which instances only pass once their user data has
		// verified the GStreamer elements and built kvssink (see cdk/user-data.sh) and
		// the backend has loaded every SRTP key and bound both listeners (see
		// backend/src/HealthServer.ts) - so the cutover flips traffic only to a fleet
		// that is genuinely ready to ingest.
		//
		// Splitting the gates by transport keeps an SRTP-only dependency failure
		// (e.g. keys not yet provisioned) from delaying the *unencrypted* listeners'
		// re-pointing behind SRTP readiness: each listener set waits only on its own
		// transport's target groups. Note that fleet-wide SRTP readiness remains a
		// *deployment requirement* of the cutover by design: if the SRTP gate ultimately
		// fails, the stack update fails and rolls back as a whole - a cutover onto a
		// fleet that cannot ingest SRTP traffic must not silently proceed. Provision
		// every port's key (see scripts/provision-srtp-key.sh and
		// docs/TESTING-SRTP-INGESTION.md) before deploying one.
		//
		// The CDK custom-resource provider framework drives each wait: isComplete is
		// polled once a minute for up to FLEET_READINESS_TIMEOUT (an hour, covering
		// the long instance bootstrap including the kvssink build). Transient
		// DescribeTargetHealth errors are treated as "not ready yet" and retried on
		// the next interval, and if a handler throws or the fleet never becomes ready,
		// the framework itself sends CloudFormation a bounded FAILED response (never a
		// hung update) and the deployment rolls back - with the retained old fleet
		// still sole receiver until the verified flip. The gates are scoped by the
		// serving generation so each cutover runs fresh checks; within an unchanged
		// generation their properties are stable, so ordinary stack updates do not
		// re-run them.
		const fleetReadinessEntry = join(
			__dirname,
			'..',
			'lambda',
			'fleet-cutover-readiness',
			'index.ts',
		)
		const fleetReadinessOnEvent = new lambdanode.NodejsFunction(
			this,
			`FleetReadinessOnEvent-${servingGeneration}`,
			{
				entry: fleetReadinessEntry,
				runtime: lambda.Runtime.NODEJS_24_X,
				handler: 'onEvent',
				timeout: Duration.minutes(1),
				bundling: {
					format: lambdanode.OutputFormat.ESM,
				},
			},
		)
		const fleetReadinessIsComplete = new lambdanode.NodejsFunction(
			this,
			`FleetReadinessIsComplete-${servingGeneration}`,
			{
				entry: fleetReadinessEntry,
				runtime: lambda.Runtime.NODEJS_24_X,
				handler: 'isComplete',
				timeout: Duration.minutes(1),
				bundling: {
					format: lambdanode.OutputFormat.ESM,
				},
			},
		)
		for (const readinessFunction of [
			fleetReadinessOnEvent,
			fleetReadinessIsComplete,
		]) {
			readinessFunction.addToRolePolicy(
				new iam.PolicyStatement({
					effect: iam.Effect.ALLOW,
					actions: ['elasticloadbalancing:DescribeTargetHealth'],
					resources: [
						...servingFleet.unencryptedTargetGroups.map(
							(targetGroup) => targetGroup.targetGroupArn,
						),
						...servingFleet.srtpTargetGroups.map(
							(targetGroup) => targetGroup.targetGroupArn,
						),
					],
				}),
			)
		}
		const fleetCutoverReadinessProvider = new custom_resources.Provider(
			this,
			`FleetCutoverReadinessProvider-${servingGeneration}`,
			{
				onEventHandler: fleetReadinessOnEvent,
				isCompleteHandler: fleetReadinessIsComplete,
				queryInterval: Duration.minutes(1),
				totalTimeout: FLEET_READINESS_TIMEOUT,
			},
		)
		// (A raw CfnResource with a Custom:: type - the L2 CustomResource's type
		// declarations are truncated in this aws-cdk-lib version; ServiceToken is what
		// CloudFormation's custom-resource framework reads, everything else passes
		// through to the provider's onEvent/isComplete handlers.)
		const createReadinessGate = (
			id: string,
			targetGroups: elbv2.NetworkTargetGroup[],
		): CfnResource =>
			new CfnResource(this, id, {
				type: 'Custom::FleetCutoverReadiness',
				properties: {
					ServiceToken: fleetCutoverReadinessProvider.serviceToken,
					TargetGroupArns: targetGroups.map(
						(targetGroup) => targetGroup.targetGroupArn,
					),
					MinHealthyTargets: ASG_MIN_CAPACITY,
				},
			})
		const unencryptedReadinessGate = createReadinessGate(
			`FleetCutoverReadinessUnencryptedCR-${servingGeneration}`,
			servingFleet.unencryptedTargetGroups,
		)
		const srtpReadinessGate =
			servingFleet.srtpTargetGroups.length > 0
				? createReadinessGate(
						`FleetCutoverReadinessSrtpCR-${servingGeneration}`,
						servingFleet.srtpTargetGroups,
					)
				: undefined
		// Each gate runs once the serving ASG exists (it registers the instances into
		// the target groups it depends on), and each listener set depends on its own
		// transport's gate - so the cutover order per transport is: new fleet up and
		// ingest-ready -> that transport's listeners flip -> the retained old fleet
		// (still serving its draining flows, untouched by this update) is removed on
		// the next deploy. Within an unchanged generation these dependency chains are
		// a no-op.
		unencryptedReadinessGate.node.addDependency(this.autoScalingGroup)
		for (const listener of unencryptedListeners) {
			listener.node.addDependency(unencryptedReadinessGate)
		}
		if (srtpReadinessGate !== undefined) {
			srtpReadinessGate.node.addDependency(this.autoScalingGroup)
			for (const listener of srtpListeners) {
				listener.node.addDependency(srtpReadinessGate)
			}
		}

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

		const kvsIncomingMetrics: Record<string, cloudwatch.IMetric> = {}
		this.kinesisVideoStreams.forEach((_, i) => {
			const streamName = `${KINESIS_STREAM_NAME_PREFIX}-${i + 1}`
			kvsIncomingMetrics[`s${i}`] = new cloudwatch.Metric({
				namespace: 'AWS/KinesisVideo',
				metricName: 'PutMedia.IncomingBytes',
				dimensionsMap: { StreamName: streamName },
				statistic: 'Sum',
				period: Duration.minutes(1),
			})
		})
		const kvsIncomingSum = new cloudwatch.MathExpression({
			expression: this.kinesisVideoStreams.map((_, i) => `s${i}`).join('+'),
			usingMetrics: kvsIncomingMetrics,
			period: Duration.minutes(1),
			label: 'PutMedia Incoming Bytes (all streams)',
		})
		const kvsNoIngestionAlarm = new cloudwatch.Alarm(
			this,
			'KVSNoPutMediaIngestionAlarm',
			{
				alarmName: `${Stack.of(this).stackName}-KVS-PutMedia-Incoming-Zero`,
				alarmDescription:
					'Sum of PutMedia.IncomingBytes across all Kinesis Video Streams is 0',
				metric: kvsIncomingSum,
				threshold: 0,
				evaluationPeriods: 5,
				comparisonOperator:
					cloudwatch.ComparisonOperator.LESS_THAN_OR_EQUAL_TO_THRESHOLD,
				treatMissingData: cloudwatch.TreatMissingData.BREACHING,
			},
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

		// No separate SRTP alarm needed: SRTP and unencrypted ingestion now write into the
		// same 10 Kinesis Video Streams (see the stream-creation loop above), so
		// `kvsNoIngestionAlarm` already reflects ingestion health for whichever transport a
		// given device actually uses, and `nlbUdpBytesAlarm` already covers traffic on both
		// port ranges (it's load-balancer-wide). The composite above needs no SRTP-specific
		// equivalent.

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
			description: `Network Load Balancer DNS name for UDP video streaming (unencrypted MPEG-TS/H.264 on ports 5000-5009, SRTP-encrypted RTP/H.264 on ports 6000-6009). Dual-stack: resolves to both A (IPv4) and AAAA (IPv6) records.`,
		})

		new CfnOutput(this, 'NLBIPv4Address', {
			value: eip.ref,
			description: `NLB fixed IPv4 address (Elastic IP) in ${primarySubnet.availabilityZone}`,
		})
	}
}
