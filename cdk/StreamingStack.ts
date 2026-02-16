import {
	CfnOutput,
	Duration,
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
			natGateways: 1,
			subnetConfiguration: [
				{
					cidrMask: 24,
					name: 'Public',
					subnetType: ec2.SubnetType.PUBLIC,
				},
				{
					cidrMask: 24,
					name: 'Private',
					subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
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

		// Kinesis Video Streams: one per UDP port (5000-5009)
		const kinesisStreamPortStart = 5000
		const kinesisStreamPortEnd = 5009
		const kinesisStreamPrefix = `${this.stackName}-video`
		for (
			let port = kinesisStreamPortStart;
			port <= kinesisStreamPortEnd;
			port++
		) {
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

		// Allow UDP ingress on ports 5000-5009
		this.udpSecurityGroup.addIngressRule(
			ec2.Peer.anyIpv4(),
			ec2.Port.udpRange(5000, 5009),
			'Allow UDP video ingestion on ports 5000-5009',
		)
		// Allow TCP health checks from NLB (originates within VPC)
		this.udpSecurityGroup.addIngressRule(
			ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
			ec2.Port.tcp(9999),
			'Allow NLB TCP health checks on port 9999',
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

		// Create Network Load Balancer with fixed IPv4 address
		this.networkLoadBalancer = new elbv2.NetworkLoadBalancer(
			this,
			'VideoStreamingNLB',
			{
				vpc: this.vpc,
				internetFacing: true,
				ipAddressType: elbv2.IpAddressType.IPV4,
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
			},
		]

		// Create target groups for UDP ports 5000-5009
		const targetGroups: elbv2.NetworkTargetGroup[] = []
		for (let port = 5000; port <= 5009; port++) {
			const targetGroup = new elbv2.NetworkTargetGroup(
				this,
				`TargetGroup${port}`,
				{
					vpc: this.vpc,
					port,
					protocol: elbv2.Protocol.UDP,
					targetType: elbv2.TargetType.INSTANCE,
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
				},
			)

			// Enable stickiness for single active instance pattern
			targetGroup.setAttribute('stickiness.enabled', 'true')
			targetGroup.setAttribute('stickiness.type', 'source_ip')

			targetGroups.push(targetGroup)
		}

		// Create UDP listeners for ports 5000-5009
		for (let i = 0; i < targetGroups.length; i++) {
			const port = 5000 + i
			const targetGroup = targetGroups[i]
			if (!targetGroup) {
				throw new Error(`Target group for port ${port} is undefined`)
			}
			this.networkLoadBalancer.addListener(`UDPListener${port}`, {
				port,
				protocol: elbv2.Protocol.UDP,
				defaultAction: elbv2.NetworkListenerAction.forward([targetGroup]),
			})
		}

		// Attach all target groups to the Auto Scaling Group
		// This enables automatic registration/deregistration of instances
		for (const targetGroup of targetGroups) {
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
				runtime: lambda.Runtime.NODEJS_22_X,
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
			topicName: 'video-streaming-alarms',
		})

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
			description: `Network Load Balancer DNS name for UDP video streaming (ports 5000-5009)`,
		})

		new CfnOutput(this, 'NLBIPv4Address', {
			value: eip.ref,
			description: `NLB fixed IPv4 address (Elastic IP) in ${primarySubnet.availabilityZone}`,
		})
	}
}
