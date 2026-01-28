import {
	CfnOutput,
	Duration,
	RemovalPolicy,
	Size,
	Stack,
	type Environment,
} from 'aws-cdk-lib'
import * as autoscaling from 'aws-cdk-lib/aws-autoscaling'
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront'
import * as origins from 'aws-cdk-lib/aws-cloudfront-origins'
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch'
import * as cloudwatch_actions from 'aws-cdk-lib/aws-cloudwatch-actions'
import * as cognito from 'aws-cdk-lib/aws-cognito'
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb'
import * as ec2 from 'aws-cdk-lib/aws-ec2'
import * as iam from 'aws-cdk-lib/aws-iam'
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
	public readonly videoBucket: s3.Bucket
	public readonly streamTable: dynamodb.Table
	public readonly identityPool: cognito.CfnIdentityPool
	public readonly unauthRole: iam.Role
	public readonly udpSecurityGroup: ec2.SecurityGroup
	public readonly distribution: cloudfront.Distribution
	public readonly ec2Role: iam.Role
	public readonly autoScalingGroup: autoscaling.AutoScalingGroup
	public readonly codeBucket: s3.Bucket

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

		// Task 2.1: Define VPC and networking
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

		// Task 2.2: Define S3 buckets
		this.videoBucket = new s3.Bucket(this, 'VideoBucket', {
			encryption: s3.BucketEncryption.S3_MANAGED,
			blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
			removalPolicy: RemovalPolicy.DESTROY,
			autoDeleteObjects: true,
			lifecycleRules: [
				{
					expiration: Duration.days(30),
					id: 'DeleteOldVideos',
				},
			],
			cors: [
				{
					allowedMethods: [s3.HttpMethods.GET, s3.HttpMethods.HEAD],
					allowedOrigins: ['*'],
					allowedHeaders: ['*'],
					maxAge: 3000,
				},
			],
		})

		// Task 2.3: Define DynamoDB table
		this.streamTable = new dynamodb.Table(this, 'StreamMetadata', {
			partitionKey: { name: 'port', type: dynamodb.AttributeType.NUMBER },
			billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
			removalPolicy: RemovalPolicy.DESTROY,
		})

		// Task 2.4: Configure Cognito Identity Pool
		this.identityPool = new cognito.CfnIdentityPool(
			this,
			'StreamViewerIdentityPool',
			{
				allowUnauthenticatedIdentities: true,
				identityPoolName: 'VideoStreamViewers',
			},
		)

		// Create IAM role for unauthenticated users with read-only DynamoDB access
		this.unauthRole = new iam.Role(this, 'UnauthRole', {
			assumedBy: new iam.FederatedPrincipal(
				'cognito-identity.amazonaws.com',
				{
					StringEquals: {
						'cognito-identity.amazonaws.com:aud': this.identityPool.ref,
					},
					'ForAnyValue:StringLike': {
						'cognito-identity.amazonaws.com:amr': 'unauthenticated',
					},
				},
				'sts:AssumeRoleWithWebIdentity',
			),
		})

		// Grant read-only access to StreamMetadata table
		this.streamTable.grantReadData(this.unauthRole)

		// Attach the role to the identity pool
		new cognito.CfnIdentityPoolRoleAttachment(
			this,
			'IdentityPoolRoleAttachment',
			{
				identityPoolId: this.identityPool.ref,
				roles: {
					unauthenticated: this.unauthRole.roleArn,
				},
			},
		)

		// Task 2.5: Define security groups
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

		// Allow HTTPS egress for AWS service communication
		this.udpSecurityGroup.addEgressRule(
			ec2.Peer.anyIpv4(),
			ec2.Port.tcp(443),
			'Allow HTTPS egress for AWS service communication',
		)

		// Task 6.3: Configure IAM role for EC2 instances
		this.ec2Role = new iam.Role(this, 'EC2InstanceRole', {
			assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
			description: 'IAM role for EC2 instances running UDP listener service',
			managedPolicies: [
				iam.ManagedPolicy.fromAwsManagedPolicyName(
					'AmazonSSMManagedInstanceCore',
				),
			],
		})

		// Grant S3 permissions for video storage
		this.videoBucket.grantReadWrite(this.ec2Role)

		// Grant DynamoDB permissions for stream metadata
		this.streamTable.grantReadWriteData(this.ec2Role)

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

		// Task 6.2: Add EC2 Auto Scaling Group
		// Read user data script
		const userDataScriptPath = join(__dirname, 'user-data.sh')
		let userDataScript = readFileSync(userDataScriptPath, 'utf-8')

		// Replace placeholders in user data script
		userDataScript = userDataScript
			.replace(/__AWS_REGION__/g, this.region)
			.replace(/__S3_BUCKET__/g, this.videoBucket.bucketName)
			.replace(/__DYNAMODB_TABLE_NAME__/g, this.streamTable.tableName)

		// Add code download commands to user data
		const codeDownloadCommands = `
# Download application code from S3
aws s3 sync s3://${this.codeBucket.bucketName}/backend/ /opt/video-streaming/ --region ${this.region}

# Install dependencies
cd /opt/video-streaming
npm install --production

# Start the service
systemctl start video-streaming.service
`

		userDataScript += codeDownloadCommands

		const userData = ec2.UserData.custom(userDataScript)

		// Create Launch Template explicitly (AWS is deprecating Launch Configurations)
		const launchTemplate = new ec2.LaunchTemplate(this, 'LaunchTemplate', {
			instanceType: ec2.InstanceType.of(
				ec2.InstanceClass.C5,
				ec2.InstanceSize.XLARGE,
			),
			machineImage: ec2.MachineImage.latestAmazonLinux2023(),
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
				vpcSubnets: {
					subnetType: ec2.SubnetType.PUBLIC,
				},
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

		// Configure health check separately to avoid deprecation warnings
		const cfnAsg = this.autoScalingGroup.node
			.defaultChild as autoscaling.CfnAutoScalingGroup
		cfnAsg.healthCheckType = 'EC2'
		cfnAsg.healthCheckGracePeriod = Duration.minutes(5).toSeconds()

		// Task 6.4: CloudFront distribution for video delivery with HLS optimization
		this.distribution = new cloudfront.Distribution(
			this,
			'StreamingDistribution',
			{
				defaultBehavior: {
					origin: origins.S3BucketOrigin.withOriginAccessControl(
						this.videoBucket,
					),
					viewerProtocolPolicy:
						cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
					cachePolicy: cloudfront.CachePolicy.CACHING_OPTIMIZED,
					allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
					cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
				},
				additionalBehaviors: {
					// HLS manifest files (.m3u8) - short TTL for live streaming
					'*.m3u8': {
						origin: origins.S3BucketOrigin.withOriginAccessControl(
							this.videoBucket,
						),
						viewerProtocolPolicy:
							cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
						cachePolicy: new cloudfront.CachePolicy(this, 'HLSManifestCache', {
							cachePolicyName: `${Stack.of(this).stackName}-HLSManifestCachePolicy`,
							comment: 'Cache policy for HLS manifest files',
							defaultTtl: Duration.seconds(2),
							minTtl: Duration.seconds(0),
							maxTtl: Duration.seconds(10),
							headerBehavior: cloudfront.CacheHeaderBehavior.none(),
							queryStringBehavior: cloudfront.CacheQueryStringBehavior.none(),
							cookieBehavior: cloudfront.CacheCookieBehavior.none(),
							enableAcceptEncodingGzip: false,
							enableAcceptEncodingBrotli: false,
						}),
						allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
						cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
					},
					// HLS video segments (.ts) - longer TTL
					'*.ts': {
						origin: origins.S3BucketOrigin.withOriginAccessControl(
							this.videoBucket,
						),
						viewerProtocolPolicy:
							cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
						cachePolicy: new cloudfront.CachePolicy(this, 'HLSSegmentCache', {
							cachePolicyName: `${Stack.of(this).stackName}-HLSSegmentCachePolicy`,
							comment: 'Cache policy for HLS video segments',
							defaultTtl: Duration.seconds(86400), // 24 hours
							minTtl: Duration.seconds(0),
							maxTtl: Duration.days(30),
							headerBehavior: cloudfront.CacheHeaderBehavior.none(),
							queryStringBehavior: cloudfront.CacheQueryStringBehavior.none(),
							cookieBehavior: cloudfront.CacheCookieBehavior.none(),
							enableAcceptEncodingGzip: false,
							enableAcceptEncodingBrotli: false,
						}),
						allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
						cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
					},
					// Snapshots (.jpg) - medium TTL
					'snapshots/*': {
						origin: origins.S3BucketOrigin.withOriginAccessControl(
							this.videoBucket,
						),
						viewerProtocolPolicy:
							cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
						cachePolicy: new cloudfront.CachePolicy(this, 'SnapshotCache', {
							cachePolicyName: `${Stack.of(this).stackName}-SnapshotCachePolicy`,
							comment: 'Cache policy for stream snapshots',
							defaultTtl: Duration.seconds(60),
							minTtl: Duration.seconds(0),
							maxTtl: Duration.seconds(300),
							headerBehavior: cloudfront.CacheHeaderBehavior.none(),
							queryStringBehavior: cloudfront.CacheQueryStringBehavior.none(),
							cookieBehavior: cloudfront.CacheCookieBehavior.none(),
							enableAcceptEncodingGzip: false,
							enableAcceptEncodingBrotli: false,
						}),
						allowedMethods: cloudfront.AllowedMethods.ALLOW_GET_HEAD,
						cachedMethods: cloudfront.CachedMethods.CACHE_GET_HEAD,
					},
				},
				comment: 'CloudFront distribution for Video Streaming',
				enableLogging: true,
				priceClass: cloudfront.PriceClass.PRICE_CLASS_100,
			},
		)

		// Task 6.5: Configure CloudWatch alarms
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

		// Alarm for FFmpeg process failures
		const ffmpegFailureAlarm = new cloudwatch.Alarm(
			this,
			'FFmpegFailureAlarm',
			{
				alarmName: `${Stack.of(this).stackName}-FFmpegFailures`,
				alarmDescription: 'Alarm when FFmpeg process failures exceed threshold',
				metric: new cloudwatch.Metric({
					namespace: Stack.of(this).stackName,
					metricName: 'FFmpegProcessFailures',
					statistic: 'Sum',
					period: Duration.minutes(5),
				}),
				threshold: 3,
				evaluationPeriods: 1,
				comparisonOperator:
					cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
				treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
			},
		)
		ffmpegFailureAlarm.addAlarmAction(
			new cloudwatch_actions.SnsAction(alarmTopic),
		)

		// Alarm for S3 upload failures
		const s3UploadFailureAlarm = new cloudwatch.Alarm(
			this,
			'S3UploadFailureAlarm',
			{
				alarmName: `${Stack.of(this).stackName}-S3UploadFailures`,
				alarmDescription: 'Alarm when S3 upload failures exceed threshold',
				metric: new cloudwatch.Metric({
					namespace: Stack.of(this).stackName,
					metricName: 'S3UploadFailures',
					statistic: 'Sum',
					period: Duration.minutes(5),
				}),
				threshold: 10,
				evaluationPeriods: 1,
				comparisonOperator:
					cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
				treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
			},
		)
		s3UploadFailureAlarm.addAlarmAction(
			new cloudwatch_actions.SnsAction(alarmTopic),
		)

		// Alarm for DynamoDB throttling
		const dynamoThrottleAlarm = new cloudwatch.Alarm(
			this,
			'DynamoThrottleAlarm',
			{
				alarmName: `${Stack.of(this).stackName}-DynamoDBThrottling`,
				alarmDescription: 'Alarm when DynamoDB requests are throttled',
				metric: this.streamTable.metricUserErrors({
					statistic: 'Sum',
					period: Duration.minutes(5),
				}),
				threshold: 5,
				evaluationPeriods: 2,
				comparisonOperator:
					cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
				treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
			},
		)
		dynamoThrottleAlarm.addAlarmAction(
			new cloudwatch_actions.SnsAction(alarmTopic),
		)

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
		new CfnOutput(this, 'IdentityPoolId', {
			value: this.identityPool.ref,
			description: 'Cognito Identity Pool ID for frontend',
		})

		new CfnOutput(this, 'DynamoDBTableName', {
			value: this.streamTable.tableName,
			description: 'DynamoDB table name for stream metadata',
		})

		new CfnOutput(this, 'VideoBucketName', {
			value: this.videoBucket.bucketName,
			description: 'S3 bucket name for video storage',
		})

		new CfnOutput(this, 'CloudFrontURL', {
			value: this.distribution.distributionDomainName,
			description: 'CloudFront distribution URL for video delivery',
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
	}
}
