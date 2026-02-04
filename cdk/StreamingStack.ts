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
import * as cognito from 'aws-cdk-lib/aws-cognito'
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb'
import * as ec2 from 'aws-cdk-lib/aws-ec2'
import * as iam from 'aws-cdk-lib/aws-iam'
import * as kinesisvideo from 'aws-cdk-lib/aws-kinesisvideo'
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
	public readonly userPool: cognito.UserPool
	public readonly userPoolClient: cognito.UserPoolClient
	public readonly identityPool: cognito.CfnIdentityPool
	public readonly unauthRole: iam.Role
	public readonly authRole: iam.Role
	public readonly udpSecurityGroup: ec2.SecurityGroup
	public readonly ec2Role: iam.Role
	public readonly autoScalingGroup: autoscaling.AutoScalingGroup
	public readonly codeBucket: s3.Bucket
	public readonly kinesisVideoStreams: kinesisvideo.CfnStream[] = []

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
					dataRetentionInHours: 1,
					mediaType: 'video/h264',
				},
			)
			this.kinesisVideoStreams.push(stream)
		}

		// Create Cognito User Pool for authenticated access
		this.userPool = new cognito.UserPool(this, 'UserPool', {
			userPoolName: `${this.stackName}-users`,
			selfSignUpEnabled: true,
			signInAliases: {
				email: true,
			},
			autoVerify: {
				email: true,
			},
			standardAttributes: {
				email: {
					required: true,
					mutable: true,
				},
			},
			passwordPolicy: {
				minLength: 8,
				requireLowercase: true,
				requireUppercase: true,
				requireDigits: true,
				requireSymbols: false,
			},
			accountRecovery: cognito.AccountRecovery.EMAIL_ONLY,
			removalPolicy: RemovalPolicy.DESTROY,
			// Email configuration - use Cognito default for now
			// To use custom SES: configure email property with SES settings
			email: cognito.UserPoolEmail.withCognito('noreply@verificationemail.com'),
			// Verification messages
			userVerification: {
				emailSubject: 'Verify your email for Video Streaming',
				emailBody: 'Thank you for signing up! Your verification code is {####}',
				emailStyle: cognito.VerificationEmailStyle.CODE,
			},
		})

		// Create User Pool Client for frontend
		this.userPoolClient = this.userPool.addClient('WebClient', {
			userPoolClientName: `${this.stackName}-web-client`,
			authFlows: {
				userPassword: true,
				userSrp: true,
			},
			oAuth: {
				flows: {
					authorizationCodeGrant: true,
					implicitCodeGrant: true,
				},
				scopes: [
					cognito.OAuthScope.EMAIL,
					cognito.OAuthScope.OPENID,
					cognito.OAuthScope.PROFILE,
				],
				callbackUrls: [
					'http://localhost:8080/auth/callback',
					'https://video.thingy.rocks/auth/callback',
				],
				logoutUrls: ['http://localhost:8080/', 'https://video.thingy.rocks/'],
			},
			preventUserExistenceErrors: true,
		})

		// Add domain for hosted UI
		this.userPool.addDomain('UserPoolDomain', {
			cognitoDomain: {
				domainPrefix: `${this.stackName}-${this.account}`.toLowerCase(),
			},
		})

		this.identityPool = new cognito.CfnIdentityPool(
			this,
			'StreamViewerIdentityPool',
			{
				allowUnauthenticatedIdentities: true,
				identityPoolName: `${this.stackName}-viewers`,
				cognitoIdentityProviders: [
					{
						clientId: this.userPoolClient.userPoolClientId,
						providerName: this.userPool.userPoolProviderName,
					},
				],
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
		// Also grant Scan permission for listing all streams
		this.unauthRole.addToPolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: ['dynamodb:Scan'],
				resources: [this.streamTable.tableArn],
			}),
		)

		// Create IAM role for authenticated users with read-only DynamoDB access
		this.authRole = new iam.Role(this, 'AuthRole', {
			assumedBy: new iam.FederatedPrincipal(
				'cognito-identity.amazonaws.com',
				{
					StringEquals: {
						'cognito-identity.amazonaws.com:aud': this.identityPool.ref,
					},
					'ForAnyValue:StringLike': {
						'cognito-identity.amazonaws.com:amr': 'authenticated',
					},
				},
				'sts:AssumeRoleWithWebIdentity',
			),
		})

		// Grant read-only access to StreamMetadata table for authenticated users
		this.streamTable.grantReadData(this.authRole)
		// Also grant Scan permission for listing all streams
		this.authRole.addToPolicy(
			new iam.PolicyStatement({
				effect: iam.Effect.ALLOW,
				actions: ['dynamodb:Scan'],
				resources: [this.streamTable.tableArn],
			}),
		)

		// Attach the roles to the identity pool
		new cognito.CfnIdentityPoolRoleAttachment(
			this,
			'IdentityPoolRoleAttachment',
			{
				identityPoolId: this.identityPool.ref,
				roles: {
					unauthenticated: this.unauthRole.roleArn,
					authenticated: this.authRole.roleArn,
				},
			},
		)

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
		new CfnOutput(this, 'UserPoolId', {
			value: this.userPool.userPoolId,
			description: 'Cognito User Pool ID',
		})

		new CfnOutput(this, 'UserPoolURL', {
			value: `https://cognito-idp.${this.region}.amazonaws.com/${this.userPool.userPoolId}/`,
			description: 'Cognito User Pool URL for OIDC',
		})

		new CfnOutput(this, 'UserPoolClientId', {
			value: this.userPoolClient.userPoolClientId,
			description: 'Cognito User Pool Client ID',
		})

		new CfnOutput(this, 'IdentityPoolId', {
			value: this.identityPool.ref,
			description: 'Cognito Identity Pool ID for frontend',
		})

		new CfnOutput(this, 'DynamoDBTableName', {
			value: this.streamTable.tableName,
			description: 'DynamoDB table name for stream metadata',
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
