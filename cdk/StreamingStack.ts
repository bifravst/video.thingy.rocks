import {
	CfnOutput,
	Duration,
	RemovalPolicy,
	Stack,
	type Environment,
} from 'aws-cdk-lib'
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront'
import * as origins from 'aws-cdk-lib/aws-cloudfront-origins'
import * as cognito from 'aws-cdk-lib/aws-cognito'
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb'
import * as ec2 from 'aws-cdk-lib/aws-ec2'
import * as iam from 'aws-cdk-lib/aws-iam'
import * as s3 from 'aws-cdk-lib/aws-s3'
import type { Construct } from 'constructs'

export class StreamingStack extends Stack {
	public readonly vpc: ec2.Vpc
	public readonly videoBucket: s3.Bucket
	public readonly streamTable: dynamodb.Table
	public readonly identityPool: cognito.CfnIdentityPool
	public readonly unauthRole: iam.Role
	public readonly udpSecurityGroup: ec2.SecurityGroup
	public readonly distribution: cloudfront.Distribution

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

		// CloudFront distribution for video delivery
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
				},
			},
		)

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
	}
}
