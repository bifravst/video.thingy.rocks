import { createConfig } from './vite/config.ts'

// Configuration defaults from deployed stack
// After deploying the CDK stack, run: ./scripts/get-stack-outputs.sh
// Then update these defaults or set environment variables

export default createConfig({
	domainName: process.env.DOMAIN_NAME ?? 'video.thingy.rocks',
	// Cognito User Pool URL - get from stack output: UserPoolURL
	cognitoUserPoolURL: new URL(
		process.env.COGNITO_USER_POOL_URL ??
			'https://cognito-idp.eu-central-1.amazonaws.com/eu-central-1_GjNgDoLVR/',
	),
	// Cognito User Pool Client ID - get from stack output: UserPoolClientId
	cognitoUserPoolClientId:
		process.env.COGNITO_USER_POOL_CLIENT_ID ?? '548eqg40gjju4fidu7ddl4348v',
	// Cognito Identity Pool ID - get from stack output: IdentityPoolId
	cognitoIdentityPoolId:
		process.env.COGNITO_IDENTITY_POOL_ID ??
		'eu-central-1:8769a410-4de8-4fa7-9f5a-c91fc77bf67d',
	// DynamoDB table name - get from stack output: DynamoDBTableName
	dynamodbTableName:
		process.env.DYNAMODB_TABLE_NAME ??
		'video-streaming-StreamMetadataD1FE2960-1IL7LYVPF9K2B',
	// CloudFront domain - get from stack output: CloudFrontURL
	cloudfrontDomainName:
		process.env.CLOUDFRONT_DOMAIN_NAME ?? 'd2a51hbjpjqs00.cloudfront.net',
})
