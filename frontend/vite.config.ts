import { createConfig } from './vite/config.ts'

// Configuration defaults from deployed stack
// After deploying the CDK stack, run: ./scripts/get-stack-outputs.sh
// Then update these defaults or set environment variables

export default createConfig({
	domainName: process.env.DOMAIN_NAME ?? 'video.thingy.rocks',
	// Cognito User Pool URL - get from stack output: UserPoolURL
	cognitoUserPoolURL: new URL(
		process.env.COGNITO_USER_POOL_URL ??
			'https://cognito-idp.eu-central-1.amazonaws.com/eu-central-1_AIb3Ir43E/',
	),
	// Cognito User Pool Client ID - get from stack output: UserPoolClientId
	cognitoUserPoolClientId:
		process.env.COGNITO_USER_POOL_CLIENT_ID ?? '5tkfrh5baeh3t0tp51ail9nc8f',
	// Cognito Identity Pool ID - get from stack output: IdentityPoolId
	cognitoIdentityPoolId:
		process.env.COGNITO_IDENTITY_POOL_ID ??
		'eu-central-1:945b0d2a-eab5-49d7-8c8e-768e80f1153c',
	// DynamoDB table name - get from stack output: DynamoDBTableName
	dynamodbTableName:
		process.env.DYNAMODB_TABLE_NAME ??
		'video-streaming-StreamMetadataD1FE2960-1IL7LYVPF9K2B',
	// CloudFront domain - get from stack output: CloudFrontURL
	cloudfrontDomainName:
		process.env.CLOUDFRONT_DOMAIN_NAME ?? 'PLACEHOLDER.cloudfront.net',
})
