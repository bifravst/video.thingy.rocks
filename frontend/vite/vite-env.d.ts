// These constants are string-replaced compile time.

// See https://vitejs.dev/config/shared-options.html#define
declare const VERSION: string
declare const HOMEPAGE: string
declare const BUILD_TIME: string
declare const COGNITO_USER_POOL_URL: string
declare const COGNITO_USER_POOL_CLIENT_ID: string
declare const COGNITO_IDENTITY_POOL_ID: string
/** Domain name where app is hosted. */
declare const DOMAIN_NAME: string
/** DynamoDB table name for stream metadata. */
declare const DYNAMODB_TABLE_NAME: string
/** CloudFront domain name for video content. */
declare const CLOUDFRONT_DOMAIN_NAME: string

// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
interface ImportMeta {
	readonly env: ImportMetaEnv
}
