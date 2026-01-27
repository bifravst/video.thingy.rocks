import { CognitoIdentityClient } from '@aws-sdk/client-cognito-identity'
import {
	fromCognitoIdentityPool,
	type CognitoIdentityCredentials,
} from '@aws-sdk/credential-provider-cognito-identity'
import type { User } from 'oidc-client-ts'

const userPoolURL = new URL(COGNITO_USER_POOL_URL)
const identityPoolId = COGNITO_IDENTITY_POOL_ID
const region = identityPoolId.split(':')[0]!

console.log('[AWS Auth]', 'region', region)
console.log('[AWS Auth]', 'identityPoolId', identityPoolId)
const userPoolHostName = (userPoolURL.hostname + userPoolURL.pathname).replace(
	/\/$/,
	'',
)
console.log('[AWS Auth]', 'User pool hostname', userPoolHostName)

export type AWSConfig = { region: string; credentials: any }

/**
 * Get AWS credentials using the ID token from the authenticated user
 */
export const getCredentialsFromUser = async (
	user: User,
): Promise<CognitoIdentityCredentials> => {
	if (user?.id_token === undefined) {
		throw new Error('User is not authenticated or ID token is missing')
	}

	const cognitoIdentityClient = new CognitoIdentityClient({
		region,
	})

	const credentialsProvider = fromCognitoIdentityPool({
		client: cognitoIdentityClient,
		identityPoolId,
		logins: {
			[userPoolHostName]: user.id_token,
		},
	})

	return credentialsProvider()
}

/**
 * Create AWS SDK client configuration with credentials from the authenticated user
 */
export const createAwsClientConfig = async (user: User): Promise<AWSConfig> => {
	const credentials = await getCredentialsFromUser(user)

	return {
		region,
		credentials,
	}
}
