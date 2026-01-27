import { createConfig } from './vite/config.ts'

export default createConfig({
	domainName: process.env.DOMAIN_NAME ?? 'app.sudo.pricing.nrfcloud.com',
	cognitoUserPoolURL: new URL(
		process.env.COGNITO_USER_POOL_URL ??
			'https://cognito-idp.eu-central-1.amazonaws.com/eu-central-1_AIb3Ir43E/',
	),
	cognitoUserPoolClientId:
		process.env.COGNITO_USER_POOL_CLIENT_ID ?? '5tkfrh5baeh3t0tp51ail9nc8f',
	cognitoIdentityPoolId:
		process.env.COGNITO_IDENTITY_POOL_ID ??
		'eu-central-1:945b0d2a-eab5-49d7-8c8e-768e80f1153c',
})
