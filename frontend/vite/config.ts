import { preact } from '@preact/preset-vite'
import { defineConfig, type PluginOption } from 'vite'
import { replaceInIndex } from './replaceInIndex.ts'
import { homepage, version } from './siteInfo.ts'

export const createConfig = ({
	cognitoUserPoolURL,
	cognitoUserPoolClientId,
	cognitoIdentityPoolId,
	domainName,
	dynamodbTableName,
	cloudfrontDomainName,
	plugins,
}: {
	cognitoUserPoolURL: URL
	cognitoUserPoolClientId: string
	cognitoIdentityPoolId: string
	domainName: string
	dynamodbTableName: string
	cloudfrontDomainName: string
	plugins?: PluginOption[]
}): ReturnType<typeof defineConfig> => {
	const define: Record<string, string> = {
		HOMEPAGE: JSON.stringify(homepage),
		VERSION: JSON.stringify(version),
		BUILD_TIME: JSON.stringify(new Date().toISOString()),
		DOMAIN_NAME: JSON.stringify(domainName),
		COGNITO_USER_POOL_URL: JSON.stringify(cognitoUserPoolURL.toString()),
		COGNITO_USER_POOL_CLIENT_ID: JSON.stringify(cognitoUserPoolClientId),
		COGNITO_IDENTITY_POOL_ID: JSON.stringify(cognitoIdentityPoolId),
		DYNAMODB_TABLE_NAME: JSON.stringify(dynamodbTableName),
		CLOUDFRONT_DOMAIN_NAME: JSON.stringify(cloudfrontDomainName),
	}
	for (const [k, v] of Object.entries(define)) {
		console.debug(`[vite define] ${k}:`, v)
	}

	return defineConfig({
		plugins: [
			preact({
				babel: {
					plugins: ['@babel/plugin-syntax-import-assertions'],
				},
			}),
			replaceInIndex({
				version,
			}),
			...(plugins ?? []),
		],
		preview: {
			host: 'localhost',
			port: 8080,
		},
		server: {
			host: 'localhost',
			port: 8080,
		},
		resolve: {
			alias: [
				{ find: '#components/', replacement: '/src/components/' },
				{ find: '#context/', replacement: '/src/context/' },
				{ find: '#page/', replacement: '/src/page/' },
				{ find: '#utils/', replacement: '/src/utils/' },
			],
		},
		build: {
			outDir: './build',
			sourcemap: true,
		},
		esbuild: {
			logOverride: { 'this-is-undefined-in-esm': 'silent' },
		},
		// string values will be used as raw expressions, so if defining a string constant, it needs to be explicitly quoted
		define,
	})
}
