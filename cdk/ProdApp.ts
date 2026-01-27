import { App } from 'aws-cdk-lib'

export class ProdApp extends App {
	constructor(args: {
		repository: { owner: string; repo: string }
		gitHubOICDProviderArn: string
		context: {
			version: string
		} & Record<string, any>
	}) {
		super({
			context: {
				...args.context,
				isTest: false,
			},
		})
	}
}
