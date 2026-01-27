import { App, type Environment } from 'aws-cdk-lib'
import { StreamingStack } from './StreamingStack.ts'
import { STREAMING_STACK_NAME } from './stackName.ts'

export class ProdApp extends App {
	constructor(args: {
		repository: { owner: string; repo: string }
		gitHubOICDProviderArn: string
		context: {
			version: string
		} & Record<string, any>
		env: Environment
		availabilityZones: Set<string>
	}) {
		super({
			context: {
				...args.context,
				isTest: false,
			},
		})

		new StreamingStack(this, STREAMING_STACK_NAME, {
			env: args.env,
			availabilityZones: args.availabilityZones,
		})
	}
}
