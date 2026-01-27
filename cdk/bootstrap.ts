import { BootstrapEnvironments, Toolkit } from '@aws-cdk/toolkit-lib'
import { env } from '../aws/env.ts'

const e = await env()

const cdk = new Toolkit()

await cdk.bootstrap(
	BootstrapEnvironments.fromList([`aws://${e.account}/${e.region}`]),
)
