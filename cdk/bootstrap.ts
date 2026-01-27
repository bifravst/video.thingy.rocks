import { BootstrapEnvironments, Toolkit } from '@aws-cdk/toolkit-lib'
import { readFile } from 'node:fs/promises'
import path from 'node:path'
import { env } from '../aws/env.ts'

const e = await env()

const cdk = new Toolkit()

const ctx = JSON.parse(
	await readFile(path.join(process.cwd(), 'cdk.context.json'), 'utf-8'),
)

await cdk.bootstrap(
	BootstrapEnvironments.fromList([`aws://${e.account}/${e.region}`]),
)

await cdk.bootstrap(
	BootstrapEnvironments.fromList([
		`aws://${e.account}/${ctx.pricingService.region}`,
	]),
)
