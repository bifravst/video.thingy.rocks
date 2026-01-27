import {
	DiffMethod,
	MemoryContext,
	StackSelectionStrategy,
	Toolkit,
} from '@aws-cdk/toolkit-lib'
import { IAMClient } from '@aws-sdk/client-iam'
import { ensureGitHubOIDCProvider } from '@bifravst/ci'
import { fromEnv } from '@bifravst/from-env'
import commandLineArgs from 'command-line-args'
import { readFile } from 'node:fs/promises'
import path from 'node:path'
import { repoInfo } from './helper/repo.ts'
import { ProdApp } from './ProdApp.ts'

const options = commandLineArgs([
	{
		name: 'diff',
		type: Boolean,
		defaultValue: false,
	},
	{
		name: 'destroy',
		type: Boolean,
		defaultValue: false,
	},
	{
		name: 'stackName',
		type: String,
	},
])

const iam = new IAMClient({})

const { version } = fromEnv({
	version: 'VERSION',
})(process.env)

const ctx = {
	...JSON.parse(
		await readFile(path.join(process.cwd(), 'cdk.context.json'), 'utf-8'),
	),
	version,
}

const app = new ProdApp({
	repository: repoInfo(),
	gitHubOICDProviderArn: await ensureGitHubOIDCProvider({
		iam,
	}),
	context: ctx,
})

const cdk = new Toolkit()

const cx = await cdk.fromAssemblyBuilder(async () => app.synth(), {
	contextStore: new MemoryContext(ctx),
})

const stacks =
	options.stackName !== undefined
		? {
				strategy: StackSelectionStrategy.PATTERN_MUST_MATCH_SINGLE,
				patterns: [options.stackName],
			}
		: undefined

if (options.diff === true) {
	await cdk.diff(cx, { method: DiffMethod.TemplateOnly(), stacks })
} else if (options.destroy === true) {
	await cdk.destroy(cx, {
		stacks,
	})
} else {
	await cdk.deploy(cx, {
		stacks,
	})
}
