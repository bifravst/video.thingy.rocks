import pJSON from '../../package.json' with { type: 'json' }

export type RepositoryInfo = { owner: string; repo: string }

export const repoInfo = (): RepositoryInfo => {
	const repoUrl = new URL(pJSON.repository.url)
	const repository = {
		owner: repoUrl.pathname.split('/')[1] ?? 'nRFCloud',
		repo:
			repoUrl.pathname.split('/')[2]?.replace(/\.git$/, '') ??
			'account-service-next',
	}

	return repository
}
