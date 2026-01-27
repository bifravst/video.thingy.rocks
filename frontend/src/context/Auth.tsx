import { UserManager, type User } from 'oidc-client-ts'
import { createContext, type ComponentChildren } from 'preact'
import { useContext, useEffect, useState } from 'preact/hooks'
import { createAwsClientConfig, type AWSConfig } from '../utils/aws-auth.ts'

// create a UserManager instance
const loginRedirectURL = new URL(
	document.location.protocol + '//' + document.location.host + '/auth/callback',
)
const logoutRedirectURL = new URL(
	document.location.protocol + '//' + document.location.host + '/',
)
const userManager = new UserManager({
	authority: COGNITO_USER_POOL_URL,
	client_id: COGNITO_USER_POOL_CLIENT_ID,
	redirect_uri: loginRedirectURL.toString(),
	scope: 'email openid profile',
})

export const AuthContext = createContext<{
	logout: () => void
	user?: User
	awsConfig?: AWSConfig | undefined
}>({
	logout: () => undefined,
})

export const Provider = ({ children }: { children: ComponentChildren }) => {
	const [user, setUser] = useState<undefined | User>()
	const [awsConfig, setAwsConfig] = useState<undefined | AWSConfig>()

	useEffect(() => {
		console.debug(`[Auth]`, `Checking user session...`)
		userManager
			.signinCallback()
			.then((maybeUser) => {
				console.debug(`[Auth]`, `Signin callback processed successfully`)
				if (maybeUser !== undefined) {
					console.debug(`[Auth]`, `User session found:`, maybeUser)
					setUser(maybeUser)
				}
			})
			.catch(() => {
				// If signinCallback fails, it means we are not in a redirect callback
				console.debug(
					`[Auth]`,
					`No signin callback found, checking user session...`,
				)
				userManager
					.getUser()
					.then((maybeUser) => {
						if (maybeUser !== null) {
							console.debug(`[Auth]`, `User session found:`, maybeUser)
							setUser(maybeUser)
							return
						}
						console.debug(
							`[Auth]`,
							`No user session found, redirecting to login...`,
						)
						userManager
							.signinRedirect()
							.then(() => {
								console.debug(`[Auth]`, `Redirected to login`)
							})
							.catch((err) => {
								console.error(`[Auth]`, `Failed to redirect to login`, err)
							})
					})
					.catch((err) => {
						console.error(`[Auth]`, `Failed to get user session`, err)
					})
			})
	}, [])

	useEffect(() => {
		if (user === undefined) return
		createAwsClientConfig(user)
			.then(setAwsConfig)
			.catch((error) => {
				console.error('[Auth]', 'Failed to get AWS credentials', error)
			})
	}, [user])

	return (
		<AuthContext.Provider
			value={{
				logout: () => {
					console.debug(`[Auth]`, `Redirecting to logout...`)
					userManager
						.signoutRedirect({
							extraQueryParams: {
								client_id: userManager.settings.client_id,
								logout_uri: logoutRedirectURL.toString(),
							},
						})
						.then(() => {
							console.debug(`[Auth]`, `Redirected to logout`)
						})
						.catch((err) => {
							console.error(`[Auth]`, `Failed to redirect to logout`, err)
						})
				},
				user,
				awsConfig,
			}}
		>
			{children}
		</AuthContext.Provider>
	)
}

export const Consumer = AuthContext.Consumer

export const useAuth = () => useContext(AuthContext)
