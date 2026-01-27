import { formatDistanceToNow } from 'date-fns'
import { render } from 'preact'
import { App } from './App.tsx'

console.debug('version', VERSION)
console.debug(
	'build time',
	BUILD_TIME,
	formatDistanceToNow(new Date(BUILD_TIME), {
		addSuffix: true,
	}),
)
console.debug('Cognito UserPool URL', COGNITO_USER_POOL_URL)
console.debug('Cognito UserPool Client ID', COGNITO_USER_POOL_CLIENT_ID)

const root = document.getElementById('root')

if (root === null) {
	console.error(`Could not find root element!`)
} else {
	render(<App />, root)
}
