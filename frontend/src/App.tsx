import { Provider as AuthProvider, useAuth } from '#context/Auth.tsx'
import { Cameras } from '#page/Cameras.tsx'
import { LoggingIn } from '#page/LoggingIn.tsx'
import { Component } from 'preact'
import { route, Route, Router } from 'preact-router'

export const App = () => (
	<AuthProvider>
		<Routing />
	</AuthProvider>
)

export const Routing = () => {
	const { user } = useAuth()
	if (user === undefined)
		return (
			<Router>
				<Route path="/" component={LoggingIn} />
				<Route path="/auth/callback" component={LoggingIn} />
			</Router>
		)
	return (
		<Router>
			<Route path="/" component={Cameras} />
			<Redirect path="/auth/callback" to="/" />
		</Router>
	)
}

class Redirect extends Component<{ to: string }> {
	override componentWillMount() {
		route(this.props.to, true)
	}

	render() {
		return null
	}
}
