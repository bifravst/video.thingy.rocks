import { Provider as AuthProvider, useAuth } from '#context/Auth.tsx'
import { Cameras } from '#page/Cameras.tsx'
import { LoggingIn } from '#page/LoggingIn.tsx'
import { StreamPlayer } from '#page/StreamPlayer.tsx'
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
			<Route path="/stream/:port" component={StreamPlayerRoute} />
			<Redirect path="/auth/callback" to="/" />
		</Router>
	)
}

const StreamPlayerRoute = ({ port }: { port: string }) => {
	const portNumber = parseInt(port, 10)
	if (isNaN(portNumber)) {
		return (
			<div style={{ padding: '2rem', textAlign: 'center' }}>
				Invalid port number
			</div>
		)
	}
	return <StreamPlayer port={portNumber} />
}

class Redirect extends Component<{ to: string }> {
	override componentWillMount() {
		route(this.props.to, true)
	}

	render() {
		return null
	}
}
