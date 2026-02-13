import net from 'node:net'

const HEALTH_PORT = 9999

/**
 * Simple TCP server for NLB health checks.
 * NLB uses TCP (not UDP) for health checks; the backend only listens on UDP.
 * This server accepts connections on a dedicated port - a successful TCP
 * connection is enough for the health check to pass.
 */
export class HealthServer {
	private server?: net.Server

	async start(): Promise<void> {
		return new Promise((resolve) => {
			this.server = net.createServer((socket) => {
				socket.end()
			})
			this.server.listen(HEALTH_PORT, '0.0.0.0', () => {
				resolve()
			})
		})
	}

	async stop(): Promise<void> {
		if (!this.server) return
		return new Promise((resolve) => {
			this.server!.close(() => resolve())
		})
	}
}
