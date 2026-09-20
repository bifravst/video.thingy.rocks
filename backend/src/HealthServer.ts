import net from 'node:net'

/**
 * Simple TCP server for NLB health checks.
 * NLB uses TCP (not UDP) for health checks; the backend only listens on UDP.
 * This server accepts connections on a dedicated port - a successful TCP
 * connection is enough for the health check to pass.
 *
 * The port is what ties the signal to a transport: the unencrypted target groups
 * health-check the default port (9999), which index.ts only opens once the
 * unencrypted listener is bound; the SRTP target groups health-check 9998, which
 * only opens after the SRTP keys are loaded AND the SRTP listener is bound. An
 * open port therefore means "this instance can ingest on this transport", for both
 * the NLB's own health checks and the fleet-cutover readiness gate.
 */
export class HealthServer {
	private readonly port: number
	private server?: net.Server

	constructor(port = 9999) {
		this.port = port
	}

	async start(): Promise<void> {
		return new Promise((resolve) => {
			this.server = net.createServer((socket) => {
				socket.end()
			})
			this.server.listen(this.port, '::', () => {
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
