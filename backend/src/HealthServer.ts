import net from 'node:net'

import { Logger } from './Logger.ts'

const DEFAULT_HEALTH_PORT = 9999

/**
 * Simple TCP server for NLB health checks.
 * NLB uses TCP (not UDP) for health checks; the backend only listens on UDP.
 * This server accepts connections on a dedicated port - a successful TCP
 * connection is enough for the health check to pass.
 *
 * An open port means "this instance's backend is up", and the NLB target groups for
 * every ingest port health-check it. It deliberately does NOT represent per-transport
 * or per-port ingest readiness: the Auto Scaling group uses ELB health checks, so any
 * target group reporting an instance unhealthy gets that instance replaced - and a
 * condition that is identical on every instance (a missing plugin, an SSM outage, an
 * unprovisioned key) would then churn the whole fleet into identically broken
 * replacements instead of failing one instance that replacement can actually fix.
 */
export class HealthServer {
	readonly port: number
	private server?: net.Server
	private readonly logger: Logger

	constructor(port: number = DEFAULT_HEALTH_PORT) {
		this.port = port
		this.logger = new Logger('HealthServer')
	}

	/**
	 * Binds the health port.
	 *
	 * Rejects if the port cannot be bound. net.Server emits 'error' on an EventEmitter
	 * with no listener, which Node turns into an uncaught exception rather than anything
	 * a caller can catch - so without the listener attached before listen(), a port
	 * conflict here terminates the process instead of being reported to the caller.
	 */
	async start(): Promise<void> {
		if (this.server !== undefined) {
			throw new Error(`Health server already started on port ${this.port}`)
		}
		const server = net.createServer((socket) => {
			socket.end()
		})
		this.server = server

		try {
			await new Promise<void>((resolve, reject) => {
				const onError = (err: Error): void => {
					server.removeListener('listening', onListening)
					reject(err)
				}
				const onListening = (): void => {
					server.removeListener('error', onError)
					resolve()
				}
				server.once('error', onError)
				server.once('listening', onListening)
				server.listen(this.port, '::')
			})
		} catch (err) {
			// Leave no half-open server behind, so a caller that retries or continues
			// without this port does not leak a handle that keeps the process alive.
			this.server = undefined
			server.close()
			throw err
		}

		// Past listen(), an error must not become uncaught either.
		server.on('error', (err) => {
			this.logger.error('Health server error', err, { port: this.port })
		})
		this.logger.info('Listening', { port: this.port })
	}

	/** Idempotent: safe to call when never started, or more than once. */
	async stop(): Promise<void> {
		const server = this.server
		if (server === undefined) return
		this.server = undefined
		return new Promise((resolve) => {
			server.close(() => resolve())
		})
	}
}
