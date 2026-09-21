import assert from 'node:assert/strict'
import net from 'node:net'
import { afterEach, describe, it } from 'node:test'

import { HealthServer } from './HealthServer.ts'

/** Binds a port the OS picks, so the tests never collide with a real service. */
const bindEphemeral = async (): Promise<{
	port: number
	close: () => Promise<void>
}> => {
	const blocker = net.createServer()
	const port = await new Promise<number>((resolve, reject) => {
		blocker.once('error', reject)
		blocker.listen(0, '::', () => {
			const address = blocker.address()
			if (address === null || typeof address === 'string') {
				reject(new Error('expected an AddressInfo'))
				return
			}
			resolve(address.port)
		})
	})
	return {
		port,
		close: async () =>
			new Promise<void>((resolve) => {
				blocker.close(() => resolve())
			}),
	}
}

const freePort = async (): Promise<number> => {
	const { port, close } = await bindEphemeral()
	await close()
	return port
}

void describe('HealthServer', () => {
	const started: HealthServer[] = []
	const track = (server: HealthServer): HealthServer => {
		started.push(server)
		return server
	}

	afterEach(async () => {
		while (started.length > 0) await started.pop()!.stop()
	})

	void it('accepts a TCP connection and closes it immediately', async () => {
		const port = await freePort()
		const server = track(new HealthServer(port))
		await server.start()

		await new Promise<void>((resolve, reject) => {
			// The server binds '::' (dual-stack); connect over IPv4 because some CI
			// sandboxes have no reachable IPv6 loopback even though '::' binds fine.
			const socket = net.connect({ port, host: '127.0.0.1' })
			const timer = setTimeout(() => {
				socket.destroy()
				reject(new Error('health port did not accept a connection'))
			}, 5000)
			socket.once('error', (err) => {
				clearTimeout(timer)
				reject(err)
			})
			// The server calls socket.end(), so the client sees the connection close
			// without having sent or received anything - which is all the NLB checks.
			socket.once('close', () => {
				clearTimeout(timer)
				resolve()
			})
		})
	})

	void it('rejects instead of throwing uncaught when the port is taken', async () => {
		const { port, close } = await bindEphemeral()
		try {
			const server = new HealthServer(port)
			// Before the 'error' listener was attached ahead of listen(), net.Server
			// emitted 'error' with no handler, which Node escalates to an uncaught
			// exception: the caller could not catch it and the process died.
			await assert.rejects(
				async () => server.start(),
				(err: unknown) =>
					err instanceof Error && err.message.includes('EADDRINUSE'),
			)
			// A failed start must not leave a server behind that keeps the event loop alive.
			await server.stop()
		} finally {
			await close()
		}
	})

	void it('reports the port it was constructed with, defaulting to 9999', () => {
		assert.strictEqual(new HealthServer().port, 9999)
		assert.strictEqual(new HealthServer(9998).port, 9998)
	})

	void it('can be stopped when never started, and stopped twice', async () => {
		const server = new HealthServer(await freePort())
		await server.stop()
		await server.start()
		await server.stop()
		await server.stop()
	})

	void it('can be started again after being stopped', async () => {
		const port = await freePort()
		const server = track(new HealthServer(port))
		await server.start()
		await server.stop()
		await server.start()
	})

	void it('refuses a second concurrent start', async () => {
		const server = track(new HealthServer(await freePort()))
		await server.start()
		await assert.rejects(async () => server.start(), /already started/)
	})
})
