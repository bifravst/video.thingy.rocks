import { spawn, spawnSync } from 'node:child_process'
import { EventEmitter } from 'node:events'
import * as fs from 'node:fs'
import * as net from 'node:net'
import * as os from 'node:os'
import * as path from 'node:path'
import type { Writable } from 'node:stream'

import { fromNodeProviderChain } from '@aws-sdk/credential-providers'
import { endChildProcess, watchForExit } from './ChildProcessExit.ts'
import { Logger } from './Logger.ts'

export type KinesisIngestionPipelineConfig = {
	streamNamePrefix: string
	region: string
	portRange: { start: number; end: number }
	/**
	 * Max packets to buffer before writing to GStreamer stdin (0 = write immediately).
	 * Packets are emitted in receive order; does not fix network reordering.
	 * Default 128. Helps smooth bursts and applies backpressure.
	 */
	reorderBufferSize?: number
	/**
	 * When true, log GStreamer/KVS stdout and stderr. Disabled by default to avoid noisy logs.
	 */
	logGstreamerOutput?: boolean
	/** How long a closed input gets to end the process on its own; see stop(). */
	stopDrainMs?: number
	/** How long SIGTERM gets before SIGKILL follows it; see stop(). */
	stopSigkillAfterMs?: number
	/**
	 * How long GStreamer gets to open its input and take the initial data before the
	 * start fails; see openInput.
	 */
	inputTimeoutMs?: number
	/** Replaces child_process.spawn, so tests can stand in for GStreamer. */
	spawn?: typeof spawn
}

const DEFAULT_REORDER_BUFFER_SIZE = 128

type ReorderState = {
	nextSeq: number
	nextToEmit: number
	buffer: Map<number, Buffer>
}

/** Per-port throttle for GStreamer stderr warning categories. */
type GstStderrThrottle = { lastLog: Record<string, number> }

/** Per-port throttle for noisy GStreamer/KVS stdout lines (CONTINUITY, 0x30000005, etc.). */
type GstStdoutThrottle = { lastLog: Record<string, number> }

type PortPipeline = {
	gst: ReturnType<typeof spawn>
	/** Stream we write TS to (stdin pipe or FIFO). */
	inputStream: Writable
	reorder: ReorderState
	gstStderrThrottle: GstStderrThrottle
	gstStdoutThrottle: GstStdoutThrottle
	/** FIFO path when using filesrc; unlink on stop. */
	fifoPath?: string
}

/**
 * How long closing the input gets to end GStreamer before it is signalled.
 *
 * kvssink uploads what it is holding when the stream ends, so this is the window in
 * which a clean stop keeps the tail of the recording.
 */
const STOP_DRAIN_MS = 15_000
/** How long SIGTERM gets to end the stream cleanly before SIGKILL follows it. */
const STOP_SIGKILL_AFTER_MS = 8_000
/**
 * How long GStreamer gets, once spawned, to open its input and take the initial data.
 *
 * Generous, because it is not just process start: a pipeline changes state from its
 * sink upwards, so kvssink sets itself up - its Kinesis Video client included - before
 * filesrc opens the FIFO, and the initial data is the whole startup buffer, megabytes
 * against a pipe that holds 64 KiB. It only has to be finite, so that a child that
 * never opens its input, or opens it and stops reading, cannot hold the port and its
 * lock for good.
 */
const INPUT_TIMEOUT_MS = 30_000
/** How often the FIFO is tried while GStreamer has not opened its end yet. */
const INPUT_OPEN_POLL_MS = 20
/** Throttle repeated GStreamer stderr warnings (same category) per port. */
const GST_STDERR_THROTTLE_MS = 60_000
/** Throttle noisy stdout (CONTINUITY, KVS 0x30000005, "Could not write to resource") per port. */
const GST_STDOUT_THROTTLE_MS = 60_000

/**
 * Per-port pipeline: UDP packets -> GStreamer (TS -> H.264) -> kvssink -> Kinesis Video.
 * One GStreamer process per active port; kvssink sends directly to Kinesis (no Node PutMedia).
 */
export class KinesisIngestionPipeline extends EventEmitter {
	private readonly config: KinesisIngestionPipelineConfig
	private readonly logger: Logger
	private readonly activePipelines: Map<number, PortPipeline> = new Map()
	/** Dedupe concurrent start(port) so only one credential fetch + spawn runs per port. */
	private readonly pendingStarts: Map<number, Promise<void>> = new Map()

	constructor(config: KinesisIngestionPipelineConfig) {
		super()
		this.config = config
		this.logger = new Logger('KinesisIngestionPipeline')
	}

	streamNameForPort(port: number): string {
		return `${this.config.streamNamePrefix}-${port}`
	}

	isPortInRange(port: number): boolean {
		return (
			port >= this.config.portRange.start && port <= this.config.portRange.end
		)
	}

	/**
	 * Logs GStreamer stderr; throttles repeated warnings per category.
	 */
	private logGstStderr(
		port: number,
		streamName: string,
		text: string,
		throttle: GstStderrThrottle,
	): void {
		if (this.config.logGstreamerOutput !== true) return
		const now = Date.now()
		const lines = text.split(/\r?\n/)
		for (const raw of lines) {
			const line = raw.trim()
			if (line.length === 0) continue

			let category: string | null = null
			if (/error|ERROR/i.test(line)) category = 'error'
			else if (/warning|WARN/i.test(line)) category = 'warning'

			if (category !== null) {
				const last = throttle.lastLog[category] ?? 0
				if (now - last < GST_STDERR_THROTTLE_MS) continue
				throttle.lastLog[category] = now
			}

			this.logger.warn('GStreamer stderr', {
				port,
				streamName,
				message: line.slice(0, 500),
			})
		}
	}

	/**
	 * Logs GStreamer stdout; throttles noisy lines (CONTINUITY, KVS 0x30000005, "Could not write to resource").
	 */
	private logGstStdout(
		port: number,
		streamName: string,
		text: string,
		throttle: GstStdoutThrottle,
	): void {
		if (this.config.logGstreamerOutput !== true) return
		const now = Date.now()
		const lines = text.split(/\r?\n/)
		for (const raw of lines) {
			const line = raw.trim()
			if (line.length === 0) continue
			if (/streamLatencyPressure/i.test(line)) continue
			if (/droppedFrame callback/i.test(line)) continue
			if (/viewItemRemoved.*Reporting a dropped frame\/fragment/i.test(line))
				continue
			if (/Failed to submit ACK|0x52000047|status code: 0x52/i.test(line))
				continue

			let category: string | null = null
			if (/CONTINUITY:\s*Mismatch/i.test(line)) category = 'continuity'
			else if (
				/0x30000005|putKinesisVideoFrame.*Failed|Put frame.*failed/i.test(line)
			)
				category = 'kvs_putframe'
			else if (/Could not write to resource/i.test(line))
				category = 'write_resource'

			if (category !== null) {
				const last = throttle.lastLog[category] ?? 0
				if (now - last < GST_STDOUT_THROTTLE_MS) continue
				throttle.lastLog[category] = now
			}

			this.logger.info('GStreamer stdout', {
				port,
				streamName,
				message: line.slice(0, 1000),
			})
		}
	}

	/**
	 * Starts the pipeline for a port: spawns GStreamer with kvssink.
	 * Idempotent: no-op if already running for this port.
	 * Concurrent calls for the same port are deduped (single credential fetch + spawn).
	 * Optional initialData is written to stdin immediately after spawn so fdsrc has data when it first reads.
	 */
	async start(port: number, initialData?: Buffer): Promise<void> {
		if (!this.isPortInRange(port)) return
		if (this.activePipelines.has(port)) return

		const existing = this.pendingStarts.get(port)
		if (existing !== undefined) {
			await existing
			return
		}

		const promise = this.runStartForPort(port, initialData).finally(() => {
			this.pendingStarts.delete(port)
		})
		this.pendingStarts.set(port, promise)
		await promise
	}

	/**
	 * Single-run start logic for a port (credentials + spawn). Call only via start() so dedupe applies.
	 * If initialData is provided, it is written to stdin immediately after spawn so fdsrc has data when it first reads.
	 */
	private async runStartForPort(
		port: number,
		initialData?: Buffer,
	): Promise<void> {
		if (this.activePipelines.has(port)) return

		const streamName = this.streamNameForPort(port)
		const region = this.config.region

		// Resolve credentials before any GStreamer setup or spawn. kvssink (C++ SDK) does not use
		// the same credential chain as Node; we pass them via env so the child finds them.
		const credentialProvider = fromNodeProviderChain({
			timeout: 10_000,
			maxRetries: 5,
		})
		const maxResolutionAttempts = 3
		const resolutionDelayMs = 2000
		let credentials: Awaited<
			ReturnType<ReturnType<typeof fromNodeProviderChain>>
		>
		try {
			for (let attempt = 1; attempt <= maxResolutionAttempts; attempt++) {
				try {
					credentials = await credentialProvider()
					break
				} catch (e) {
					if (attempt === maxResolutionAttempts) throw e
					this.logger.warn('Credentials not yet available, retrying', {
						port,
						streamName,
						attempt,
						nextAttemptInMs: resolutionDelayMs,
					})
					await new Promise((r) => setTimeout(r, resolutionDelayMs))
				}
			}
		} catch (err) {
			this.logger.error(
				'Failed to resolve AWS credentials for kvssink',
				err instanceof Error ? err : new Error(String(err)),
				{ port, streamName },
			)
			return
		}

		// Build env with credentials and plugin paths; only then build pipeline and spawn.
		const env = { ...process.env }
		env.AWS_ACCESS_KEY_ID = credentials!.accessKeyId
		env.AWS_SECRET_ACCESS_KEY = credentials!.secretAccessKey
		if (
			credentials!.sessionToken !== undefined &&
			credentials!.sessionToken !== ''
		) {
			env.AWS_SESSION_TOKEN = credentials!.sessionToken
		}
		env.AWS_REGION = region
		if (env.KINESIS_GST_PLUGIN_PATH !== undefined) {
			env.GST_PLUGIN_PATH = env.KINESIS_GST_PLUGIN_PATH
		}
		if (env.KINESIS_LD_LIBRARY_PATH !== undefined) {
			env.LD_LIBRARY_PATH = env.KINESIS_LD_LIBRARY_PATH
		}

		const logConfigPath =
			process.env.KVS_LOG_CONFIG_PATH ??
			'/opt/video-streaming/kvs_log_configuration'

		// Use a FIFO so GStreamer reads via filesrc (real path). filesrc's open blocks until we open for write, so data is ready when it reads; avoids fdsrc "not-linked" / stream error with pipes.
		const fifoPath = path.join(
			os.tmpdir(),
			`kinesis-${port}-${process.pid}-${Date.now()}.fifo`,
		)
		const mkfifo = spawnSync('mkfifo', [fifoPath], {
			encoding: 'utf8',
			timeout: 5000,
		})
		const failed =
			mkfifo.error != null || (mkfifo.status != null && mkfifo.status !== 0)
		if (failed) {
			const stderrStr = mkfifo.stderr != null ? String(mkfifo.stderr) : ''
			const msg =
				mkfifo.error?.message ??
				(stderrStr.trim() || `mkfifo exit code ${mkfifo.status ?? 'unknown'}`)
			this.logger.error(
				'Failed to create FIFO for GStreamer',
				mkfifo.error ?? new Error(msg),
				{
					port,
					streamName,
					fifoPath,
					exitCode: mkfifo.status ?? undefined,
					stderr: stderrStr.slice(0, 500) || undefined,
				},
			)
			return
		}

		// tsdemux creates pads like video_0_0c00 (template video_%01x_%05x), not "video_0". Use "d." to link to any pad so delayed linking succeeds regardless of PID.
		const pipelineStr = `filesrc location="${fifoPath}" ! capsfilter caps="video/mpegts,systemstream=(boolean)true" ! queue ! tsparse set-timestamps=true ! tsdemux name=d d. ! queue ! h264parse ! capsfilter caps="video/x-h264,stream-format=avc,alignment=au" ! kvssink stream-name="${streamName}" aws-region="${region}" storage-size=128 log-config="${logConfigPath}"`
		const shellCmd = `gst-launch-1.0 ${pipelineStr}`
		this.logger.info('GStreamer command', {
			port,
			streamName,
			pipelineStr,
			shellCmd,
		})
		const gst = (this.config.spawn ?? spawn)('sh', ['-c', shellCmd], {
			stdio: ['ignore', 'pipe', 'pipe'],
			env,
		})

		// Every listener goes on now, before anything below waits - the rule
		// SrtpProducer's attachHelper states for its own child. They used to go on
		// after the input was open, which is precisely what a child that dies early
		// never lets happen: its exit was missed, its startup output went unread, and
		// its 'error' had no listener.
		const throttles = this.attachGst(port, streamName, gst)

		const watch = watchForExit(gst)
		let inputStream: Writable
		try {
			inputStream = await this.openInput(
				port,
				streamName,
				fifoPath,
				initialData,
				watch.gone,
			)
		} catch (err) {
			watch.cancel()
			// Whether it died or is merely stuck, it must not outlive this start: a
			// child that opened its input later would be a pipeline nobody owns.
			await endChildProcess(gst, {
				sigkillAfterMs: this.config.stopSigkillAfterMs ?? STOP_SIGKILL_AFTER_MS,
			})
			try {
				fs.unlinkSync(fifoPath)
			} catch {
				// Already gone.
			}
			throw err
		}
		watch.cancel()

		// A child that died just as its initial data was taken has already had its exit
		// handled, and handled as expected, because the port was not registered yet.
		// Registering it now would leave a dead pipeline counted as running that nothing
		// ever reports. Checked with nothing awaited between here and the registration,
		// so no exit can fall in between: from the registration on, the exit handler
		// sees the port and reports it.
		if (gst.exitCode !== null || gst.signalCode !== null) {
			inputStream.destroy()
			try {
				fs.unlinkSync(fifoPath)
			} catch {
				// Already gone.
			}
			throw new Error(
				`GStreamer exited while its input was being opened for port ${String(port)}`,
			)
		}

		const reorderBufferSize =
			this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		const reorder: ReorderState = {
			nextSeq: 0,
			nextToEmit: 1,
			buffer: new Map(),
		}
		this.activePipelines.set(port, {
			gst,
			inputStream,
			reorder,
			...throttles,
			fifoPath,
		})
		this.logger.info('Kinesis ingestion started', {
			port,
			streamName,
			reorderBufferSize,
		})
	}

	/**
	 * Opens the FIFO for writing and hands it the initial data, within one deadline.
	 *
	 * Nothing here waits on a threadpool thread. The FIFO is opened non-blocking, which
	 * fails at once until GStreamer's filesrc has the other end open, so it is retried
	 * until it succeeds, the child dies or the deadline passes; and it is written
	 * through a net.Socket, which waits for a full pipe on the event loop. The blocking
	 * alternative held one of libuv's four pool threads - shared with dns.lookup, which
	 * every AWS SDK call goes through - for as long as GStreamer did not read, and a
	 * write can wait on it for good: the initial data is megabytes, the pipe holds
	 * 64 KiB, and a GStreamer that opened its input and stalled reads nothing more.
	 *
	 * The deadline covers the write as well as the open, because a start that ends only
	 * once the open has succeeded is not bounded: it held the port in Starting, with
	 * its lock, for as long as the child held its input open without reading.
	 */
	private async openInput(
		port: number,
		streamName: string,
		fifoPath: string,
		initialData: Buffer | undefined,
		childGone: Promise<void>,
	): Promise<Writable> {
		const timeoutMs = this.config.inputTimeoutMs ?? INPUT_TIMEOUT_MS
		let timer: NodeJS.Timeout | undefined
		const expired = new Promise<'timed out'>((resolve) => {
			timer = setTimeout(() => resolve('timed out'), timeoutMs)
		})
		const exited = childGone.then(() => 'exited' as const)
		try {
			const fd = await this.openWhenRead(fifoPath, exited, expired)
			if (typeof fd !== 'number') {
				throw new Error(
					fd === 'exited'
						? `GStreamer exited before opening its input for port ${String(port)}`
						: `GStreamer did not open its input for port ${String(port)} within ${String(timeoutMs)}ms`,
				)
			}

			const input = new net.Socket({ fd, readable: false, writable: true })
			// On the stream before the first write, not after it: a write that fails - a
			// child that opened its input and died - otherwise emits 'error' with no
			// listener, and Node rethrows that, ending the process.
			input.on('error', (err: NodeJS.ErrnoException) => {
				if (err.code !== 'EPIPE') {
					this.logger.warn('GStreamer FIFO write error', {
						port,
						streamName,
						code: err.code,
						message: err.message,
					})
				}
			})
			const data = initialData ?? Buffer.alloc(0)
			if (data.length === 0) return input
			const written = await Promise.race([
				new Promise<'written' | Error>((resolve) => {
					input.write(data, (err) => resolve(err ?? 'written'))
				}),
				exited,
				expired,
			])
			if (written === 'written') return input
			input.destroy()
			if (written instanceof Error) throw written
			throw new Error(
				written === 'exited'
					? `GStreamer exited before taking its initial data for port ${String(port)}`
					: `GStreamer did not take its initial data for port ${String(port)} within ${String(timeoutMs)}ms`,
			)
		} finally {
			clearTimeout(timer)
		}
	}

	/** The FIFO's write end, once a reader has the other end open. */
	private async openWhenRead(
		fifoPath: string,
		exited: Promise<'exited'>,
		expired: Promise<'timed out'>,
	): Promise<number | 'exited' | 'timed out'> {
		for (;;) {
			try {
				return fs.openSync(
					fifoPath,
					fs.constants.O_WRONLY | fs.constants.O_NONBLOCK,
				)
			} catch (err) {
				// ENXIO is "no reader yet"; anything else will not change by waiting.
				if ((err as NodeJS.ErrnoException).code !== 'ENXIO') throw err
			}
			let poll: NodeJS.Timeout | undefined
			const outcome = await Promise.race([
				new Promise<'retry'>((resolve) => {
					poll = setTimeout(() => resolve('retry'), INPUT_OPEN_POLL_MS)
				}),
				exited,
				expired,
			])
			clearTimeout(poll)
			if (outcome !== 'retry') return outcome
		}
	}

	/** Every listener the child needs, attached before anything waits on it. */
	private attachGst(
		port: number,
		streamName: string,
		gst: ReturnType<typeof spawn>,
	): Pick<PortPipeline, 'gstStderrThrottle' | 'gstStdoutThrottle'> {
		const gstStderrThrottle: GstStderrThrottle = { lastLog: {} }
		const gstStdoutThrottle: GstStdoutThrottle = { lastLog: {} }
		// Reading a pipe is not the same as handling its errors, and an unhandled
		// 'error' on either would end the process rather than this one pipeline. The
		// child's own exit handling is what rebuilds it, so there is nothing to do here
		// beyond having a listener.
		for (const [stream, target] of Object.entries({
			stdout: gst.stdout,
			stderr: gst.stderr,
		})) {
			target?.on('error', (err: Error) => {
				this.logger.warn('GStreamer stream error', {
					port,
					streamName,
					stream,
					message: err.message,
				})
			})
		}
		gst.stdout?.on('data', (data: Buffer) => {
			const text = data.toString()
			if (text.trim().length > 0) {
				this.logGstStdout(port, streamName, text, gstStdoutThrottle)
			}
		})
		gst.stderr?.on('data', (data: Buffer) => {
			this.logGstStderr(port, streamName, data.toString(), gstStderrThrottle)
		})
		gst.on('error', (err) => {
			this.logger.error('GStreamer error', err, { port, streamName })
			this.activePipelines.delete(port)
		})
		gst.on('exit', (code, signal) => {
			// Still registered means nobody stopped it: stop() unregisters a pipeline
			// before ending it, and a start that fails never registers one.
			const pipeline = this.activePipelines.get(port)
			const wasUnexpected = pipeline?.gst === gst
			this.logger.info('GStreamer exited', {
				port,
				streamName,
				code: code ?? undefined,
				signal: signal ?? undefined,
				unexpected: wasUnexpected,
			})
			if (!wasUnexpected) return
			this.activePipelines.delete(port)
			// stop() never runs for this pipeline, since it is no longer registered, so
			// its input and FIFO are released here or not at all.
			pipeline.inputStream.destroy()
			if (pipeline.fifoPath !== undefined) {
				try {
					fs.unlinkSync(pipeline.fifoPath)
				} catch {
					// Already gone.
				}
			}
			this.emit('pipelineExited', { port, code, signal })
		})
		return { gstStderrThrottle, gstStdoutThrottle }
	}

	private drain(port: number, stdin: Writable): void {
		const pipeline = this.activePipelines.get(port)
		if (!pipeline) return
		const { reorder } = pipeline
		while (reorder.buffer.has(reorder.nextToEmit)) {
			const data = reorder.buffer.get(reorder.nextToEmit)
			reorder.buffer.delete(reorder.nextToEmit)
			reorder.nextToEmit += 1
			const ok = stdin.write(data)
			if (!ok) {
				stdin.once('drain', () => this.drain(port, stdin))
				return
			}
		}
		// If buffer overfull, skip missing packets so we don't stall on loss
		const maxBuf = this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		if (maxBuf > 0 && reorder.buffer.size >= maxBuf) {
			while (
				!reorder.buffer.has(reorder.nextToEmit) &&
				reorder.buffer.size > 0
			) {
				reorder.nextToEmit += 1
			}
			this.drain(port, stdin)
		}
	}

	/** True while a producer is running for this port. */
	isActive(port: number): boolean {
		return this.activePipelines.has(port)
	}

	/**
	 * Writes a UDP packet into the GStreamer stdin for that port.
	 * When reorderBufferSize > 0, packets are buffered and emitted in receive order.
	 */
	writePacket(port: number, data: Buffer): void {
		const pipeline = this.activePipelines.get(port)
		const inputStream = pipeline?.inputStream
		if (inputStream?.writable !== true) return

		const maxBuf = this.config.reorderBufferSize ?? DEFAULT_REORDER_BUFFER_SIZE
		if (maxBuf <= 0) {
			const ok = inputStream.write(data)
			if (!ok) inputStream.once('drain', () => {})
			return
		}

		if (!pipeline) return
		const { reorder } = pipeline
		reorder.nextSeq += 1
		reorder.buffer.set(reorder.nextSeq, data)
		this.drain(port, inputStream)
	}

	/**
	 * Stops the pipeline for a port and returns only once GStreamer is gone.
	 *
	 * Flushes the reorder buffer, closes the input so kvssink can upload what it holds,
	 * and then waits the child out - escalating to SIGTERM and SIGKILL if the flush
	 * does not end it. Returning earlier than that is not an option: PortIngestion
	 * releases the port's lock as soon as this resolves, so another instance may
	 * acquire the same Kinesis stream from that moment, and a signalled-but-living
	 * kvssink is still writing to it.
	 */
	async stop(port: number): Promise<void> {
		const pipeline = this.activePipelines.get(port)
		if (!pipeline) return

		this.activePipelines.delete(port)
		const { gst, inputStream, reorder, fifoPath } = pipeline

		if (inputStream.writable) {
			while (reorder.buffer.has(reorder.nextToEmit)) {
				const buf = reorder.buffer.get(reorder.nextToEmit)
				reorder.buffer.delete(reorder.nextToEmit)
				reorder.nextToEmit += 1
				if (buf !== undefined) inputStream.write(buf)
			}
			inputStream.end()
		}
		if (fifoPath !== undefined) {
			try {
				fs.unlinkSync(fifoPath)
			} catch (e) {
				// Gone already when this is a retry of a stop that failed.
				if ((e as NodeJS.ErrnoException).code !== 'ENOENT') {
					this.logger.warn('Failed to unlink FIFO', {
						port,
						fifoPath,
						message: e instanceof Error ? e.message : String(e),
					})
				}
			}
		}

		try {
			await endChildProcess(gst, {
				drainMs: this.config.stopDrainMs ?? STOP_DRAIN_MS,
				sigkillAfterMs: this.config.stopSigkillAfterMs ?? STOP_SIGKILL_AFTER_MS,
				onSignal: (signal) => {
					this.logger.warn('GStreamer did not stop on its own; signalling it', {
						port,
						signal,
					})
				},
			})
		} catch (err) {
			// Not known to be gone, so this must not return as if it were: the caller
			// releases the lock when it does. Registered again so that a retry reaches
			// the child, and so that its exit, whenever it comes, is still reported.
			this.logger.warn('Error waiting for pipeline stop', {
				port,
				error: err instanceof Error ? err.message : String(err),
			})
			if (!this.activePipelines.has(port))
				this.activePipelines.set(port, pipeline)
			throw err
		}
		this.logger.info('Kinesis ingestion stopped', { port })
	}

	/**
	 * Stops all active pipelines (e.g. on shutdown).
	 */
	async stopAll(): Promise<void> {
		const ports = Array.from(this.activePipelines.keys())
		await Promise.all(ports.map(async (port) => this.stop(port)))
	}
}
