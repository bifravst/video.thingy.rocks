import { spawn, type ChildProcess } from 'node:child_process'
import { EventEmitter } from 'node:events'

export type BitrateProfile = {
	name: string // "1080p", "720p", "480p", "360p"
	resolution: string // "1920x1080"
	videoBitrate: string // "5000k"
	audioBitrate: string // "128k"
}

export type TranscodingConfig = {
	port: number
	outputPaths: {
		raw: string // S3 path prefix for raw segments
		hls: string // S3 path prefix for HLS segments
		snapshot: string // S3 path for snapshot
	}
	hlsProfiles: BitrateProfile[]
	segmentDuration: number // seconds
	s3Bucket: string
}

export type TranscodingStatus = {
	isRunning: boolean
	currentSegment: number
	lastError?: string
	retryCount: number
}

export class FFmpegTranscoder extends EventEmitter {
	private readonly config: TranscodingConfig
	private process?: ChildProcess
	private readonly status: TranscodingStatus
	private readonly maxRetries = 3
	private retryTimeout?: NodeJS.Timeout

	// Default bitrate profiles
	static readonly DEFAULT_PROFILES: BitrateProfile[] = [
		{
			name: '1080p',
			resolution: '1920x1080',
			videoBitrate: '5000k',
			audioBitrate: '128k',
		},
		{
			name: '720p',
			resolution: '1280x720',
			videoBitrate: '3000k',
			audioBitrate: '128k',
		},
		{
			name: '480p',
			resolution: '854x480',
			videoBitrate: '1500k',
			audioBitrate: '128k',
		},
		{
			name: '360p',
			resolution: '640x360',
			videoBitrate: '800k',
			audioBitrate: '128k',
		},
	]

	constructor(config: TranscodingConfig) {
		super()
		this.config = config
		this.status = {
			isRunning: false,
			currentSegment: 0,
			retryCount: 0,
		}
	}

	async start(): Promise<void> {
		if (this.status.isRunning) {
			console.warn(
				`[FFmpegTranscoder] Transcoder for port ${this.config.port} is already running`,
			)
			return
		}

		try {
			const command = this.buildFFmpegCommand()
			console.log(
				`[FFmpegTranscoder] Starting FFmpeg for port ${this.config.port}`,
			)
			console.log(`[FFmpegTranscoder] Command: ffmpeg ${command.join(' ')}`)

			this.process = spawn('ffmpeg', command, {
				stdio: ['pipe', 'pipe', 'pipe'],
			})

			this.setupProcessHandlers()
			this.status.isRunning = true
			this.status.retryCount = 0
			this.emit('started', this.config.port)

			console.log(
				`[FFmpegTranscoder] FFmpeg process started for port ${this.config.port}`,
			)
		} catch (error) {
			this.status.lastError =
				error instanceof Error ? error.message : String(error)
			console.error(
				`[FFmpegTranscoder] Failed to start FFmpeg for port ${this.config.port}:`,
				error,
			)
			throw error
		}
	}

	async stop(): Promise<void> {
		if (this.retryTimeout) {
			clearTimeout(this.retryTimeout)
			this.retryTimeout = undefined
		}

		if (!this.process) {
			return
		}

		return new Promise<void>((resolve) => {
			if (!this.process) {
				resolve()
				return
			}

			this.process.once('exit', () => {
				console.log(
					`[FFmpegTranscoder] FFmpeg process stopped for port ${this.config.port}`,
				)
				this.status.isRunning = false
				this.emit('stopped', this.config.port)
				resolve()
			})

			// Send SIGTERM for graceful shutdown
			this.process.kill('SIGTERM')

			// Force kill after 5 seconds if not stopped
			setTimeout(() => {
				if (this.process && !this.process.killed) {
					console.warn(
						`[FFmpegTranscoder] Force killing FFmpeg process for port ${this.config.port}`,
					)
					this.process.kill('SIGKILL')
				}
			}, 5000)
		})
	}

	async restart(): Promise<void> {
		console.log(
			`[FFmpegTranscoder] Restarting FFmpeg for port ${this.config.port}`,
		)
		await this.stop()
		await this.start()
	}

	write(data: Buffer): boolean {
		if (!this.process?.stdin) {
			console.warn(
				`[FFmpegTranscoder] Cannot write to FFmpeg stdin for port ${this.config.port}: process not running`,
			)
			return false
		}

		try {
			return this.process.stdin.write(data)
		} catch (error) {
			console.error(
				`[FFmpegTranscoder] Error writing to FFmpeg stdin for port ${this.config.port}:`,
				error,
			)
			return false
		}
	}

	getStatus(): TranscodingStatus {
		return { ...this.status }
	}

	buildFFmpegCommand(): string[] {
		const args: string[] = []

		// Input from stdin
		args.push('-i', 'pipe:0')

		// Raw stream output (copy codec, no transcoding)
		args.push('-c:v', 'copy')
		args.push('-f', 'segment')
		args.push('-segment_time', this.config.segmentDuration.toString())
		args.push('-segment_format', 'mpegts')
		args.push(
			'-segment_list',
			`s3://${this.config.s3Bucket}/${this.config.outputPaths.raw}/playlist.m3u8`,
		)
		args.push(
			`s3://${this.config.s3Bucket}/${this.config.outputPaths.raw}/segment_%05d.ts`,
		)

		// Create filter complex for splitting video into multiple outputs
		const splitOutputs = this.config.hlsProfiles
			.map((_, i) => `[v${i}]`)
			.join('')
		args.push(
			'-filter_complex',
			`[0:v]split=${this.config.hlsProfiles.length}${splitOutputs}`,
		)

		// HLS outputs for each profile
		for (let i = 0; i < this.config.hlsProfiles.length; i++) {
			const profile = this.config.hlsProfiles[i]
			if (!profile) continue

			args.push('-map', `[v${i}]`)
			args.push('-s', profile.resolution)
			args.push('-b:v', profile.videoBitrate)
			args.push('-c:v', 'libx264')
			args.push('-preset', 'fast')
			args.push('-f', 'hls')
			args.push('-hls_time', this.config.segmentDuration.toString())
			args.push('-hls_list_size', '10')
			args.push('-hls_flags', 'delete_segments')
			args.push(
				`s3://${this.config.s3Bucket}/${this.config.outputPaths.hls}/${profile.name}/playlist.m3u8`,
			)
		}

		// Snapshot extraction (I-frames only)
		args.push('-vf', "select='eq(pict_type,I)'")
		args.push('-vsync', 'vfr')
		args.push('-frames:v', '1')
		args.push('-update', '1')
		args.push(
			`s3://${this.config.s3Bucket}/${this.config.outputPaths.snapshot}/last_frame.jpg`,
		)

		return args
	}

	private setupProcessHandlers(): void {
		if (!this.process) {
			return
		}

		// Handle stdout
		this.process.stdout?.on('data', (data: Buffer) => {
			const output = data.toString()
			console.log(`[FFmpegTranscoder:${this.config.port}] ${output}`)
		})

		// Handle stderr (FFmpeg outputs progress info to stderr)
		this.process.stderr?.on('data', (data: Buffer) => {
			const output = data.toString()
			// Parse segment information if available
			if (output.includes('segment:')) {
				this.status.currentSegment++
			}
			console.log(`[FFmpegTranscoder:${this.config.port}] ${output}`)
		})

		// Handle process exit
		this.process.on('exit', (code, signal) => {
			console.log(
				`[FFmpegTranscoder] FFmpeg process exited for port ${this.config.port} with code ${code} and signal ${signal}`,
			)
			this.status.isRunning = false

			if (code !== 0 && code !== null) {
				this.status.lastError = `Process exited with code ${code}`
				this.emit('error', this.config.port, this.status.lastError)
				this.handleProcessCrash()
			} else {
				this.emit('stopped', this.config.port)
			}
		})

		// Handle process errors
		this.process.on('error', (error) => {
			console.error(
				`[FFmpegTranscoder] Process error for port ${this.config.port}:`,
				error,
			)
			this.status.lastError = error.message
			this.status.isRunning = false
			this.emit('error', this.config.port, error.message)
			this.handleProcessCrash()
		})
	}

	private handleProcessCrash(): void {
		if (this.status.retryCount >= this.maxRetries) {
			console.error(
				`[FFmpegTranscoder] Max retries (${this.maxRetries}) reached for port ${this.config.port}, giving up`,
			)
			this.emit('failed', this.config.port, this.status.lastError)
			return
		}

		this.status.retryCount++
		const backoffMs = Math.pow(2, this.status.retryCount - 1) * 1000

		console.log(
			`[FFmpegTranscoder] Scheduling restart for port ${this.config.port} in ${backoffMs}ms (attempt ${this.status.retryCount}/${this.maxRetries})`,
		)

		this.retryTimeout = setTimeout(() => {
			void this.restart().catch((error) => {
				console.error(
					`[FFmpegTranscoder] Failed to restart FFmpeg for port ${this.config.port}:`,
					error,
				)
			})
		}, backoffMs)
	}
}
