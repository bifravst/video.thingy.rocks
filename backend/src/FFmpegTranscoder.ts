import { spawn, type ChildProcess } from 'node:child_process'
import { EventEmitter } from 'node:events'
import { watch } from 'node:fs'
import { mkdir, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { MasterPlaylistGenerator } from './MasterPlaylistGenerator.ts'
import { S3UploadService } from './S3UploadService.ts'

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
	localOutputDir: string // Local directory for FFmpeg output
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
	private readonly s3UploadService: S3UploadService
	private readonly fileWatchers: Map<string, ReturnType<typeof watch>> =
		new Map()
	private readonly uploadedFiles: Set<string> = new Set()
	private readonly masterPlaylistGenerator: MasterPlaylistGenerator

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
		this.s3UploadService = new S3UploadService({
			bucket: config.s3Bucket,
		})
		this.masterPlaylistGenerator = new MasterPlaylistGenerator({
			port: config.port,
			profiles: config.hlsProfiles,
			localOutputDir: config.localOutputDir,
		})
	}

	async start(): Promise<void> {
		if (this.status.isRunning) {
			console.warn(
				`[FFmpegTranscoder] Transcoder for port ${this.config.port} is already running`,
			)
			return
		}

		try {
			// Create local output directories
			await this.createOutputDirectories()

			// Set up file watchers for automatic S3 upload
			await this.setupFileWatchers()

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

			// Generate and write master playlist after FFmpeg starts
			await this.masterPlaylistGenerator.writeMasterPlaylist()

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

		// Stop file watchers
		for (const watcher of this.fileWatchers.values()) {
			watcher.close()
		}
		this.fileWatchers.clear()

		// Stop S3 upload service
		await this.s3UploadService.stop()

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

			// Close stdin to signal end of input
			if (this.process.stdin && !this.process.stdin.destroyed) {
				this.process.stdin.end()
			}

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

		// Input from stdin (MPEG-TS format)
		args.push('-f', 'mpegts')
		args.push('-i', 'pipe:0')

		// Force overwrite without asking
		args.push('-y')

		// Output 1: HLS with multiple bitrate profiles
		for (const profile of this.config.hlsProfiles) {
			const profileDir = join(
				this.config.localOutputDir,
				'hls',
				this.config.port.toString(),
				profile.name,
			)

			args.push('-map', '0:v')
			args.push('-map', '0:a?') // Optional audio
			args.push('-c:v', 'libx264')
			args.push('-preset', 'veryfast') // Fast encoding for low latency
			args.push('-tune', 'zerolatency')
			args.push('-s', profile.resolution)
			args.push('-b:v', profile.videoBitrate)
			args.push('-maxrate', profile.videoBitrate)
			args.push('-bufsize', `${parseInt(profile.videoBitrate) * 2}k`)
			args.push('-c:a', 'aac')
			args.push('-b:a', profile.audioBitrate)
			args.push('-f', 'hls')
			args.push('-hls_time', this.config.segmentDuration.toString())
			args.push('-hls_list_size', '10')
			// Live streaming flags: mark as event stream, append to playlist, omit endlist tag
			args.push('-hls_playlist_type', 'event')
			args.push('-hls_flags', 'delete_segments+append_list+omit_endlist')
			args.push('-hls_segment_filename', `${profileDir}/segment_%05d.ts`)
			args.push(`${profileDir}/playlist.m3u8`)
		}

		console.log(
			`[FFmpegTranscoder] Complete FFmpeg command: ffmpeg ${args.join(' ')}`,
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

	private async createOutputDirectories(): Promise<void> {
		const dirs = [
			join(this.config.localOutputDir, 'hls', this.config.port.toString()),
			join(
				this.config.localOutputDir,
				'snapshots',
				this.config.port.toString(),
			),
		]

		// Create profile directories
		for (const profile of this.config.hlsProfiles) {
			dirs.push(
				join(
					this.config.localOutputDir,
					'hls',
					this.config.port.toString(),
					profile.name,
				),
			)
		}

		for (const dir of dirs) {
			await mkdir(dir, { recursive: true })
		}

		console.log(
			`[FFmpegTranscoder] Created output directories for port ${this.config.port}`,
		)
	}

	private async setupFileWatchers(): Promise<void> {
		// Watch master playlist directory
		const masterPlaylistDir = join(
			this.config.localOutputDir,
			'hls',
			this.config.port.toString(),
		)

		const masterWatcher = watch(masterPlaylistDir, (eventType, filename) => {
			if (filename === 'master.m3u8') {
				void this.uploadMasterPlaylist()
			}
		})

		this.fileWatchers.set('master', masterWatcher)

		// Watch HLS directories for new segments and playlists
		for (const profile of this.config.hlsProfiles) {
			const profileDir = join(
				this.config.localOutputDir,
				'hls',
				this.config.port.toString(),
				profile.name,
			)

			const watcher = watch(profileDir, (eventType, filename) => {
				if (
					filename === null ||
					filename === undefined ||
					filename === '' ||
					filename.endsWith('.tmp')
				)
					return

				const filePath = join(profileDir, filename)

				// Avoid uploading the same file multiple times
				if (this.uploadedFiles.has(filePath)) return

				void (async () => {
					try {
						// Wait a bit for file to be fully written
						await new Promise((resolve) => setTimeout(resolve, 1000))

						const fileData = await readFile(filePath)
						const s3Key = `${this.config.outputPaths.hls}/${profile.name}/${filename}`

						await this.s3UploadService.uploadData(fileData, s3Key, {
							contentType: filename.endsWith('.m3u8')
								? 'application/vnd.apple.mpegurl'
								: 'video/mp2t',
							receptionTimestamp: new Date(),
						})

						this.uploadedFiles.add(filePath)

						console.log(
							`[FFmpegTranscoder] Uploaded ${filename} for port ${this.config.port} profile ${profile.name}`,
						)

						// Clean up old uploaded files from tracking set
						if (this.uploadedFiles.size > 1000) {
							const toDelete = Array.from(this.uploadedFiles).slice(0, 500)
							for (const path of toDelete) {
								this.uploadedFiles.delete(path)
							}
						}
					} catch (error) {
						console.error(
							`[FFmpegTranscoder] Error uploading ${filename}:`,
							error,
						)
					}
				})()
			})

			this.fileWatchers.set(profileDir, watcher)
		}

		// Watch snapshot directory
		const snapshotDir = join(
			this.config.localOutputDir,
			'snapshots',
			this.config.port.toString(),
		)

		const snapshotWatcher = watch(snapshotDir, (eventType, filename) => {
			if (
				filename === null ||
				filename === undefined ||
				filename === '' ||
				!filename.endsWith('.jpg')
			)
				return

			const filePath = join(snapshotDir, filename)

			void (async () => {
				try {
					await new Promise((resolve) => setTimeout(resolve, 100))

					const fileData = await readFile(filePath)
					const s3Key = `${this.config.outputPaths.snapshot}/${filename}`

					await this.s3UploadService.uploadData(fileData, s3Key, {
						contentType: 'image/jpeg',
						receptionTimestamp: new Date(),
					})

					console.log(
						`[FFmpegTranscoder] Uploaded snapshot for port ${this.config.port}`,
					)
				} catch (error) {
					console.error(`[FFmpegTranscoder] Error uploading snapshot:`, error)
				}
			})()
		})

		this.fileWatchers.set(snapshotDir, snapshotWatcher)

		console.log(
			`[FFmpegTranscoder] Set up file watchers for port ${this.config.port}`,
		)
	}

	private async uploadMasterPlaylist(): Promise<void> {
		try {
			// Wait a bit for file to be fully written
			await new Promise((resolve) => setTimeout(resolve, 500))

			const masterPlaylistPath =
				this.masterPlaylistGenerator.getMasterPlaylistPath()
			const fileData = await readFile(masterPlaylistPath)

			await this.s3UploadService.uploadMasterPlaylist(
				this.config.port,
				fileData.toString('utf-8'),
			)

			console.log(
				`[FFmpegTranscoder] Uploaded master playlist for port ${this.config.port}`,
			)
		} catch (error) {
			console.error(
				`[FFmpegTranscoder] Error uploading master playlist for port ${this.config.port}:`,
				error,
			)
		}
	}
}
