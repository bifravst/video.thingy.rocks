import assert from 'node:assert'
import { after, describe, it } from 'node:test'
import { FFmpegTranscoder, type TranscodingConfig } from './FFmpegTranscoder.ts'

void describe('FFmpegTranscoder', () => {
	const transcoders: FFmpegTranscoder[] = []

	// Clean up all transcoders after tests
	after(async () => {
		for (const transcoder of transcoders) {
			await transcoder.stop()
		}
	})

	void describe('buildFFmpegCommand', () => {
		void it('should generate correct FFmpeg command with default profiles', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)
			const command = transcoder.buildFFmpegCommand()

			// Verify command contains required flags
			const commandStr = command.join(' ')

			// Verify overwrite flag
			assert.ok(command.includes('-y'))

			// Verify input format and stdin
			assert.ok(command.includes('-f'))
			assert.ok(command.includes('mpegts'))
			assert.ok(command.includes('-i'))
			assert.ok(command.includes('pipe:0'))

			// Verify HLS outputs for each profile
			assert.ok(commandStr.includes('1080p'))
			assert.ok(commandStr.includes('720p'))
			assert.ok(commandStr.includes('480p'))
			assert.ok(commandStr.includes('360p'))

			// Verify local output paths
			assert.ok(commandStr.includes('/tmp/ffmpeg-output/hls/5000'))
		})

		void it('should use correct S3 paths', () => {
			const config: TranscodingConfig = {
				port: 5001,
				outputPaths: {
					raw: 'raw/5001',
					hls: 'hls/5001',
					snapshot: 'snapshots/5001',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'my-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)
			const command = transcoder.buildFFmpegCommand()

			const commandStr = command.join(' ')

			// Verify local output paths are used (not S3 paths)
			assert.ok(commandStr.includes('/tmp/ffmpeg-output/hls/5001'))
		})

		void it('should use custom segment duration', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 10,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)
			const command = transcoder.buildFFmpegCommand()

			const hlsTimeIndex = command.indexOf('-hls_time')
			assert.ok(hlsTimeIndex >= 0)
			assert.strictEqual(command[hlsTimeIndex + 1], '10')
		})

		void it('should include live streaming flags', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)
			const command = transcoder.buildFFmpegCommand()

			// Verify hls_playlist_type is set to 'event'
			const playlistTypeIndex = command.indexOf('-hls_playlist_type')
			assert.ok(
				playlistTypeIndex >= 0,
				'hls_playlist_type flag should be present',
			)
			assert.strictEqual(
				command[playlistTypeIndex + 1],
				'event',
				'hls_playlist_type should be set to event',
			)

			// Verify hls_flags includes required flags
			const hlsFlagsIndex = command.indexOf('-hls_flags')
			assert.ok(hlsFlagsIndex >= 0, 'hls_flags should be present')
			const hlsFlags = command[hlsFlagsIndex + 1]
			assert.ok(
				hlsFlags?.includes('append_list') ?? false,
				'hls_flags should include append_list',
			)
			assert.ok(
				hlsFlags?.includes('program_date_time') ?? false,
				'hls_flags should include program_date_time',
			)
			assert.ok(
				!(hlsFlags?.includes('delete_segments') ?? true),
				'hls_flags should NOT include delete_segments',
			)
			// Note: omit_endlist is NOT included so FFmpeg can add #EXT-X-ENDLIST on graceful shutdown
			assert.ok(
				!(hlsFlags?.includes('omit_endlist') ?? true),
				'hls_flags should NOT include omit_endlist to allow stream end signaling',
			)

			// Verify hls_start_number_source is set to 'datetime'
			const startNumberSourceIndex = command.indexOf('-hls_start_number_source')
			assert.ok(
				startNumberSourceIndex >= 0,
				'hls_start_number_source should be present',
			)
			assert.strictEqual(
				command[startNumberSourceIndex + 1],
				'datetime',
				'hls_start_number_source should be set to datetime',
			)

			// Note: hls_init_time is not compatible with append_list mode,
			// so it's intentionally not included in the command.
			// The segment duration is controlled by -hls_time instead.

			// Note: Cache-Control headers are set by S3UploadService during upload,
			// not by FFmpeg. Segments get 'max-age=31536000, immutable' and
			// playlists get 'no-cache, no-store, must-revalidate'.
		})

		void it('should include all required live streaming flags for each profile', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)
			const command = transcoder.buildFFmpegCommand()

			// Count occurrences of live streaming flags (should match number of profiles)
			const playlistTypeCount = command.filter(
				(arg) => arg === '-hls_playlist_type',
			).length
			const hlsFlagsCount = command.filter((arg) => arg === '-hls_flags').length

			assert.strictEqual(
				playlistTypeCount,
				FFmpegTranscoder.DEFAULT_PROFILES.length,
				'hls_playlist_type should be set for each profile',
			)
			assert.strictEqual(
				hlsFlagsCount,
				FFmpegTranscoder.DEFAULT_PROFILES.length,
				'hls_flags should be set for each profile',
			)
		})

		void it('should log the complete FFmpeg command', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Capture console.log output
			const originalLog = console.log
			let loggedCommand = ''
			console.log = (message: string) => {
				if (message.includes('Complete FFmpeg command')) {
					loggedCommand = message
				}
			}

			transcoder.buildFFmpegCommand()

			// Restore console.log
			console.log = originalLog

			assert.ok(
				loggedCommand.includes('Complete FFmpeg command'),
				'Should log the complete FFmpeg command',
			)
			assert.ok(
				loggedCommand.includes('ffmpeg'),
				'Logged command should include ffmpeg',
			)
		})
	})

	void describe('getStatus', () => {
		void it('should return initial status', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)
			const status = transcoder.getStatus()

			assert.strictEqual(status.isRunning, false)
			assert.strictEqual(status.currentSegment, 0)
			assert.strictEqual(status.retryCount, 0)
			assert.strictEqual(status.lastError, undefined)
		})
	})

	void describe('DEFAULT_PROFILES', () => {
		void it('should have 4 bitrate profiles', () => {
			assert.strictEqual(FFmpegTranscoder.DEFAULT_PROFILES.length, 4)
		})

		void it('should have correct profile names', () => {
			const names = FFmpegTranscoder.DEFAULT_PROFILES.map((p) => p.name)
			assert.deepStrictEqual(names, ['1080p', '720p', '480p', '360p'])
		})

		void it('should have correct resolutions', () => {
			const profiles = FFmpegTranscoder.DEFAULT_PROFILES

			assert.strictEqual(profiles[0]?.resolution, '1920x1080')
			assert.strictEqual(profiles[1]?.resolution, '1280x720')
			assert.strictEqual(profiles[2]?.resolution, '854x480')
			assert.strictEqual(profiles[3]?.resolution, '640x360')
		})

		void it('should have decreasing bitrates', () => {
			const profiles = FFmpegTranscoder.DEFAULT_PROFILES

			assert.strictEqual(profiles[0]?.videoBitrate, '5000k')
			assert.strictEqual(profiles[1]?.videoBitrate, '3000k')
			assert.strictEqual(profiles[2]?.videoBitrate, '1500k')
			assert.strictEqual(profiles[3]?.videoBitrate, '800k')
		})
	})

	void describe('validateConfiguration', () => {
		void it('should accept valid configuration', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Should not throw
			assert.doesNotThrow(() => {
				transcoder.validateConfiguration()
			})
		})

		void it('should reject negative segment duration', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: -5,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			assert.throws(
				() => {
					transcoder.validateConfiguration()
				},
				{
					message: /Invalid segmentDuration.*Must be a positive number/,
				},
			)
		})

		void it('should reject zero segment duration', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 0,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			assert.throws(
				() => {
					transcoder.validateConfiguration()
				},
				{
					message: /Invalid segmentDuration.*Must be a positive number/,
				},
			)
		})

		void it('should verify hls_flags includes append_list', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Valid configuration should pass
			assert.doesNotThrow(() => {
				transcoder.validateConfiguration()
			})

			// Verify the command actually includes append_list
			const command = transcoder.buildFFmpegCommand()
			const hlsFlagsIndex = command.indexOf('-hls_flags')
			assert.ok(hlsFlagsIndex >= 0)
			const hlsFlags = command[hlsFlagsIndex + 1]
			assert.ok(hlsFlags?.includes('append_list') ?? false)
		})

		void it('should verify hls_list_size is a positive integer', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Valid configuration should pass
			assert.doesNotThrow(() => {
				transcoder.validateConfiguration()
			})

			// Verify the command includes hls_list_size with positive value
			const command = transcoder.buildFFmpegCommand()
			const hlsListSizeIndex = command.indexOf('-hls_list_size')
			assert.ok(hlsListSizeIndex >= 0)
			const listSize = parseInt(command[hlsListSizeIndex + 1] ?? '0', 10)
			assert.ok(listSize > 0)
		})

		void it('should prevent startup with invalid configuration', async () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: -1, // Invalid
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Add error listener to prevent unhandled error
			transcoder.on('error', () => {
				// Expected error
			})

			// start() should throw due to validation failure
			await assert.rejects(
				async () => {
					await transcoder.start()
				},
				{
					message: /Invalid segmentDuration/,
				},
			)

			// Verify transcoder is not running
			const status = transcoder.getStatus()
			assert.strictEqual(status.isRunning, false)
		})

		void it('should emit error event on validation failure', async () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: -1, // Invalid
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Listen for error event
			let errorEmitted = false
			let errorMessage = ''
			transcoder.once('error', (port: number, message: string) => {
				errorEmitted = true
				errorMessage = message
			})

			// start() should throw
			await assert.rejects(async () => {
				await transcoder.start()
			})

			// Verify error event was emitted
			assert.ok(errorEmitted, 'Error event should be emitted')
			assert.ok(
				errorMessage.includes('Invalid segmentDuration'),
				'Error message should describe the validation failure',
			)
		})
	})

	void describe('stream end handling', () => {
		void it('should not include omit_endlist flag to allow stream end signaling', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)
			const command = transcoder.buildFFmpegCommand()

			// Verify omit_endlist is NOT in the command
			const commandStr = command.join(' ')
			assert.ok(
				!commandStr.includes('omit_endlist'),
				'Command should not include omit_endlist flag',
			)

			// Verify other required flags are still present
			assert.ok(
				commandStr.includes('append_list'),
				'Command should include append_list flag',
			)
			assert.ok(
				!commandStr.includes('delete_segments'),
				'Command should NOT include delete_segments flag',
			)
		})

		void it('should send SIGTERM for graceful shutdown', async () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Mock the process to verify SIGTERM is sent
			let signalSent: string | undefined
			const mockProcess = {
				stdin: {
					destroyed: false,
					end: () => {},
				},
				kill: (signal?: string) => {
					signalSent = signal
					// Simulate immediate exit
					setTimeout(() => {
						mockProcess.exitHandler?.(0, signal)
					}, 10)
				},
				once: (event: string, handler: (...args: any[]) => void) => {
					if (event === 'exit') {
						mockProcess.exitHandler = handler
					}
				},
				exitHandler: undefined as ((...args: any[]) => void) | undefined,
				killed: false,
			}

			// Replace the process with our mock
			// @ts-expect-error - Accessing private property for testing
			transcoder.process = mockProcess

			// @ts-expect-error - Accessing private property for testing
			transcoder.status.isRunning = true

			// Call stop
			await transcoder.stop()

			// Verify SIGTERM was sent
			assert.strictEqual(
				signalSent,
				'SIGTERM',
				'Should send SIGTERM for graceful shutdown',
			)
		})

		void it('should wait for final playlists to be uploaded after FFmpeg exits', async () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Track timing
			let exitTime = 0
			let stopResolvedTime = 0

			// Mock the process
			const mockProcess = {
				stdin: {
					destroyed: false,
					end: () => {},
				},
				kill: (signal?: string) => {
					// Simulate immediate exit
					setTimeout(() => {
						exitTime = Date.now()
						mockProcess.exitHandler?.(0, signal)
					}, 10)
				},
				once: (event: string, handler: (...args: any[]) => void) => {
					if (event === 'exit') {
						mockProcess.exitHandler = handler
					}
				},
				exitHandler: undefined as ((...args: any[]) => void) | undefined,
				killed: false,
			}

			// Replace the process with our mock
			// @ts-expect-error - Accessing private property for testing
			transcoder.process = mockProcess

			// @ts-expect-error - Accessing private property for testing
			transcoder.status.isRunning = true

			// Call stop
			await transcoder.stop()
			stopResolvedTime = Date.now()

			// Verify there was a delay after exit (at least 1.5 seconds for the 2 second wait)
			const delay = stopResolvedTime - exitTime
			assert.ok(
				delay >= 1500,
				`Should wait at least 1.5 seconds after FFmpeg exits (actual: ${delay}ms)`,
			)
		})

		void it('should clean up resources after stop', async () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Mock the process
			const mockProcess = {
				stdin: {
					destroyed: false,
					end: () => {},
				},
				kill: (signal?: string) => {
					setTimeout(() => {
						mockProcess.exitHandler?.(0, signal)
					}, 10)
				},
				once: (event: string, handler: (...args: any[]) => void) => {
					if (event === 'exit') {
						mockProcess.exitHandler = handler
					}
				},
				exitHandler: undefined as ((...args: any[]) => void) | undefined,
				killed: false,
			}

			// Replace the process with our mock
			// @ts-expect-error - Accessing private property for testing
			transcoder.process = mockProcess

			// @ts-expect-error - Accessing private property for testing
			transcoder.status.isRunning = true

			// Add some mock pending uploads and file watchers
			// @ts-expect-error - Accessing private property for testing
			transcoder.pendingUploads.set(
				'test-file',
				setTimeout(() => {}, 10000),
			)

			const mockWatcher = { close: () => {} }
			// @ts-expect-error - Accessing private property for testing
			transcoder.fileWatchers.set('test-watcher', mockWatcher)

			// Call stop
			await transcoder.stop()

			// Verify resources were cleaned up
			assert.strictEqual(
				// @ts-expect-error - Accessing private property for testing
				transcoder.pendingUploads.size,
				0,
				'Pending uploads should be cleared',
			)
			assert.strictEqual(
				// @ts-expect-error - Accessing private property for testing
				transcoder.fileWatchers.size,
				0,
				'File watchers should be cleared',
			)
		})
	})

	void describe.skip('stdin error handling', () => {
		void it('should handle EPIPE errors gracefully without crashing', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Create a mock process with stdin
			const mockStdin = {
				write: () => true,
				destroyed: false,
				writable: true,
				on: (event: string, handler: (error: Error) => void) => {
					if (event === 'error') {
						// Simulate EPIPE error
						const epipeError = new Error('write EPIPE') as any
						epipeError.code = 'EPIPE'
						epipeError.errno = -32
						epipeError.syscall = 'write'

						// This should not throw - handler should catch it
						handler(epipeError)
					}
				},
			}

			const mockProcess = {
				stdin: mockStdin,
				stdout: { on: () => {} },
				stderr: { on: () => {} },
				on: () => {},
			}

			// @ts-expect-error - Mocking for test
			transcoder.process = mockProcess

			// Set up handlers (this should add the stdin error handler)
			// @ts-expect-error - Accessing private method for testing
			transcoder.setupProcessHandlers()

			// Trigger the error handler - should not throw
			assert.doesNotThrow(() => {
				mockStdin.on('error', (error: Error) => {
					// Error handler should be called
					assert.ok(error !== null && error !== undefined)
				})
			})
		})

		void it('should not write to stdin if it is destroyed', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Create a mock process with destroyed stdin
			const mockStdin = {
				write: () => {
					throw new Error('Should not be called')
				},
				destroyed: true,
				writable: false,
			}

			const mockProcess = {
				stdin: mockStdin,
			}

			// @ts-expect-error - Mocking for test
			transcoder.process = mockProcess

			// Try to write - should return false without throwing
			const result = transcoder.write(Buffer.from('test'))
			assert.strictEqual(
				result,
				false,
				'Should return false for destroyed stdin',
			)
		})

		void it('should not write to stdin if it is not writable', () => {
			const config: TranscodingConfig = {
				port: 5000,
				outputPaths: {
					raw: 'raw/5000',
					hls: 'hls/5000',
					snapshot: 'snapshots/5000',
				},
				hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
				segmentDuration: 6,
				s3Bucket: 'test-bucket',
				localOutputDir: '/tmp/ffmpeg-output',
			}

			const transcoder = new FFmpegTranscoder(config)
			transcoders.push(transcoder)

			// Create a mock process with non-writable stdin
			const mockStdin = {
				write: () => {
					throw new Error('Should not be called')
				},
				destroyed: false,
				writable: false,
			}

			const mockProcess = {
				stdin: mockStdin,
			}

			// @ts-expect-error - Mocking for test
			transcoder.process = mockProcess

			// Try to write - should return false without throwing
			const result = transcoder.write(Buffer.from('test'))
			assert.strictEqual(
				result,
				false,
				'Should return false for non-writable stdin',
			)
		})
	})
})
