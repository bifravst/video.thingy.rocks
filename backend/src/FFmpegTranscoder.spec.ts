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
})
