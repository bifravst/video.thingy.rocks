import assert from 'node:assert'
import { describe, it } from 'node:test'
import { FFmpegTranscoder, type TranscodingConfig } from './FFmpegTranscoder.ts'

void describe('FFmpegTranscoder', () => {
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
			}

			const transcoder = new FFmpegTranscoder(config)
			const command = transcoder.buildFFmpegCommand()

			// Verify input from stdin
			assert.strictEqual(command[0], '-i')
			assert.strictEqual(command[1], 'pipe:0')

			// Verify raw stream output
			assert.ok(command.includes('-c:v'))
			assert.ok(command.includes('copy'))
			assert.ok(command.includes('-f'))
			assert.ok(command.includes('segment'))

			// Verify filter complex for splitting
			assert.ok(command.includes('-filter_complex'))
			const filterIndex = command.indexOf('-filter_complex')
			const filterValue = command[filterIndex + 1]
			assert.ok(filterValue?.includes('split=4') ?? false)

			// Verify HLS outputs for each profile
			const commandStr = command.join(' ')
			assert.ok(commandStr.includes('1080p'))
			assert.ok(commandStr.includes('720p'))
			assert.ok(commandStr.includes('480p'))
			assert.ok(commandStr.includes('360p'))

			// Verify snapshot extraction
			assert.ok(command.includes('-vf'))
			assert.ok(command.includes("select='eq(pict_type,I)'"))
			assert.ok(command.includes('-frames:v'))
			assert.ok(command.includes('1'))
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
			}

			const transcoder = new FFmpegTranscoder(config)
			const command = transcoder.buildFFmpegCommand()

			const commandStr = command.join(' ')

			// Verify S3 bucket is used
			assert.ok(commandStr.includes('s3://my-bucket/raw/5001'))
			assert.ok(commandStr.includes('s3://my-bucket/hls/5001'))
			assert.ok(commandStr.includes('s3://my-bucket/snapshots/5001'))
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
			}

			const transcoder = new FFmpegTranscoder(config)
			const command = transcoder.buildFFmpegCommand()

			const segmentTimeIndex = command.indexOf('-segment_time')
			assert.ok(segmentTimeIndex >= 0)
			assert.strictEqual(command[segmentTimeIndex + 1], '10')

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
			}

			const transcoder = new FFmpegTranscoder(config)
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
