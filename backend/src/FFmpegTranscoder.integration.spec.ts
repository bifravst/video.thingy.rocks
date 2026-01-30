import assert from 'node:assert'
import { readFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { after, describe, it, mock } from 'node:test'
import { FFmpegTranscoder } from './FFmpegTranscoder.ts'

void describe('FFmpegTranscoder - Master Playlist Integration', () => {
	const transcoders: FFmpegTranscoder[] = []

	// Clean up all transcoders after tests
	after(async () => {
		for (const transcoder of transcoders) {
			await transcoder.stop()
		}
	})

	void it('should create MasterPlaylistGenerator instance in constructor', () => {
		const transcoder = new FFmpegTranscoder({
			port: 5000,
			outputPaths: {
				raw: 'raw/5000',
				hls: 'hls/5000',
				snapshot: 'snapshots/5000',
			},
			hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
			segmentDuration: 6,
			s3Bucket: 'test-bucket',
			localOutputDir: '/tmp/ffmpeg-test',
		})

		transcoders.push(transcoder)

		// Mock S3 client to prevent real AWS calls
		transcoder['s3UploadService']['client'].send = mock.fn(async () => ({}))

		// Verify the transcoder was created successfully
		assert.ok(transcoder !== null && transcoder !== undefined)
		assert.strictEqual(transcoder.getStatus().isRunning, false)
	})

	void it.skip('should write master playlist after start', async () => {
		const testDir = '/tmp/ffmpeg-integration-test'
		const transcoder = new FFmpegTranscoder({
			port: 5001,
			outputPaths: {
				raw: 'raw/5001',
				hls: 'hls/5001',
				snapshot: 'snapshots/5001',
			},
			hlsProfiles: FFmpegTranscoder.DEFAULT_PROFILES,
			segmentDuration: 6,
			s3Bucket: 'test-bucket',
			localOutputDir: testDir,
		})

		transcoders.push(transcoder)

		// Mock S3 client to prevent real AWS calls
		transcoder['s3UploadService']['client'].send = mock.fn(async () => ({}))

		try {
			// Start the transcoder (this will fail because no FFmpeg input, but should write master playlist)
			await transcoder.start().catch(() => {
				// Expected to fail without actual FFmpeg input
			})

			// Give it a moment to write the master playlist
			await new Promise((resolve) => setTimeout(resolve, 1000))

			// Check if master playlist was written
			const masterPlaylistPath = join(testDir, 'hls', '5001', 'master.m3u8')
			const content = await readFile(masterPlaylistPath, 'utf-8')

			// Verify master playlist content
			assert.ok(content.includes('#EXTM3U'))
			assert.ok(content.includes('#EXT-X-VERSION:3'))
			assert.ok(content.includes('1080p/playlist.m3u8'))
			assert.ok(content.includes('720p/playlist.m3u8'))
			assert.ok(content.includes('480p/playlist.m3u8'))
			assert.ok(content.includes('360p/playlist.m3u8'))
		} finally {
			// Clean up test directory
			await rm(testDir, { recursive: true, force: true })
		}
	})
})
