import assert from 'node:assert'
import { mkdir, readFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, it } from 'node:test'
import type { BitrateProfile } from './FFmpegTranscoder.ts'
import { MasterPlaylistGenerator } from './MasterPlaylistGenerator.ts'

void describe('MasterPlaylistGenerator', () => {
	const testOutputDir = join(process.cwd(), 'test-output')
	const testPort = 5000

	const testProfiles: BitrateProfile[] = [
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

	beforeEach(async () => {
		// Create test output directory
		await mkdir(join(testOutputDir, 'hls', testPort.toString()), {
			recursive: true,
		})
	})

	afterEach(async () => {
		// Clean up test output directory
		await rm(testOutputDir, { recursive: true, force: true })
	})

	void describe('constructor', () => {
		void it('should accept port, profiles, and output directory', () => {
			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			assert.ok(generator)
		})
	})

	void describe('generateMasterPlaylist', () => {
		void it('should generate master playlist with correct header', () => {
			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			const content = generator.generateMasterPlaylist()

			assert.ok(content.includes('#EXTM3U'))
			assert.ok(content.includes('#EXT-X-VERSION:3'))
		})

		void it('should include #EXT-X-STREAM-INF tags for each profile', () => {
			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			const content = generator.generateMasterPlaylist()

			// Should have 4 stream info tags (one per profile)
			const streamInfoCount = (content.match(/#EXT-X-STREAM-INF/g) || []).length
			assert.strictEqual(streamInfoCount, 4)
		})

		void it('should include bandwidth for each profile', () => {
			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			const content = generator.generateMasterPlaylist()

			// 1080p: 5000k video + 128k audio = 5128k = 5128000 bits/sec
			assert.ok(content.includes('BANDWIDTH=5128000'))

			// 720p: 3000k video + 128k audio = 3128k = 3128000 bits/sec
			assert.ok(content.includes('BANDWIDTH=3128000'))

			// 480p: 1500k video + 128k audio = 1628k = 1628000 bits/sec
			assert.ok(content.includes('BANDWIDTH=1628000'))

			// 360p: 800k video + 128k audio = 928k = 928000 bits/sec
			assert.ok(content.includes('BANDWIDTH=928000'))
		})

		void it('should include resolution for each profile', () => {
			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			const content = generator.generateMasterPlaylist()

			assert.ok(content.includes('RESOLUTION=1920x1080'))
			assert.ok(content.includes('RESOLUTION=1280x720'))
			assert.ok(content.includes('RESOLUTION=854x480'))
			assert.ok(content.includes('RESOLUTION=640x360'))
		})

		void it('should include relative playlist paths for each profile', () => {
			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			const content = generator.generateMasterPlaylist()

			assert.ok(content.includes('1080p/playlist.m3u8'))
			assert.ok(content.includes('720p/playlist.m3u8'))
			assert.ok(content.includes('480p/playlist.m3u8'))
			assert.ok(content.includes('360p/playlist.m3u8'))
		})

		void it('should generate valid HLS master playlist format', () => {
			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			const content = generator.generateMasterPlaylist()
			const lines = content.split('\n')

			// First line should be #EXTM3U
			assert.strictEqual(lines[0], '#EXTM3U')

			// Second line should be version
			assert.strictEqual(lines[1], '#EXT-X-VERSION:3')

			// Each profile should have stream info followed by path
			// Line 2: #EXT-X-STREAM-INF for 1080p
			assert.ok(lines[2]?.startsWith('#EXT-X-STREAM-INF'))
			// Line 3: 1080p/playlist.m3u8
			assert.strictEqual(lines[3], '1080p/playlist.m3u8')
		})

		void it('should handle single profile configuration', () => {
			const singleProfile: BitrateProfile[] = [
				{
					name: '720p',
					resolution: '1280x720',
					videoBitrate: '3000k',
					audioBitrate: '128k',
				},
			]

			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: singleProfile,
				localOutputDir: testOutputDir,
			})

			const content = generator.generateMasterPlaylist()

			assert.ok(content.includes('#EXTM3U'))
			assert.ok(content.includes('BANDWIDTH=3128000'))
			assert.ok(content.includes('RESOLUTION=1280x720'))
			assert.ok(content.includes('720p/playlist.m3u8'))
		})

		void it('should handle profiles with different bitrate formats', () => {
			const customProfiles: BitrateProfile[] = [
				{
					name: 'high',
					resolution: '1920x1080',
					videoBitrate: '8000k',
					audioBitrate: '256k',
				},
			]

			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: customProfiles,
				localOutputDir: testOutputDir,
			})

			const content = generator.generateMasterPlaylist()

			// 8000k + 256k = 8256k = 8256000 bits/sec
			assert.ok(content.includes('BANDWIDTH=8256000'))
			assert.ok(content.includes('RESOLUTION=1920x1080'))
		})
	})

	void describe('writeMasterPlaylist', () => {
		void it('should write master playlist to disk', async () => {
			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			await generator.writeMasterPlaylist()

			const filePath = generator.getMasterPlaylistPath()
			const content = await readFile(filePath, 'utf-8')

			assert.ok(content.includes('#EXTM3U'))
			assert.ok(content.includes('#EXT-X-VERSION:3'))
		})

		void it('should write content matching generateMasterPlaylist output', async () => {
			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			const expectedContent = generator.generateMasterPlaylist()
			await generator.writeMasterPlaylist()

			const filePath = generator.getMasterPlaylistPath()
			const actualContent = await readFile(filePath, 'utf-8')

			assert.strictEqual(actualContent, expectedContent)
		})

		void it('should overwrite existing master playlist', async () => {
			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			// Write first time
			await generator.writeMasterPlaylist()

			// Write second time (should overwrite)
			await generator.writeMasterPlaylist()

			const filePath = generator.getMasterPlaylistPath()
			const content = await readFile(filePath, 'utf-8')

			assert.ok(content.includes('#EXTM3U'))
		})
	})

	void describe('getMasterPlaylistPath', () => {
		void it('should return correct path to master.m3u8', () => {
			const generator = new MasterPlaylistGenerator({
				port: testPort,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			const path = generator.getMasterPlaylistPath()
			const expectedPath = join(
				testOutputDir,
				'hls',
				testPort.toString(),
				'master.m3u8',
			)

			assert.strictEqual(path, expectedPath)
		})

		void it('should return different paths for different ports', () => {
			const generator1 = new MasterPlaylistGenerator({
				port: 5000,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			const generator2 = new MasterPlaylistGenerator({
				port: 5001,
				profiles: testProfiles,
				localOutputDir: testOutputDir,
			})

			const path1 = generator1.getMasterPlaylistPath()
			const path2 = generator2.getMasterPlaylistPath()

			assert.notStrictEqual(path1, path2)
			assert.ok(path1.includes('5000'))
			assert.ok(path2.includes('5001'))
		})
	})
})
