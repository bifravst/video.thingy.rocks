import * as fc from 'fast-check'
import assert from 'node:assert'
import { describe, it } from 'node:test'
import type { BitrateProfile } from './FFmpegTranscoder.ts'
import { MasterPlaylistGenerator } from './MasterPlaylistGenerator.ts'

/**
 * Property-Based Tests for Master Playlist Generation
 *
 * These tests verify universal properties that should hold across all valid
 * profile configurations.
 */
void describe('MasterPlaylistGenerator - Property Tests', () => {
	/**
	 * Property 5: Master Playlist Content
	 * **Validates: Requirements 4.2, 4.4**
	 *
	 * For any generated master.m3u8 file, it should reference all configured
	 * quality profiles with correct bandwidth and resolution information for
	 * each profile.
	 */
	void describe('Property 5: Master Playlist Content', () => {
		// Arbitrary generator for profile names
		const profileNameArb = fc.constantFrom(
			'1080p',
			'720p',
			'480p',
			'360p',
			'240p',
			'144p',
			'4k',
			'2k',
			'high',
			'medium',
			'low',
		)

		// Arbitrary generator for resolutions (width x height)
		const resolutionArb = fc
			.tuple(
				fc.integer({ min: 320, max: 3840 }), // width
				fc.integer({ min: 240, max: 2160 }), // height
			)
			.map(([width, height]) => `${width}x${height}`)

		// Arbitrary generator for bitrates (in kbps)
		const bitrateArb = fc
			.integer({ min: 100, max: 10000 })
			.map((kbps) => `${kbps}k`)

		// Arbitrary generator for a single BitrateProfile
		const bitrateProfileArb: fc.Arbitrary<BitrateProfile> = fc.record({
			name: profileNameArb,
			resolution: resolutionArb,
			videoBitrate: bitrateArb,
			audioBitrate: bitrateArb,
		})

		// Arbitrary generator for an array of unique profiles (1-10 profiles)
		const profilesArb = fc
			.array(bitrateProfileArb, { minLength: 1, maxLength: 10 })
			.map((profiles) => {
				// Ensure unique profile names by appending index if needed
				const uniqueProfiles: BitrateProfile[] = []
				const nameCount = new Map<string, number>()

				for (const profile of profiles) {
					const count = nameCount.get(profile.name) || 0
					nameCount.set(profile.name, count + 1)

					const uniqueName =
						count > 0 ? `${profile.name}-${count}` : profile.name
					uniqueProfiles.push({
						...profile,
						name: uniqueName,
					})
				}

				return uniqueProfiles
			})

		// Arbitrary generator for port numbers
		const portArb = fc.integer({ min: 1024, max: 65535 })

		void it('should reference all configured profiles in the master playlist', () => {
			fc.assert(
				fc.property(profilesArb, portArb, (profiles, port) => {
					const generator = new MasterPlaylistGenerator({
						port,
						profiles,
						localOutputDir: '/tmp/test',
					})

					const content = generator.generateMasterPlaylist()

					// Verify each profile is referenced in the master playlist
					for (const profile of profiles) {
						const playlistPath = `${profile.name}/playlist.m3u8`
						assert.ok(
							content.includes(playlistPath),
							`Master playlist should reference ${playlistPath}`,
						)
					}
				}),
				{ numRuns: 100 },
			)
		})

		void it('should include correct bandwidth for each profile', () => {
			fc.assert(
				fc.property(profilesArb, portArb, (profiles, port) => {
					const generator = new MasterPlaylistGenerator({
						port,
						profiles,
						localOutputDir: '/tmp/test',
					})

					const content = generator.generateMasterPlaylist()

					// Verify bandwidth calculation for each profile
					for (const profile of profiles) {
						const videoBitrateKbps = parseInt(profile.videoBitrate)
						const audioBitrateKbps = parseInt(profile.audioBitrate)
						const expectedBandwidth =
							(videoBitrateKbps + audioBitrateKbps) * 1000

						assert.ok(
							content.includes(`BANDWIDTH=${expectedBandwidth}`),
							`Master playlist should include BANDWIDTH=${expectedBandwidth} for profile ${profile.name}`,
						)
					}
				}),
				{ numRuns: 100 },
			)
		})

		void it('should include correct resolution for each profile', () => {
			fc.assert(
				fc.property(profilesArb, portArb, (profiles, port) => {
					const generator = new MasterPlaylistGenerator({
						port,
						profiles,
						localOutputDir: '/tmp/test',
					})

					const content = generator.generateMasterPlaylist()

					// Verify resolution for each profile
					for (const profile of profiles) {
						assert.ok(
							content.includes(`RESOLUTION=${profile.resolution}`),
							`Master playlist should include RESOLUTION=${profile.resolution} for profile ${profile.name}`,
						)
					}
				}),
				{ numRuns: 100 },
			)
		})

		void it('should have #EXT-X-STREAM-INF tag for each profile', () => {
			fc.assert(
				fc.property(profilesArb, portArb, (profiles, port) => {
					const generator = new MasterPlaylistGenerator({
						port,
						profiles,
						localOutputDir: '/tmp/test',
					})

					const content = generator.generateMasterPlaylist()

					// Count #EXT-X-STREAM-INF tags
					const streamInfoCount = (content.match(/#EXT-X-STREAM-INF/g) || [])
						.length

					assert.strictEqual(
						streamInfoCount,
						profiles.length,
						`Master playlist should have ${profiles.length} #EXT-X-STREAM-INF tags`,
					)
				}),
				{ numRuns: 100 },
			)
		})

		void it('should always start with #EXTM3U header', () => {
			fc.assert(
				fc.property(profilesArb, portArb, (profiles, port) => {
					const generator = new MasterPlaylistGenerator({
						port,
						profiles,
						localOutputDir: '/tmp/test',
					})

					const content = generator.generateMasterPlaylist()
					const lines = content.split('\n')

					assert.strictEqual(
						lines[0],
						'#EXTM3U',
						'Master playlist should start with #EXTM3U',
					)
				}),
				{ numRuns: 100 },
			)
		})

		void it('should always include #EXT-X-VERSION:3', () => {
			fc.assert(
				fc.property(profilesArb, portArb, (profiles, port) => {
					const generator = new MasterPlaylistGenerator({
						port,
						profiles,
						localOutputDir: '/tmp/test',
					})

					const content = generator.generateMasterPlaylist()

					assert.ok(
						content.includes('#EXT-X-VERSION:3'),
						'Master playlist should include #EXT-X-VERSION:3',
					)
				}),
				{ numRuns: 100 },
			)
		})

		void it('should have stream info line followed by playlist path for each profile', () => {
			fc.assert(
				fc.property(profilesArb, portArb, (profiles, port) => {
					const generator = new MasterPlaylistGenerator({
						port,
						profiles,
						localOutputDir: '/tmp/test',
					})

					const content = generator.generateMasterPlaylist()
					const lines = content.split('\n').filter((line) => line.length > 0)

					// Skip header lines (#EXTM3U and #EXT-X-VERSION:3)
					let lineIndex = 2

					for (const profile of profiles) {
						// Check that we have a stream info line
						assert.ok(
							lines[lineIndex]?.startsWith('#EXT-X-STREAM-INF'),
							`Line ${lineIndex} should be #EXT-X-STREAM-INF tag`,
						)

						// Check that the next line is the playlist path
						assert.strictEqual(
							lines[lineIndex + 1],
							`${profile.name}/playlist.m3u8`,
							`Line ${lineIndex + 1} should be ${profile.name}/playlist.m3u8`,
						)

						lineIndex += 2
					}
				}),
				{ numRuns: 100 },
			)
		})

		void it('should maintain correct line count (header + 2 lines per profile)', () => {
			fc.assert(
				fc.property(profilesArb, portArb, (profiles, port) => {
					const generator = new MasterPlaylistGenerator({
						port,
						profiles,
						localOutputDir: '/tmp/test',
					})

					const content = generator.generateMasterPlaylist()
					const lines = content.split('\n').filter((line) => line.length > 0)

					// Expected: 2 header lines + 2 lines per profile
					const expectedLineCount = 2 + profiles.length * 2

					assert.strictEqual(
						lines.length,
						expectedLineCount,
						`Master playlist should have ${expectedLineCount} lines`,
					)
				}),
				{ numRuns: 100 },
			)
		})

		void it('should handle profiles with extreme bitrate values', () => {
			fc.assert(
				fc.property(
					fc.array(
						fc.record({
							name: profileNameArb,
							resolution: resolutionArb,
							videoBitrate: fc
								.integer({ min: 1, max: 50000 })
								.map((kbps) => `${kbps}k`),
							audioBitrate: fc
								.integer({ min: 1, max: 1000 })
								.map((kbps) => `${kbps}k`),
						}),
						{ minLength: 1, maxLength: 5 },
					),
					portArb,
					(profiles, port) => {
						const generator = new MasterPlaylistGenerator({
							port,
							profiles,
							localOutputDir: '/tmp/test',
						})

						const content = generator.generateMasterPlaylist()

						// Should not throw and should include all profiles
						assert.ok(content.includes('#EXTM3U'))
						assert.strictEqual(
							(content.match(/#EXT-X-STREAM-INF/g) || []).length,
							profiles.length,
						)
					},
				),
				{ numRuns: 100 },
			)
		})

		void it('should handle profiles with extreme resolution values', () => {
			fc.assert(
				fc.property(
					fc.array(
						fc.record({
							name: profileNameArb,
							resolution: fc
								.tuple(
									fc.integer({ min: 160, max: 7680 }), // width (up to 8K)
									fc.integer({ min: 120, max: 4320 }), // height (up to 8K)
								)
								.map(([width, height]) => `${width}x${height}`),
							videoBitrate: bitrateArb,
							audioBitrate: bitrateArb,
						}),
						{ minLength: 1, maxLength: 5 },
					),
					portArb,
					(profiles, port) => {
						const generator = new MasterPlaylistGenerator({
							port,
							profiles,
							localOutputDir: '/tmp/test',
						})

						const content = generator.generateMasterPlaylist()

						// Verify all resolutions are included
						for (const profile of profiles) {
							assert.ok(
								content.includes(`RESOLUTION=${profile.resolution}`),
								`Should include resolution ${profile.resolution}`,
							)
						}
					},
				),
				{ numRuns: 100 },
			)
		})
	})
})
