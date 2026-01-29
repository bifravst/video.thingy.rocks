import { writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import type { BitrateProfile } from './FFmpegTranscoder.ts'

export type MasterPlaylistConfig = {
	port: number
	profiles: BitrateProfile[]
	localOutputDir: string
}

/**
 * Generates and maintains the master.m3u8 playlist file that references
 * all quality profiles for adaptive bitrate streaming.
 */
export class MasterPlaylistGenerator {
	private readonly config: MasterPlaylistConfig

	constructor(config: MasterPlaylistConfig) {
		this.config = config
	}

	/**
	 * Generate the master playlist content in HLS format.
	 * The master playlist includes #EXT-X-STREAM-INF tags with bandwidth
	 * and resolution information for each quality profile.
	 *
	 * @returns The master.m3u8 file content as a string
	 */
	generateMasterPlaylist(): string {
		const lines: string[] = []

		// HLS header
		lines.push('#EXTM3U')
		lines.push('#EXT-X-VERSION:3')

		// Add stream variant for each profile
		for (const profile of this.config.profiles) {
			// Parse resolution (e.g., "1920x1080" -> width: 1920, height: 1080)
			const [width, height] = profile.resolution.split('x').map(Number)

			// Parse bandwidth from videoBitrate (e.g., "5000k" -> 5000000 bits/sec)
			// Add audioBitrate to get total bandwidth
			const videoBitrateKbps = parseInt(profile.videoBitrate)
			const audioBitrateKbps = parseInt(profile.audioBitrate)
			const totalBandwidth = (videoBitrateKbps + audioBitrateKbps) * 1000

			// Add stream info tag
			lines.push(
				`#EXT-X-STREAM-INF:BANDWIDTH=${totalBandwidth},RESOLUTION=${width}x${height}`,
			)

			// Add relative path to profile playlist
			lines.push(`${profile.name}/playlist.m3u8`)
		}

		// Join with newlines and add final newline
		return lines.join('\n') + '\n'
	}

	/**
	 * Write the master playlist to disk.
	 * Creates the master.m3u8 file in the HLS output directory for this port.
	 *
	 * @returns Promise that resolves when the file is written
	 */
	async writeMasterPlaylist(): Promise<void> {
		const content = this.generateMasterPlaylist()
		const filePath = this.getMasterPlaylistPath()

		await writeFile(filePath, content, 'utf-8')

		console.log(
			`[MasterPlaylistGenerator] Wrote master playlist for port ${this.config.port}`,
		)
	}

	/**
	 * Get the local file system path to the master playlist.
	 *
	 * @returns Absolute path to master.m3u8 file
	 */
	getMasterPlaylistPath(): string {
		return join(
			this.config.localOutputDir,
			'hls',
			this.config.port.toString(),
			'master.m3u8',
		)
	}
}
