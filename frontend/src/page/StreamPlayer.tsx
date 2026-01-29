import Hls from 'hls.js'
import { useEffect, useRef, useState } from 'preact/hooks'
import { useAuth } from '../context/Auth.js'
import type { StreamDetailResponse } from '../types.js'
import { StreamDynamoDBClient } from '../utils/StreamDynamoDBClient.js'

const TABLE_NAME = DYNAMODB_TABLE_NAME
const CLOUDFRONT_DOMAIN = CLOUDFRONT_DOMAIN_NAME
const POLL_INTERVAL = 5000 // 5 seconds

type StreamPlayerProps = {
	port: number
}

export const StreamPlayer = ({ port }: StreamPlayerProps) => {
	const { awsConfig } = useAuth()
	const [streamDetail, setStreamDetail] = useState<StreamDetailResponse | null>(
		null,
	)
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState<string | null>(null)
	const [streamMode, setStreamMode] = useState<'adaptive' | 'raw'>('adaptive')
	const [currentBitrate, setCurrentBitrate] = useState<string>('')
	const [currentResolution, setCurrentResolution] = useState<string>('')
	const [availableLevels, setAvailableLevels] = useState<
		Array<{ index: number; label: string }>
	>([])
	const [selectedLevel, setSelectedLevel] = useState<number>(-1) // -1 = auto
	const [previousStatus, setPreviousStatus] = useState<
		'active' | 'inactive' | null
	>(null)
	const [retryCount, setRetryCount] = useState(0)
	const [isRetrying, setIsRetrying] = useState(false)
	const [maxRetriesReached, setMaxRetriesReached] = useState(false)
	const [playlistRefreshFailures, setPlaylistRefreshFailures] = useState(0)
	const [playlistRefreshError, setPlaylistRefreshError] = useState<
		string | null
	>(null)

	const videoRef = useRef<HTMLVideoElement>(null)
	const hlsRef = useRef<Hls | null>(null)
	const retryTimeoutRef = useRef<number | null>(null)
	const playlistRetryTimeoutRef = useRef<number | null>(null)

	// Fetch stream details
	const fetchStreamDetail = async (isManualRetry = false) => {
		if (!awsConfig) {
			return
		}

		// Don't retry automatically if max retries reached (unless manual retry)
		if (maxRetriesReached && !isManualRetry) {
			return
		}

		try {
			const client = new StreamDynamoDBClient(
				awsConfig,
				TABLE_NAME,
				CLOUDFRONT_DOMAIN,
			)
			const detail = await client.getStreamDetail(port)
			setStreamDetail(detail)
			setError(null)
			setRetryCount(0) // Reset retry count on success
			setIsRetrying(false)
			setMaxRetriesReached(false)
		} catch (err) {
			console.error('[StreamPlayer] Failed to fetch stream detail:', err)
			const errorMessage =
				err instanceof Error ? err.message : 'Failed to load stream'
			setError(errorMessage)

			// Check if we should retry (before incrementing count)
			const nextRetryCount = retryCount + 1
			if (nextRetryCount <= 5 && !maxRetriesReached) {
				const backoffDelay = Math.min(1000 * Math.pow(2, retryCount), 30000) // Max 30 seconds
				console.log(
					`[StreamPlayer] Retrying in ${backoffDelay}ms (attempt ${nextRetryCount}/5)`,
				)
				setIsRetrying(true)
				setRetryCount(nextRetryCount)

				retryTimeoutRef.current = window.setTimeout(() => {
					void fetchStreamDetail()
				}, backoffDelay)
			} else {
				setIsRetrying(false)
				setMaxRetriesReached(true)
				console.log(
					'[StreamPlayer] Max retries reached, stopping automatic retries',
				)
			}
		} finally {
			setLoading(false)
		}
	}

	// Cleanup retry timeouts on unmount
	useEffect(() => {
		return () => {
			if (retryTimeoutRef.current !== null) {
				clearTimeout(retryTimeoutRef.current)
			}
			if (playlistRetryTimeoutRef.current !== null) {
				clearTimeout(playlistRetryTimeoutRef.current)
			}
		}
	}, [])

	// Poll for stream status updates
	useEffect(() => {
		if (!awsConfig) {
			return
		}

		void fetchStreamDetail()

		const intervalId = setInterval(() => {
			if (!maxRetriesReached) {
				void fetchStreamDetail()
			}
		}, POLL_INTERVAL)

		return () => clearInterval(intervalId)
	}, [awsConfig, port, maxRetriesReached])

	// Detect stream status changes and handle transitions
	useEffect(() => {
		if (streamDetail === null) {
			return
		}

		// Detect transition from offline to active
		if (previousStatus === 'inactive' && streamDetail.status === 'active') {
			console.log(
				'[StreamPlayer] Stream resumed - transitioning from offline to live',
			)
			// The HLS player will automatically initialize in the next effect
		}

		// Detect transition from active to offline
		if (previousStatus === 'active' && streamDetail.status === 'inactive') {
			console.log('[StreamPlayer] Stream went offline - showing last frame')
			// Clean up HLS player
			if (hlsRef.current !== null) {
				hlsRef.current.destroy()
				hlsRef.current = null
			}
		}

		setPreviousStatus(streamDetail.status)
	}, [streamDetail?.status])

	// Initialize HLS player
	useEffect(() => {
		if (
			streamDetail === null ||
			!videoRef.current ||
			streamDetail.status !== 'active'
		) {
			return
		}

		const video = videoRef.current
		const streamUrl =
			streamMode === 'adaptive'
				? streamDetail.hlsManifestUrl
				: streamDetail.rawStreamUrl

		if (Hls.isSupported()) {
			const hls = new Hls({
				enableWorker: true,
				lowLatencyMode: true,
				// Enable adaptive bitrate streaming
				startLevel: -1, // Start with auto quality selection
				capLevelToPlayerSize: true, // Limit quality based on player size
				maxBufferLength: 30, // Maximum buffer length in seconds
				maxMaxBufferLength: 60, // Maximum max buffer length
				// Live streaming configuration
				liveSyncDurationCount: 3, // Stay 3 segments behind live edge
				liveMaxLatencyDurationCount: 10, // Max 10 segments behind
				liveDurationInfinity: true, // Handle infinite duration streams
				// Manifest loading retry configuration
				manifestLoadingTimeOut: 10000, // 10 second timeout
				manifestLoadingMaxRetry: 5, // Retry up to 5 times
				manifestLoadingRetryDelay: 1000, // Start with 1 second delay
				manifestLoadingMaxRetryTimeout: 64000, // Max 64 seconds between retries (exponential backoff)
			})

			hlsRef.current = hls

			hls.loadSource(streamUrl)
			hls.attachMedia(video)

			hls.on(Hls.Events.MANIFEST_PARSED, () => {
				console.log('[StreamPlayer] HLS manifest parsed')

				// Populate available quality levels
				const levels = hls.levels.map((level, index) => ({
					index,
					label: `${level.height}p (${Math.round(level.bitrate / 1000)} kbps)`,
				}))
				setAvailableLevels(levels)

				video.play().catch((err) => {
					console.error('[StreamPlayer] Failed to autoplay:', err)
				})
			})

			hls.on(Hls.Events.LEVEL_SWITCHED, (_event, data) => {
				const level = hls.levels[data.level]
				if (level) {
					setCurrentBitrate(`${Math.round(level.bitrate / 1000)} kbps`)
					setCurrentResolution(`${level.width}x${level.height}`)
					console.log(`[StreamPlayer] Quality switched to ${level.height}p`)
				}
			})

			hls.on(Hls.Events.ERROR, (_event, data) => {
				console.error('[StreamPlayer] HLS error:', data)

				// Handle manifest/playlist loading errors specifically
				if (
					data.type === Hls.ErrorTypes.NETWORK_ERROR &&
					data.details === 'manifestLoadError'
				) {
					console.log(
						'[StreamPlayer] Playlist refresh failed, implementing retry logic',
					)

					const currentFailures = playlistRefreshFailures + 1
					setPlaylistRefreshFailures(currentFailures)

					if (currentFailures <= 5) {
						// Exponential backoff: 1s, 2s, 4s, 8s, 16s
						const backoffDelay = 1000 * Math.pow(2, currentFailures - 1)
						console.log(
							`[StreamPlayer] Retrying playlist refresh in ${backoffDelay}ms (attempt ${currentFailures}/5)`,
						)

						// Continue playing buffered segments - don't destroy the player
						// HLS.js will automatically continue playing buffered content

						// Schedule retry
						playlistRetryTimeoutRef.current = window.setTimeout(() => {
							if (hlsRef.current) {
								console.log('[StreamPlayer] Retrying playlist load...')
								hlsRef.current.startLoad()
							}
						}, backoffDelay)
					} else {
						// Max retries reached - emit error event
						console.error(
							'[StreamPlayer] Playlist refresh failed after 5 consecutive attempts',
						)
						setPlaylistRefreshError(
							'Unable to refresh playlist after multiple attempts. The stream may be temporarily unavailable.',
						)

						// Continue playing buffered segments - don't destroy the player
						// User can still watch what's buffered
					}

					// Don't treat this as fatal - let buffered content play
					return
				}

				// Handle other fatal errors
				if (data.fatal) {
					switch (data.type) {
						case Hls.ErrorTypes.NETWORK_ERROR:
							console.log(
								'[StreamPlayer] Network error, attempting to recover...',
							)
							// Reset playlist refresh failure count on successful recovery
							setPlaylistRefreshFailures(0)
							setPlaylistRefreshError(null)

							// Retry loading with exponential backoff
							setTimeout(() => {
								if (hlsRef.current) {
									hls.startLoad()
								}
							}, 1000)
							break
						case Hls.ErrorTypes.MEDIA_ERROR:
							console.log(
								'[StreamPlayer] Media error, attempting to recover...',
							)
							hls.recoverMediaError()

							// If recovery fails, try fallback to lower quality
							setTimeout(() => {
								if (hlsRef.current && hlsRef.current.levels.length > 1) {
									const currentLevel = hlsRef.current.currentLevel
									if (currentLevel > 0) {
										console.log('[StreamPlayer] Falling back to lower quality')
										hlsRef.current.currentLevel = currentLevel - 1
									}
								}
							}, 2000)
							break
						default:
							console.error('[StreamPlayer] Fatal error, cannot recover')
							setError(
								'Playback error occurred. Please try refreshing the page.',
							)
							break
					}
				}
			})

			// Listen for successful manifest loads to reset failure counter
			hls.on(Hls.Events.MANIFEST_LOADED, () => {
				// Reset playlist refresh failure count on successful load
				if (playlistRefreshFailures > 0) {
					console.log(
						'[StreamPlayer] Playlist refresh successful, resuming normal operation',
					)
					setPlaylistRefreshFailures(0)
					setPlaylistRefreshError(null)
				}
			})

			return () => {
				hls.destroy()
				hlsRef.current = null
			}
		} else if (video.canPlayType('application/vnd.apple.mpegurl')) {
			// Native HLS support (Safari)
			video.src = streamUrl
			const handleLoadedMetadata = () => {
				video.play().catch((err) => {
					console.error('[StreamPlayer] Failed to autoplay:', err)
				})
			}
			video.addEventListener('loadedmetadata', handleLoadedMetadata)

			return () => {
				video.removeEventListener('loadedmetadata', handleLoadedMetadata)
			}
		} else {
			setError('HLS is not supported in this browser')
			return
		}
	}, [streamDetail, streamMode])

	const handleModeToggle = () => {
		setStreamMode((prev) => (prev === 'adaptive' ? 'raw' : 'adaptive'))
	}

	const handleQualityChange = (event: Event) => {
		const target = event.target as HTMLSelectElement
		const level = parseInt(target.value, 10)

		if (hlsRef.current) {
			if (level === -1) {
				// Auto quality
				hlsRef.current.currentLevel = -1
				setSelectedLevel(-1)
				console.log('[StreamPlayer] Switched to auto quality')
			} else {
				// Manual quality selection
				hlsRef.current.currentLevel = level
				setSelectedLevel(level)
				console.log(`[StreamPlayer] Manually selected quality level ${level}`)
			}
		}
	}

	const handleRetry = () => {
		setLoading(true)
		setError(null)
		setRetryCount(0)
		setIsRetrying(false)
		setMaxRetriesReached(false)
		if (retryTimeoutRef.current !== null) {
			clearTimeout(retryTimeoutRef.current)
		}
		void fetchStreamDetail(true) // Manual retry
	}

	if (!awsConfig) {
		return (
			<main style={{ padding: '2rem', textAlign: 'center' }}>
				<p>Loading authentication...</p>
			</main>
		)
	}

	if (loading) {
		return (
			<main style={{ padding: '2rem', textAlign: 'center' }}>
				<p>Loading stream...</p>
			</main>
		)
	}

	if (error !== null && error !== '') {
		return (
			<main style={{ padding: '2rem', textAlign: 'center' }}>
				<h1>Stream Port {port}</h1>
				<div
					style={{
						marginTop: '2rem',
						padding: '2rem',
						backgroundColor: '#fee',
						borderRadius: '8px',
						border: '1px solid #fcc',
						maxWidth: '600px',
						margin: '2rem auto',
					}}
				>
					<p
						style={{ color: '#c00', marginBottom: '1rem', fontWeight: 'bold' }}
					>
						{error.includes('not found')
							? 'Stream Not Found'
							: 'Connection Error'}
					</p>
					<p style={{ color: '#666', marginBottom: '1rem' }}>{error}</p>
					{isRetrying && (
						<p
							style={{
								color: '#666',
								fontSize: '0.875rem',
								marginBottom: '1rem',
							}}
						>
							Retrying automatically... (Attempt {retryCount + 1}/5)
						</p>
					)}
					<button
						onClick={handleRetry}
						disabled={isRetrying}
						style={{
							padding: '0.5rem 1rem',
							backgroundColor: isRetrying ? '#ccc' : '#007bff',
							color: 'white',
							border: 'none',
							borderRadius: '4px',
							cursor: isRetrying ? 'not-allowed' : 'pointer',
						}}
					>
						{isRetrying ? 'Retrying...' : 'Retry Now'}
					</button>
				</div>
			</main>
		)
	}

	if (streamDetail === null) {
		return (
			<main style={{ padding: '2rem', textAlign: 'center' }}>
				<p>Stream not found</p>
			</main>
		)
	}

	return (
		<main style={{ padding: '2rem', maxWidth: '1200px', margin: '0 auto' }}>
			<div style={{ marginBottom: '1rem' }}>
				<a href="/" style={{ color: '#007bff', textDecoration: 'none' }}>
					← Back to streams
				</a>
			</div>

			<h1>Stream Port {port}</h1>

			<div
				style={{
					display: 'grid',
					gridTemplateColumns: '1fr 300px',
					gap: '2rem',
					marginTop: '2rem',
				}}
			>
				{/* Video Player */}
				<div>
					{streamDetail.status === 'active' ? (
						<>
							<div
								style={{
									position: 'relative',
									backgroundColor: '#000',
									borderRadius: '8px',
									overflow: 'hidden',
								}}
							>
								<video
									ref={videoRef}
									controls
									style={{ width: '100%', display: 'block' }}
								/>
								<div
									style={{
										position: 'absolute',
										top: '1rem',
										right: '1rem',
										padding: '0.5rem 1rem',
										backgroundColor: 'rgba(0, 0, 0, 0.7)',
										color: 'white',
										borderRadius: '4px',
										fontSize: '0.875rem',
									}}
								>
									LIVE
								</div>
							</div>

							{/* Playlist Refresh Error Warning */}
							{playlistRefreshError !== null && (
								<div
									style={{
										marginTop: '1rem',
										padding: '1rem',
										backgroundColor: '#fff3cd',
										border: '1px solid #ffc107',
										borderRadius: '4px',
										color: '#856404',
									}}
								>
									<p style={{ margin: 0, fontWeight: 'bold' }}>
										⚠️ Playlist Refresh Issue
									</p>
									<p style={{ margin: '0.5rem 0 0 0', fontSize: '0.875rem' }}>
										{playlistRefreshError}
									</p>
									<p style={{ margin: '0.5rem 0 0 0', fontSize: '0.875rem' }}>
										Buffered content will continue playing. The player will
										automatically resume when the stream becomes available.
									</p>
								</div>
							)}
						</>
					) : (
						<div
							style={{
								position: 'relative',
								backgroundColor: '#000',
								borderRadius: '8px',
								overflow: 'hidden',
							}}
						>
							<img
								src={streamDetail.lastFrameUrl}
								alt="Last frame"
								style={{ width: '100%', display: 'block' }}
								onError={(e) => {
									e.currentTarget.src =
										'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="800" height="600"%3E%3Crect fill="%23333" width="800" height="600"/%3E%3Ctext x="50%25" y="50%25" text-anchor="middle" dy=".3em" fill="%23999" font-size="24"%3ENo Image Available%3C/text%3E%3C/svg%3E'
								}}
							/>
							<div
								style={{
									position: 'absolute',
									top: '1rem',
									right: '1rem',
									padding: '0.5rem 1rem',
									backgroundColor: 'rgba(108, 117, 125, 0.9)',
									color: 'white',
									borderRadius: '4px',
									fontSize: '0.875rem',
									fontWeight: 'bold',
								}}
							>
								OFFLINE
							</div>
						</div>
					)}

					{/* Mode Toggle */}
					{streamDetail.status === 'active' && (
						<div style={{ marginTop: '1rem' }}>
							<div
								style={{
									display: 'flex',
									alignItems: 'center',
									gap: '1rem',
									marginBottom: '1rem',
								}}
							>
								<label
									style={{
										display: 'flex',
										alignItems: 'center',
										gap: '0.5rem',
										cursor: 'pointer',
									}}
								>
									<input
										type="checkbox"
										checked={streamMode === 'raw'}
										onChange={handleModeToggle}
										style={{ cursor: 'pointer' }}
									/>
									<span>Raw Stream Mode</span>
								</label>
								<span style={{ fontSize: '0.875rem', color: '#666' }}>
									{streamMode === 'adaptive'
										? 'Adaptive bitrate enabled'
										: 'Original quality'}
								</span>
							</div>

							{/* Quality Selection Dropdown */}
							{streamMode === 'adaptive' && availableLevels.length > 0 && (
								<div
									style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}
								>
									<label style={{ fontSize: '0.875rem', fontWeight: 'bold' }}>
										Quality:
									</label>
									<select
										value={selectedLevel}
										onChange={handleQualityChange}
										style={{
											padding: '0.5rem',
											borderRadius: '4px',
											border: '1px solid #ddd',
											backgroundColor: 'white',
											cursor: 'pointer',
										}}
									>
										<option value={-1}>Auto (Adaptive)</option>
										{availableLevels.map((level) => (
											<option key={level.index} value={level.index}>
												{level.label}
											</option>
										))}
									</select>
								</div>
							)}
						</div>
					)}
				</div>

				{/* Metadata Panel */}
				<div
					style={{
						border: '1px solid #ddd',
						borderRadius: '8px',
						padding: '1.5rem',
					}}
				>
					<h2
						style={{ marginTop: 0, marginBottom: '1rem', fontSize: '1.25rem' }}
					>
						Stream Info
					</h2>

					<div style={{ marginBottom: '1rem' }}>
						<div
							style={{
								fontSize: '0.875rem',
								color: '#666',
								marginBottom: '0.25rem',
							}}
						>
							Status
						</div>
						<div
							style={{
								fontWeight: 'bold',
								color: streamDetail.status === 'active' ? '#28a745' : '#6c757d',
							}}
						>
							{streamDetail.status === 'active' ? 'Active' : 'Offline'}
						</div>
					</div>

					<div style={{ marginBottom: '1rem' }}>
						<div
							style={{
								fontSize: '0.875rem',
								color: '#666',
								marginBottom: '0.25rem',
							}}
						>
							Port
						</div>
						<div style={{ fontWeight: 'bold' }}>{streamDetail.port}</div>
					</div>

					<div style={{ marginBottom: '1rem' }}>
						<div
							style={{
								fontSize: '0.875rem',
								color: '#666',
								marginBottom: '0.25rem',
							}}
						>
							Last Update
						</div>
						<div>{new Date(streamDetail.lastPacketTime).toLocaleString()}</div>
					</div>

					{streamDetail.status === 'active' && currentBitrate && (
						<>
							<div style={{ marginBottom: '1rem' }}>
								<div
									style={{
										fontSize: '0.875rem',
										color: '#666',
										marginBottom: '0.25rem',
									}}
								>
									Current Bitrate
								</div>
								<div style={{ fontWeight: 'bold' }}>{currentBitrate}</div>
							</div>

							<div style={{ marginBottom: '1rem' }}>
								<div
									style={{
										fontSize: '0.875rem',
										color: '#666',
										marginBottom: '0.25rem',
									}}
								>
									Resolution
								</div>
								<div style={{ fontWeight: 'bold' }}>{currentResolution}</div>
							</div>
						</>
					)}

					<div
						style={{
							marginTop: '1.5rem',
							paddingTop: '1rem',
							borderTop: '1px solid #eee',
						}}
					>
						<div style={{ fontSize: '0.75rem', color: '#999' }}>
							Created:{' '}
							{new Date(streamDetail.metadata.createdAt).toLocaleString()}
						</div>
						<div
							style={{
								fontSize: '0.75rem',
								color: '#999',
								marginTop: '0.25rem',
							}}
						>
							Updated:{' '}
							{new Date(streamDetail.metadata.updatedAt).toLocaleString()}
						</div>
					</div>
				</div>
			</div>
		</main>
	)
}
