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

/**
 * Check if a recording is available by performing a HEAD request to the manifest URL
 * @param manifestUrl - The HLS manifest URL to check
 * @returns Promise<boolean> - true if recording is available (200 status), false otherwise
 * Requirements: 1.2, 1.3, 1.4
 */
const checkRecordingAvailability = async (
	manifestUrl: string,
): Promise<boolean> => {
	try {
		console.log('[StreamPlayer] Checking recording availability:', manifestUrl)

		// Create an AbortController for timeout handling
		const controller = new AbortController()
		const timeoutId = setTimeout(() => controller.abort(), 5000) // 5 second timeout

		const response = await fetch(manifestUrl, {
			method: 'HEAD',
			signal: controller.signal,
		})

		clearTimeout(timeoutId)

		const isAvailable = response.status === 200
		console.log(
			`[StreamPlayer] Recording availability check result: ${isAvailable ? 'available' : 'not available'} (status: ${response.status})`,
		)

		return isAvailable
	} catch (error) {
		// Handle network errors and timeouts
		if (error instanceof Error) {
			if (error.name === 'AbortError') {
				console.log(
					'[StreamPlayer] Recording availability check timed out after 5 seconds',
				)
			} else {
				console.log(
					'[StreamPlayer] Recording availability check failed:',
					error.message,
				)
			}
		} else {
			console.log(
				'[StreamPlayer] Recording availability check failed with unknown error',
			)
		}
		return false
	}
}

/**
 * Create HLS configuration for live streaming mode
 * @returns HLS configuration object with live streaming parameters
 * Requirements: 3.1, 3.2, 3.3
 */
const createLiveHLSConfig = () => {
	return {
		enableWorker: true,
		lowLatencyMode: true,
		// Enable adaptive bitrate streaming
		startLevel: -1, // Start with auto quality selection
		capLevelToPlayerSize: true, // Limit quality based on player size
		maxBufferLength: 30, // Maximum buffer length in seconds
		maxMaxBufferLength: 60, // Maximum max buffer length
		// Live streaming configuration
		liveSyncDurationCount: 3, // Stay 3 segments behind live edge
		liveMaxLatencyDurationCount: 100, // Max 100 segments behind (playlist contains last 100 segments)
		liveDurationInfinity: true, // Handle infinite duration streams
		// Manifest loading retry configuration
		manifestLoadingTimeOut: 10000, // 10 second timeout
		manifestLoadingMaxRetry: 5, // Retry up to 5 times
		manifestLoadingRetryDelay: 1000, // Start with 1 second delay
		manifestLoadingMaxRetryTimeout: 64000, // Max 64 seconds between retries (exponential backoff)
	}
}

/**
 * Create HLS configuration for VOD (Video-On-Demand) playback mode
 * @returns HLS configuration object without live streaming parameters
 * Requirements: 2.3, 3.1, 3.2, 3.3, 3.4
 */
const createVODHLSConfig = () => {
	return {
		enableWorker: true,
		lowLatencyMode: false, // Disable low latency mode for VOD
		// Enable adaptive bitrate streaming
		startLevel: -1, // Start with auto quality selection
		capLevelToPlayerSize: true, // Limit quality based on player size
		maxBufferLength: 30, // Maximum buffer length in seconds
		maxMaxBufferLength: 60, // Maximum max buffer length
		// VOD playback configuration
		startPosition: 0, // Start playback from the beginning
		// Manifest loading retry configuration
		manifestLoadingTimeOut: 10000, // 10 second timeout
		manifestLoadingMaxRetry: 5, // Retry up to 5 times
		manifestLoadingRetryDelay: 1000, // Start with 1 second delay
		manifestLoadingMaxRetryTimeout: 64000, // Max 64 seconds between retries (exponential backoff)
		// Explicitly exclude live parameters (not set):
		// - liveSyncDurationCount: not included for VOD
		// - liveMaxLatencyDurationCount: not included for VOD
		// - liveDurationInfinity: not included for VOD
	}
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
	const [playbackMode, setPlaybackMode] = useState<'live' | 'vod' | 'offline'>(
		'offline',
	)
	// eslint-disable-next-line @typescript-eslint/no-unused-vars -- Will be used in subsequent tasks
	const [recordingAvailable, setRecordingAvailable] = useState<boolean>(false)

	const videoRef = useRef<HTMLVideoElement>(null)
	const hlsRef = useRef<Hls | null>(null)
	const retryTimeoutRef = useRef<number | null>(null)
	const playlistRetryTimeoutRef = useRef<number | null>(null)

	// Fetch stream details
	const fetchStreamDetail = async (
		isManualRetry = false,
		isInitialLoad = false,
	) => {
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

			// Requirements: 10.1, 10.2, 10.5
			// On initial load, if stream is inactive, immediately check for recording availability
			if (isInitialLoad && detail.status === 'inactive') {
				console.log(
					'[StreamPlayer] Initial load with inactive stream - checking for recording immediately',
				)
				try {
					const isAvailable = await checkRecordingAvailability(
						detail.hlsManifestUrl,
					)
					if (isAvailable) {
						setRecordingAvailable(true)
						setPlaybackMode('vod')
						console.log(
							'[StreamPlayer] Initial load: Recording available, setting mode to VOD',
						)
					} else {
						setRecordingAvailable(false)
						setPlaybackMode('offline')
						console.log(
							'[StreamPlayer] Initial load: No recording available, setting mode to offline',
						)
					}
				} catch (error) {
					console.error(
						'[StreamPlayer] Error checking recording availability on initial load:',
						error,
					)
					setRecordingAvailable(false)
					setPlaybackMode('offline')
					console.log(
						'[StreamPlayer] Initial load: Error checking recording, setting mode to offline',
					)
				}
			} else if (isInitialLoad && detail.status === 'active') {
				// If stream is active on initial load, set mode to live
				console.log(
					'[StreamPlayer] Initial load with active stream - setting mode to live',
				)
				setPlaybackMode('live')
			}
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

		// Initial load with flag to trigger immediate recording check
		void fetchStreamDetail(false, true)

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

		// Detect transition from inactive to active
		if (previousStatus === 'inactive' && streamDetail.status === 'active') {
			console.log(
				'[StreamPlayer] Stream resumed - transitioning from offline to live',
			)
			// Set playback mode to live
			setPlaybackMode('live')
			console.log('[StreamPlayer] Mode transition: offline/vod -> live')
			// The HLS player will automatically initialize in the next effect
		}

		// Detect transition from active to inactive
		if (previousStatus === 'active' && streamDetail.status === 'inactive') {
			console.log('[StreamPlayer] Stream went offline - checking for recording')
			// Clean up HLS player
			if (hlsRef.current !== null) {
				hlsRef.current.destroy()
				hlsRef.current = null
			}

			// Reset stream mode to adaptive when transitioning away from live
			// Requirements: 9.3, 9.4
			setStreamMode('adaptive')
			console.log('[StreamPlayer] Reset stream mode to adaptive')

			// Check if recording is available
			void (async () => {
				try {
					const isAvailable = await checkRecordingAvailability(
						streamDetail.hlsManifestUrl,
					)
					if (isAvailable) {
						setRecordingAvailable(true)
						setPlaybackMode('vod')
						console.log(
							'[StreamPlayer] Mode transition: live -> vod (recording available)',
						)
					} else {
						setRecordingAvailable(false)
						setPlaybackMode('offline')
						console.log(
							'[StreamPlayer] Mode transition: live -> offline (no recording)',
						)
					}
				} catch (error) {
					console.error(
						'[StreamPlayer] Error checking recording availability:',
						error,
					)
					setRecordingAvailable(false)
					setPlaybackMode('offline')
					console.log(
						'[StreamPlayer] Mode transition: live -> offline (error checking recording)',
					)
				}
			})()
		}

		setPreviousStatus(streamDetail.status)
	}, [streamDetail?.status])

	// Initialize HLS player
	useEffect(() => {
		// Return early if no stream detail or video element
		if (streamDetail === null || !videoRef.current) {
			return
		}

		// Return early if playback mode is offline
		if (playbackMode === 'offline') {
			return
		}

		const video = videoRef.current

		// Determine stream URL and HLS config based on playback mode
		let streamUrl: string
		let hlsConfig: ReturnType<
			typeof createLiveHLSConfig | typeof createVODHLSConfig
		>

		if (playbackMode === 'live') {
			// Live mode: use streamMode logic (adaptive vs raw)
			streamUrl =
				streamMode === 'adaptive'
					? streamDetail.hlsManifestUrl
					: streamDetail.rawStreamUrl
			hlsConfig = createLiveHLSConfig()
			console.log(
				`[StreamPlayer] Initializing live HLS player with ${streamMode} mode`,
			)
		} else if (playbackMode === 'vod') {
			// VOD mode: always use adaptive manifest URL
			streamUrl = streamDetail.hlsManifestUrl
			hlsConfig = createVODHLSConfig()
			console.log('[StreamPlayer] Initializing VOD HLS player')
		} else {
			// Should not reach here, but return early as safety
			return
		}

		if (Hls.isSupported()) {
			const hls = new Hls(hlsConfig)

			hlsRef.current = hls

			hls.loadSource(streamUrl)
			hls.attachMedia(video)

			hls.on(Hls.Events.MANIFEST_PARSED, () => {
				console.log(`[StreamPlayer] HLS manifest parsed (${playbackMode} mode)`)

				// Populate available quality levels (for both live and VOD)
				const levels = hls.levels.map((level, index) => ({
					index,
					label: `${level.height}p (${Math.round(level.bitrate / 1000)} kbps)`,
				}))
				setAvailableLevels(levels)

				// Autoplay for both live and VOD modes
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
				console.error(`[StreamPlayer] HLS error (${playbackMode} mode):`, data)

				// Handle fragment/segment loading errors (missing segments)
				// Requirements: 8.3, 8.4
				if (
					data.type === Hls.ErrorTypes.NETWORK_ERROR &&
					(data.details === 'fragLoadError' ||
						data.details === 'fragLoadTimeOut')
				) {
					console.warn(
						`[StreamPlayer] Segment load error in ${playbackMode} mode:`,
						{
							details: data.details,
							frag: data.frag,
							url: data.frag?.url,
						},
					)

					// For VOD mode, allow HLS.js to attempt automatic recovery
					// HLS.js will skip to the next available segment
					if (playbackMode === 'vod') {
						console.log(
							'[StreamPlayer] VOD segment missing - allowing HLS.js to skip to next available segment',
						)
						// Don't treat as fatal - HLS.js will handle recovery automatically
						// Continue playback with available segments
						return
					}

					// For live mode, also allow automatic recovery
					console.log(
						'[StreamPlayer] Live segment missing - allowing HLS.js to recover',
					)
					return
				}

				// Handle manifest/playlist loading errors specifically
				if (
					data.type === Hls.ErrorTypes.NETWORK_ERROR &&
					data.details === 'manifestLoadError'
				) {
					// For VOD mode, fall back to offline mode on manifest load error
					if (playbackMode === 'vod') {
						console.error(
							'[StreamPlayer] VOD manifest load failed, falling back to offline mode',
						)
						setPlaybackMode('offline')
						setRecordingAvailable(false)
						setError(
							'Recording playback failed. The recording manifest could not be loaded.',
						)
						return
					}

					// For live mode, implement retry logic
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
							// For VOD mode, fall back to offline mode on fatal network error
							if (playbackMode === 'vod') {
								console.error(
									'[StreamPlayer] Fatal VOD network error, falling back to offline mode',
								)
								setPlaybackMode('offline')
								setRecordingAvailable(false)
								setError(
									'Recording playback failed. The recording may be temporarily unavailable.',
								)
								break
							}

							// For live mode, attempt recovery
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
							// For VOD mode, fall back to offline mode on fatal media error
							if (playbackMode === 'vod') {
								console.error(
									'[StreamPlayer] Fatal VOD media error, falling back to offline mode',
								)
								setPlaybackMode('offline')
								setRecordingAvailable(false)
								setError(
									'Recording playback failed. The recording media could not be played.',
								)
								break
							}

							// For live mode, attempt recovery
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
							if (playbackMode === 'vod') {
								setPlaybackMode('offline')
								setRecordingAvailable(false)
								setError(
									'Recording playback failed. Please try refreshing the page.',
								)
							} else {
								setError(
									'Playback error occurred. Please try refreshing the page.',
								)
							}
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
	}, [streamDetail, streamMode, playbackMode])

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
					{playbackMode === 'live' || playbackMode === 'vod' ? (
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
										backgroundColor:
											playbackMode === 'live'
												? 'rgba(40, 167, 69, 0.9)' // Green for LIVE
												: 'rgba(52, 58, 64, 0.9)', // Dark gray/blue for RECORDED
										color: 'white',
										borderRadius: '4px',
										fontSize: '0.875rem',
										fontWeight: 'bold',
									}}
								>
									{playbackMode === 'live' ? 'LIVE' : 'RECORDED'}
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
					{playbackMode === 'live' && (
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
