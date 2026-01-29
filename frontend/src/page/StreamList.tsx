import { useEffect, useState } from 'preact/hooks'
import { useAuth } from '../context/Auth.js'
import type { StreamSummary } from '../types.js'
import { StreamDynamoDBClient } from '../utils/StreamDynamoDBClient.js'

const POLL_INTERVAL = 5000 // 5 seconds
const TABLE_NAME = DYNAMODB_TABLE_NAME
const CLOUDFRONT_DOMAIN = CLOUDFRONT_DOMAIN_NAME

export const StreamList = () => {
	const { awsConfig } = useAuth()
	const [streams, setStreams] = useState<StreamSummary[]>([])
	const [loading, setLoading] = useState(true)
	const [error, setError] = useState<string | null>(null)
	const [retryCount, setRetryCount] = useState(0)
	const [isRetrying, setIsRetrying] = useState(false)

	const fetchStreams = async () => {
		if (!awsConfig) {
			return
		}

		try {
			const client = new StreamDynamoDBClient(
				awsConfig,
				TABLE_NAME,
				CLOUDFRONT_DOMAIN,
			)
			const response = await client.listStreams()
			setStreams(response.streams)
			setError(null)
			setRetryCount(0)
			setIsRetrying(false)
		} catch (err) {
			console.error('[StreamList] Failed to fetch streams:', err)
			const errorMessage =
				err instanceof Error ? err.message : 'Failed to load streams'
			setError(errorMessage)

			// Implement exponential backoff for retries
			if (retryCount < 5) {
				const backoffDelay = Math.min(1000 * Math.pow(2, retryCount), 30000) // Max 30 seconds
				console.log(
					`[StreamList] Retrying in ${backoffDelay}ms (attempt ${retryCount + 1})`,
				)
				setIsRetrying(true)

				setTimeout(() => {
					setRetryCount((prev) => prev + 1)
					void fetchStreams()
				}, backoffDelay)
			} else {
				setIsRetrying(false)
			}
		} finally {
			setLoading(false)
		}
	}

	useEffect(() => {
		if (!awsConfig) {
			return
		}

		// Initial fetch
		void fetchStreams()

		// Set up polling
		const intervalId = setInterval(() => {
			void fetchStreams()
		}, POLL_INTERVAL)

		return () => clearInterval(intervalId)
	}, [awsConfig])

	const handleRetry = () => {
		setLoading(true)
		setError(null)
		setRetryCount(0)
		setIsRetrying(false)
		void fetchStreams()
	}

	const handleStreamClick = (port: number) => {
		window.location.href = `/stream/${port}`
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
			<main style={{ padding: '2rem' }}>
				<h1>Video Streams</h1>
				<div
					style={{
						display: 'grid',
						gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
						gap: '1rem',
						marginTop: '2rem',
					}}
				>
					{[...Array(10)].map((_, i) => (
						<div
							key={i}
							style={{
								border: '1px solid #ddd',
								borderRadius: '8px',
								padding: '1rem',
								backgroundColor: '#f5f5f5',
							}}
						>
							<div
								style={{
									width: '100%',
									height: '200px',
									backgroundColor: '#e0e0e0',
									borderRadius: '4px',
									marginBottom: '1rem',
								}}
							/>
							<div
								style={{
									height: '20px',
									backgroundColor: '#e0e0e0',
									borderRadius: '4px',
									marginBottom: '0.5rem',
								}}
							/>
							<div
								style={{
									height: '16px',
									backgroundColor: '#e0e0e0',
									borderRadius: '4px',
									width: '60%',
								}}
							/>
						</div>
					))}
				</div>
			</main>
		)
	}

	if (error !== null && error !== '') {
		return (
			<main style={{ padding: '2rem', textAlign: 'center' }}>
				<h1>Video Streams</h1>
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
						Connection Error
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

	if (streams.length === 0) {
		return (
			<main style={{ padding: '2rem', textAlign: 'center' }}>
				<h1>Video Streams</h1>
				<p style={{ marginTop: '2rem', color: '#666' }}>No streams available</p>
			</main>
		)
	}

	return (
		<main style={{ padding: '2rem' }}>
			<h1>Video Streams</h1>
			<div
				style={{
					display: 'grid',
					gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
					gap: '1rem',
					marginTop: '2rem',
				}}
			>
				{streams.map((stream) => (
					<div
						key={stream.port}
						onClick={() => handleStreamClick(stream.port)}
						style={{
							border: '1px solid #ddd',
							borderRadius: '8px',
							padding: '1rem',
							cursor: 'pointer',
							transition: 'transform 0.2s, box-shadow 0.2s',
						}}
						onMouseEnter={(e) => {
							e.currentTarget.style.transform = 'translateY(-4px)'
							e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)'
						}}
						onMouseLeave={(e) => {
							e.currentTarget.style.transform = 'translateY(0)'
							e.currentTarget.style.boxShadow = 'none'
						}}
					>
						<div style={{ position: 'relative' }}>
							<img
								src={stream.thumbnailUrl}
								alt={`Stream ${stream.port}`}
								style={{
									width: '100%',
									height: '200px',
									objectFit: 'cover',
									borderRadius: '4px',
									backgroundColor: '#f0f0f0',
								}}
								onError={(e) => {
									e.currentTarget.src =
										'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="300" height="200"%3E%3Crect fill="%23ddd" width="300" height="200"/%3E%3Ctext x="50%25" y="50%25" text-anchor="middle" dy=".3em" fill="%23999"%3ENo Image%3C/text%3E%3C/svg%3E'
								}}
							/>
							<span
								style={{
									position: 'absolute',
									top: '0.5rem',
									right: '0.5rem',
									padding: '0.25rem 0.5rem',
									borderRadius: '4px',
									fontSize: '0.75rem',
									fontWeight: 'bold',
									backgroundColor:
										stream.status === 'active' ? '#28a745' : '#6c757d',
									color: 'white',
								}}
							>
								{stream.status === 'active' ? 'ACTIVE' : 'OFFLINE'}
							</span>
						</div>
						<h3 style={{ marginTop: '1rem', marginBottom: '0.5rem' }}>
							Port {stream.port}
						</h3>
						<p style={{ fontSize: '0.875rem', color: '#666', margin: 0 }}>
							Last update: {new Date(stream.lastPacketTime).toLocaleString()}
						</p>
					</div>
				))}
			</div>
		</main>
	)
}
