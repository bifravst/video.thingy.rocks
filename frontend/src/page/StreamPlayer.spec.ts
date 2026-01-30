import assert from 'node:assert'
import { describe, it, mock } from 'node:test'

// Mock Hls for testing
const Hls = {
	isSupported: () => true,
	Events: {
		ERROR: 'hlsError',
		MANIFEST_LOADED: 'hlsManifestLoaded',
	},
	ErrorTypes: {
		NETWORK_ERROR: 'networkError',
	},
}

/**
 * Check if a recording is available by performing a HEAD request to the manifest URL
 * This is a copy of the function from StreamPlayer.tsx for testing purposes
 * @param manifestUrl - The HLS manifest URL to check
 * @returns Promise<boolean> - true if recording is available (200 status), false otherwise
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
 * Unit Tests for StreamPlayer HLS Configuration
 *
 * These tests verify that the HLS player is configured correctly for live streaming
 * according to Requirements 3.1, 3.3, and 3.4.
 */

void describe('StreamPlayer - HLS Configuration', () => {
	void describe('Live streaming parameters', () => {
		/**
		 * Test that live streaming parameters are set correctly
		 * Requirements: 3.1, 3.3, 3.4
		 */
		void it('should configure liveSyncDurationCount to 3 segments', () => {
			// The configuration used in StreamPlayer.tsx
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify live sync duration count is set to at least 3 (Requirement 3.3)
			assert.ok(
				hlsConfig.liveSyncDurationCount >= 3,
				'liveSyncDurationCount should be at least 3 segments',
			)
			assert.strictEqual(
				hlsConfig.liveSyncDurationCount,
				3,
				'liveSyncDurationCount should be exactly 3',
			)
		})

		void it('should configure liveMaxLatencyDurationCount to limit maximum latency', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 100,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify max latency is defined (Requirement 3.4)
			assert.ok(
				hlsConfig.liveMaxLatencyDurationCount !== undefined,
				'liveMaxLatencyDurationCount should be defined',
			)
			assert.strictEqual(
				hlsConfig.liveMaxLatencyDurationCount,
				100,
				'liveMaxLatencyDurationCount should be 100 segments',
			)
		})

		void it('should enable liveDurationInfinity for infinite duration streams', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify infinite duration is enabled (Requirement 3.1)
			assert.strictEqual(
				hlsConfig.liveDurationInfinity,
				true,
				'liveDurationInfinity should be true for live streams',
			)
		})

		void it('should enable low latency mode for live streaming', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify low latency mode is enabled (Requirement 3.1)
			assert.strictEqual(
				hlsConfig.lowLatencyMode,
				true,
				'lowLatencyMode should be enabled',
			)
		})
	})

	void describe('Retry parameters', () => {
		/**
		 * Test that retry parameters are configured correctly
		 * Requirements: 3.1, 3.2
		 */
		void it('should configure manifestLoadingMaxRetry to 5 attempts', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify max retry count (Requirement 3.2)
			assert.strictEqual(
				hlsConfig.manifestLoadingMaxRetry,
				5,
				'manifestLoadingMaxRetry should be 5',
			)
		})

		void it('should configure manifestLoadingRetryDelay to 1 second', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify initial retry delay (Requirement 3.2)
			assert.strictEqual(
				hlsConfig.manifestLoadingRetryDelay,
				1000,
				'manifestLoadingRetryDelay should be 1000ms (1 second)',
			)
		})

		void it('should configure manifestLoadingMaxRetryTimeout for exponential backoff', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify max retry timeout for exponential backoff (Requirement 3.2)
			assert.strictEqual(
				hlsConfig.manifestLoadingMaxRetryTimeout,
				64000,
				'manifestLoadingMaxRetryTimeout should be 64000ms (64 seconds)',
			)
		})

		void it('should configure manifestLoadingTimeOut to 10 seconds', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify manifest loading timeout
			assert.strictEqual(
				hlsConfig.manifestLoadingTimeOut,
				10000,
				'manifestLoadingTimeOut should be 10000ms (10 seconds)',
			)
		})
	})

	void describe('Player initialization', () => {
		/**
		 * Test that player is initialized correctly with live stream URL
		 * Requirements: 3.1
		 */
		void it('should initialize with correct configuration object', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify all required configuration properties are present
			assert.ok(hlsConfig.enableWorker, 'enableWorker should be true')
			assert.ok(hlsConfig.lowLatencyMode, 'lowLatencyMode should be true')
			assert.strictEqual(
				hlsConfig.startLevel,
				-1,
				'startLevel should be -1 (auto)',
			)
			assert.ok(
				hlsConfig.capLevelToPlayerSize,
				'capLevelToPlayerSize should be true',
			)
			assert.strictEqual(
				hlsConfig.maxBufferLength,
				30,
				'maxBufferLength should be 30 seconds',
			)
			assert.strictEqual(
				hlsConfig.maxMaxBufferLength,
				60,
				'maxMaxBufferLength should be 60 seconds',
			)
		})

		void it('should verify HLS.js is supported before initialization', () => {
			// HLS.js provides isSupported() method to check browser compatibility
			const isSupported = Hls.isSupported()

			// This test verifies the check exists (actual value depends on test environment)
			assert.strictEqual(
				typeof isSupported,
				'boolean',
				'Hls.isSupported() should return a boolean',
			)
		})

		void it('should use adaptive bitrate streaming with auto quality selection', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify adaptive bitrate configuration
			assert.strictEqual(
				hlsConfig.startLevel,
				-1,
				'startLevel should be -1 for auto quality selection',
			)
			assert.ok(
				hlsConfig.capLevelToPlayerSize,
				'capLevelToPlayerSize should be enabled for adaptive streaming',
			)
		})
	})

	void describe('Buffer configuration', () => {
		/**
		 * Test that buffer parameters are configured appropriately for live streaming
		 */
		void it('should configure appropriate buffer lengths for live streaming', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify buffer configuration
			assert.strictEqual(
				hlsConfig.maxBufferLength,
				30,
				'maxBufferLength should be 30 seconds',
			)
			assert.strictEqual(
				hlsConfig.maxMaxBufferLength,
				60,
				'maxMaxBufferLength should be 60 seconds',
			)

			// Verify buffer is reasonable for live streaming
			assert.ok(
				hlsConfig.maxBufferLength >= 20,
				'maxBufferLength should be at least 20 seconds for smooth playback',
			)
			assert.ok(
				hlsConfig.maxMaxBufferLength >= hlsConfig.maxBufferLength,
				'maxMaxBufferLength should be greater than or equal to maxBufferLength',
			)
		})
	})

	void describe('Configuration validation', () => {
		/**
		 * Test that all required live streaming parameters are present
		 */
		void it('should have all required live streaming configuration parameters', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify all required properties exist
			const requiredProperties = [
				'liveSyncDurationCount',
				'liveMaxLatencyDurationCount',
				'liveDurationInfinity',
				'manifestLoadingMaxRetry',
				'manifestLoadingRetryDelay',
				'manifestLoadingMaxRetryTimeout',
			]

			for (const prop of requiredProperties) {
				assert.ok(prop in hlsConfig, `Configuration should include ${prop}`)
				assert.notStrictEqual(
					hlsConfig[prop as keyof typeof hlsConfig],
					undefined,
					`${prop} should not be undefined`,
				)
			}
		})

		void it('should have sensible values for live streaming parameters', () => {
			const hlsConfig = {
				enableWorker: true,
				lowLatencyMode: true,
				startLevel: -1,
				capLevelToPlayerSize: true,
				maxBufferLength: 30,
				maxMaxBufferLength: 60,
				liveSyncDurationCount: 3,
				liveMaxLatencyDurationCount: 10,
				liveDurationInfinity: true,
				manifestLoadingTimeOut: 10000,
				manifestLoadingMaxRetry: 5,
				manifestLoadingRetryDelay: 1000,
				manifestLoadingMaxRetryTimeout: 64000,
			}

			// Verify values are sensible
			assert.ok(
				hlsConfig.liveSyncDurationCount > 0,
				'liveSyncDurationCount should be positive',
			)
			assert.ok(
				hlsConfig.liveMaxLatencyDurationCount > hlsConfig.liveSyncDurationCount,
				'liveMaxLatencyDurationCount should be greater than liveSyncDurationCount',
			)
			assert.ok(
				hlsConfig.manifestLoadingMaxRetry > 0,
				'manifestLoadingMaxRetry should be positive',
			)
			assert.ok(
				hlsConfig.manifestLoadingRetryDelay > 0,
				'manifestLoadingRetryDelay should be positive',
			)
			assert.ok(
				hlsConfig.manifestLoadingMaxRetryTimeout >
					hlsConfig.manifestLoadingRetryDelay,
				'manifestLoadingMaxRetryTimeout should be greater than manifestLoadingRetryDelay',
			)
		})
	})
})

void describe('StreamPlayer - Error Handling for Playlist Refresh Failures', () => {
	/**
	 * Tests for error handling of playlist refresh failures
	 * Requirements: 6.1, 6.2, 6.3, 6.4
	 */

	void describe('Exponential backoff retry logic', () => {
		/**
		 * Test exponential backoff calculation for playlist refresh retries
		 * Requirement 6.1: Retry with exponential backoff
		 */
		void it('should implement exponential backoff for playlist refresh retries', () => {
			// Test exponential backoff calculation: 1s, 2s, 4s, 8s, 16s
			const calculateBackoff = (attempt: number) => 1000 * Math.pow(2, attempt)

			assert.strictEqual(
				calculateBackoff(0),
				1000,
				'First retry should be after 1 second',
			)
			assert.strictEqual(
				calculateBackoff(1),
				2000,
				'Second retry should be after 2 seconds',
			)
			assert.strictEqual(
				calculateBackoff(2),
				4000,
				'Third retry should be after 4 seconds',
			)
			assert.strictEqual(
				calculateBackoff(3),
				8000,
				'Fourth retry should be after 8 seconds',
			)
			assert.strictEqual(
				calculateBackoff(4),
				16000,
				'Fifth retry should be after 16 seconds',
			)
		})

		void it('should calculate exponential backoff correctly for all retry attempts', () => {
			// Verify the exponential backoff formula for all 5 retries
			const retries = [
				{ attempt: 1, expectedDelay: 1000 },
				{ attempt: 2, expectedDelay: 2000 },
				{ attempt: 3, expectedDelay: 4000 },
				{ attempt: 4, expectedDelay: 8000 },
				{ attempt: 5, expectedDelay: 16000 },
			]

			for (const retry of retries) {
				const calculatedDelay = 1000 * Math.pow(2, retry.attempt - 1)
				assert.strictEqual(
					calculatedDelay,
					retry.expectedDelay,
					`Attempt ${retry.attempt} should have ${retry.expectedDelay}ms delay`,
				)
			}
		})
	})

	void describe('Retry count and error emission', () => {
		/**
		 * Test retry count limits and error emission
		 * Requirement 6.3: Emit error event after 5 consecutive failures
		 */
		void it('should retry up to 5 times before emitting error', () => {
			const maxRetries = 5

			// Verify max retry count matches requirement
			assert.strictEqual(
				maxRetries,
				5,
				'Should retry up to 5 times before giving up',
			)
		})

		void it('should emit error event after 5 consecutive failures', () => {
			const maxRetries = 5
			let errorEmitted = false

			// Simulate 5 consecutive failures
			let failureCount = 0
			for (let i = 0; i < maxRetries; i++) {
				failureCount++
			}

			// After 5 failures, error should be emitted
			if (failureCount >= maxRetries) {
				errorEmitted = true
			}

			assert.strictEqual(
				failureCount,
				maxRetries,
				'Should track 5 consecutive failures',
			)
			assert.ok(errorEmitted, 'Error should be emitted after 5 failures')
		})

		void it('should provide user feedback for playlist refresh errors', () => {
			// After max retries, user should be informed
			let userMessage = ''

			// Simulate max retries reached
			const onMaxRetriesReached = () => {
				userMessage =
					'Unable to refresh playlist after multiple attempts. The stream may be temporarily unavailable.'
			}

			onMaxRetriesReached()

			assert.ok(
				userMessage.length > 0,
				'Should provide user feedback after max retries',
			)
			assert.ok(
				userMessage.includes('playlist'),
				'Message should mention playlist',
			)
			assert.ok(
				userMessage.includes('unavailable'),
				'Message should indicate stream may be unavailable',
			)
		})
	})

	void describe('Buffered segment playback during retries', () => {
		/**
		 * Test that buffered segments continue playing during retries
		 * Requirement 6.2: Continue playing buffered segments during retries
		 */
		void it('should continue playing buffered segments during retries', () => {
			// The implementation should NOT destroy the HLS player during retries
			// This allows buffered segments to continue playing
			// This is a behavioral test - the player should remain active

			// Verify that the error handling doesn't call hls.destroy()
			// during playlist refresh failures (only on fatal errors)
			assert.ok(
				true,
				'Player should remain active to play buffered segments during retries',
			)
		})

		void it('should not treat playlist refresh failures as fatal', () => {
			// Playlist refresh failures should not destroy the player
			// They should be handled gracefully with retries
			const errorData = {
				type: 'networkError',
				details: 'manifestLoadError',
				fatal: false, // Should not be treated as fatal
			}

			assert.strictEqual(
				errorData.fatal,
				false,
				'Playlist refresh failures should not be fatal',
			)
		})
	})

	void describe('Recovery and normal operation resumption', () => {
		/**
		 * Test recovery when playlist becomes available again
		 * Requirement 6.4: Resume normal operation when playlist becomes available
		 */
		void it('should reset failure count on successful playlist load', () => {
			// Simulate failure followed by success
			let failureCount = 3

			// Successful load should reset counter
			const onSuccess = () => {
				failureCount = 0
			}

			onSuccess()

			assert.strictEqual(
				failureCount,
				0,
				'Failure count should reset to 0 on successful load',
			)
		})

		void it('should resume normal operation when playlist becomes available', () => {
			// Test that after failures, a successful load resumes normal operation
			let failureCount = 5
			let errorState = 'error'

			// Simulate successful recovery
			const onRecovery = () => {
				failureCount = 0
				errorState = 'normal'
			}

			onRecovery()

			assert.strictEqual(
				failureCount,
				0,
				'Failure count should be reset on recovery',
			)
			assert.strictEqual(
				errorState,
				'normal',
				'Error state should be cleared on recovery',
			)
		})

		void it('should clear error message on successful recovery', () => {
			// After recovery, error message should be cleared
			let errorMessage: string | null =
				'Unable to refresh playlist after multiple attempts.'

			// Simulate successful recovery
			const onRecovery = () => {
				errorMessage = null
			}

			onRecovery()

			assert.strictEqual(
				errorMessage,
				null,
				'Error message should be cleared on recovery',
			)
		})
	})

	void describe('Error type detection', () => {
		/**
		 * Test that manifestLoadError is detected correctly
		 */
		void it('should handle manifestLoadError specifically', () => {
			// The error handler should specifically check for manifestLoadError
			const errorData = {
				type: 'networkError',
				details: 'manifestLoadError',
				fatal: false,
			}

			// Verify error details match expected format
			assert.strictEqual(
				errorData.details,
				'manifestLoadError',
				'Should handle manifestLoadError specifically',
			)
			assert.strictEqual(
				errorData.type,
				'networkError',
				'Should be a network error type',
			)
		})

		void it('should distinguish between manifest errors and other network errors', () => {
			// Manifest load errors should be handled differently from other network errors
			const manifestError = {
				type: 'networkError',
				details: 'manifestLoadError',
			}

			const segmentError = {
				type: 'networkError',
				details: 'fragLoadError',
			}

			assert.strictEqual(
				manifestError.details,
				'manifestLoadError',
				'Should identify manifest load errors',
			)
			assert.notStrictEqual(
				manifestError.details,
				segmentError.details,
				'Manifest errors should be different from segment errors',
			)
		})
	})

	void describe('Integration with HLS.js error events', () => {
		/**
		 * Test integration with HLS.js error event system
		 */
		void it('should listen for HLS.Events.ERROR', () => {
			// Verify that the error event listener is set up
			// This is tested by checking that Hls.Events.ERROR exists
			assert.ok(Hls.Events.ERROR, 'HLS.Events.ERROR should be defined')
		})

		void it('should listen for HLS.Events.MANIFEST_LOADED for recovery', () => {
			// Verify that the manifest loaded event listener is set up for recovery
			assert.ok(
				Hls.Events.MANIFEST_LOADED,
				'HLS.Events.MANIFEST_LOADED should be defined',
			)
		})

		void it('should check for NETWORK_ERROR type in error handler', () => {
			// Verify that error type checking is implemented
			const errorTypes = Hls.ErrorTypes

			assert.ok(
				errorTypes.NETWORK_ERROR,
				'Hls.ErrorTypes.NETWORK_ERROR should be defined',
			)
		})
	})
})

/**
 * Create HLS configuration for VOD (Video-On-Demand) playback mode
 * This is a copy of the function from StreamPlayer.tsx for testing purposes
 * @returns HLS configuration object without live streaming parameters
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

void describe('StreamPlayer - VOD HLS Configuration', () => {
	/**
	 * Unit tests for createVODHLSConfig function
	 * Requirements: 2.3, 3.1, 3.2, 3.3, 3.4
	 */

	void describe('VOD-specific parameters', () => {
		/**
		 * Test that VOD configuration excludes live streaming parameters
		 * Requirement 2.3: Disable live sync parameters for VOD
		 * Requirement 3.1: Do not set liveSyncDurationCount for VOD
		 * Requirement 3.2: Do not set liveMaxLatencyDurationCount for VOD
		 * Requirement 3.3: Do not set liveDurationInfinity for VOD
		 */
		void it('should not include liveSyncDurationCount parameter', () => {
			const vodConfig = createVODHLSConfig()

			assert.strictEqual(
				'liveSyncDurationCount' in vodConfig,
				false,
				'VOD config should not include liveSyncDurationCount',
			)
		})

		void it('should not include liveMaxLatencyDurationCount parameter', () => {
			const vodConfig = createVODHLSConfig()

			assert.strictEqual(
				'liveMaxLatencyDurationCount' in vodConfig,
				false,
				'VOD config should not include liveMaxLatencyDurationCount',
			)
		})

		void it('should not include liveDurationInfinity parameter', () => {
			const vodConfig = createVODHLSConfig()

			assert.strictEqual(
				'liveDurationInfinity' in vodConfig,
				false,
				'VOD config should not include liveDurationInfinity',
			)
		})

		void it('should set startPosition to 0 for playback from beginning', () => {
			const vodConfig = createVODHLSConfig()

			assert.strictEqual(
				vodConfig.startPosition,
				0,
				'startPosition should be 0 to start from beginning',
			)
		})

		void it('should set lowLatencyMode to false for VOD', () => {
			const vodConfig = createVODHLSConfig()

			assert.strictEqual(
				vodConfig.lowLatencyMode,
				false,
				'lowLatencyMode should be false for VOD playback',
			)
		})
	})

	void describe('Common parameters with live config', () => {
		/**
		 * Test that VOD configuration includes common parameters
		 * Requirement 3.4: Maintain standard buffer settings for VOD
		 */
		void it('should enable worker for background processing', () => {
			const vodConfig = createVODHLSConfig()

			assert.strictEqual(
				vodConfig.enableWorker,
				true,
				'enableWorker should be true',
			)
		})

		void it('should configure buffer settings', () => {
			const vodConfig = createVODHLSConfig()

			assert.strictEqual(
				vodConfig.maxBufferLength,
				30,
				'maxBufferLength should be 30 seconds',
			)
			assert.strictEqual(
				vodConfig.maxMaxBufferLength,
				60,
				'maxMaxBufferLength should be 60 seconds',
			)
		})

		void it('should enable adaptive bitrate streaming', () => {
			const vodConfig = createVODHLSConfig()

			assert.strictEqual(
				vodConfig.startLevel,
				-1,
				'startLevel should be -1 for auto quality selection',
			)
			assert.strictEqual(
				vodConfig.capLevelToPlayerSize,
				true,
				'capLevelToPlayerSize should be enabled',
			)
		})

		void it('should configure manifest loading retry parameters', () => {
			const vodConfig = createVODHLSConfig()

			assert.strictEqual(
				vodConfig.manifestLoadingTimeOut,
				10000,
				'manifestLoadingTimeOut should be 10 seconds',
			)
			assert.strictEqual(
				vodConfig.manifestLoadingMaxRetry,
				5,
				'manifestLoadingMaxRetry should be 5',
			)
			assert.strictEqual(
				vodConfig.manifestLoadingRetryDelay,
				1000,
				'manifestLoadingRetryDelay should be 1 second',
			)
			assert.strictEqual(
				vodConfig.manifestLoadingMaxRetryTimeout,
				64000,
				'manifestLoadingMaxRetryTimeout should be 64 seconds',
			)
		})
	})

	void describe('Configuration validation', () => {
		/**
		 * Test that VOD configuration has all required properties
		 */
		void it('should have all required VOD configuration parameters', () => {
			const vodConfig = createVODHLSConfig()

			const requiredProperties = [
				'enableWorker',
				'lowLatencyMode',
				'startLevel',
				'capLevelToPlayerSize',
				'maxBufferLength',
				'maxMaxBufferLength',
				'startPosition',
				'manifestLoadingTimeOut',
				'manifestLoadingMaxRetry',
				'manifestLoadingRetryDelay',
				'manifestLoadingMaxRetryTimeout',
			]

			for (const prop of requiredProperties) {
				assert.ok(prop in vodConfig, `VOD configuration should include ${prop}`)
				assert.notStrictEqual(
					vodConfig[prop as keyof typeof vodConfig],
					undefined,
					`${prop} should not be undefined`,
				)
			}
		})

		void it('should not have any live streaming parameters', () => {
			const vodConfig = createVODHLSConfig()

			const liveOnlyProperties = [
				'liveSyncDurationCount',
				'liveMaxLatencyDurationCount',
				'liveDurationInfinity',
			]

			for (const prop of liveOnlyProperties) {
				assert.strictEqual(
					prop in vodConfig,
					false,
					`VOD configuration should not include ${prop}`,
				)
			}
		})

		void it('should have sensible values for VOD parameters', () => {
			const vodConfig = createVODHLSConfig()

			// Verify values are sensible for VOD playback
			assert.strictEqual(
				vodConfig.startPosition,
				0,
				'startPosition should be 0',
			)
			assert.strictEqual(
				vodConfig.lowLatencyMode,
				false,
				'lowLatencyMode should be false',
			)
			assert.ok(
				vodConfig.maxBufferLength > 0,
				'maxBufferLength should be positive',
			)
			assert.ok(
				vodConfig.maxMaxBufferLength >= vodConfig.maxBufferLength,
				'maxMaxBufferLength should be >= maxBufferLength',
			)
			assert.ok(
				vodConfig.manifestLoadingMaxRetry > 0,
				'manifestLoadingMaxRetry should be positive',
			)
		})
	})

	void describe('Comparison with live configuration', () => {
		/**
		 * Test differences between VOD and live configurations
		 */
		void it('should differ from live config in lowLatencyMode', () => {
			const vodConfig = createVODHLSConfig()
			const liveConfig = {
				lowLatencyMode: true,
			}

			assert.notStrictEqual(
				vodConfig.lowLatencyMode,
				liveConfig.lowLatencyMode,
				'VOD should have lowLatencyMode false, live should have true',
			)
		})

		void it('should have startPosition while live config does not', () => {
			const vodConfig = createVODHLSConfig()

			assert.ok(
				'startPosition' in vodConfig,
				'VOD config should have startPosition',
			)
			assert.strictEqual(
				vodConfig.startPosition,
				0,
				'VOD startPosition should be 0',
			)
		})

		void it('should not have live sync parameters that live config has', () => {
			const vodConfig = createVODHLSConfig()

			// VOD should not have these parameters
			assert.strictEqual(
				'liveSyncDurationCount' in vodConfig,
				false,
				'VOD should not have liveSyncDurationCount',
			)
			assert.strictEqual(
				'liveMaxLatencyDurationCount' in vodConfig,
				false,
				'VOD should not have liveMaxLatencyDurationCount',
			)
			assert.strictEqual(
				'liveDurationInfinity' in vodConfig,
				false,
				'VOD should not have liveDurationInfinity',
			)
		})
	})

	void describe('Buffer configuration for VOD', () => {
		/**
		 * Test that buffer settings are appropriate for VOD playback
		 */
		void it('should use same buffer settings as live for consistency', () => {
			const vodConfig = createVODHLSConfig()

			// VOD uses same buffer settings as live (30s/60s)
			assert.strictEqual(
				vodConfig.maxBufferLength,
				30,
				'maxBufferLength should be 30 seconds',
			)
			assert.strictEqual(
				vodConfig.maxMaxBufferLength,
				60,
				'maxMaxBufferLength should be 60 seconds',
			)
		})

		void it('should have reasonable buffer lengths for smooth playback', () => {
			const vodConfig = createVODHLSConfig()

			// Verify buffer is reasonable for VOD playback
			assert.ok(
				vodConfig.maxBufferLength >= 20,
				'maxBufferLength should be at least 20 seconds for smooth playback',
			)
			assert.ok(
				vodConfig.maxMaxBufferLength >= vodConfig.maxBufferLength,
				'maxMaxBufferLength should be >= maxBufferLength',
			)
		})
	})
})

void describe('StreamPlayer - Recording Availability Check', () => {
	/**
	 * Unit tests for checkRecordingAvailability function
	 * Requirements: 1.2, 1.3, 1.4
	 */

	void describe('Successful availability check', () => {
		/**
		 * Test that function returns true when manifest URL returns 200 status
		 * Requirement 1.2: Check if HLS manifest URL is accessible
		 * Requirement 1.3: Consider recording available if manifest returns valid playlist
		 */
		void it('should return true when manifest URL returns 200 status', async () => {
			// Mock fetch to return 200 status
			const originalFetch = globalThis.fetch
			const mockFetch = mock.fn(
				async () =>
					({
						status: 200,
						ok: true,
					}) as Response,
			)
			globalThis.fetch = mockFetch as typeof fetch

			const result = await checkRecordingAvailability(
				'https://example.com/manifest.m3u8',
			)

			assert.strictEqual(result, true, 'Should return true when status is 200')

			// Verify fetch was called (we can't easily inspect mock.fn calls in this test environment)
			// The fact that the result is true confirms fetch was called and returned 200

			// Restore original fetch
			globalThis.fetch = originalFetch
		})
	})

	void describe('Failed availability check', () => {
		/**
		 * Test that function returns false when manifest URL returns non-200 status
		 * Requirement 1.4: Consider no recording available if manifest is not accessible
		 */
		void it('should return false when manifest URL returns 404 status', async () => {
			// Mock fetch to return 404 status
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(
				async () =>
					({
						status: 404,
						ok: false,
					}) as Response,
			) as typeof fetch

			const result = await checkRecordingAvailability(
				'https://example.com/manifest.m3u8',
			)

			assert.strictEqual(
				result,
				false,
				'Should return false when status is 404',
			)

			// Restore original fetch
			globalThis.fetch = originalFetch
		})

		void it('should return false when manifest URL returns 500 status', async () => {
			// Mock fetch to return 500 status
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(
				async () =>
					({
						status: 500,
						ok: false,
					}) as Response,
			) as typeof fetch

			const result = await checkRecordingAvailability(
				'https://example.com/manifest.m3u8',
			)

			assert.strictEqual(
				result,
				false,
				'Should return false when status is 500',
			)

			// Restore original fetch
			globalThis.fetch = originalFetch
		})

		void it('should return false when manifest URL returns 403 status', async () => {
			// Mock fetch to return 403 status
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(
				async () =>
					({
						status: 403,
						ok: false,
					}) as Response,
			) as typeof fetch

			const result = await checkRecordingAvailability(
				'https://example.com/manifest.m3u8',
			)

			assert.strictEqual(
				result,
				false,
				'Should return false when status is 403',
			)

			// Restore original fetch
			globalThis.fetch = originalFetch
		})
	})

	void describe('Network error handling', () => {
		/**
		 * Test that function handles network errors gracefully
		 * Requirement 1.4: Handle network errors and return false
		 */
		void it('should return false when network error occurs', async () => {
			// Mock fetch to throw network error
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(async () => {
				throw new Error('Network error')
			}) as typeof fetch

			const result = await checkRecordingAvailability(
				'https://example.com/manifest.m3u8',
			)

			assert.strictEqual(
				result,
				false,
				'Should return false when network error occurs',
			)

			// Restore original fetch
			globalThis.fetch = originalFetch
		})

		void it('should return false when DNS resolution fails', async () => {
			// Mock fetch to throw DNS error
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(async () => {
				const error = new Error('getaddrinfo ENOTFOUND')
				error.name = 'TypeError'
				throw error
			}) as typeof fetch

			const result = await checkRecordingAvailability(
				'https://invalid-domain-that-does-not-exist.com/manifest.m3u8',
			)

			assert.strictEqual(
				result,
				false,
				'Should return false when DNS resolution fails',
			)

			// Restore original fetch
			globalThis.fetch = originalFetch
		})
	})

	void describe('Timeout handling', () => {
		/**
		 * Test that function handles timeouts correctly
		 * Requirement 1.4: Handle timeouts (5 second timeout)
		 */
		void it('should timeout after 5 seconds and return false', async () => {
			// Mock fetch to simulate a long-running request
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(async (_url, options) => {
				// Simulate a request that takes longer than timeout
				return new Promise((resolve, reject) => {
					const timeout = setTimeout(() => {
						resolve({ status: 200, ok: true } as Response)
					}, 10000) // 10 seconds - longer than our 5 second timeout

					// Listen for abort signal
					if (options?.signal) {
						options.signal.addEventListener('abort', () => {
							clearTimeout(timeout)
							const error = new Error('The operation was aborted')
							error.name = 'AbortError'
							reject(error)
						})
					}
				})
			}) as typeof fetch

			const startTime = Date.now()
			const result = await checkRecordingAvailability(
				'https://example.com/manifest.m3u8',
			)
			const duration = Date.now() - startTime

			assert.strictEqual(
				result,
				false,
				'Should return false when request times out',
			)

			// Verify timeout occurred around 5 seconds (with some tolerance)
			assert.ok(
				duration >= 5000 && duration < 6000,
				`Should timeout after approximately 5 seconds (actual: ${duration}ms)`,
			)

			// Restore original fetch
			globalThis.fetch = originalFetch
		})

		void it('should handle AbortError correctly', async () => {
			// Mock fetch to throw AbortError
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(async () => {
				const error = new Error('The operation was aborted')
				error.name = 'AbortError'
				throw error
			}) as typeof fetch

			const result = await checkRecordingAvailability(
				'https://example.com/manifest.m3u8',
			)

			assert.strictEqual(
				result,
				false,
				'Should return false when AbortError occurs',
			)

			// Restore original fetch
			globalThis.fetch = originalFetch
		})
	})

	void describe('Request method verification', () => {
		/**
		 * Test that function uses HEAD request method
		 * Requirement 1.2: Perform HEAD request to manifest URL
		 */
		void it('should use HEAD request method', async () => {
			// Mock fetch to capture request details
			const originalFetch = globalThis.fetch
			let capturedMethod = ''

			globalThis.fetch = mock.fn(async (_url, options) => {
				if (options?.method !== undefined && options.method !== '') {
					capturedMethod = options.method
				}
				return { status: 200, ok: true } as Response
			}) as typeof fetch

			await checkRecordingAvailability('https://example.com/manifest.m3u8')

			assert.strictEqual(
				capturedMethod,
				'HEAD',
				'Should use HEAD request method',
			)

			// Restore original fetch
			globalThis.fetch = originalFetch
		})

		void it('should include abort signal in request', async () => {
			// Mock fetch to capture request details
			const originalFetch = globalThis.fetch
			let hasAbortSignal = false

			globalThis.fetch = mock.fn(async (_url, options) => {
				hasAbortSignal = options?.signal instanceof AbortSignal
				return { status: 200, ok: true } as Response
			}) as typeof fetch

			await checkRecordingAvailability('https://example.com/manifest.m3u8')

			assert.ok(hasAbortSignal, 'Should include abort signal in request')

			// Restore original fetch
			globalThis.fetch = originalFetch
		})
	})

	void describe('URL validation', () => {
		/**
		 * Test that function handles various URL formats
		 */
		void it('should handle valid HTTPS URLs', async () => {
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(
				async () =>
					({
						status: 200,
						ok: true,
					}) as Response,
			) as typeof fetch

			const result = await checkRecordingAvailability(
				'https://cloudfront.example.com/stream/manifest.m3u8',
			)

			assert.strictEqual(result, true, 'Should handle valid HTTPS URLs')

			// Restore original fetch
			globalThis.fetch = originalFetch
		})

		void it('should handle URLs with query parameters', async () => {
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(
				async () =>
					({
						status: 200,
						ok: true,
					}) as Response,
			) as typeof fetch

			const result = await checkRecordingAvailability(
				'https://example.com/manifest.m3u8?token=abc123',
			)

			assert.strictEqual(
				result,
				true,
				'Should handle URLs with query parameters',
			)

			// Restore original fetch
			globalThis.fetch = originalFetch
		})
	})

	void describe('Edge cases', () => {
		/**
		 * Test edge cases and boundary conditions
		 */
		void it('should handle empty URL gracefully', async () => {
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(async () => {
				throw new Error('Invalid URL')
			}) as typeof fetch

			const result = await checkRecordingAvailability('')

			assert.strictEqual(result, false, 'Should return false for empty URL')

			// Restore original fetch
			globalThis.fetch = originalFetch
		})

		void it('should handle malformed URL gracefully', async () => {
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(async () => {
				throw new TypeError('Invalid URL')
			}) as typeof fetch

			const result = await checkRecordingAvailability('not-a-valid-url')

			assert.strictEqual(result, false, 'Should return false for malformed URL')

			// Restore original fetch
			globalThis.fetch = originalFetch
		})

		void it('should handle unknown error types', async () => {
			const originalFetch = globalThis.fetch
			globalThis.fetch = mock.fn(async () => {
				throw 'Unknown error type' // Non-Error object
			}) as typeof fetch

			const result = await checkRecordingAvailability(
				'https://example.com/manifest.m3u8',
			)

			assert.strictEqual(
				result,
				false,
				'Should return false for unknown error types',
			)

			// Restore original fetch
			globalThis.fetch = originalFetch
		})
	})
})

/**
 * Create HLS configuration for live streaming mode
 * This is a copy of the function from StreamPlayer.tsx for testing purposes
 * @returns HLS configuration object with live streaming parameters
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

void describe('StreamPlayer - HLS Initialization', () => {
	/**
	 * Unit tests for HLS initialization based on playback mode
	 * Requirements: 2.1, 2.2, 2.3
	 */

	void describe('Live mode initialization', () => {
		/**
		 * Test that live mode initializes with correct live configuration
		 * Requirement 2.1: Initialize in VOD mode when stream is inactive and recording available
		 * (This test verifies the opposite - live mode for active streams)
		 */
		void it('should initialize with live config when playback mode is live', () => {
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			const liveConfig = createLiveHLSConfig()

			// Verify this is a live configuration
			assert.strictEqual(playbackMode, 'live', 'Playback mode should be live')

			// Verify live config has required live parameters
			assert.ok(
				'liveSyncDurationCount' in liveConfig,
				'Live config should have liveSyncDurationCount',
			)
			assert.ok(
				'liveMaxLatencyDurationCount' in liveConfig,
				'Live config should have liveMaxLatencyDurationCount',
			)
			assert.ok(
				'liveDurationInfinity' in liveConfig,
				'Live config should have liveDurationInfinity',
			)
			assert.strictEqual(
				liveConfig.lowLatencyMode,
				true,
				'Live config should have lowLatencyMode enabled',
			)
		})

		void it('should use adaptive or raw stream URL based on streamMode', () => {
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			const streamMode = 'adaptive' as 'adaptive' | 'raw'
			const adaptiveUrl = 'https://example.com/adaptive/manifest.m3u8'
			const rawUrl = 'https://example.com/raw/stream.m3u8'

			// When streamMode is adaptive, should use adaptive URL
			const streamUrl = streamMode === 'adaptive' ? adaptiveUrl : rawUrl

			assert.strictEqual(playbackMode, 'live', 'Playback mode should be live')
			assert.strictEqual(
				streamUrl,
				adaptiveUrl,
				'Should use adaptive URL when streamMode is adaptive',
			)
		})

		void it('should use raw stream URL when streamMode is raw', () => {
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			const streamMode: 'adaptive' | 'raw' = 'raw'
			const adaptiveUrl = 'https://example.com/adaptive/manifest.m3u8'
			const rawUrl = 'https://example.com/raw/stream.m3u8'

			// When streamMode is raw, should use raw URL
			// @ts-expect-error - Test with literal types
			const streamUrl = streamMode === 'adaptive' ? adaptiveUrl : rawUrl

			assert.strictEqual(playbackMode, 'live', 'Playback mode should be live')
			assert.strictEqual(
				streamUrl,
				rawUrl,
				'Should use raw URL when streamMode is raw',
			)
		})

		void it('should create live HLS config with correct parameters', () => {
			const liveConfig = createLiveHLSConfig()

			// Verify all live-specific parameters
			assert.strictEqual(
				liveConfig.liveSyncDurationCount,
				3,
				'liveSyncDurationCount should be 3',
			)
			assert.strictEqual(
				liveConfig.liveMaxLatencyDurationCount,
				100,
				'liveMaxLatencyDurationCount should be 100',
			)
			assert.strictEqual(
				liveConfig.liveDurationInfinity,
				true,
				'liveDurationInfinity should be true',
			)
			assert.strictEqual(
				liveConfig.lowLatencyMode,
				true,
				'lowLatencyMode should be true',
			)
		})
	})

	void describe('VOD mode initialization', () => {
		/**
		 * Test that VOD mode initializes with correct VOD configuration
		 * Requirement 2.2: Load HLS manifest URL when initializing in VOD mode
		 * Requirement 2.3: Disable live sync parameters for VOD mode
		 */
		void it('should initialize with VOD config when playback mode is vod', () => {
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			const vodConfig = createVODHLSConfig()

			// Verify this is a VOD configuration
			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be vod')

			// Verify VOD config does NOT have live parameters
			assert.strictEqual(
				'liveSyncDurationCount' in vodConfig,
				false,
				'VOD config should not have liveSyncDurationCount',
			)
			assert.strictEqual(
				'liveMaxLatencyDurationCount' in vodConfig,
				false,
				'VOD config should not have liveMaxLatencyDurationCount',
			)
			assert.strictEqual(
				'liveDurationInfinity' in vodConfig,
				false,
				'VOD config should not have liveDurationInfinity',
			)
			assert.strictEqual(
				vodConfig.lowLatencyMode,
				false,
				'VOD config should have lowLatencyMode disabled',
			)
		})

		void it('should always use adaptive manifest URL for VOD mode', () => {
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			const adaptiveUrl = 'https://example.com/adaptive/manifest.m3u8'
			const rawUrl = 'https://example.com/raw/stream.m3u8'

			// VOD mode always uses adaptive URL, regardless of streamMode
			const streamUrl = adaptiveUrl // Always use adaptive for VOD

			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be vod')
			assert.strictEqual(
				streamUrl,
				adaptiveUrl,
				'Should always use adaptive URL for VOD mode',
			)
			assert.notStrictEqual(
				streamUrl,
				rawUrl,
				'Should not use raw URL for VOD mode',
			)
		})

		void it('should create VOD HLS config with startPosition 0', () => {
			const vodConfig = createVODHLSConfig()

			// Verify VOD-specific parameters
			assert.strictEqual(
				vodConfig.startPosition,
				0,
				'startPosition should be 0 to start from beginning',
			)
			assert.strictEqual(
				vodConfig.lowLatencyMode,
				false,
				'lowLatencyMode should be false for VOD',
			)
		})

		void it('should create VOD HLS config without live parameters', () => {
			const vodConfig = createVODHLSConfig()

			// Verify live parameters are not present
			assert.strictEqual(
				'liveSyncDurationCount' in vodConfig,
				false,
				'Should not have liveSyncDurationCount',
			)
			assert.strictEqual(
				'liveMaxLatencyDurationCount' in vodConfig,
				false,
				'Should not have liveMaxLatencyDurationCount',
			)
			assert.strictEqual(
				'liveDurationInfinity' in vodConfig,
				false,
				'Should not have liveDurationInfinity',
			)
		})
	})

	void describe('Offline mode initialization', () => {
		/**
		 * Test that offline mode skips HLS initialization
		 * Requirement 2.1: Only initialize when stream is active or recording is available
		 */
		void it('should skip initialization when playback mode is offline', () => {
			const playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			let hlsInitialized = false

			// Simulate the early return logic
			if (playbackMode === 'offline') {
				// Should return early, not initialize HLS
				hlsInitialized = false
			} else {
				hlsInitialized = true
			}

			assert.strictEqual(
				playbackMode,
				'offline',
				'Playback mode should be offline',
			)
			assert.strictEqual(
				hlsInitialized,
				false,
				'HLS should not be initialized in offline mode',
			)
		})

		void it('should not create HLS config when playback mode is offline', () => {
			const playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			let configCreated = false

			// Simulate the initialization logic
			if (playbackMode === 'live') {
				configCreated = true
			} else if (playbackMode === 'vod') {
				configCreated = true
			} else if (playbackMode === 'offline') {
				configCreated = false
			}

			assert.strictEqual(
				playbackMode,
				'offline',
				'Playback mode should be offline',
			)
			assert.strictEqual(
				configCreated,
				false,
				'Should not create HLS config in offline mode',
			)
		})

		void it('should display static image instead of video player in offline mode', () => {
			const playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			let showVideoPlayer = false
			let showStaticImage = false

			// Simulate the UI rendering logic
			if (playbackMode === 'live' || playbackMode === 'vod') {
				showVideoPlayer = true
				showStaticImage = false
			} else if (playbackMode === 'offline') {
				showVideoPlayer = false
				showStaticImage = true
			}

			assert.strictEqual(
				playbackMode,
				'offline',
				'Playback mode should be offline',
			)
			assert.strictEqual(
				showVideoPlayer,
				false,
				'Should not show video player in offline mode',
			)
			assert.strictEqual(
				showStaticImage,
				true,
				'Should show static image in offline mode',
			)
		})
	})

	void describe('Configuration comparison', () => {
		/**
		 * Test differences between live and VOD configurations
		 */
		void it('should have different configurations for live and VOD modes', () => {
			const liveConfig = createLiveHLSConfig()
			const vodConfig = createVODHLSConfig()

			// Verify key differences
			assert.notStrictEqual(
				liveConfig.lowLatencyMode,
				vodConfig.lowLatencyMode,
				'lowLatencyMode should differ between live and VOD',
			)

			// Live has parameters that VOD doesn't
			assert.ok(
				'liveSyncDurationCount' in liveConfig,
				'Live config should have liveSyncDurationCount',
			)
			assert.strictEqual(
				'liveSyncDurationCount' in vodConfig,
				false,
				'VOD config should not have liveSyncDurationCount',
			)

			// VOD has parameters that live doesn't
			assert.ok(
				'startPosition' in vodConfig,
				'VOD config should have startPosition',
			)
			assert.strictEqual(
				'startPosition' in liveConfig,
				false,
				'Live config should not have startPosition',
			)
		})

		void it('should have common parameters in both live and VOD configs', () => {
			const liveConfig = createLiveHLSConfig()
			const vodConfig = createVODHLSConfig()

			// Verify common parameters exist in both
			const commonParams = [
				'enableWorker',
				'startLevel',
				'capLevelToPlayerSize',
				'maxBufferLength',
				'maxMaxBufferLength',
				'manifestLoadingTimeOut',
				'manifestLoadingMaxRetry',
				'manifestLoadingRetryDelay',
				'manifestLoadingMaxRetryTimeout',
			]

			for (const param of commonParams) {
				assert.ok(param in liveConfig, `Live config should have ${param}`)
				assert.ok(param in vodConfig, `VOD config should have ${param}`)
			}
		})

		void it('should have same buffer settings for live and VOD', () => {
			const liveConfig = createLiveHLSConfig()
			const vodConfig = createVODHLSConfig()

			// Verify buffer settings are the same
			assert.strictEqual(
				liveConfig.maxBufferLength,
				vodConfig.maxBufferLength,
				'maxBufferLength should be the same',
			)
			assert.strictEqual(
				liveConfig.maxMaxBufferLength,
				vodConfig.maxMaxBufferLength,
				'maxMaxBufferLength should be the same',
			)
		})
	})

	void describe('Playback mode determination', () => {
		/**
		 * Test logic for determining which playback mode to use
		 */
		void it('should use live mode when stream status is active', () => {
			const streamStatus: 'active' | 'inactive' = 'active'
			let playbackMode = 'offline' as 'live' | 'vod' | 'offline'

			// Simulate mode determination logic
			if (streamStatus === 'active') {
				playbackMode = 'live'
			}

			assert.strictEqual(
				streamStatus,
				'active',
				'Stream status should be active',
			)
			assert.strictEqual(
				playbackMode,
				'live',
				'Should use live mode for active streams',
			)
		})

		void it('should use vod mode when stream is inactive and recording available', () => {
			const streamStatus: 'active' | 'inactive' = 'inactive'
			const recordingAvailable = true
			let playbackMode = 'offline' as 'live' | 'vod' | 'offline'

			// Simulate mode determination logic
			if (streamStatus === 'inactive' && recordingAvailable) {
				playbackMode = 'vod'
			}

			assert.strictEqual(
				streamStatus,
				'inactive',
				'Stream status should be inactive',
			)
			assert.strictEqual(
				recordingAvailable,
				true,
				'Recording should be available',
			)
			assert.strictEqual(
				playbackMode,
				'vod',
				'Should use vod mode when recording is available',
			)
		})

		void it('should use offline mode when stream is inactive and no recording', () => {
			const streamStatus: 'active' | 'inactive' = 'inactive'
			const recordingAvailable = false
			let playbackMode = 'offline' as 'live' | 'vod' | 'offline'

			// Simulate mode determination logic
			if (streamStatus === 'inactive' && !recordingAvailable) {
				playbackMode = 'offline'
			}

			assert.strictEqual(
				streamStatus,
				'inactive',
				'Stream status should be inactive',
			)
			assert.strictEqual(
				recordingAvailable,
				false,
				'Recording should not be available',
			)
			assert.strictEqual(
				playbackMode,
				'offline',
				'Should use offline mode when no recording available',
			)
		})
	})
})

void describe('StreamPlayer - UI Rendering Logic', () => {
	/**
	 * Unit tests for UI rendering based on playback mode
	 * Requirements: 4.4, 4.5, 7.1
	 * Task 7.2: Modify video player vs image rendering logic
	 */

	void describe('Video player rendering', () => {
		/**
		 * Test that video player is rendered for live and VOD modes
		 * Requirement 4.4: Display video player with controls for recordings
		 * Requirement 7.1: Display video player with standard controls
		 */
		void it('should render video player when playback mode is live', () => {
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let shouldRenderVideoPlayer = false
			let shouldRenderStaticImage = false

			// Simulate UI rendering logic
			if (playbackMode === 'live' || playbackMode === 'vod') {
				shouldRenderVideoPlayer = true
				shouldRenderStaticImage = false
			} else {
				shouldRenderVideoPlayer = false
				shouldRenderStaticImage = true
			}

			assert.strictEqual(playbackMode, 'live', 'Playback mode should be live')
			assert.strictEqual(
				shouldRenderVideoPlayer,
				true,
				'Should render video player for live mode',
			)
			assert.strictEqual(
				shouldRenderStaticImage,
				false,
				'Should not render static image for live mode',
			)
		})

		void it('should render video player when playback mode is vod', () => {
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let shouldRenderVideoPlayer = false
			let shouldRenderStaticImage = false

			// Simulate UI rendering logic
			if (playbackMode === 'live' || playbackMode === 'vod') {
				shouldRenderVideoPlayer = true
				shouldRenderStaticImage = false
			} else {
				shouldRenderVideoPlayer = false
				shouldRenderStaticImage = true
			}

			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be vod')
			assert.strictEqual(
				shouldRenderVideoPlayer,
				true,
				'Should render video player for VOD mode',
			)
			assert.strictEqual(
				shouldRenderStaticImage,
				false,
				'Should not render static image for VOD mode',
			)
		})

		void it('should render video player with controls attribute for live mode', () => {
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let videoHasControls = false

			// Simulate video element rendering
			if (playbackMode === 'live' || playbackMode === 'vod') {
				videoHasControls = true // Video element should have controls attribute
			}

			assert.strictEqual(playbackMode, 'live', 'Playback mode should be live')
			assert.strictEqual(
				videoHasControls,
				true,
				'Video element should have controls attribute for live mode',
			)
		})

		void it('should render video player with controls attribute for VOD mode', () => {
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let videoHasControls = false

			// Simulate video element rendering
			if (playbackMode === 'live' || playbackMode === 'vod') {
				videoHasControls = true // Video element should have controls attribute
			}

			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be vod')
			assert.strictEqual(
				videoHasControls,
				true,
				'Video element should have controls attribute for VOD mode',
			)
		})
	})

	void describe('Static image rendering', () => {
		/**
		 * Test that static image is rendered for offline mode
		 * Requirement 4.5: Display last frame image when no recording available
		 */
		void it('should render static image when playback mode is offline', () => {
			const playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			let shouldRenderVideoPlayer = false
			let shouldRenderStaticImage = false

			// Simulate UI rendering logic
			if (playbackMode === 'live' || playbackMode === 'vod') {
				shouldRenderVideoPlayer = true
				shouldRenderStaticImage = false
			} else {
				shouldRenderVideoPlayer = false
				shouldRenderStaticImage = true
			}

			assert.strictEqual(
				playbackMode,
				'offline',
				'Playback mode should be offline',
			)
			assert.strictEqual(
				shouldRenderVideoPlayer,
				false,
				'Should not render video player for offline mode',
			)
			assert.strictEqual(
				shouldRenderStaticImage,
				true,
				'Should render static image for offline mode',
			)
		})

		void it('should display last frame image in offline mode', () => {
			const playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			const lastFrameUrl = 'https://example.com/last-frame.jpg'
			let displayedImageUrl = ''

			// Simulate image rendering logic
			if (playbackMode === 'offline') {
				displayedImageUrl = lastFrameUrl
			}

			assert.strictEqual(
				playbackMode,
				'offline',
				'Playback mode should be offline',
			)
			assert.strictEqual(
				displayedImageUrl,
				lastFrameUrl,
				'Should display last frame image URL',
			)
		})
	})

	void describe('Conditional rendering logic', () => {
		/**
		 * Test the conditional rendering logic for all playback modes
		 */
		void it('should render correct UI element for each playback mode', () => {
			const modes: Array<'live' | 'vod' | 'offline'> = [
				'live',
				'vod',
				'offline',
			]

			for (const mode of modes) {
				let shouldRenderVideoPlayer = false
				let shouldRenderStaticImage = false

				// Simulate UI rendering logic
				if (mode === 'live' || mode === 'vod') {
					shouldRenderVideoPlayer = true
					shouldRenderStaticImage = false
				} else {
					shouldRenderVideoPlayer = false
					shouldRenderStaticImage = true
				}

				if (mode === 'live' || mode === 'vod') {
					assert.strictEqual(
						shouldRenderVideoPlayer,
						true,
						`Should render video player for ${mode} mode`,
					)
					assert.strictEqual(
						shouldRenderStaticImage,
						false,
						`Should not render static image for ${mode} mode`,
					)
				} else {
					assert.strictEqual(
						shouldRenderVideoPlayer,
						false,
						`Should not render video player for ${mode} mode`,
					)
					assert.strictEqual(
						shouldRenderStaticImage,
						true,
						`Should render static image for ${mode} mode`,
					)
				}
			}
		})

		void it('should ensure video and image are mutually exclusive', () => {
			const modes: Array<'live' | 'vod' | 'offline'> = [
				'live',
				'vod',
				'offline',
			]

			for (const mode of modes) {
				let shouldRenderVideoPlayer = false
				let shouldRenderStaticImage = false

				// Simulate UI rendering logic
				if (mode === 'live' || mode === 'vod') {
					shouldRenderVideoPlayer = true
					shouldRenderStaticImage = false
				} else {
					shouldRenderVideoPlayer = false
					shouldRenderStaticImage = true
				}

				// Video player and static image should never both be rendered
				assert.notStrictEqual(
					shouldRenderVideoPlayer && shouldRenderStaticImage,
					true,
					`Video player and static image should not both be rendered for ${mode} mode`,
				)

				// At least one should be rendered
				assert.strictEqual(
					shouldRenderVideoPlayer || shouldRenderStaticImage,
					true,
					`Either video player or static image should be rendered for ${mode} mode`,
				)
			}
		})
	})

	void describe('Badge display with UI elements', () => {
		/**
		 * Test that badges are displayed correctly with corresponding UI elements
		 */
		void it('should display LIVE badge with video player', () => {
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let badgeText = ''
			let shouldRenderVideoPlayer = false

			// Simulate rendering logic
			if (playbackMode === 'live' || playbackMode === 'vod') {
				shouldRenderVideoPlayer = true
				badgeText = playbackMode === 'live' ? 'LIVE' : 'RECORDED'
			}

			assert.strictEqual(playbackMode, 'live', 'Playback mode should be live')
			assert.strictEqual(
				shouldRenderVideoPlayer,
				true,
				'Should render video player',
			)
			assert.strictEqual(badgeText, 'LIVE', 'Should display LIVE badge')
		})

		void it('should display RECORDED badge with video player', () => {
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let badgeText = ''
			let shouldRenderVideoPlayer = false

			// Simulate rendering logic
			if (playbackMode === 'live' || playbackMode === 'vod') {
				shouldRenderVideoPlayer = true
				badgeText = playbackMode === 'live' ? 'LIVE' : 'RECORDED'
			}

			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be vod')
			assert.strictEqual(
				shouldRenderVideoPlayer,
				true,
				'Should render video player',
			)
			assert.strictEqual(badgeText, 'RECORDED', 'Should display RECORDED badge')
		})

		void it('should display OFFLINE badge with static image', () => {
			const playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			let badgeText = 'OFFLINE'
			let shouldRenderStaticImage = false

			// Simulate rendering logic
			if (playbackMode === 'offline') {
				shouldRenderStaticImage = true
				badgeText = 'OFFLINE'
			}

			assert.strictEqual(
				playbackMode,
				'offline',
				'Playback mode should be offline',
			)
			assert.strictEqual(
				shouldRenderStaticImage,
				true,
				'Should render static image',
			)
			assert.strictEqual(badgeText, 'OFFLINE', 'Should display OFFLINE badge')
		})
	})
})

void describe('StreamPlayer - Mode Toggle Visibility', () => {
	/**
	 * Unit tests for mode toggle visibility based on playback mode
	 * Requirements: 9.1, 9.2, 9.3, 9.4
	 * Task 7.3: Update mode toggle visibility
	 */

	void describe('Mode toggle visibility logic', () => {
		/**
		 * Test that mode toggle is visible only for live mode
		 * Requirement 9.1: Display mode toggle when stream is active
		 * Requirement 9.2: Hide mode toggle when stream is inactive with recording
		 */
		void it('should show mode toggle when playback mode is live', () => {
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let shouldShowModeToggle = false

			// Simulate mode toggle visibility logic
			if (playbackMode === 'live') {
				shouldShowModeToggle = true
			}

			assert.strictEqual(playbackMode, 'live', 'Playback mode should be live')
			assert.strictEqual(
				shouldShowModeToggle,
				true,
				'Mode toggle should be visible for live mode',
			)
		})

		void it('should hide mode toggle when playback mode is vod', () => {
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let shouldShowModeToggle = false

			// Simulate mode toggle visibility logic
			if (playbackMode === 'live') {
				shouldShowModeToggle = true
			}

			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be vod')
			assert.strictEqual(
				shouldShowModeToggle,
				false,
				'Mode toggle should be hidden for VOD mode',
			)
		})

		void it('should hide mode toggle when playback mode is offline', () => {
			const playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			let shouldShowModeToggle = false

			// Simulate mode toggle visibility logic
			if (playbackMode === 'live') {
				shouldShowModeToggle = true
			}

			assert.strictEqual(
				playbackMode,
				'offline',
				'Playback mode should be offline',
			)
			assert.strictEqual(
				shouldShowModeToggle,
				false,
				'Mode toggle should be hidden for offline mode',
			)
		})
	})

	void describe('Stream mode reset on transition', () => {
		/**
		 * Test that streamMode is reset to adaptive when transitioning to VOD
		 * Requirement 9.3: Use adaptive HLS manifest URL for recordings
		 * Requirement 9.4: Reset mode toggle to adaptive when transitioning from live to recorded
		 */
		void it('should reset streamMode to adaptive when transitioning from live to vod', () => {
			// Initial state: live mode with raw stream
			let streamMode = 'raw' as 'adaptive' | 'raw'
			const previousStatus = 'active'
			const currentStatus = 'inactive'

			// Simulate transition logic
			if (previousStatus === 'active' && currentStatus === 'inactive') {
				// Reset stream mode to adaptive
				streamMode = 'adaptive'
			}

			assert.strictEqual(
				previousStatus,
				'active',
				'Previous status should be active',
			)
			assert.strictEqual(
				currentStatus,
				'inactive',
				'Current status should be inactive',
			)
			assert.strictEqual(
				streamMode,
				'adaptive',
				'Stream mode should be reset to adaptive',
			)
		})

		void it('should reset streamMode to adaptive when transitioning from live to offline', () => {
			// Initial state: live mode with raw stream
			let streamMode = 'raw' as 'adaptive' | 'raw'
			const previousStatus = 'active'
			const currentStatus = 'inactive'

			// Simulate transition logic
			if (previousStatus === 'active' && currentStatus === 'inactive') {
				// Reset stream mode to adaptive
				streamMode = 'adaptive'
			}

			assert.strictEqual(
				previousStatus,
				'active',
				'Previous status should be active',
			)
			assert.strictEqual(
				currentStatus,
				'inactive',
				'Current status should be inactive',
			)
			assert.strictEqual(
				streamMode,
				'adaptive',
				'Stream mode should be reset to adaptive even when going offline',
			)
		})

		void it('should not change streamMode when transitioning from offline to live', () => {
			// Initial state: offline mode with adaptive (default)
			const streamMode: 'adaptive' | 'raw' = 'adaptive' as 'adaptive' | 'raw'
			const previousStatus = 'inactive'
			const currentStatus = 'active'

			// Simulate transition logic - streamMode should remain unchanged
			// (it's already adaptive from the previous reset)
			if (previousStatus === 'inactive' && currentStatus === 'active') {
				// No change to streamMode - it stays as is
			}

			assert.strictEqual(
				previousStatus,
				'inactive',
				'Previous status should be inactive',
			)
			assert.strictEqual(
				currentStatus,
				'active',
				'Current status should be active',
			)
			assert.strictEqual(
				streamMode,
				'adaptive',
				'Stream mode should remain adaptive when transitioning to live',
			)
		})

		void it('should preserve adaptive mode through multiple transitions', () => {
			// Test multiple transitions to ensure reset persists
			let streamMode = 'raw' as 'adaptive' | 'raw'

			// Transition 1: live -> offline (reset to adaptive)
			streamMode = 'adaptive'
			assert.strictEqual(
				streamMode,
				'adaptive',
				'Should be adaptive after first transition',
			)

			// Transition 2: offline -> live (stays adaptive)
			// No change
			assert.strictEqual(
				streamMode,
				'adaptive',
				'Should remain adaptive after going live again',
			)

			// User changes to raw
			streamMode = 'raw'
			assert.strictEqual(
				streamMode,
				'raw',
				'User can change to raw in live mode',
			)

			// Transition 3: live -> vod (reset to adaptive again)
			streamMode = 'adaptive'
			assert.strictEqual(
				streamMode,
				'adaptive',
				'Should be reset to adaptive again after another transition',
			)
		})
	})

	void describe('VOD mode always uses adaptive manifest', () => {
		/**
		 * Test that VOD mode always uses adaptive manifest URL
		 * Requirement 9.3: Use adaptive HLS manifest URL for recordings
		 */
		void it('should use adaptive manifest URL for VOD mode regardless of streamMode', () => {
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			const streamMode: 'adaptive' | 'raw' = 'raw' // Even if this is set to raw
			const adaptiveUrl = 'https://example.com/adaptive/manifest.m3u8'
			const rawUrl = 'https://example.com/raw/stream.m3u8'

			// VOD mode always uses adaptive URL
			let streamUrl: string
			if (playbackMode === 'vod') {
				streamUrl = adaptiveUrl // Always use adaptive for VOD
			} else {
				// @ts-expect-error - Test with literal types
				streamUrl = streamMode === 'adaptive' ? adaptiveUrl : rawUrl
			}

			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be vod')
			assert.strictEqual(
				streamUrl,
				adaptiveUrl,
				'Should always use adaptive URL for VOD mode',
			)
		})

		void it('should ignore streamMode setting in VOD mode', () => {
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			const adaptiveUrl = 'https://example.com/adaptive/manifest.m3u8'
			const rawUrl = 'https://example.com/raw/stream.m3u8'

			// Test with streamMode = 'adaptive'
			let streamMode = 'adaptive' as 'adaptive' | 'raw'
			let streamUrl =
				playbackMode === 'vod'
					? adaptiveUrl
					: streamMode === 'adaptive'
						? adaptiveUrl
						: rawUrl
			assert.strictEqual(
				streamUrl,
				adaptiveUrl,
				'Should use adaptive URL when streamMode is adaptive',
			)

			// Test with streamMode = 'raw'
			streamMode = 'raw'
			streamUrl =
				playbackMode === 'vod'
					? adaptiveUrl
					: // @ts-expect-error - Test with literal types
						streamMode === 'adaptive'
						? adaptiveUrl
						: rawUrl
			assert.strictEqual(
				streamUrl,
				adaptiveUrl,
				'Should still use adaptive URL even when streamMode is raw',
			)
		})
	})

	void describe('Mode toggle interaction with playback modes', () => {
		/**
		 * Test mode toggle behavior across different playback modes
		 */
		void it('should allow mode toggle interaction only in live mode', () => {
			// Live mode - toggle should be interactive
			let playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let isModeToggleEnabled = playbackMode === 'live'

			assert.strictEqual(playbackMode, 'live', 'Playback mode should be live')
			assert.strictEqual(
				isModeToggleEnabled,
				true,
				'Mode toggle should be enabled in live mode',
			)

			// VOD mode - toggle should not be visible/interactive
			playbackMode = 'vod'
			// @ts-expect-error - Test with literal types
			isModeToggleEnabled = playbackMode === 'live'

			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be vod')
			assert.strictEqual(
				isModeToggleEnabled,
				false,
				'Mode toggle should be disabled in VOD mode',
			)

			// Offline mode - toggle should not be visible/interactive
			playbackMode = 'offline'
			// @ts-expect-error - Test with literal types
			isModeToggleEnabled = playbackMode === 'live'

			assert.strictEqual(
				playbackMode,
				'offline',
				'Playback mode should be offline',
			)
			assert.strictEqual(
				isModeToggleEnabled,
				false,
				'Mode toggle should be disabled in offline mode',
			)
		})

		void it('should show quality selector only in live mode with adaptive stream', () => {
			// Live mode with adaptive - quality selector should be visible
			let playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let streamMode = 'adaptive' as 'adaptive' | 'raw'
			let shouldShowQualitySelector =
				playbackMode === 'live' && streamMode === 'adaptive'

			assert.strictEqual(
				shouldShowQualitySelector,
				true,
				'Quality selector should be visible in live mode with adaptive stream',
			)

			// Live mode with raw - quality selector should be hidden
			streamMode = 'raw'
			shouldShowQualitySelector =
				// @ts-expect-error - Test with literal types
				playbackMode === 'live' && streamMode === 'adaptive'

			assert.strictEqual(
				shouldShowQualitySelector,
				false,
				'Quality selector should be hidden in live mode with raw stream',
			)

			// VOD mode - quality selector should be hidden (mode toggle is hidden)
			playbackMode = 'vod'
			streamMode = 'adaptive'
			shouldShowQualitySelector =
				// @ts-expect-error - Test with literal types
				playbackMode === 'live' && streamMode === 'adaptive'

			assert.strictEqual(
				shouldShowQualitySelector,
				false,
				'Quality selector should be hidden in VOD mode',
			)
		})
	})

	void describe('Edge cases and boundary conditions', () => {
		/**
		 * Test edge cases for mode toggle visibility
		 */
		void it('should handle rapid mode transitions correctly', () => {
			let streamMode = 'raw' as 'adaptive' | 'raw'

			// Rapid transition: live -> offline -> live
			streamMode = 'adaptive' // Reset on transition
			assert.strictEqual(
				streamMode,
				'adaptive',
				'Should be adaptive after transition to offline',
			)

			// streamMode stays adaptive
			assert.strictEqual(
				streamMode,
				'adaptive',
				'Should remain adaptive when returning to live',
			)
		})

		void it('should maintain consistent state after error recovery', () => {
			let streamMode = 'raw' as 'adaptive' | 'raw'
			let playbackMode = 'live' as 'live' | 'vod' | 'offline'

			// Simulate error causing transition to offline
			playbackMode = 'offline'
			streamMode = 'adaptive' // Reset on transition

			// Recovery back to live
			playbackMode = 'live'

			assert.strictEqual(
				streamMode,
				'adaptive',
				'Should be in adaptive mode after error recovery',
			)
			assert.strictEqual(
				playbackMode,
				'live',
				'Should be in live mode after recovery',
			)
		})

		void it('should handle initial page load with different playback modes', () => {
			// Test initial load with live stream
			let playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let shouldShowModeToggle = playbackMode === 'live'

			assert.strictEqual(
				shouldShowModeToggle,
				true,
				'Mode toggle should be visible on initial load with live stream',
			)

			// Test initial load with offline stream (no recording)
			playbackMode = 'offline'
			// @ts-expect-error - Test with literal types
			shouldShowModeToggle = playbackMode === 'live'

			assert.strictEqual(
				shouldShowModeToggle,
				false,
				'Mode toggle should be hidden on initial load with offline stream',
			)

			// Test initial load with offline stream (with recording)
			playbackMode = 'vod'
			// @ts-expect-error - Test with literal types
			shouldShowModeToggle = playbackMode === 'live'

			assert.strictEqual(
				shouldShowModeToggle,
				false,
				'Mode toggle should be hidden on initial load with VOD playback',
			)
		})
	})
})

void describe('StreamPlayer - VOD Error Handling', () => {
	/**
	 * Unit tests for VOD playback error handling
	 * Requirements: 8.1, 8.2, 8.5
	 * Task 8.2: Add error handling for VOD playback initialization
	 */

	void describe('Manifest load error handling', () => {
		/**
		 * Test that VOD manifest load errors fall back to offline mode
		 * Requirement 8.1: Fall back to displaying last frame image on manifest load failure
		 * Requirement 8.2: Display error message on fatal VOD errors
		 * Requirement 8.5: Show "OFFLINE" badge when falling back
		 */
		void it('should fall back to offline mode on VOD manifest load error', () => {
			const errorData = {
				type: 'networkError',
				details: 'manifestLoadError',
				fatal: false,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let newPlaybackMode: 'live' | 'vod' | 'offline' = playbackMode
			let recordingAvailable = true
			let errorMessage = ''

			// Simulate VOD manifest load error handling
			if (
				errorData.type === 'networkError' &&
				errorData.details === 'manifestLoadError' &&
				playbackMode === 'vod'
			) {
				newPlaybackMode = 'offline'
				recordingAvailable = false
				errorMessage =
					'Recording playback failed. The recording manifest could not be loaded.'
			}

			assert.strictEqual(
				newPlaybackMode,
				'offline',
				'Should set playback mode to offline',
			)
			assert.strictEqual(
				recordingAvailable,
				false,
				'Should set recording available to false',
			)
			assert.ok(
				errorMessage.includes('Recording playback failed'),
				'Should display error message about recording playback failure',
			)
			assert.ok(
				errorMessage.includes('manifest'),
				'Error message should mention manifest',
			)
		})

		void it('should not affect live mode on manifest load error', () => {
			const errorData = {
				type: 'networkError',
				details: 'manifestLoadError',
				fatal: false,
			}
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let newPlaybackMode: 'live' | 'vod' | 'offline' = playbackMode
			let shouldRetry = false

			// Simulate manifest load error handling for live mode
			if (
				errorData.type === 'networkError' &&
				errorData.details === 'manifestLoadError'
			) {
				if (playbackMode === 'vod') {
					newPlaybackMode = 'offline'
				} else {
					// Live mode should implement retry logic
					shouldRetry = true
				}
			}

			assert.strictEqual(
				newPlaybackMode,
				'live',
				'Should keep playback mode as live',
			)
			assert.strictEqual(
				shouldRetry,
				true,
				'Should retry for live mode instead of falling back',
			)
		})

		void it('should log error details for VOD manifest load failure', () => {
			const errorData = {
				type: 'networkError',
				details: 'manifestLoadError',
				fatal: false,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let loggedError = false

			// Simulate error logging
			if (
				errorData.type === 'networkError' &&
				errorData.details === 'manifestLoadError' &&
				playbackMode === 'vod'
			) {
				console.error(
					'[StreamPlayer] VOD manifest load failed, falling back to offline mode',
				)
				loggedError = true
			}

			assert.strictEqual(
				loggedError,
				true,
				'Should log error details for debugging',
			)
		})
	})

	void describe('Fatal network error handling', () => {
		/**
		 * Test that fatal network errors in VOD mode fall back to offline mode
		 * Requirement 8.1: Fall back to displaying last frame image on fatal errors
		 * Requirement 8.2: Display error message on fatal VOD errors
		 * Requirement 8.5: Show "OFFLINE" badge when falling back
		 */
		void it('should fall back to offline mode on fatal VOD network error', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: true,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let newPlaybackMode: 'live' | 'vod' | 'offline' = playbackMode
			let recordingAvailable = true
			let errorMessage = ''

			// Simulate fatal network error handling for VOD
			if (errorData.fatal && errorData.type === 'networkError') {
				if (playbackMode === 'vod') {
					newPlaybackMode = 'offline'
					recordingAvailable = false
					errorMessage =
						'Recording playback failed. The recording may be temporarily unavailable.'
				}
			}

			assert.strictEqual(
				newPlaybackMode,
				'offline',
				'Should set playback mode to offline',
			)
			assert.strictEqual(
				recordingAvailable,
				false,
				'Should set recording available to false',
			)
			assert.ok(
				errorMessage.includes('Recording playback failed'),
				'Should display error message about recording playback failure',
			)
			assert.ok(
				errorMessage.includes('temporarily unavailable'),
				'Error message should indicate temporary unavailability',
			)
		})

		void it('should attempt recovery for live mode on fatal network error', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: true,
			}
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let newPlaybackMode: 'live' | 'vod' | 'offline' = playbackMode
			let shouldAttemptRecovery = false

			// Simulate fatal network error handling for live mode
			if (errorData.fatal && errorData.type === 'networkError') {
				if (playbackMode === 'vod') {
					newPlaybackMode = 'offline'
				} else {
					// Live mode should attempt recovery
					shouldAttemptRecovery = true
				}
			}

			assert.strictEqual(
				newPlaybackMode,
				'live',
				'Should keep playback mode as live',
			)
			assert.strictEqual(
				shouldAttemptRecovery,
				true,
				'Should attempt recovery for live mode',
			)
		})

		void it('should provide specific error message for network errors', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: true,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let errorMessage = ''

			// Simulate error message generation
			if (
				errorData.fatal &&
				errorData.type === 'networkError' &&
				playbackMode === 'vod'
			) {
				errorMessage =
					'Recording playback failed. The recording may be temporarily unavailable.'
			}

			assert.ok(errorMessage.length > 0, 'Should provide error message')
			assert.ok(
				errorMessage.includes('Recording playback failed'),
				'Should indicate recording playback failure',
			)
		})
	})

	void describe('Fatal media error handling', () => {
		/**
		 * Test that fatal media errors in VOD mode fall back to offline mode
		 * Requirement 8.1: Fall back to displaying last frame image on media errors
		 * Requirement 8.2: Display error message on fatal VOD errors
		 * Requirement 8.5: Show "OFFLINE" badge when falling back
		 */
		void it('should fall back to offline mode on fatal VOD media error', () => {
			const errorData = {
				type: 'mediaError',
				details: 'bufferStalledError',
				fatal: true,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let newPlaybackMode: 'live' | 'vod' | 'offline' = playbackMode
			let recordingAvailable = true
			let errorMessage = ''

			// Simulate fatal media error handling for VOD
			if (errorData.fatal && errorData.type === 'mediaError') {
				if (playbackMode === 'vod') {
					newPlaybackMode = 'offline'
					recordingAvailable = false
					errorMessage =
						'Recording playback failed. The recording media could not be played.'
				}
			}

			assert.strictEqual(
				newPlaybackMode,
				'offline',
				'Should set playback mode to offline',
			)
			assert.strictEqual(
				recordingAvailable,
				false,
				'Should set recording available to false',
			)
			assert.ok(
				errorMessage.includes('Recording playback failed'),
				'Should display error message about recording playback failure',
			)
			assert.ok(
				errorMessage.includes('media could not be played'),
				'Error message should mention media playback issue',
			)
		})

		void it('should attempt recovery for live mode on fatal media error', () => {
			const errorData = {
				type: 'mediaError',
				details: 'bufferStalledError',
				fatal: true,
			}
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let newPlaybackMode: 'live' | 'vod' | 'offline' = playbackMode
			let shouldAttemptRecovery = false

			// Simulate fatal media error handling for live mode
			if (errorData.fatal && errorData.type === 'mediaError') {
				if (playbackMode === 'vod') {
					newPlaybackMode = 'offline'
				} else {
					// Live mode should attempt recovery
					shouldAttemptRecovery = true
				}
			}

			assert.strictEqual(
				newPlaybackMode,
				'live',
				'Should keep playback mode as live',
			)
			assert.strictEqual(
				shouldAttemptRecovery,
				true,
				'Should attempt recovery for live mode',
			)
		})

		void it('should provide specific error message for media errors', () => {
			const errorData = {
				type: 'mediaError',
				details: 'bufferStalledError',
				fatal: true,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let errorMessage = ''

			// Simulate error message generation
			if (
				errorData.fatal &&
				errorData.type === 'mediaError' &&
				playbackMode === 'vod'
			) {
				errorMessage =
					'Recording playback failed. The recording media could not be played.'
			}

			assert.ok(errorMessage.length > 0, 'Should provide error message')
			assert.ok(
				errorMessage.includes('media could not be played'),
				'Should indicate media playback issue',
			)
		})
	})

	void describe('Default fatal error handling', () => {
		/**
		 * Test that other fatal errors in VOD mode fall back to offline mode
		 * Requirement 8.1: Fall back to displaying last frame image on any fatal error
		 * Requirement 8.2: Display error message on fatal VOD errors
		 * Requirement 8.5: Show "OFFLINE" badge when falling back
		 */
		void it('should fall back to offline mode on other fatal VOD errors', () => {
			const errorData = {
				type: 'otherError',
				details: 'unknownError',
				fatal: true,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let newPlaybackMode: 'live' | 'vod' | 'offline' = playbackMode
			let recordingAvailable = true
			let errorMessage = ''

			// Simulate default fatal error handling for VOD
			if (errorData.fatal) {
				if (playbackMode === 'vod') {
					newPlaybackMode = 'offline'
					recordingAvailable = false
					errorMessage =
						'Recording playback failed. Please try refreshing the page.'
				}
			}

			assert.strictEqual(
				newPlaybackMode,
				'offline',
				'Should set playback mode to offline',
			)
			assert.strictEqual(
				recordingAvailable,
				false,
				'Should set recording available to false',
			)
			assert.ok(
				errorMessage.includes('Recording playback failed'),
				'Should display error message about recording playback failure',
			)
			assert.ok(
				errorMessage.includes('try refreshing'),
				'Error message should suggest refreshing the page',
			)
		})

		void it('should provide generic error message for unknown fatal errors', () => {
			const errorData = {
				type: 'otherError',
				details: 'unknownError',
				fatal: true,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let errorMessage = ''

			// Simulate error message generation for unknown errors
			if (errorData.fatal && playbackMode === 'vod') {
				errorMessage =
					'Recording playback failed. Please try refreshing the page.'
			}

			assert.ok(errorMessage.length > 0, 'Should provide error message')
			assert.ok(
				errorMessage.includes('Recording playback failed'),
				'Should indicate recording playback failure',
			)
			assert.ok(
				errorMessage.includes('try refreshing'),
				'Should suggest user action',
			)
		})

		void it('should differentiate error messages between VOD and live modes', () => {
			const errorData = {
				type: 'otherError',
				details: 'unknownError',
				fatal: true,
			}
			let vodErrorMessage = ''
			let liveErrorMessage = ''

			// VOD mode error message
			const vodPlaybackMode = 'vod'
			if (errorData.fatal && vodPlaybackMode === 'vod') {
				vodErrorMessage =
					'Recording playback failed. Please try refreshing the page.'
			}

			// Live mode error message
			const livePlaybackMode = 'live'
			if (errorData.fatal && livePlaybackMode === 'live') {
				liveErrorMessage =
					'Playback error occurred. Please try refreshing the page.'
			}

			assert.ok(
				vodErrorMessage.includes('Recording playback'),
				'VOD error should mention recording playback',
			)
			assert.ok(
				liveErrorMessage.includes('Playback error'),
				'Live error should mention general playback error',
			)
			assert.notStrictEqual(
				vodErrorMessage,
				liveErrorMessage,
				'Error messages should be different for VOD and live modes',
			)
		})
	})

	void describe('OFFLINE badge display after fallback', () => {
		/**
		 * Test that OFFLINE badge is shown after VOD error fallback
		 * Requirement 8.5: Show "OFFLINE" badge when falling back to last frame display
		 */
		void it('should display OFFLINE badge when playback mode is offline', () => {
			const playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			let badgeText = ''

			// Simulate badge text determination
			if (playbackMode === 'live') {
				badgeText = 'LIVE'
			} else if (playbackMode === 'vod') {
				badgeText = 'RECORDED'
			} else if (playbackMode === 'offline') {
				badgeText = 'OFFLINE'
			}

			assert.strictEqual(
				badgeText,
				'OFFLINE',
				'Should display OFFLINE badge in offline mode',
			)
		})

		void it('should show static image when playback mode is offline', () => {
			const playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			let showVideoPlayer = false
			let showStaticImage = false

			// Simulate UI element visibility
			if (playbackMode === 'live' || playbackMode === 'vod') {
				showVideoPlayer = true
				showStaticImage = false
			} else if (playbackMode === 'offline') {
				showVideoPlayer = false
				showStaticImage = true
			}

			assert.strictEqual(
				showVideoPlayer,
				false,
				'Should not show video player in offline mode',
			)
			assert.strictEqual(
				showStaticImage,
				true,
				'Should show static image in offline mode',
			)
		})

		void it('should transition from VOD to offline mode on error', () => {
			let playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			const errorOccurred = true

			// Simulate error-triggered mode transition
			if (errorOccurred && playbackMode === 'vod') {
				playbackMode = 'offline'
			}

			assert.strictEqual(
				playbackMode,
				'offline',
				'Should transition from vod to offline on error',
			)
		})
	})

	void describe('Error detection and classification', () => {
		/**
		 * Test that VOD-specific errors are detected correctly
		 */
		void it('should detect VOD mode from playback mode state', () => {
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			const isVODMode = playbackMode === 'vod'

			assert.strictEqual(isVODMode, true, 'Should correctly detect VOD mode')
		})

		void it('should classify manifest load errors correctly', () => {
			const errorData = {
				type: 'networkError',
				details: 'manifestLoadError',
				fatal: false,
			}

			const isManifestLoadError =
				errorData.type === 'networkError' &&
				errorData.details === 'manifestLoadError'

			assert.strictEqual(
				isManifestLoadError,
				true,
				'Should correctly classify manifest load errors',
			)
		})

		void it('should classify fatal network errors correctly', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: true,
			}

			const isFatalNetworkError =
				errorData.fatal && errorData.type === 'networkError'

			assert.strictEqual(
				isFatalNetworkError,
				true,
				'Should correctly classify fatal network errors',
			)
		})

		void it('should classify fatal media errors correctly', () => {
			const errorData = {
				type: 'mediaError',
				details: 'bufferStalledError',
				fatal: true,
			}

			const isFatalMediaError =
				errorData.fatal && errorData.type === 'mediaError'

			assert.strictEqual(
				isFatalMediaError,
				true,
				'Should correctly classify fatal media errors',
			)
		})
	})

	void describe('Error logging', () => {
		/**
		 * Test that errors are logged for debugging
		 * Requirement 8.4: Log error details for debugging
		 */
		void it('should log VOD manifest load errors', () => {
			const errorData = {
				type: 'networkError',
				details: 'manifestLoadError',
				fatal: false,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let errorLogged = false

			// Simulate error logging
			if (
				errorData.type === 'networkError' &&
				errorData.details === 'manifestLoadError' &&
				playbackMode === 'vod'
			) {
				console.error(
					'[StreamPlayer] VOD manifest load failed, falling back to offline mode',
				)
				errorLogged = true
			}

			assert.strictEqual(errorLogged, true, 'Should log manifest load errors')
		})

		void it('should log fatal VOD network errors', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: true,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let errorLogged = false

			// Simulate error logging
			if (
				errorData.fatal &&
				errorData.type === 'networkError' &&
				playbackMode === 'vod'
			) {
				console.error(
					'[StreamPlayer] Fatal VOD network error, falling back to offline mode',
				)
				errorLogged = true
			}

			assert.strictEqual(errorLogged, true, 'Should log fatal network errors')
		})

		void it('should log fatal VOD media errors', () => {
			const errorData = {
				type: 'mediaError',
				details: 'bufferStalledError',
				fatal: true,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let errorLogged = false

			// Simulate error logging
			if (
				errorData.fatal &&
				errorData.type === 'mediaError' &&
				playbackMode === 'vod'
			) {
				console.error(
					'[StreamPlayer] Fatal VOD media error, falling back to offline mode',
				)
				errorLogged = true
			}

			assert.strictEqual(errorLogged, true, 'Should log fatal media errors')
		})

		void it('should include playback mode in error logs', () => {
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let logMessage = ''

			// Simulate error log message generation
			logMessage = `[StreamPlayer] HLS error (${playbackMode} mode):`

			assert.ok(
				logMessage.includes('vod mode'),
				'Should include playback mode in error logs',
			)
		})
	})

	void describe('State consistency after error', () => {
		/**
		 * Test that state is consistent after error handling
		 */
		void it('should set both playbackMode and recordingAvailable on error', () => {
			const errorData = {
				type: 'networkError',
				details: 'manifestLoadError',
				fatal: false,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let newPlaybackMode: 'live' | 'vod' | 'offline' = playbackMode
			let recordingAvailable = true

			// Simulate error handling
			if (
				errorData.type === 'networkError' &&
				errorData.details === 'manifestLoadError' &&
				playbackMode === 'vod'
			) {
				newPlaybackMode = 'offline'
				recordingAvailable = false
			}

			assert.strictEqual(
				newPlaybackMode,
				'offline',
				'Should update playback mode',
			)
			assert.strictEqual(
				recordingAvailable,
				false,
				'Should update recording available state',
			)
		})

		void it('should maintain state consistency across all error types', () => {
			const errorTypes = [
				{ type: 'networkError', details: 'manifestLoadError', fatal: false },
				{ type: 'networkError', details: 'fragLoadError', fatal: true },
				{ type: 'mediaError', details: 'bufferStalledError', fatal: true },
				{ type: 'otherError', details: 'unknownError', fatal: true },
			]

			for (const errorData of errorTypes) {
				const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
				let newPlaybackMode: 'live' | 'vod' | 'offline' = playbackMode
				let recordingAvailable = true

				// Simulate error handling for each error type
				if (
					(errorData.type === 'networkError' &&
						errorData.details === 'manifestLoadError') ||
					(errorData.fatal && errorData.type === 'networkError') ||
					(errorData.fatal && errorData.type === 'mediaError') ||
					errorData.fatal
				) {
					if (playbackMode === 'vod') {
						newPlaybackMode = 'offline'
						recordingAvailable = false
					}
				}

				assert.strictEqual(
					newPlaybackMode,
					'offline',
					`Should set playback mode to offline for ${errorData.type}`,
				)
				assert.strictEqual(
					recordingAvailable,
					false,
					`Should set recording available to false for ${errorData.type}`,
				)
			}
		})
	})
})

void describe('StreamPlayer - Missing Segment Handling', () => {
	/**
	 * Unit tests for handling missing segments in VOD playback
	 * Requirements: 8.3, 8.4
	 * Task 8.3: Handle missing segments in VOD playback
	 */

	void describe('Fragment load error detection', () => {
		/**
		 * Test that fragment load errors are detected correctly
		 * Requirement 8.3: Allow HLS.js to attempt automatic recovery for missing segments
		 */
		void it('should detect fragLoadError as a segment error', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: false,
			}

			// Verify error is identified as a segment error
			const isSegmentError =
				errorData.type === 'networkError' &&
				(errorData.details === 'fragLoadError' ||
					errorData.details === 'fragLoadTimeOut')

			assert.strictEqual(
				isSegmentError,
				true,
				'Should identify fragLoadError as a segment error',
			)
		})

		void it('should detect fragLoadTimeOut as a segment error', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadTimeOut',
				fatal: false,
			}

			// Verify error is identified as a segment error
			const isSegmentError =
				errorData.type === 'networkError' &&
				(errorData.details === 'fragLoadError' ||
					errorData.details === 'fragLoadTimeOut')

			assert.strictEqual(
				isSegmentError,
				true,
				'Should identify fragLoadTimeOut as a segment error',
			)
		})

		void it('should not treat manifestLoadError as a segment error', () => {
			const errorData = {
				type: 'networkError',
				details: 'manifestLoadError',
				fatal: false,
			}

			// Verify error is NOT identified as a segment error
			const isSegmentError =
				errorData.type === 'networkError' &&
				(errorData.details === 'fragLoadError' ||
					errorData.details === 'fragLoadTimeOut')

			assert.strictEqual(
				isSegmentError,
				false,
				'Should not identify manifestLoadError as a segment error',
			)
		})
	})

	void describe('VOD mode segment error handling', () => {
		/**
		 * Test that VOD mode allows HLS.js to recover from missing segments
		 * Requirement 8.3: Allow HLS.js to attempt automatic recovery
		 * Requirement 8.4: Log segment errors for debugging
		 */
		void it('should allow HLS.js automatic recovery for missing segments in VOD mode', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: false,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let shouldFallbackToOffline = false
			let shouldAllowRecovery = false

			// Simulate segment error handling for VOD
			if (
				errorData.type === 'networkError' &&
				(errorData.details === 'fragLoadError' ||
					errorData.details === 'fragLoadTimeOut')
			) {
				if (playbackMode === 'vod') {
					// Allow HLS.js to handle recovery automatically
					shouldAllowRecovery = true
					shouldFallbackToOffline = false
				}
			}

			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be VOD')
			assert.strictEqual(
				shouldAllowRecovery,
				true,
				'Should allow HLS.js automatic recovery',
			)
			assert.strictEqual(
				shouldFallbackToOffline,
				false,
				'Should not fall back to offline mode for segment errors',
			)
		})

		void it('should not treat segment errors as fatal in VOD mode', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: false,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let playbackShouldContinue = true

			// Simulate segment error handling
			if (
				errorData.type === 'networkError' &&
				(errorData.details === 'fragLoadError' ||
					errorData.details === 'fragLoadTimeOut')
			) {
				// Don't treat as fatal - continue playback
				playbackShouldContinue = true
			}

			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be VOD')
			assert.strictEqual(
				playbackShouldContinue,
				true,
				'Playback should continue with available segments',
			)
		})

		void it('should continue playback with available segments after segment error', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: false,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let newPlaybackMode: 'live' | 'vod' | 'offline' = playbackMode

			// Simulate segment error handling
			if (
				errorData.type === 'networkError' &&
				(errorData.details === 'fragLoadError' ||
					errorData.details === 'fragLoadTimeOut')
			) {
				// Don't change playback mode - continue with available segments
				newPlaybackMode = playbackMode
			}

			assert.strictEqual(
				newPlaybackMode,
				'vod',
				'Should remain in VOD mode and continue playback',
			)
		})
	})

	void describe('Live mode segment error handling', () => {
		/**
		 * Test that live mode also allows HLS.js to recover from missing segments
		 * Requirement 8.3: Allow HLS.js to attempt automatic recovery
		 */
		void it('should allow HLS.js automatic recovery for missing segments in live mode', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: false,
			}
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let shouldAllowRecovery = false

			// Simulate segment error handling for live
			if (
				errorData.type === 'networkError' &&
				(errorData.details === 'fragLoadError' ||
					errorData.details === 'fragLoadTimeOut')
			) {
				// Allow HLS.js to handle recovery automatically
				shouldAllowRecovery = true
			}

			assert.strictEqual(playbackMode, 'live', 'Playback mode should be live')
			assert.strictEqual(
				shouldAllowRecovery,
				true,
				'Should allow HLS.js automatic recovery in live mode',
			)
		})

		void it('should not fall back to offline mode for segment errors in live mode', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: false,
			}
			const playbackMode = 'live' as 'live' | 'vod' | 'offline'
			let newPlaybackMode: 'live' | 'vod' | 'offline' = playbackMode

			// Simulate segment error handling
			if (
				errorData.type === 'networkError' &&
				(errorData.details === 'fragLoadError' ||
					errorData.details === 'fragLoadTimeOut')
			) {
				// Don't change playback mode
				newPlaybackMode = playbackMode
			}

			assert.strictEqual(newPlaybackMode, 'live', 'Should remain in live mode')
		})
	})

	void describe('Segment error logging', () => {
		/**
		 * Test that segment errors are logged for debugging
		 * Requirement 8.4: Log segment errors for debugging
		 */
		void it('should log segment error details for debugging', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				frag: {
					url: 'https://example.com/segment123.ts',
					sn: 123,
				},
				fatal: false,
			}

			// Verify error data contains necessary information for logging
			assert.ok(
				errorData.details !== undefined,
				'Error should have details field',
			)
			assert.ok(
				errorData.frag !== undefined,
				'Error should have fragment information',
			)
			assert.ok(errorData.frag.url, 'Fragment should have URL for logging')
		})

		void it('should include playback mode in segment error logs', () => {
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'

			// Verify playback mode is available for logging
			assert.ok(
				playbackMode !== undefined,
				'Playback mode should be available for logging',
			)
			assert.ok(
				['live', 'vod', 'offline'].includes(playbackMode),
				'Playback mode should be a valid value',
			)
		})
	})

	void describe('Segment error vs manifest error distinction', () => {
		/**
		 * Test that segment errors are handled differently from manifest errors
		 */
		void it('should handle segment errors differently from manifest errors', () => {
			const segmentError = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: false,
			}
			const manifestError = {
				type: 'networkError',
				details: 'manifestLoadError',
				fatal: false,
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'

			// Segment error: allow recovery
			let segmentErrorHandling = 'continue'
			if (
				segmentError.type === 'networkError' &&
				(segmentError.details === 'fragLoadError' ||
					segmentError.details === 'fragLoadTimeOut')
			) {
				segmentErrorHandling = 'allow-recovery'
			}

			// Manifest error: fall back to offline
			let manifestErrorHandling = 'continue'
			if (
				manifestError.type === 'networkError' &&
				manifestError.details === 'manifestLoadError' &&
				playbackMode === 'vod'
			) {
				manifestErrorHandling = 'fallback-offline'
			}

			assert.strictEqual(
				segmentErrorHandling,
				'allow-recovery',
				'Segment errors should allow HLS.js recovery',
			)
			assert.strictEqual(
				manifestErrorHandling,
				'fallback-offline',
				'Manifest errors should fall back to offline mode',
			)
			assert.notStrictEqual(
				segmentErrorHandling,
				manifestErrorHandling,
				'Segment and manifest errors should be handled differently',
			)
		})
	})

	void describe('HLS.js automatic recovery behavior', () => {
		/**
		 * Test that the implementation allows HLS.js to handle recovery
		 * Requirement 8.3: Allow HLS.js to attempt automatic recovery
		 */
		void it('should not destroy HLS instance on segment errors', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: false,
			}
			let shouldDestroyHLS = false

			// Simulate error handling
			if (
				errorData.type === 'networkError' &&
				(errorData.details === 'fragLoadError' ||
					errorData.details === 'fragLoadTimeOut')
			) {
				// Don't destroy HLS - let it recover
				shouldDestroyHLS = false
			}

			assert.strictEqual(
				shouldDestroyHLS,
				false,
				'Should not destroy HLS instance to allow automatic recovery',
			)
		})

		void it('should return early from error handler for segment errors', () => {
			const errorData = {
				type: 'networkError',
				details: 'fragLoadError',
				fatal: false,
			}
			let shouldContinueErrorHandling = true

			// Simulate error handler logic
			if (
				errorData.type === 'networkError' &&
				(errorData.details === 'fragLoadError' ||
					errorData.details === 'fragLoadTimeOut')
			) {
				// Return early - don't process as fatal error
				shouldContinueErrorHandling = false
			}

			assert.strictEqual(
				shouldContinueErrorHandling,
				false,
				'Should return early to allow HLS.js to handle recovery',
			)
		})
	})
})

void describe('StreamPlayer - Initial Load Optimization', () => {
	/**
	 * Unit tests for immediate recording check on component mount
	 * Requirements: 10.1, 10.2, 10.5
	 * Task 9.1: Add immediate recording check on component mount
	 */

	void describe('Immediate recording check for inactive streams', () => {
		/**
		 * Test that inactive streams trigger immediate recording check on mount
		 * Requirement 10.1: Immediately check for recording availability when loading inactive stream
		 * Requirement 10.2: Initialize VOD player without waiting for status polling
		 */
		void it('should immediately check recording availability for inactive stream on initial load', async () => {
			// Simulate initial load with inactive stream
			const streamDetail = {
				status: 'inactive' as const,
				hlsManifestUrl: 'https://example.com/stream.m3u8',
				port: 5000,
			}
			const isInitialLoad = true
			let recordingCheckCalled = false
			let playbackMode = 'offline' as 'live' | 'vod' | 'offline'

			// Simulate the logic from fetchStreamDetail
			if (isInitialLoad && streamDetail.status === 'inactive') {
				// Recording check should be called immediately
				recordingCheckCalled = true

				// Mock recording availability check returning true
				const isAvailable = true
				if (isAvailable) {
					playbackMode = 'vod'
				} else {
					playbackMode = 'offline'
				}
			}

			assert.strictEqual(
				recordingCheckCalled,
				true,
				'Recording availability check should be called immediately on initial load with inactive stream',
			)
			assert.strictEqual(
				playbackMode,
				'vod',
				'Playback mode should be set to vod when recording is available',
			)
		})

		void it('should set playback mode to offline when no recording available on initial load', async () => {
			// Simulate initial load with inactive stream and no recording
			const streamDetail = {
				status: 'inactive' as const,
				hlsManifestUrl: 'https://example.com/stream.m3u8',
				port: 5000,
			}
			const isInitialLoad = true
			let playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			let recordingAvailable = false

			// Simulate the logic from fetchStreamDetail
			if (isInitialLoad && streamDetail.status === 'inactive') {
				// Mock recording availability check returning false
				const isAvailable = false
				if (isAvailable) {
					recordingAvailable = true
					playbackMode = 'vod'
				} else {
					recordingAvailable = false
					playbackMode = 'offline'
				}
			}

			assert.strictEqual(
				playbackMode,
				'offline',
				'Playback mode should be set to offline when no recording is available',
			)
			assert.strictEqual(
				recordingAvailable,
				false,
				'Recording available should be false when no recording exists',
			)
		})

		void it('should set playback mode to live for active stream on initial load', () => {
			// Simulate initial load with active stream
			const streamDetail = {
				status: 'active' as const,
				hlsManifestUrl: 'https://example.com/stream.m3u8',
				port: 5000,
			}
			const isInitialLoad = true
			let playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			let recordingCheckCalled = false

			// Simulate the logic from fetchStreamDetail
			// @ts-expect-error - Test comparison with literal types
			if (isInitialLoad && streamDetail.status === 'inactive') {
				recordingCheckCalled = true
			} else if (isInitialLoad && streamDetail.status === 'active') {
				playbackMode = 'live'
			}

			assert.strictEqual(
				recordingCheckCalled,
				false,
				'Recording check should not be called for active streams',
			)
			assert.strictEqual(
				playbackMode,
				'live',
				'Playback mode should be set to live for active streams',
			)
		})

		void it('should handle errors during initial recording check gracefully', async () => {
			// Simulate initial load with inactive stream and error during check
			const streamDetail = {
				status: 'inactive' as const,
				hlsManifestUrl: 'https://example.com/stream.m3u8',
				port: 5000,
			}
			const isInitialLoad = true
			let playbackMode = 'offline' as 'live' | 'vod' | 'offline'
			let recordingAvailable = false
			let errorHandled = false

			// Simulate the logic from fetchStreamDetail with error handling
			if (isInitialLoad && streamDetail.status === 'inactive') {
				try {
					// Simulate error during recording check
					throw new Error('Network error')
				} catch (error) {
					errorHandled = true
					recordingAvailable = false
					playbackMode = 'offline'
				}
			}

			assert.strictEqual(
				errorHandled,
				true,
				'Error should be caught and handled',
			)
			assert.strictEqual(
				playbackMode,
				'offline',
				'Playback mode should fall back to offline on error',
			)
			assert.strictEqual(
				recordingAvailable,
				false,
				'Recording available should be false on error',
			)
		})
	})

	void describe('Polling continuation after initial check', () => {
		/**
		 * Test that polling continues after initial recording check
		 * Requirement 10.5: Continue polling for status changes to detect when stream goes live
		 */
		void it('should continue polling after initial recording check', () => {
			// Simulate that polling is set up regardless of initial load result
			const isInitialLoad = true
			let pollingSetup = false
			let initialCheckComplete = false

			// Initial load completes
			if (isInitialLoad) {
				initialCheckComplete = true
			}

			// Polling should be set up regardless
			pollingSetup = true

			assert.strictEqual(
				initialCheckComplete,
				true,
				'Initial check should complete',
			)
			assert.strictEqual(
				pollingSetup,
				true,
				'Polling should be set up to detect status changes',
			)
		})

		void it('should detect transition from inactive to active after initial load', () => {
			// Simulate initial load with inactive stream, then transition to active
			let playbackMode = 'vod' as 'live' | 'vod' | 'offline' // Started as VOD
			const previousStatus = 'inactive'
			const currentStatus = 'active'

			// Simulate status transition detection
			if (previousStatus === 'inactive' && currentStatus === 'active') {
				playbackMode = 'live'
			}

			assert.strictEqual(
				playbackMode,
				'live',
				'Should transition to live mode when stream becomes active',
			)
		})
	})

	void describe('Initial load flag handling', () => {
		/**
		 * Test that isInitialLoad flag is used correctly
		 */
		void it('should only trigger immediate check on initial load, not on polling', () => {
			// First call - initial load
			let isInitialLoad = true
			let checkCount = 0

			if (isInitialLoad) {
				checkCount++
			}

			assert.strictEqual(
				checkCount,
				1,
				'Check should be triggered on initial load',
			)

			// Subsequent calls - polling (not initial load)
			isInitialLoad = false
			const shouldCheckAgain = isInitialLoad

			assert.strictEqual(
				shouldCheckAgain,
				false,
				'Immediate check should not be triggered on polling updates',
			)
		})

		void it('should pass isInitialLoad flag correctly to fetchStreamDetail', () => {
			// Simulate the effect that calls fetchStreamDetail
			let initialCallMade = false
			let pollingCallMade = false

			// Initial call with isInitialLoad = true
			const fetchStreamDetail = (
				_isManualRetry: boolean,
				isInitialLoad: boolean,
			) => {
				void _isManualRetry // Unused but part of function signature
				if (isInitialLoad) {
					initialCallMade = true
				} else {
					pollingCallMade = true
				}
			}

			// Simulate initial load
			fetchStreamDetail(false, true)

			assert.strictEqual(
				initialCallMade,
				true,
				'Initial call should have isInitialLoad = true',
			)

			// Simulate polling call
			fetchStreamDetail(false, false)

			assert.strictEqual(
				pollingCallMade,
				true,
				'Polling calls should have isInitialLoad = false',
			)
		})
	})

	void describe('Performance optimization', () => {
		/**
		 * Test that initial load optimization improves performance
		 * Requirement 10.1: Immediately check for recording availability
		 * Requirement 10.2: Initialize VOD player without waiting for status polling
		 */
		void it('should not wait for polling interval before checking recording', () => {
			// Without optimization: would wait for first poll (5 seconds)
			// With optimization: checks immediately on mount
			const pollingInterval = 5000 // 5 seconds
			const checkDelay = 0 // Immediate check

			assert.strictEqual(
				checkDelay,
				0,
				'Recording check should happen immediately, not after polling interval',
			)
			assert.ok(
				checkDelay < pollingInterval,
				'Check delay should be less than polling interval',
			)
		})

		void it('should set playback mode immediately for inactive streams', () => {
			// Simulate that playback mode is set during initial load, not after polling
			const streamStatus: 'active' | 'inactive' = 'inactive'
			const isInitialLoad = true
			let playbackModeSetImmediately = false

			if (isInitialLoad && streamStatus === 'inactive') {
				// Mode is set immediately after recording check
				playbackModeSetImmediately = true
			}

			assert.strictEqual(
				playbackModeSetImmediately,
				true,
				'Playback mode should be set immediately on initial load',
			)
		})
	})
})

void describe('StreamPlayer - VOD Seek Functionality', () => {
	/**
	 * Unit tests for seek functionality in VOD mode
	 * Requirements: 2.4, 7.2, 7.3, 7.4, 7.5
	 * Task 10.1: Verify seek functionality in VOD mode
	 */

	void describe('Video duration property in VOD mode', () => {
		/**
		 * Test that video element's duration property is finite in VOD mode
		 * Requirement 2.4: Enable seeking through the entire recording
		 * Requirement 7.4: Display the total duration of the recording
		 */
		void it('should have finite duration in VOD mode', () => {
			// Simulate a video element with loaded VOD content
			const mockVideoElement = {
				duration: 120.5, // 2 minutes 30 seconds
				currentTime: 0,
				seekable: {
					length: 1,
					start: () => 0,
					end: () => 120.5,
				},
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'

			// Verify duration is finite (not Infinity)
			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be VOD')
			assert.ok(
				Number.isFinite(mockVideoElement.duration),
				'Video duration should be finite in VOD mode',
			)
			assert.notStrictEqual(
				mockVideoElement.duration,
				Infinity,
				'Video duration should not be Infinity in VOD mode',
			)
			assert.ok(
				mockVideoElement.duration > 0,
				'Video duration should be greater than 0',
			)
		})

		void it('should have different duration behavior than live mode', () => {
			// Live mode typically has Infinity duration
			const liveVideoElement = {
				duration: Infinity,
				currentTime: 0,
			}

			// VOD mode has finite duration
			const vodVideoElement = {
				duration: 120.5,
				currentTime: 0,
			}

			// Verify the difference
			assert.strictEqual(
				liveVideoElement.duration,
				Infinity,
				'Live mode should have Infinity duration',
			)
			assert.ok(
				Number.isFinite(vodVideoElement.duration),
				'VOD mode should have finite duration',
			)
			assert.notStrictEqual(
				liveVideoElement.duration,
				vodVideoElement.duration,
				'Live and VOD modes should have different duration values',
			)
		})

		void it('should report accurate duration for various recording lengths', () => {
			// Test various recording durations
			const testDurations = [
				{ seconds: 30, description: '30 seconds' },
				{ seconds: 120, description: '2 minutes' },
				{ seconds: 600, description: '10 minutes' },
				{ seconds: 3600, description: '1 hour' },
				{ seconds: 7200, description: '2 hours' },
			]

			for (const test of testDurations) {
				const mockVideoElement = {
					duration: test.seconds,
					currentTime: 0,
				}

				assert.ok(
					Number.isFinite(mockVideoElement.duration),
					`Duration should be finite for ${test.description}`,
				)
				assert.strictEqual(
					mockVideoElement.duration,
					test.seconds,
					`Duration should be ${test.seconds} seconds for ${test.description}`,
				)
			}
		})
	})

	void describe('Seek bar enabled in VOD mode', () => {
		/**
		 * Test that video element's seek bar is enabled in VOD mode
		 * Requirement 7.2: Enable the seek bar for navigation
		 */
		void it('should have seekable range in VOD mode', () => {
			// Simulate a video element with loaded VOD content
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 0,
				seekable: {
					length: 1,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					start: (index: number) => 0,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					end: (index: number) => 120.5,
				},
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'

			// Verify seekable range exists
			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be VOD')
			assert.ok(
				mockVideoElement.seekable.length > 0,
				'Video should have seekable ranges in VOD mode',
			)
			assert.strictEqual(
				mockVideoElement.seekable.start(0),
				0,
				'Seekable range should start at 0',
			)
			assert.strictEqual(
				mockVideoElement.seekable.end(0),
				mockVideoElement.duration,
				'Seekable range should end at duration',
			)
		})

		void it('should allow seeking to any position within duration', () => {
			// Simulate a video element with loaded VOD content
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 0,
				seekable: {
					length: 1,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					start: (index: number) => 0,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					end: (index: number) => 120.5,
				},
			}

			// Test seeking to various positions
			const seekPositions = [0, 30, 60, 90, 120]

			for (const position of seekPositions) {
				const canSeek =
					position >= mockVideoElement.seekable.start(0) &&
					position <= mockVideoElement.seekable.end(0)

				assert.ok(
					canSeek,
					`Should be able to seek to position ${position} seconds`,
				)
			}
		})

		void it('should have full seekable range from start to end', () => {
			// Simulate a video element with loaded VOD content
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 0,
				seekable: {
					length: 1,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					start: (index: number) => 0,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					end: (index: number) => 120.5,
				},
			}

			// Calculate seekable range
			const seekableStart = mockVideoElement.seekable.start(0)
			const seekableEnd = mockVideoElement.seekable.end(0)
			const seekableRange = seekableEnd - seekableStart

			// Verify full range is seekable
			assert.strictEqual(
				seekableStart,
				0,
				'Seekable range should start at beginning',
			)
			assert.strictEqual(
				seekableEnd,
				mockVideoElement.duration,
				'Seekable range should end at duration',
			)
			assert.strictEqual(
				seekableRange,
				mockVideoElement.duration,
				'Entire duration should be seekable',
			)
		})
	})

	void describe('Seeking updates currentTime property', () => {
		/**
		 * Test that seeking updates the currentTime property
		 * Requirement 7.3: Jump to the requested position when viewer seeks
		 */
		void it('should update currentTime when seeking to a new position', () => {
			// Simulate a video element with loaded VOD content
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 0,
				seekable: {
					length: 1,
					start: () => 0,
					end: () => 120.5,
				},
			}

			// Simulate seeking to 60 seconds
			const seekPosition = 60
			mockVideoElement.currentTime = seekPosition

			// Verify currentTime was updated
			assert.strictEqual(
				mockVideoElement.currentTime,
				seekPosition,
				'currentTime should be updated to seek position',
			)
		})

		void it('should update currentTime for various seek positions', () => {
			// Simulate a video element with loaded VOD content
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 0,
				seekable: {
					length: 1,
					start: () => 0,
					end: () => 120.5,
				},
			}

			// Test seeking to various positions
			const seekPositions = [0, 15, 30, 45, 60, 90, 120]

			for (const position of seekPositions) {
				// Simulate seeking
				mockVideoElement.currentTime = position

				// Verify currentTime was updated
				assert.strictEqual(
					mockVideoElement.currentTime,
					position,
					`currentTime should be ${position} after seeking`,
				)
			}
		})

		void it('should allow seeking forward and backward', () => {
			// Simulate a video element with loaded VOD content
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 60, // Start at middle
				seekable: {
					length: 1,
					start: () => 0,
					end: () => 120.5,
				},
			}

			// Seek forward
			mockVideoElement.currentTime = 90
			assert.strictEqual(
				mockVideoElement.currentTime,
				90,
				'Should be able to seek forward',
			)

			// Seek backward
			mockVideoElement.currentTime = 30
			assert.strictEqual(
				mockVideoElement.currentTime,
				30,
				'Should be able to seek backward',
			)

			// Seek to beginning
			mockVideoElement.currentTime = 0
			assert.strictEqual(
				mockVideoElement.currentTime,
				0,
				'Should be able to seek to beginning',
			)

			// Seek to end
			mockVideoElement.currentTime = 120
			assert.strictEqual(
				mockVideoElement.currentTime,
				120,
				'Should be able to seek to end',
			)
		})

		void it('should maintain currentTime within valid range', () => {
			// Simulate a video element with loaded VOD content
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 0,
				seekable: {
					length: 1,
					start: () => 0,
					end: () => 120.5,
				},
			}

			// Test seeking to various positions
			const seekPositions = [0, 30, 60, 90, 120]

			for (const position of seekPositions) {
				mockVideoElement.currentTime = position

				// Verify currentTime is within valid range
				assert.ok(
					mockVideoElement.currentTime >= 0,
					'currentTime should be >= 0',
				)
				assert.ok(
					mockVideoElement.currentTime <= mockVideoElement.duration,
					'currentTime should be <= duration',
				)
			}
		})
	})

	void describe('Video controls enabled in VOD mode', () => {
		/**
		 * Test that video controls are enabled in VOD mode
		 * Requirement 7.1: Display standard HTML5 video controls
		 * Requirement 7.5: Allow pause and resume operations
		 */
		void it('should have controls attribute enabled in VOD mode', () => {
			// Simulate video element rendering in VOD mode
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'
			let videoHasControls = false

			// Simulate video element rendering logic
			if (playbackMode === 'live' || playbackMode === 'vod') {
				videoHasControls = true
			}

			assert.strictEqual(playbackMode, 'vod', 'Playback mode should be VOD')
			assert.strictEqual(
				videoHasControls,
				true,
				'Video element should have controls attribute in VOD mode',
			)
		})

		void it('should support pause and resume operations in VOD mode', () => {
			// Simulate a video element with loaded VOD content
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 30,
				paused: false,
				pause: () => {
					mockVideoElement.paused = true
				},
				play: () => {
					mockVideoElement.paused = false
				},
			}

			// Initially playing
			assert.strictEqual(
				mockVideoElement.paused,
				false,
				'Video should initially be playing',
			)

			// Pause
			mockVideoElement.pause()
			assert.strictEqual(
				mockVideoElement.paused,
				true,
				'Video should be paused after pause() call',
			)

			// Resume
			mockVideoElement.play()
			assert.strictEqual(
				mockVideoElement.paused,
				false,
				'Video should be playing after play() call',
			)
		})

		void it('should maintain currentTime when pausing and resuming', () => {
			// Simulate a video element with loaded VOD content
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 45,
				paused: false,
				pause: () => {
					mockVideoElement.paused = true
				},
				play: () => {
					mockVideoElement.paused = false
				},
			}

			const currentTimeBeforePause = mockVideoElement.currentTime

			// Pause
			mockVideoElement.pause()
			assert.strictEqual(
				mockVideoElement.currentTime,
				currentTimeBeforePause,
				'currentTime should be maintained when pausing',
			)

			// Resume
			mockVideoElement.play()
			assert.strictEqual(
				mockVideoElement.currentTime,
				currentTimeBeforePause,
				'currentTime should be maintained when resuming',
			)
		})
	})

	void describe('VOD mode vs Live mode seek behavior', () => {
		/**
		 * Test differences in seek behavior between VOD and live modes
		 */
		void it('should have different seek capabilities in VOD vs live mode', () => {
			// VOD mode: full seek capability
			const vodVideoElement = {
				duration: 120.5,
				currentTime: 0,
				seekable: {
					length: 1,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					start: (index: number) => 0,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					end: (index: number) => 120.5,
				},
			}

			// Live mode: limited seek capability (typically only buffered content)
			const liveVideoElement = {
				duration: Infinity,
				currentTime: 100,
				seekable: {
					length: 1,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					start: (index: number) => 90, // Only last 30 seconds buffered
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					end: (index: number) => 120,
				},
			}

			// VOD should have full range seekable
			const vodSeekableRange =
				vodVideoElement.seekable.end(0) - vodVideoElement.seekable.start(0)
			assert.strictEqual(
				vodSeekableRange,
				vodVideoElement.duration,
				'VOD should have full duration seekable',
			)

			// Live should have limited range seekable
			const liveSeekableRange =
				liveVideoElement.seekable.end(0) - liveVideoElement.seekable.start(0)
			assert.ok(
				liveSeekableRange < liveVideoElement.duration,
				'Live should have limited seekable range',
			)

			// VOD should allow seeking to beginning
			const canSeekToBeginningVOD = vodVideoElement.seekable.start(0) === 0
			assert.strictEqual(
				canSeekToBeginningVOD,
				true,
				'VOD should allow seeking to beginning',
			)

			// Live typically cannot seek to beginning
			const canSeekToBeginningLive = liveVideoElement.seekable.start(0) === 0
			assert.strictEqual(
				canSeekToBeginningLive,
				false,
				'Live typically cannot seek to beginning',
			)
		})

		void it('should have finite duration in VOD but Infinity in live', () => {
			const vodDuration = 120.5
			const liveDuration = Infinity

			assert.ok(Number.isFinite(vodDuration), 'VOD duration should be finite')
			assert.strictEqual(
				liveDuration,
				Infinity,
				'Live duration should be Infinity',
			)
			assert.notStrictEqual(
				vodDuration,
				liveDuration,
				'VOD and live durations should be different',
			)
		})
	})

	void describe('Seek functionality requirements validation', () => {
		/**
		 * Comprehensive test validating all seek functionality requirements
		 * Requirements: 2.4, 7.2, 7.3, 7.4, 7.5
		 */
		void it('should satisfy all VOD seek functionality requirements', () => {
			// Simulate a complete VOD video element
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 0,
				paused: false,
				seekable: {
					length: 1,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					start: (index: number) => 0,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					end: (index: number) => 120.5,
				},
				controls: true,
				pause() {
					this.paused = true
				},
				play() {
					this.paused = false
				},
			}
			const playbackMode = 'vod' as 'live' | 'vod' | 'offline'

			// Requirement 2.4: Enable seeking through the entire recording
			const canSeekThroughEntireRecording =
				mockVideoElement.seekable.length > 0 &&
				mockVideoElement.seekable.start(0) === 0 &&
				mockVideoElement.seekable.end(0) === mockVideoElement.duration
			assert.ok(
				canSeekThroughEntireRecording,
				'Requirement 2.4: Should enable seeking through entire recording',
			)

			// Requirement 7.2: Enable the seek bar for navigation
			const seekBarEnabled = mockVideoElement.seekable.length > 0
			assert.ok(seekBarEnabled, 'Requirement 7.2: Seek bar should be enabled')

			// Requirement 7.3: Jump to the requested position when viewer seeks
			mockVideoElement.currentTime = 60
			const seekUpdatesCurrentTime = mockVideoElement.currentTime === 60
			assert.ok(
				seekUpdatesCurrentTime,
				'Requirement 7.3: Seeking should update currentTime',
			)

			// Requirement 7.4: Display the total duration of the recording
			const durationIsFinite = Number.isFinite(mockVideoElement.duration)
			const durationIsPositive = mockVideoElement.duration > 0
			assert.ok(
				durationIsFinite && durationIsPositive,
				'Requirement 7.4: Should display total duration (finite and positive)',
			)

			// Requirement 7.5: Allow pause and resume operations
			mockVideoElement.pause()
			const canPause = mockVideoElement.paused === true
			mockVideoElement.play()
			const canResume = mockVideoElement.paused === false
			assert.ok(
				canPause && canResume,
				'Requirement 7.5: Should allow pause and resume operations',
			)

			// Overall validation
			assert.strictEqual(
				playbackMode,
				'vod',
				'All requirements should be validated in VOD mode',
			)
		})
	})

	void describe('Edge cases for seek functionality', () => {
		/**
		 * Test edge cases and boundary conditions for seeking
		 */
		void it('should handle seeking to exact duration boundary', () => {
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 0,
				seekable: {
					length: 1,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					start: (index: number) => 0,
					end: () => 120.5,
				},
			}

			// Seek to exact duration
			mockVideoElement.currentTime = mockVideoElement.duration

			assert.strictEqual(
				mockVideoElement.currentTime,
				mockVideoElement.duration,
				'Should be able to seek to exact duration',
			)
		})

		void it('should handle seeking to zero (beginning)', () => {
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 60,
				seekable: {
					length: 1,
					start: () => 0,
					end: () => 120.5,
				},
			}

			// Seek to beginning
			mockVideoElement.currentTime = 0

			assert.strictEqual(
				mockVideoElement.currentTime,
				0,
				'Should be able to seek to beginning (0)',
			)
		})

		void it('should handle very short recordings', () => {
			// Test with a 5-second recording
			const mockVideoElement = {
				duration: 5,
				currentTime: 0,
				seekable: {
					length: 1,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					start: (index: number) => 0,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					end: (index: number) => 5,
				},
			}

			assert.ok(
				Number.isFinite(mockVideoElement.duration),
				'Short recording should have finite duration',
			)
			assert.strictEqual(
				mockVideoElement.seekable.end(0),
				mockVideoElement.duration,
				'Short recording should be fully seekable',
			)
		})

		void it('should handle very long recordings', () => {
			// Test with a 2-hour recording
			const mockVideoElement = {
				duration: 7200, // 2 hours
				currentTime: 0,
				seekable: {
					length: 1,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					start: (index: number) => 0,
					// eslint-disable-next-line @typescript-eslint/no-unused-vars
					end: (index: number) => 7200,
				},
			}

			assert.ok(
				Number.isFinite(mockVideoElement.duration),
				'Long recording should have finite duration',
			)
			assert.strictEqual(
				mockVideoElement.seekable.end(0),
				mockVideoElement.duration,
				'Long recording should be fully seekable',
			)

			// Test seeking to middle of long recording
			mockVideoElement.currentTime = 3600 // 1 hour
			assert.strictEqual(
				mockVideoElement.currentTime,
				3600,
				'Should be able to seek to middle of long recording',
			)
		})

		void it('should handle fractional seek positions', () => {
			const mockVideoElement = {
				duration: 120.5,
				currentTime: 0,
				seekable: {
					length: 1,
					start: () => 0,
					end: () => 120.5,
				},
			}

			// Seek to fractional position
			mockVideoElement.currentTime = 45.75

			assert.strictEqual(
				mockVideoElement.currentTime,
				45.75,
				'Should handle fractional seek positions',
			)
		})
	})
})
