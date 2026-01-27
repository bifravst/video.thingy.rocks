import assert from 'node:assert'
import { describe, it, mock } from 'node:test'
import { Logger } from './Logger.ts'

void describe('Logger', () => {
	void describe('Logging output format', () => {
		void it('should log INFO messages with correct format', () => {
			const logger = new Logger('TestService')

			// Mock console.log
			const logMock = mock.method(console, 'log', () => {})

			logger.info('Test message', { port: 5000 })

			assert.strictEqual(logMock.mock.calls.length, 1)
			const loggedMessage = logMock.mock.calls[0]?.arguments[0] as string
			const parsed = JSON.parse(loggedMessage)

			assert.strictEqual(parsed.level, 'INFO')
			assert.strictEqual(parsed.service, 'TestService')
			assert.strictEqual(parsed.message, 'Test message')
			assert.strictEqual(parsed.port, 5000)
			assert.ok(parsed.timestamp !== undefined)

			logMock.mock.restore()
		})

		void it('should log WARN messages with correct format', () => {
			const logger = new Logger('TestService')

			// Mock console.warn
			const warnMock = mock.method(console, 'warn', () => {})

			logger.warn('Warning message', { port: 5001, retryCount: 2 })

			assert.strictEqual(warnMock.mock.calls.length, 1)
			const loggedMessage = warnMock.mock.calls[0]?.arguments[0] as string
			const parsed = JSON.parse(loggedMessage)

			assert.strictEqual(parsed.level, 'WARN')
			assert.strictEqual(parsed.service, 'TestService')
			assert.strictEqual(parsed.message, 'Warning message')
			assert.strictEqual(parsed.port, 5001)
			assert.strictEqual(parsed.retryCount, 2)

			warnMock.mock.restore()
		})

		void it('should log ERROR messages with stack traces', () => {
			const logger = new Logger('TestService')

			// Mock console.error
			const errorMock = mock.method(console, 'error', () => {})

			const testError = new Error('Test error')
			logger.error('Error occurred', testError, { port: 5002 })

			assert.strictEqual(errorMock.mock.calls.length, 1)
			const loggedMessage = errorMock.mock.calls[0]?.arguments[0] as string
			const parsed = JSON.parse(loggedMessage)

			assert.strictEqual(parsed.level, 'ERROR')
			assert.strictEqual(parsed.service, 'TestService')
			assert.strictEqual(parsed.message, 'Error occurred')
			assert.strictEqual(parsed.port, 5002)
			assert.ok(parsed.error !== undefined)
			assert.strictEqual(parsed.error.message, 'Test error')
			assert.ok(parsed.error.stack !== undefined)
			assert.strictEqual(parsed.error.name, 'Error')

			errorMock.mock.restore()
		})

		void it('should include port, timestamp, and stream identifier in logs', () => {
			const logger = new Logger('TestService')

			// Mock console.log
			const logMock = mock.method(console, 'log', () => {})

			const testTimestamp = new Date('2024-01-01T00:00:00Z')
			logger.info('Test message', {
				port: 5003,
				timestamp: testTimestamp,
				streamIdentifier: 'stream-5003',
			})

			assert.strictEqual(logMock.mock.calls.length, 1)
			const loggedMessage = logMock.mock.calls[0]?.arguments[0] as string
			const parsed = JSON.parse(loggedMessage)

			assert.strictEqual(parsed.port, 5003)
			assert.strictEqual(parsed.timestamp, '2024-01-01T00:00:00.000Z')
			assert.strictEqual(parsed.streamIdentifier, 'stream-5003')

			logMock.mock.restore()
		})

		void it('should format Date objects in context as ISO strings', () => {
			const logger = new Logger('TestService')

			// Mock console.log
			const logMock = mock.method(console, 'log', () => {})

			const testDate = new Date('2024-01-01T12:00:00Z')
			logger.info('Test message', {
				startTime: testDate,
				endTime: testDate,
			})

			assert.strictEqual(logMock.mock.calls.length, 1)
			const loggedMessage = logMock.mock.calls[0]?.arguments[0] as string
			const parsed = JSON.parse(loggedMessage)

			assert.strictEqual(parsed.startTime, '2024-01-01T12:00:00.000Z')
			assert.strictEqual(parsed.endTime, '2024-01-01T12:00:00.000Z')

			logMock.mock.restore()
		})

		void it('should handle logging without context', () => {
			const logger = new Logger('TestService')

			// Mock console.log
			const logMock = mock.method(console, 'log', () => {})

			logger.info('Simple message')

			assert.strictEqual(logMock.mock.calls.length, 1)
			const loggedMessage = logMock.mock.calls[0]?.arguments[0] as string
			const parsed = JSON.parse(loggedMessage)

			assert.strictEqual(parsed.level, 'INFO')
			assert.strictEqual(parsed.service, 'TestService')
			assert.strictEqual(parsed.message, 'Simple message')
			assert.ok(parsed.timestamp !== undefined)

			logMock.mock.restore()
		})

		void it('should handle error logging without Error object', () => {
			const logger = new Logger('TestService')

			// Mock console.error
			const errorMock = mock.method(console, 'error', () => {})

			logger.error('Error without exception', undefined, { port: 5004 })

			assert.strictEqual(errorMock.mock.calls.length, 1)
			const loggedMessage = errorMock.mock.calls[0]?.arguments[0] as string
			const parsed = JSON.parse(loggedMessage)

			assert.strictEqual(parsed.level, 'ERROR')
			assert.strictEqual(parsed.message, 'Error without exception')
			assert.strictEqual(parsed.port, 5004)
			assert.strictEqual(parsed.error, undefined)

			errorMock.mock.restore()
		})
	})

	void describe('Context formatting', () => {
		void it('should handle nested objects in context', () => {
			const logger = new Logger('TestService')

			// Mock console.log
			const logMock = mock.method(console, 'log', () => {})

			logger.info('Test message', {
				port: 5005,
				metadata: {
					key1: 'value1',
					key2: 'value2',
				},
			})

			assert.strictEqual(logMock.mock.calls.length, 1)
			const loggedMessage = logMock.mock.calls[0]?.arguments[0] as string
			const parsed = JSON.parse(loggedMessage)

			assert.strictEqual(parsed.port, 5005)
			assert.deepStrictEqual(parsed.metadata, {
				key1: 'value1',
				key2: 'value2',
			})

			logMock.mock.restore()
		})

		void it('should handle Error objects in context', () => {
			const logger = new Logger('TestService')

			// Mock console.log
			const logMock = mock.method(console, 'log', () => {})

			const testError = new Error('Context error')
			logger.info('Test message', {
				previousError: testError,
			})

			assert.strictEqual(logMock.mock.calls.length, 1)
			const loggedMessage = logMock.mock.calls[0]?.arguments[0] as string
			const parsed = JSON.parse(loggedMessage)

			assert.ok(parsed.previousError !== undefined)
			assert.strictEqual(parsed.previousError.message, 'Context error')
			assert.ok(parsed.previousError.stack !== undefined)

			logMock.mock.restore()
		})
	})
})
