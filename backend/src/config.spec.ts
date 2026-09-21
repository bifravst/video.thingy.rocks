import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import { loadConfig } from './config.ts'

void describe('loadConfig', () => {
	void describe('Kinesis ingestion', () => {
		void it('is disabled when no stream prefix is set', () => {
			const config = loadConfig({})
			assert.strictEqual(config.kinesisIngestionEnabled, false)
			assert.strictEqual(config.kinesisStreamPrefix, '')
		})

		void it('is disabled when the stream prefix is empty', () => {
			assert.strictEqual(
				loadConfig({ KINESIS_STREAM_PREFIX: '' }).kinesisIngestionEnabled,
				false,
			)
		})

		void it('is enabled by the presence of a stream prefix', () => {
			const config = loadConfig({ KINESIS_STREAM_PREFIX: 'my-stack-video' })
			assert.strictEqual(config.kinesisIngestionEnabled, true)
			assert.strictEqual(config.kinesisStreamPrefix, 'my-stack-video')
		})
	})

	void describe('opt-in flags', () => {
		void it('accepts the documented enabled values', () => {
			for (const value of ['true', '1']) {
				assert.strictEqual(
					loadConfig({ KINESIS_INGESTION_LOG_GSTREAMER: value })
						.kinesisLogGstreamerOutput,
					true,
					value,
				)
			}
		})

		// Boolean(envValue) would enable the flag for every one of these, because a
		// non-empty string is truthy - the bug this parser exists to avoid.
		void it('treats "false", "0" and other values as disabled', () => {
			for (const value of ['false', '0', 'no', 'off', '', 'TRUE']) {
				assert.strictEqual(
					loadConfig({ KINESIS_INGESTION_LOG_GSTREAMER: value })
						.kinesisLogGstreamerOutput,
					false,
					value,
				)
			}
		})

		void it('is disabled when unset', () => {
			assert.strictEqual(loadConfig({}).kinesisLogGstreamerOutput, false)
		})
	})

	void describe('start threshold', () => {
		void it('defaults to 10 MB', () => {
			assert.strictEqual(loadConfig({}).kinesisMinBytesBeforeStart, 10485760)
		})

		void it('reads megabytes from the environment', () => {
			assert.strictEqual(
				loadConfig({ KINESIS_MIN_BYTES_BEFORE_START: '2' })
					.kinesisMinBytesBeforeStart,
				2097152,
			)
		})

		void it('allows zero, so a test can start on the first packet', () => {
			assert.strictEqual(
				loadConfig({ KINESIS_MIN_BYTES_BEFORE_START: '0' })
					.kinesisMinBytesBeforeStart,
				0,
			)
		})

		// Number('abc') is NaN, and every "buffered >= threshold" comparison against NaN
		// is false, so the pipeline would silently never start.
		void it('throws on a non-numeric or negative threshold', () => {
			assert.throws(
				() => loadConfig({ KINESIS_MIN_BYTES_BEFORE_START: 'abc' }),
				/KINESIS_MIN_BYTES_BEFORE_START/,
			)
			assert.throws(
				() => loadConfig({ KINESIS_MIN_BYTES_BEFORE_START: '-1' }),
				/KINESIS_MIN_BYTES_BEFORE_START/,
			)
		})
	})

	void describe('SRTP', () => {
		void it('is absent unless a key parameter prefix is set', () => {
			assert.strictEqual(loadConfig({}).srtp, undefined)
			assert.strictEqual(
				loadConfig({ SRTP_KEY_PARAMETER_PREFIX: '' }).srtp,
				undefined,
			)
		})

		void it('defaults to ports 6000-6009', () => {
			const srtp = loadConfig({
				SRTP_KEY_PARAMETER_PREFIX: '/stack/srtp/port',
			}).srtp
			assert.deepStrictEqual(srtp?.portRange, { start: 6000, end: 6009 })
			assert.strictEqual(srtp?.keyParameterPrefix, '/stack/srtp/port')
		})

		void it('reads an explicit range', () => {
			const srtp = loadConfig({
				SRTP_KEY_PARAMETER_PREFIX: '/stack/srtp/port',
				SRTP_PORT_RANGE_START: '7000',
				SRTP_PORT_RANGE_END: '7004',
			}).srtp
			assert.deepStrictEqual(srtp?.portRange, { start: 7000, end: 7004 })
		})

		void it('accepts a single-port range', () => {
			const srtp = loadConfig({
				SRTP_KEY_PARAMETER_PREFIX: '/stack/srtp/port',
				SRTP_PORT_RANGE_START: '6000',
				SRTP_PORT_RANGE_END: '6000',
			}).srtp
			assert.deepStrictEqual(srtp?.portRange, { start: 6000, end: 6000 })
		})

		void it('throws on an inverted range', () => {
			assert.throws(
				() =>
					loadConfig({
						SRTP_KEY_PARAMETER_PREFIX: '/stack/srtp/port',
						SRTP_PORT_RANGE_START: '6009',
						SRTP_PORT_RANGE_END: '6000',
					}),
				/must not be below/,
			)
		})

		// An overlapping range cannot bind twice, and would make the transport of a
		// received datagram ambiguous.
		void it('throws when the range overlaps the unencrypted ports', () => {
			for (const [start, end] of [
				['5000', '5009'],
				['4990', '5000'],
				['5009', '5020'],
				['4000', '9000'],
			]) {
				assert.throws(
					() =>
						loadConfig({
							SRTP_KEY_PARAMETER_PREFIX: '/stack/srtp/port',
							SRTP_PORT_RANGE_START: start,
							SRTP_PORT_RANGE_END: end,
						}),
					/must not overlap/,
					`${start}-${end}`,
				)
			}
		})

		void it('accepts a range adjacent to the unencrypted ports', () => {
			const srtp = loadConfig({
				SRTP_KEY_PARAMETER_PREFIX: '/stack/srtp/port',
				SRTP_PORT_RANGE_START: '5010',
				SRTP_PORT_RANGE_END: '5019',
			}).srtp
			assert.deepStrictEqual(srtp?.portRange, { start: 5010, end: 5019 })
		})

		void it('throws on a non-canonical or out-of-range port', () => {
			for (const value of ['abc', '06000', '-1', '70000', '60.5', '']) {
				assert.throws(
					() =>
						loadConfig({
							SRTP_KEY_PARAMETER_PREFIX: '/stack/srtp/port',
							SRTP_PORT_RANGE_START: value,
						}),
					/SRTP_PORT_RANGE_START/,
					value,
				)
			}
		})

		// SRTP does not have to be the same size as the unencrypted range: each port
		// gets its own stream, so there is nothing to pair up.
		void it('does not require the two ranges to be the same length', () => {
			const srtp = loadConfig({
				SRTP_KEY_PARAMETER_PREFIX: '/stack/srtp/port',
				SRTP_PORT_RANGE_START: '6000',
				SRTP_PORT_RANGE_END: '6002',
			}).srtp
			assert.deepStrictEqual(srtp?.portRange, { start: 6000, end: 6002 })
		})
	})

	void describe('defaults carried over from the previous inline config', () => {
		void it('keeps the unencrypted range, table, region and directories', () => {
			const config = loadConfig({})
			assert.deepStrictEqual(config.portRange, { start: 5000, end: 5009 })
			assert.strictEqual(config.dynamoDBTableName, 'StreamMetadata')
			assert.strictEqual(config.awsRegion, 'eu-central-1')
			assert.strictEqual(config.outputDirectory, '/tmp/video-streams')
			assert.strictEqual(config.inactivityTimeout, 60000)
		})

		void it('reads the overridable ones from the environment', () => {
			const config = loadConfig({
				TABLE_NAME: 'OtherTable',
				AWS_REGION: 'eu-west-1',
				OUTPUT_DIR: '/var/video-streams',
			})
			assert.strictEqual(config.dynamoDBTableName, 'OtherTable')
			assert.strictEqual(config.awsRegion, 'eu-west-1')
			assert.strictEqual(config.outputDirectory, '/var/video-streams')
		})
	})
})
