import assert from 'node:assert'
import { describe, it, mock } from 'node:test'
import { KinesisVideoSender } from './KinesisVideoSender.ts'

void describe('KinesisVideoSender', () => {
	void describe('getPutMediaEndpoint', () => {
		void it('calls GetDataEndpoint and returns endpoint with /putMedia', async () => {
			const sendMock = mock.fn(async () => ({
				DataEndpoint: 'https://xxx.kinesisvideo.eu-central-1.amazonaws.com',
			}))
			const sender = new KinesisVideoSender({
				streamName: 'test-stream',
				region: 'eu-central-1',
			})
			;(
				sender as unknown as {
					client: { send: (c: unknown) => Promise<unknown> }
				}
			).client = {
				send: sendMock as (c: unknown) => Promise<unknown>,
			}

			const endpoint = await sender.getPutMediaEndpoint()

			assert.strictEqual(sendMock.mock.calls.length, 1)
			const call = sendMock.mock.calls[0]
			assert.ok(call !== undefined)
			const firstArg = (call as { arguments: unknown[] }).arguments[0]
			const cmd = firstArg as { input: { StreamName: string; APIName: string } }
			assert.strictEqual(cmd.input.StreamName, 'test-stream')
			assert.strictEqual(cmd.input.APIName, 'PUT_MEDIA')
			assert.strictEqual(
				endpoint,
				'https://xxx.kinesisvideo.eu-central-1.amazonaws.com/putMedia',
			)
		})

		void it('appends /putMedia without double slash when endpoint has trailing slash', async () => {
			const sendMock = mock.fn(async () => ({
				DataEndpoint: 'https://yyy.kinesisvideo.eu-central-1.amazonaws.com/',
			}))
			const sender = new KinesisVideoSender({
				streamName: 'test-stream-2',
				region: 'eu-central-1',
			})
			;(
				sender as unknown as {
					client: { send: (c: unknown) => Promise<unknown> }
				}
			).client = {
				send: sendMock as (c: unknown) => Promise<unknown>,
			}

			const endpoint = await sender.getPutMediaEndpoint()

			assert.strictEqual(
				endpoint,
				'https://yyy.kinesisvideo.eu-central-1.amazonaws.com/putMedia',
			)
		})

		void it('caches endpoint until invalidateEndpoint', async () => {
			const sendMock = mock.fn(async () => ({
				DataEndpoint:
					'https://cache-test.kinesisvideo.eu-central-1.amazonaws.com',
			}))
			const sender = new KinesisVideoSender({
				streamName: 'cache-stream',
				region: 'eu-central-1',
			})
			;(
				sender as unknown as {
					client: { send: (c: unknown) => Promise<unknown> }
				}
			).client = {
				send: sendMock as (c: unknown) => Promise<unknown>,
			}

			await sender.getPutMediaEndpoint()
			await sender.getPutMediaEndpoint()
			assert.strictEqual(sendMock.mock.calls.length, 1)

			sender.invalidateEndpoint()
			await sender.getPutMediaEndpoint()
			assert.strictEqual(sendMock.mock.calls.length, 2)
		})
	})
})
