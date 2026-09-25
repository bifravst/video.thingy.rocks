import {
	type GetParametersCommand,
	type GetParametersCommandOutput,
} from '@aws-sdk/client-ssm'
import assert from 'node:assert'
import { describe, it } from 'node:test'
import {
	isStoredSrtpKey,
	isValidSrtpKeyHex,
	keyFingerprint,
	SrtpKeyStore,
} from './SrtpKeyStore.ts'

void describe('SrtpKeyStore', () => {
	void describe('keyFingerprint', () => {
		void it('is stable for the same key', () => {
			const hex = 'aB1'.repeat(20)
			assert.strictEqual(keyFingerprint(hex), keyFingerprint(hex))
		})

		void it('is the same regardless of hex casing (same key bytes)', () => {
			const lower = 'ab12cd34'.repeat(7) + 'abcd'
			const upper = lower.toUpperCase()
			assert.strictEqual(keyFingerprint(lower), keyFingerprint(upper))
		})

		void it('differs for different keys', () => {
			assert.notStrictEqual(
				keyFingerprint('a'.repeat(60)),
				keyFingerprint('b'.repeat(60)),
			)
		})
	})

	void describe('isValidSrtpKeyHex', () => {
		void it('accepts a 60-character hex string (30-byte master key+salt)', () => {
			assert.strictEqual(isValidSrtpKeyHex('a'.repeat(60)), true)
			assert.strictEqual(isValidSrtpKeyHex('aA1bB2'.repeat(10)), true)
		})

		void it('rejects strings of the wrong length', () => {
			assert.strictEqual(isValidSrtpKeyHex('a'.repeat(59)), false)
			assert.strictEqual(isValidSrtpKeyHex('a'.repeat(61)), false)
			assert.strictEqual(isValidSrtpKeyHex(''), false)
		})

		void it('rejects non-hex characters', () => {
			assert.strictEqual(isValidSrtpKeyHex('g'.repeat(60)), false)
			assert.strictEqual(isValidSrtpKeyHex(`${'a'.repeat(59)};`), false)
			assert.strictEqual(
				isValidSrtpKeyHex(`${'a'.repeat(50)}$(rm -rf /)aaa`),
				false,
			)
		})
	})

	void describe('isStoredSrtpKey', () => {
		void it('accepts a valid key with no cipher/auth override', () => {
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: 12345 }),
				true,
			)
		})

		void it('accepts a valid key with the supported cipher/auth suite', () => {
			assert.strictEqual(
				isStoredSrtpKey({
					key: 'a'.repeat(60),
					ssrc: 12345,
					cipher: 'aes-128-icm',
					auth: 'hmac-sha1-80',
				}),
				true,
			)
		})

		void it('rejects a missing or non-string key', () => {
			assert.strictEqual(isStoredSrtpKey({ ssrc: 1 }), false)
			assert.strictEqual(isStoredSrtpKey({ key: 123, ssrc: 1 }), false)
		})

		void it('rejects a non-integer, negative, or out-of-range uint32 ssrc', () => {
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: 1.5 }),
				false,
			)
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: -1 }),
				false,
			)
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: 0x1_0000_0000 }),
				false,
			)
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: '12345' }),
				false,
			)
		})

		void it('accepts the boundary uint32 values for ssrc', () => {
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: 0 }),
				true,
			)
			assert.strictEqual(
				isStoredSrtpKey({ key: 'a'.repeat(60), ssrc: 0xffffffff }),
				true,
			)
		})

		void it('rejects an unsupported cipher or auth suite', () => {
			assert.strictEqual(
				isStoredSrtpKey({
					key: 'a'.repeat(60),
					ssrc: 1,
					cipher: 'aes-256-icm',
				}),
				false,
			)
			assert.strictEqual(
				isStoredSrtpKey({
					key: 'a'.repeat(60),
					ssrc: 1,
					auth: 'hmac-sha1-32',
				}),
				false,
			)
		})
	})

	void describe('loadPorts', () => {
		const PREFIX = '/test/srtp/port'
		const nameFor = (port: number): string => `${PREFIX}/${port}/key`
		/** The port is the 5th path segment: "/test/srtp/port/{port}/key". */
		const portFromName = (name: string): number => Number(name.split('/')[4])
		const validKeyHex = 'ab'.repeat(30) // 60 hex chars

		/** Builds a store around a stub SSM client that records every command. */
		const makeKeyStore = (
			respond: (command: GetParametersCommand) => GetParametersCommandOutput,
		): { keyStore: SrtpKeyStore; commands: GetParametersCommand[] } => {
			const commands: GetParametersCommand[] = []
			const keyStore = new SrtpKeyStore({
				parameterPrefix: PREFIX,
				ssmClient: {
					send: async (command: GetParametersCommand) => {
						commands.push(command)
						return respond(command)
					},
				},
			})
			return { keyStore, commands }
		}

		/** Responds with SecureString parameters for the requested ports. */
		const secureResponder =
			(
				valueFor: (port: number) => string,
			): ((command: GetParametersCommand) => GetParametersCommandOutput) =>
			(command) => ({
				$metadata: {},
				Parameters: (command.input.Names ?? []).map((name) => ({
					Name: name,
					Value: valueFor(portFromName(name)),
					Type: 'SecureString' as const,
				})),
			})

		void it('fetches parameters in 10-name chunks with WithDecryption and populates keys for all ports', async () => {
			const ports = Array.from({ length: 12 }, (_, i) => 6000 + i)
			const { keyStore, commands } = makeKeyStore(
				secureResponder((port) =>
					JSON.stringify({ key: validKeyHex, ssrc: port }),
				),
			)
			await keyStore.loadPorts(ports)

			// 12 ports > SSM's 10-name GetParameters limit -> two chunks.
			assert.strictEqual(commands.length, 2)
			assert.deepStrictEqual(
				commands[0]!.input.Names,
				ports.slice(0, 10).map(nameFor),
			)
			assert.deepStrictEqual(
				commands[1]!.input.Names,
				ports.slice(10).map(nameFor),
			)
			// Keys are SecureString secrets, so decryption must be requested.
			assert.strictEqual(commands[0]!.input.WithDecryption, true)

			for (const port of ports) {
				const key = keyStore.getKeyForPort(port)
				assert.ok(key !== undefined, `no key loaded for port ${port}`)
				assert.strictEqual(key.keyHex, validKeyHex)
				assert.strictEqual(key.ssrc, port)
				// Defaults for the only suite supported end-to-end.
				assert.strictEqual(key.cipher, 'aes-128-icm')
				assert.strictEqual(key.auth, 'hmac-sha1-80')
				assert.strictEqual(key.keyFingerprint, keyFingerprint(validKeyHex))
			}
		})

		void it('rejects parameters that are not SecureString and does not load them', async () => {
			const { keyStore } = makeKeyStore((command) => ({
				$metadata: {},
				Parameters: (command.input.Names ?? []).map((name) => ({
					Name: name,
					Value: JSON.stringify({
						key: validKeyHex,
						ssrc: portFromName(name),
					}),
					// Port 6000 was provisioned without encryption at rest.
					Type: portFromName(name) === 6000 ? 'String' : 'SecureString',
				})),
			}))
			await keyStore.loadPorts([6000, 6001])
			assert.strictEqual(keyStore.hasKeyForPort(6000), false)
			assert.strictEqual(keyStore.hasKeyForPort(6001), true)
		})

		void it('skips missing parameters (InvalidParameters) without loading them', async () => {
			const { keyStore } = makeKeyStore((command) => ({
				$metadata: {},
				Parameters: (command.input.Names ?? [])
					.filter((name) => portFromName(name) !== 6000)
					.map((name) => ({
						Name: name,
						Value: JSON.stringify({ key: validKeyHex, ssrc: 1 }),
						Type: 'SecureString' as const,
					})),
				InvalidParameters: (command.input.Names ?? []).filter(
					(name) => portFromName(name) === 6000,
				),
			}))
			await keyStore.loadPorts([6000, 6001])
			assert.strictEqual(keyStore.hasKeyForPort(6000), false)
			assert.strictEqual(keyStore.hasKeyForPort(6001), true)
		})

		void it('skips malformed or invalid key data and loads the valid ones', async () => {
			const valueByPort = new Map<number, string>([
				[6000, 'not json'],
				[6001, JSON.stringify({ key: validKeyHex, ssrc: 'not-a-number' })],
				// Wrong length and not hex.
				[6002, JSON.stringify({ key: 'zz'.repeat(10), ssrc: 1 })],
				[6003, JSON.stringify({ key: validKeyHex, ssrc: 7 })],
			])
			const { keyStore } = makeKeyStore(
				secureResponder((port) => valueByPort.get(port)!),
			)
			await keyStore.loadPorts([6000, 6001, 6002, 6003])
			assert.strictEqual(keyStore.hasKeyForPort(6000), false)
			assert.strictEqual(keyStore.hasKeyForPort(6001), false)
			assert.strictEqual(keyStore.hasKeyForPort(6002), false)
			const key = keyStore.getKeyForPort(6003)
			assert.ok(key !== undefined)
			assert.strictEqual(key.ssrc, 7)
		})

		void it('does not call SSM for an empty port list', async () => {
			const { keyStore, commands } = makeKeyStore(secureResponder(() => ''))
			await keyStore.loadPorts([])
			assert.strictEqual(commands.length, 0)
		})
	})
})

void describe('keyedPorts', () => {
	void it('is empty before anything is loaded', () => {
		const store = new SrtpKeyStore({
			parameterPrefix: '/test/srtp/port',
			ssmClient: { send: async () => ({ $metadata: {}, Parameters: [] }) },
		})
		assert.deepStrictEqual(store.keyedPorts(), [])
	})

	// loadPorts resolving does not mean every port is keyed - it skips missing,
	// non-SecureString and malformed parameters individually.
	void it('lists only the ports that resolved a usable key, in order', async () => {
		const store = new SrtpKeyStore({
			parameterPrefix: '/test/srtp/port',
			ssmClient: {
				send: async () => ({
					$metadata: {},
					Parameters: [
						{
							Name: '/test/srtp/port/6002/key',
							Type: 'SecureString',
							Value: JSON.stringify({ key: 'a'.repeat(60), ssrc: 1 }),
						},
						{
							Name: '/test/srtp/port/6000/key',
							Type: 'SecureString',
							Value: JSON.stringify({ key: 'b'.repeat(60), ssrc: 2 }),
						},
						{
							// Plain String parameters are rejected: the key would not be
							// encrypted at rest.
							Name: '/test/srtp/port/6001/key',
							Type: 'String',
							Value: JSON.stringify({ key: 'c'.repeat(60), ssrc: 3 }),
						},
					],
					InvalidParameters: ['/test/srtp/port/6003/key'],
				}),
			},
		})
		await store.loadPorts([6000, 6001, 6002, 6003])
		assert.deepStrictEqual(store.keyedPorts(), [6000, 6002])
	})
})
