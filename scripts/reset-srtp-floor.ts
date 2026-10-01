/**
 * Resets the replay floor of one SRTP ingest port, so a sender that restarts its
 * packet numbering is not dropped as a replay of its own earlier session.
 *
 * Usage: node --experimental-transform-types scripts/reset-srtp-floor.ts <host> <port> <floor> [keyFile]
 *
 * <host>    the ingest endpoint (the NLB DNS name)
 * <port>    SRTP ingest port (6000-6009)
 * <floor>   the new floor: the packet index to count on from (required)
 * [keyFile] file holding the port's 60-hex-character key; read from stdin when omitted
 *
 * The key is read from a file or stdin, never an argument (see provision-srtp-key.sh),
 * and is never sent: the request carries an HMAC derived from it. A client does the
 * same with signFloorReset - see README.md.
 */
import { readFileSync } from 'node:fs'
import { signFloorReset } from '../backend/src/SrtpFloorResetServer.ts'

const [host, portArg, floorArg, keyFile] = process.argv.slice(2)

if (
	host === undefined ||
	portArg === undefined ||
	floorArg === undefined ||
	!/^600[0-9]$/.test(portArg) ||
	!/^[0-9]+$/.test(floorArg) ||
	!Number.isSafeInteger(Number(floorArg))
) {
	console.error(
		'Usage: reset-srtp-floor.ts <host> <port 6000-6009> <floor> [keyFile]',
	)
	process.exit(1)
}
const port = Number(portArg)
const floor = Number(floorArg)

const keyHex = readFileSync(keyFile ?? 0, 'utf8').trim()
if (!/^[0-9a-fA-F]{60}$/.test(keyHex)) {
	console.error('Error: the key must be exactly 60 hex characters')
	process.exit(1)
}

const timestamp = Date.now()
const response = await fetch(`http://${host}:8080/srtp/${port}/floor-reset`, {
	method: 'POST',
	headers: {
		'x-timestamp': String(timestamp),
		'x-signature': signFloorReset(keyHex, port, timestamp, floor),
	},
	body: JSON.stringify({ floor }),
})
console.log(response.status, await response.text())
process.exit(response.ok ? 0 : 1)
