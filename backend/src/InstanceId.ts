/**
 * Resolves the EC2 instance ID from IMDS (on EC2) or from the INSTANCE_ID env var.
 * Falls back to 'local' when not on EC2 (e.g. development).
 */
export const resolveInstanceId = async (): Promise<string> => {
	const fromEnv = process.env.INSTANCE_ID
	if (fromEnv !== undefined && fromEnv !== '') {
		return fromEnv
	}

	// Try IMDSv2 (required when requireImdsv2 is true on the launch template)
	try {
		const tokenRes = await fetch('http://169.254.169.254/latest/api/token', {
			method: 'PUT',
			headers: {
				'X-aws-ec2-metadata-token-ttl-seconds': '60',
			},
			signal: AbortSignal.timeout(2000),
		})
		if (!tokenRes.ok) {
			return 'local'
		}
		const token = await tokenRes.text()
		const instanceRes = await fetch(
			'http://169.254.169.254/latest/meta-data/instance-id',
			{
				headers: {
					'X-aws-ec2-metadata-token': token,
				},
				signal: AbortSignal.timeout(2000),
			},
		)
		if (!instanceRes.ok) {
			return 'local'
		}
		return (await instanceRes.text()).trim()
	} catch {
		return 'local'
	}
}
