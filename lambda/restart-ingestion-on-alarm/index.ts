import {
	AutoScalingClient,
	DescribeAutoScalingGroupsCommand,
} from '@aws-sdk/client-auto-scaling'
import { EC2Client, RebootInstancesCommand } from '@aws-sdk/client-ec2'

const autoscaling = new AutoScalingClient({})
const ec2 = new EC2Client({})

type CloudWatchAlarmMessage = {
	AlarmName?: string
	NewStateValue?: 'ALARM' | 'OK' | 'INSUFFICIENT_DATA'
	NewStateReason?: string
}

/**
 * Triggered by SNS when UDPTrafficNoKinesisIngestionAlarm fires (after 10 min).
 * Reboots all EC2 instances in the ingestion Auto Scaling Group.
 */
export const handler = async (event: {
	Records?: Array<{ Sns?: { Message?: string } }>
}): Promise<{ rebooted: string[]; skipped: boolean }> => {
	const asgName = process.env.AUTO_SCALING_GROUP_NAME
	if (asgName === undefined || asgName === '') {
		throw new Error('AUTO_SCALING_GROUP_NAME environment variable is required')
	}

	const record = event.Records?.[0]
	const raw = record?.Sns?.Message
	if (raw === undefined) {
		return { rebooted: [], skipped: true }
	}

	let message: CloudWatchAlarmMessage
	try {
		message = JSON.parse(raw) as CloudWatchAlarmMessage
	} catch {
		return { rebooted: [], skipped: true }
	}

	if (message.NewStateValue !== 'ALARM') {
		return { rebooted: [], skipped: true }
	}

	const groups = await autoscaling.send(
		new DescribeAutoScalingGroupsCommand({
			AutoScalingGroupNames: [asgName],
		}),
	)
	const group = groups.AutoScalingGroups?.[0]
	const instanceIds = (
		group?.Instances?.map((i: { InstanceId?: string }) => i.InstanceId) ?? []
	).filter((id): id is string => id != null)

	if (instanceIds === undefined || instanceIds.length === 0) {
		console.log('No instances in ASG to reboot')
		return { rebooted: [], skipped: false }
	}

	await ec2.send(
		new RebootInstancesCommand({
			InstanceIds: instanceIds,
		}),
	)
	console.log('Rebooted instances:', instanceIds.join(', '))
	return { rebooted: instanceIds, skipped: false }
}
