import {
	DescribeTargetHealthCommand,
	ElasticLoadBalancingV2Client,
} from '@aws-sdk/client-elastic-load-balancing-v2'

const elbv2 = new ElasticLoadBalancingV2Client({})

/** How often the readiness check polls target-group health. */
const POLL_INTERVAL_MS = 15_000
/**
 * How long to wait for the fleet to become healthy before failing the deployment (the
 * Lambda's own timeout is 10 minutes - this deadline must stay below it so the failure
 * response still gets sent).
 */
const READINESS_TIMEOUT_MS = 9 * 60_000

type CloudFormationCustomResourceEvent = {
	RequestType: 'Create' | 'Update' | 'Delete'
	ResponseURL: string
	StackId: string
	RequestId: string
	LogicalResourceId: string
	PhysicalResourceId?: string
	ResourceProperties: {
		TargetGroupArns?: string[]
		MinHealthyTargets?: number | string
	}
}

/**
 * Deployment-time readiness gate for the ingest fleet's generation cutover (see
 * cdk/StreamingStack.ts's FleetCutoverReadinessCR): blocks the NLB listeners' update
 * until every target group of the new fleet generation reports at least
 * MinHealthyTargets healthy targets - the same TCP health check the NLB itself uses -
 * so the cutover only flips traffic to instances that are genuinely ready to ingest,
 * and fails (rolling the update back) instead of cutting over into a fleet that never
 * becomes healthy.
 */
export const handler = async (
	event: CloudFormationCustomResourceEvent,
): Promise<void> => {
	if (event.RequestType === 'Delete') {
		await respond(event, 'SUCCESS')
		return
	}

	const targetGroupArns = event.ResourceProperties.TargetGroupArns ?? []
	const minHealthyTargets = Number(
		event.ResourceProperties.MinHealthyTargets ?? 0,
	)
	if (targetGroupArns.length === 0 || minHealthyTargets <= 0) {
		await respond(
			event,
			'FAILED',
			'ResourceProperties must include TargetGroupArns and a positive MinHealthyTargets',
		)
		return
	}

	const deadline = Date.now() + READINESS_TIMEOUT_MS
	const unhealthy = new Set<string>()
	for (;;) {
		unhealthy.clear()
		await Promise.all(
			targetGroupArns.map(async (targetGroupArn) => {
				const { TargetHealthDescriptions } = await elbv2.send(
					new DescribeTargetHealthCommand({ TargetGroupArn: targetGroupArn }),
				)
				const healthyCount =
					TargetHealthDescriptions?.filter(
						(description) => description.TargetHealth?.State === 'healthy',
					).length ?? 0
				if (healthyCount < minHealthyTargets) {
					unhealthy.add(targetGroupArn)
				}
			}),
		)
		if (unhealthy.size === 0) {
			console.log(
				`All ${targetGroupArns.length} target groups report at least ${minHealthyTargets} healthy targets`,
			)
			await respond(event, 'SUCCESS')
			return
		}
		if (Date.now() > deadline) {
			await respond(
				event,
				'FAILED',
				`Timed out waiting for fleet readiness: ${unhealthy.size} of ${targetGroupArns.length} target groups still have fewer than ${minHealthyTargets} healthy targets (${Array.from(unhealthy).join(', ')}). The old fleet keeps serving; fix the new one and retry the deployment.`,
			)
			return
		}
		console.log(
			`Waiting for fleet readiness: ${unhealthy.size} of ${targetGroupArns.length} target groups still below ${minHealthyTargets} healthy targets`,
		)
		await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS))
	}
}

/**
 * Responds to CloudFormation's pre-signed URL. Implemented with fetch (Node >= 18)
 * rather than a cfn-response helper: no extra dependency, and the protocol is just an
 * HTTPS PUT of the response JSON.
 */
const respond = async (
	event: CloudFormationCustomResourceEvent,
	status: 'SUCCESS' | 'FAILED',
	reason?: string,
): Promise<void> => {
	const body = JSON.stringify({
		Status: status,
		Reason:
			reason ??
			`${status} - see the FleetCutoverReadiness Lambda's CloudWatch Logs`,
		PhysicalResourceId: event.PhysicalResourceId ?? event.LogicalResourceId,
		StackId: event.StackId,
		RequestId: event.RequestId,
		LogicalResourceId: event.LogicalResourceId,
		Data: {},
	})
	const res = await fetch(event.ResponseURL, {
		method: 'PUT',
		// The pre-signed URL's signature does not cover a content-type - sending one
		// would break it, so the body is sent without the header.
		body,
	})
	if (!res.ok) {
		throw new Error(
			`Failed to respond to CloudFormation (${res.status}): ${await res.text()}`,
		)
	}
}
