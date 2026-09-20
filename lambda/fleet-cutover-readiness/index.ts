import {
	DescribeTargetHealthCommand,
	ElasticLoadBalancingV2Client,
} from '@aws-sdk/client-elastic-load-balancing-v2'

const elbv2 = new ElasticLoadBalancingV2Client({})

type FleetReadinessEvent = {
	RequestType: 'Create' | 'Update' | 'Delete'
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
 * so the cutover only flips traffic to instances that are genuinely ready to ingest.
 *
 * Driven by the CDK custom-resource provider framework: `onEvent` runs once per
 * lifecycle event and validates the properties; `isComplete` is polled (once a minute
 * by the framework's waiter) until it reports complete or the framework's total
 * timeout is reached. The framework sends CloudFormation the SUCCESS/FAILED responses
 * itself - including FAILED on a thrown error or timeout - so the update can never
 * hang waiting for a response.
 */

/** The provider framework's onEvent handler: validates the resource properties (a
 * malformed template fails the deployment immediately) and starts the wait. */
export const onEvent = async (
	event: FleetReadinessEvent,
): Promise<Record<string, never>> => {
	if (event.RequestType === 'Delete') {
		// Nothing to verify when the gate's own resources are being removed.
		return {}
	}
	const targetGroupArns = event.ResourceProperties.TargetGroupArns ?? []
	const minHealthyTargets = Number(
		event.ResourceProperties.MinHealthyTargets ?? 0,
	)
	// Number() happily yields NaN or Infinity for malformed input, and NaN passes a
	// naive `<= 0` check while making every `healthyCount < NaN` comparison false -
	// a malformed property would report the fleet ready without validating anything.
	// Require a positive integer.
	if (
		targetGroupArns.length === 0 ||
		!Number.isInteger(minHealthyTargets) ||
		minHealthyTargets <= 0
	) {
		throw new Error(
			'ResourceProperties must include TargetGroupArns and a positive integer MinHealthyTargets',
		)
	}
	return {}
}

/** The provider framework's isComplete handler, polled by the waiter state machine
 * until it reports complete (or the total timeout fails the deployment). */
export const isComplete = async (
	event: FleetReadinessEvent,
): Promise<{
	IsComplete: boolean
	Data?: { HealthyTargetGroups: number }
}> => {
	if (event.RequestType === 'Delete') {
		return { IsComplete: true }
	}
	const targetGroupArns = event.ResourceProperties.TargetGroupArns ?? []
	const minHealthyTargets = Number(
		event.ResourceProperties.MinHealthyTargets ?? 0,
	)
	// Re-validated here (not just in onEvent): a NaN/Infinity threshold would make
	// every health comparison below false and report the fleet ready without checking
	// anything. Throwing makes the provider framework send CloudFormation a bounded
	// FAILED response.
	if (
		!Number.isInteger(minHealthyTargets) ||
		minHealthyTargets <= 0 ||
		targetGroupArns.length === 0
	) {
		throw new Error(
			'ResourceProperties must include TargetGroupArns and a positive integer MinHealthyTargets',
		)
	}

	const unhealthy: string[] = []
	for (const targetGroupArn of targetGroupArns) {
		try {
			const { TargetHealthDescriptions } = await elbv2.send(
				new DescribeTargetHealthCommand({ TargetGroupArn: targetGroupArn }),
			)
			const healthyCount =
				TargetHealthDescriptions?.filter(
					(description) => description.TargetHealth?.State === 'healthy',
				).length ?? 0
			if (healthyCount < minHealthyTargets) {
				unhealthy.push(targetGroupArn)
			}
		} catch (err) {
			// A transient ELB error (throttling, a permissions blip) counts as "not
			// ready yet" and is retried on the next poll interval rather than failing
			// the deployment - the framework's total timeout bounds the overall wait.
			console.log(
				`Target group ${targetGroupArn} not ready (health check error, will retry):`,
				err,
			)
			unhealthy.push(targetGroupArn)
		}
	}

	if (unhealthy.length === 0) {
		console.log(
			`All ${targetGroupArns.length} target groups report at least ${minHealthyTargets} healthy targets`,
		)
		return {
			IsComplete: true,
			Data: { HealthyTargetGroups: targetGroupArns.length },
		}
	}
	console.log(
		`Waiting for fleet readiness: ${unhealthy.length} of ${targetGroupArns.length} target groups still below ${minHealthyTargets} healthy targets (${unhealthy.join(', ')})`,
	)
	return {
		IsComplete: false,
		Data: { HealthyTargetGroups: targetGroupArns.length - unhealthy.length },
	}
}
