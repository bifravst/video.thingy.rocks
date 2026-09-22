/**
 * Ending a producer's child process and waiting for it to actually be gone.
 *
 * This exists because "gone" is a correctness requirement rather than tidiness.
 * PortIngestion releases a port's DynamoDB lock as soon as producer.stop() returns, so
 * from that moment another instance may acquire the same Kinesis stream - and a child
 * that has been signalled but not yet reaped is still writing to it. Two writers to one
 * Kinesis Video stream is the failure this ordering exists to prevent.
 *
 * Both producers need the same guarantee, so they share one implementation of it. The
 * previous shape had each producer implement its own stop, and only one of the two
 * actually waited.
 */

/** The part of a ChildProcess this needs, so a fake can stand in for one. */
export type ExitingChild = {
	readonly exitCode: number | null
	readonly signalCode: NodeJS.Signals | null
	kill(signal?: NodeJS.Signals): boolean
	once(event: 'exit', listener: () => void): unknown
	removeListener(event: 'exit', listener: () => void): unknown
}

export type EndChildProcessOptions = {
	/**
	 * How long the child gets to exit on its own before it is signalled at all.
	 *
	 * For a pipeline whose input has just been closed, this is the flush: GStreamer
	 * ends its stream and kvssink uploads what it is holding. Omit it when closing the
	 * input is not how the child is asked to stop.
	 */
	drainMs?: number
	/** How long SIGTERM gets to work before SIGKILL follows it. */
	sigkillAfterMs: number
	/** Called as each signal is sent, for the caller to log. */
	onSignal?: (signal: NodeJS.Signals) => void
}

/** True once the child has been reaped, whether it exited or was killed. */
const hasExited = (child: ExitingChild): boolean =>
	child.exitCode !== null || child.signalCode !== null

/**
 * Resolves when the child exits, or after `ms` - whichever comes first.
 *
 * Both the timer and the listener are always cleaned up, so a caller that races these
 * repeatedly leaves nothing behind to hold the event loop open or to fire twice.
 */
const exitedWithin = async (
	child: ExitingChild,
	ms: number,
): Promise<boolean> => {
	if (hasExited(child)) return true
	return new Promise<boolean>((resolve) => {
		const onExit = (): void => {
			clearTimeout(timer)
			resolve(true)
		}
		const timer = setTimeout(() => {
			child.removeListener('exit', onExit)
			resolve(false)
		}, ms)
		child.once('exit', onExit)
	})
}

/** Resolves when the child exits, with no deadline of its own. */
const exited = async (child: ExitingChild): Promise<void> => {
	if (hasExited(child)) return
	return new Promise<void>((resolve) => {
		child.once('exit', () => resolve())
	})
}

/**
 * Ends a child process, returning only once it has exited.
 *
 * Escalates as far as it has to - an optional unsignalled drain, then SIGTERM, then
 * SIGKILL - but exiting is the only thing that resolves this. Sending SIGKILL is not
 * the same event as the child being gone, and it is the latter the caller's lock
 * release depends on.
 *
 * There is deliberately no deadline on the final wait. A child that survives SIGKILL is
 * stuck in the kernel, and returning while it might still hold the stream would trade a
 * stall for silent corruption of someone else's recording.
 */
export const endChildProcess = async (
	child: ExitingChild,
	options: EndChildProcessOptions,
): Promise<void> => {
	if (hasExited(child)) return

	if (options.drainMs !== undefined) {
		if (await exitedWithin(child, options.drainMs)) return
	}

	options.onSignal?.('SIGTERM')
	child.kill('SIGTERM')
	if (await exitedWithin(child, options.sigkillAfterMs)) return

	options.onSignal?.('SIGKILL')
	child.kill('SIGKILL')
	await exited(child)
}
