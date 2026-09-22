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
	on(event: 'exit' | 'error', listener: () => void): unknown
	removeListener(event: 'exit' | 'error', listener: () => void): unknown
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
 * Resolves true once the child is gone, or false if `ms` passes first.
 *
 * Waiting for 'exit' alone is not enough to know a child is gone, because there is a
 * case where it is never emitted: a spawn that failed. Node reports that as 'error',
 * sets exitCode as it does so, and emits no 'exit' at all - so a wait with no deadline
 * would sit there for the life of the process. The lock for the port is released when
 * the caller returns, so that is not a stall that resolves itself: the port is stuck.
 *
 * 'error' does not mean gone on its own, though. It also fires when a signal cannot be
 * delivered to a child that is very much alive, so it counts only when the exit fields
 * agree with it.
 *
 * Both the timer and the listeners are always cleaned up, so a caller that races these
 * repeatedly leaves nothing behind to hold the event loop open or to fire twice.
 */
const waitForExit = async (
	child: ExitingChild,
	ms?: number,
): Promise<boolean> => {
	if (hasExited(child)) return true
	return new Promise<boolean>((resolve) => {
		let timer: NodeJS.Timeout | undefined
		const settle = (gone: boolean): void => {
			if (timer !== undefined) clearTimeout(timer)
			child.removeListener('exit', onExit)
			child.removeListener('error', onError)
			resolve(gone)
		}
		const onExit = (): void => settle(true)
		const onError = (): void => {
			if (hasExited(child)) settle(true)
		}
		child.on('exit', onExit)
		child.on('error', onError)
		if (ms !== undefined) timer = setTimeout(() => settle(false), ms)
		// Checked again now the listeners are attached, not only before them. A real
		// ChildProcess sets its exit fields in the same turn it emits 'exit', so the
		// two cannot fall either side of the check above - but ExitingChild is a shape
		// rather than that one class, and anything that sets the fields first and
		// emits later would otherwise be waited on for an event already past.
		if (hasExited(child)) settle(true)
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
 * stall for silent corruption of someone else's recording. That is only a safe choice
 * because waitForExit also settles for a child that can never emit 'exit'; see there.
 */
export const endChildProcess = async (
	child: ExitingChild,
	options: EndChildProcessOptions,
): Promise<void> => {
	if (hasExited(child)) return

	if (options.drainMs !== undefined) {
		if (await waitForExit(child, options.drainMs)) return
	}

	options.onSignal?.('SIGTERM')
	child.kill('SIGTERM')
	if (await waitForExit(child, options.sigkillAfterMs)) return

	options.onSignal?.('SIGKILL')
	child.kill('SIGKILL')
	await waitForExit(child)
}
