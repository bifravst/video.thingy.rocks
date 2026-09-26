/**
 * The runner's only command line, parsed fail-closed.
 *
 * The suite rotates deployed keys and restarts the fleet, so a filter that
 * silently expands to every case is worse than no filter at all: a bare
 * `--only` (no value), the `--only=value` form, an empty value, or a stray
 * extra argument all used to select EVERY case - the full mutation run -
 * where the operator asked for one.
 */
export const parseOnlyFilter = (args: string[]): string | undefined => {
	if (args.length === 0) return undefined
	if (args.length === 2 && args[0] === '--only' && (args[1] ?? '').length > 0) {
		return args[1]
	}
	throw new Error(
		`unexpected arguments (${args.join(' ')}): usage is 'npm run test:e2e' for every case, or 'npm run test:e2e -- --only <case-name substring>' for one`,
	)
}
