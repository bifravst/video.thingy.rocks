export type LogLevel = 'INFO' | 'WARN' | 'ERROR'

export type LogContext = {
	port?: number
	timestamp?: Date
	streamIdentifier?: string
	[key: string]: any
}

export class Logger {
	private readonly serviceName: string

	constructor(serviceName: string) {
		this.serviceName = serviceName
	}

	info(message: string, context?: LogContext): void {
		this.log('INFO', message, context)
	}

	warn(message: string, context?: LogContext): void {
		this.log('WARN', message, context)
	}

	error(message: string, error?: Error, context?: LogContext): void {
		const errorContext = error
			? {
					...context,
					error: {
						message: error.message,
						stack: error.stack,
						name: error.name,
					},
				}
			: context

		this.log('ERROR', message, errorContext)
	}

	private log(level: LogLevel, message: string, context?: LogContext): void {
		const timestamp = context?.timestamp ?? new Date()
		const logEntry = {
			level,
			service: this.serviceName,
			timestamp: timestamp.toISOString(),
			message,
			...(context && this.formatContext(context)),
		}

		const logString = JSON.stringify(logEntry)

		switch (level) {
			case 'INFO':
				console.log(logString)
				break
			case 'WARN':
				console.warn(logString)
				break
			case 'ERROR':
				console.error(logString)
				break
		}
	}

	private formatContext(context: LogContext): Record<string, any> {
		const formatted: Record<string, any> = {}

		for (const [key, value] of Object.entries(context)) {
			if (key === 'timestamp') {
				// Skip timestamp as it's already in the log entry
				continue
			}

			if (value instanceof Date) {
				formatted[key] = value.toISOString()
			} else if (value instanceof Error) {
				formatted[key] = {
					message: value.message,
					stack: value.stack,
					name: value.name,
				}
			} else {
				formatted[key] = value
			}
		}

		return formatted
	}
}
