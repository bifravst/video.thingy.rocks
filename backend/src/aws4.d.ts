declare module 'aws4' {
	export type Request = {
		host?: string
		hostname?: string
		path?: string
		port?: number
		method?: string
		body?: string
		headers?: Record<string, string>
		service?: string
		region?: string
		extraHeadersToIgnore?: Record<string, boolean>
	}
	export function sign(requestOptions: Request, credentials?: unknown): Request
}
