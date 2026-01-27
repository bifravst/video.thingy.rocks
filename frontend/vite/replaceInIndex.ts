import Handlebars from 'handlebars'
import type { Plugin } from 'vite'

export const replaceInIndex = (data: Record<string, string>): Plugin => ({
	name: 'replace-in-index',
	transformIndexHtml: (source: string): string => {
		const template = Handlebars.compile(source)
		return template(data)
	},
})
