import { Hono } from "hono";
import type { Context } from "hono";
import type { AppEnv } from "../env";

const proxyForward = new Hono<AppEnv>();

proxyForward.all(
	"/*",
	(async (c: Context<AppEnv>) => {
		const binding = c.env.ATTEMPT_WORKER;
		if (!binding) {
			return new Response(
				JSON.stringify({
					error: {
						code: "attempt_worker_unavailable",
						message: "attempt_worker_unavailable",
					},
				}),
				{
					status: 503,
					headers: {
						"content-type": "application/json",
					},
				},
			);
		}

		const incomingUrl = new URL(c.req.url);
		const targetUrl = `https://attempt-worker${incomingUrl.pathname}${incomingUrl.search}`;
		const headers = new Headers(c.req.raw.headers);
		headers.delete("host");
		const requestInit: RequestInit = {
			method: c.req.method,
			headers,
		};
		if (c.req.method !== "GET" && c.req.method !== "HEAD") {
			requestInit.body = c.req.raw.body;
		}
		return binding.fetch(targetUrl, requestInit as never) as unknown as Response;
	}) as never,
);

export default proxyForward;
