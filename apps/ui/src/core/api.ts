import { apiBase } from "./constants";

export type ApiFetch = <T>(path: string, options?: RequestInit) => Promise<T>;

/**
 * Creates a typed API fetcher bound to the current auth token.
 *
 * Args:
 *   token: Bearer token string or null.
 *   onUnauthorized: Callback invoked on 401 responses.
 *
 * Returns:
 *   A fetch function that wraps API calls with auth headers and errors.
 */
export const createApiFetch = (
	token: string | null,
	onUnauthorized: () => void,
): ApiFetch => {
	return async <T>(path: string, options: RequestInit = {}): Promise<T> => {
		const headers = new Headers(options.headers ?? {});
		headers.set("Content-Type", "application/json");
		if (token) {
			headers.set("Authorization", `Bearer ${token}`);
		}
		const response = await fetch(`${apiBase}${path}`, {
			...options,
			headers,
		});
		if (!response.ok) {
			if (response.status === 401) {
				onUnauthorized();
			}
			const payload = (await response.json().catch(() => null)) as {
				error?: string;
				code?: string;
				trace_id?: string;
				top_reason?: string;
				attempt_failed?: number;
				failures?: Array<{
					error_code?: string | null;
					error_message?: string | null;
				}>;
			} | null;
			const fallbackMessage = `HTTP ${response.status}`;
			const errorText =
				typeof payload?.error === "string" && payload.error.trim().length > 0
					? payload.error
					: fallbackMessage;
			if (payload?.code === "proxy_all_attempts_failed") {
				const traceId =
					typeof payload.trace_id === "string" &&
					payload.trace_id.trim().length > 0
						? payload.trace_id
						: response.headers.get("x-ha-trace-id");
				const sampleFailure = Array.isArray(payload.failures)
					? payload.failures[0]
					: null;
				const sampleError =
					typeof sampleFailure?.error_message === "string" &&
					sampleFailure.error_message.trim().length > 0
						? sampleFailure.error_message
						: typeof sampleFailure?.error_code === "string" &&
								sampleFailure.error_code.trim().length > 0
							? sampleFailure.error_code
							: null;
				const attemptPart =
					typeof payload.attempt_failed === "number"
						? `，失败 ${payload.attempt_failed} 次`
						: "";
				const reasonPart =
					typeof payload.top_reason === "string" &&
					payload.top_reason.trim().length > 0
						? `，主因 ${payload.top_reason}`
						: "";
				const samplePart = sampleError ? `，示例 ${sampleError}` : "";
				const tracePart = traceId ? `，trace=${traceId}` : "";
				throw new Error(
					`请求失败：全部重试未成功${attemptPart}${reasonPart}${samplePart}${tracePart}`,
				);
			}
			throw new Error(errorText);
		}
		return response.json() as Promise<T>;
	};
};
