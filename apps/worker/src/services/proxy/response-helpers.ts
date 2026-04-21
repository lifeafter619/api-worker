import { safeJsonParse } from "../../utils/json";

const STREAM_OPTIONS_UNSUPPORTED_SNIPPET = "unsupported parameter";
const STREAM_OPTIONS_PARAM_NAME = "stream_options";
const RESPONSE_ID_PARSE_MAX_BYTES = 64 * 1024;
const RESPONSE_ID_PARSE_TIMEOUT_MS = 1500;

function normalizeMessage(value: string | null): string | null {
	if (!value) {
		return null;
	}
	const trimmed = value.trim();
	return trimmed.length > 0 ? trimmed : null;
}

function normalizeStringField(value: unknown): string | null {
	if (typeof value !== "string") {
		return null;
	}
	const trimmed = value.trim();
	return trimmed.length > 0 ? trimmed : null;
}

export function extractOpenAiResponseIdFromJson(
	payload: unknown,
): string | null {
	if (!payload || typeof payload !== "object") {
		return null;
	}
	const record = payload as Record<string, unknown>;
	const objectType = normalizeStringField(record.object)?.toLowerCase();
	if (objectType && objectType !== "response") {
		return null;
	}
	return normalizeStringField(record.id);
}

export async function extractOpenAiResponseIdFromSse(
	response: Response,
	maxBytes = RESPONSE_ID_PARSE_MAX_BYTES,
	timeoutMs = RESPONSE_ID_PARSE_TIMEOUT_MS,
): Promise<string | null> {
	if (!response.body) {
		return null;
	}
	const reader = response.body.getReader();
	const decoder = new TextDecoder();
	const startedAt = Date.now();
	let bytesRead = 0;
	let buffer = "";
	try {
		while (Date.now() - startedAt <= timeoutMs) {
			const { done, value } = await reader.read();
			if (done) {
				break;
			}
			bytesRead += value?.byteLength ?? 0;
			if (bytesRead > maxBytes) {
				await reader.cancel();
				break;
			}
			buffer += decoder.decode(value, { stream: true });
			let newlineIndex = buffer.indexOf("\n");
			while (newlineIndex !== -1) {
				const line = buffer.slice(0, newlineIndex).trim();
				buffer = buffer.slice(newlineIndex + 1);
				if (!line.startsWith("data:")) {
					newlineIndex = buffer.indexOf("\n");
					continue;
				}
				const payload = line.slice(5).trim();
				if (!payload || payload === "[DONE]") {
					newlineIndex = buffer.indexOf("\n");
					continue;
				}
				const parsed = safeJsonParse<Record<string, unknown> | null>(
					payload,
					null,
				);
				if (!parsed) {
					newlineIndex = buffer.indexOf("\n");
					continue;
				}
				const responseRecord =
					parsed.response && typeof parsed.response === "object"
						? (parsed.response as Record<string, unknown>)
						: null;
				const responseId =
					normalizeStringField(responseRecord?.id) ??
					(normalizeStringField(parsed.object)?.toLowerCase() === "response"
						? normalizeStringField(parsed.id)
						: null);
				if (responseId) {
					await reader.cancel();
					return responseId;
				}
				newlineIndex = buffer.indexOf("\n");
			}
		}
		return null;
	} catch {
		return null;
	} finally {
		reader.releaseLock();
	}
}

export function isStreamOptionsUnsupportedMessage(
	message: string | null,
): boolean {
	const normalized = normalizeMessage(message)?.toLowerCase();
	if (!normalized) {
		return false;
	}
	return (
		normalized.includes(STREAM_OPTIONS_UNSUPPORTED_SNIPPET) &&
		normalized.includes(STREAM_OPTIONS_PARAM_NAME)
	);
}
