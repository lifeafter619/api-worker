import {
	applyGeminiModelToPathViaWasm,
	buildUpstreamChatRequestViaWasm,
	detectDownstreamProviderViaWasm,
	detectEndpointTypeViaWasm,
	normalizeChatRequestViaWasm,
	parseDownstreamModelViaWasm,
	parseDownstreamStreamViaWasm,
} from "../wasm/core";
import type { EndpointOverrides } from "./site-metadata";

export type ProviderType = "openai" | "anthropic" | "gemini";

export type EndpointType =
	| "chat"
	| "responses"
	| "embeddings"
	| "images"
	| "passthrough";

export type NormalizedTool = {
	name: string;
	description?: string;
	parameters?: Record<string, unknown> | null;
};

export type NormalizedToolCall = {
	id: string;
	name: string;
	args: unknown;
};

export type NormalizedMessage = {
	role: "system" | "user" | "assistant" | "tool";
	content: string;
	toolCalls?: NormalizedToolCall[];
	toolCallId?: string | null;
};

export type NormalizedChatRequest = {
	model: string | null;
	stream: boolean;
	messages: NormalizedMessage[];
	tools: NormalizedTool[];
	toolChoice: unknown | null;
	temperature: number | null;
	topP: number | null;
	maxTokens: number | null;
	responseFormat: unknown | null;
};

export type NormalizedEmbeddingRequest = {
	model: string | null;
	inputs: string[];
};

export type NormalizedImageRequest = {
	model: string | null;
	prompt: string;
	n: number | null;
	size: string | null;
	quality: string | null;
	style: string | null;
	responseFormat: string | null;
};

export type UpstreamRequest = {
	path: string;
	fallbackPath?: string;
	absoluteUrl?: string;
	body: Record<string, unknown> | null;
};

const TEXT_PART_TYPES = new Set([
	"text",
	"input_text",
	"output_text",
	"message",
	"chunk",
]);

function toTextContent(value: unknown): string {
	if (value === null || value === undefined) {
		return "";
	}
	if (typeof value === "string") {
		return value;
	}
	if (typeof value === "number" || typeof value === "boolean") {
		return String(value);
	}
	if (Array.isArray(value)) {
		return value
			.map((entry) => {
				if (typeof entry === "string") {
					return entry;
				}
				if (entry && typeof entry === "object") {
					const part = entry as Record<string, unknown>;
					if (typeof part.text === "string") {
						return part.text;
					}
					if (
						typeof part.type === "string" &&
						TEXT_PART_TYPES.has(part.type) &&
						typeof part.text === "string"
					) {
						return part.text;
					}
				}
				return "";
			})
			.join("");
	}
	if (typeof value === "object") {
		const record = value as Record<string, unknown>;
		if (typeof record.text === "string") {
			return record.text;
		}
		if (Array.isArray(record.parts)) {
			return toTextContent(record.parts);
		}
		if (record.content !== undefined) {
			return toTextContent(record.content);
		}
	}
	return "";
}

function toNumber(value: unknown): number | null {
	if (value === null || value === undefined) {
		return null;
	}
	const parsed = Number(value);
	return Number.isFinite(parsed) ? parsed : null;
}

function normalizeOpenAiBodyForWasm(
	body: Record<string, unknown> | null,
): Record<string, unknown> | null {
	if (!body) {
		return body;
	}
	const nextBody: Record<string, unknown> = { ...body };
	if (Array.isArray(body.messages)) {
		nextBody.messages = body.messages.map((rawMessage) => {
			if (!rawMessage || typeof rawMessage !== "object" || Array.isArray(rawMessage)) {
				return rawMessage;
			}
			const message = rawMessage as Record<string, unknown>;
			const normalizedMessage: Record<string, unknown> = { ...message };
			if (
				normalizedMessage.tool_calls === undefined &&
				Array.isArray(normalizedMessage.toolCalls)
			) {
				normalizedMessage.tool_calls = normalizedMessage.toolCalls;
			}
			if (
				normalizedMessage.function_call === undefined &&
				normalizedMessage.functionCall !== undefined
			) {
				normalizedMessage.function_call = normalizedMessage.functionCall;
			}
			if (
				normalizedMessage.tool_call_id === undefined &&
				normalizedMessage.toolCallId !== undefined
			) {
				normalizedMessage.tool_call_id = normalizedMessage.toolCallId;
			}
			if (
				normalizedMessage.call_id === undefined &&
				normalizedMessage.callId !== undefined
			) {
				normalizedMessage.call_id = normalizedMessage.callId;
			}
			if (Array.isArray(normalizedMessage.tool_calls)) {
				normalizedMessage.tool_calls = normalizedMessage.tool_calls.map((rawCall) => {
					if (!rawCall || typeof rawCall !== "object" || Array.isArray(rawCall)) {
						return rawCall;
					}
					const call = rawCall as Record<string, unknown>;
					const normalizedCall: Record<string, unknown> = { ...call };
					if (
						normalizedCall.call_id === undefined &&
						normalizedCall.callId !== undefined
					) {
						normalizedCall.call_id = normalizedCall.callId;
					}
					if (
						normalizedCall.function === undefined &&
						normalizedCall.functionCall !== undefined &&
						typeof normalizedCall.functionCall === "object"
					) {
						normalizedCall.function = normalizedCall.functionCall;
					}
					return normalizedCall;
				});
			}
			return normalizedMessage;
		});
	}
	if (body.input !== undefined) {
		const rawInput = body.input;
		if (Array.isArray(rawInput)) {
			nextBody.input = rawInput.map((rawItem) => {
				if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
					return rawItem;
				}
				const item = rawItem as Record<string, unknown>;
				const normalizedItem: Record<string, unknown> = { ...item };
				if (
					normalizedItem.call_id === undefined &&
					normalizedItem.callId !== undefined
				) {
					normalizedItem.call_id = normalizedItem.callId;
				}
				if (
					normalizedItem.tool_call_id === undefined &&
					normalizedItem.toolCallId !== undefined
				) {
					normalizedItem.tool_call_id = normalizedItem.toolCallId;
				}
				return normalizedItem;
			});
		} else if (
			rawInput &&
			typeof rawInput === "object" &&
			!Array.isArray(rawInput)
		) {
			const item = rawInput as Record<string, unknown>;
			const normalizedItem: Record<string, unknown> = { ...item };
			if (normalizedItem.call_id === undefined && normalizedItem.callId !== undefined) {
				normalizedItem.call_id = normalizedItem.callId;
			}
			if (
				normalizedItem.tool_call_id === undefined &&
				normalizedItem.toolCallId !== undefined
			) {
				normalizedItem.tool_call_id = normalizedItem.toolCallId;
			}
			nextBody.input = normalizedItem;
		}
	}
	return nextBody;
}

export function detectDownstreamProvider(path: string): ProviderType {
	const provider = detectDownstreamProviderViaWasm(path);
	if (
		provider === "openai" ||
		provider === "anthropic" ||
		provider === "gemini"
	) {
		return provider;
	}
	throw new Error(`Unexpected provider from wasm: ${provider}`);
}

export function detectEndpointType(
	provider: ProviderType,
	path: string,
): EndpointType {
	const endpoint = detectEndpointTypeViaWasm(provider, path);
	if (
		endpoint === "chat" ||
		endpoint === "responses" ||
		endpoint === "embeddings" ||
		endpoint === "images" ||
		endpoint === "passthrough"
	) {
		return endpoint;
	}
	throw new Error(`Unexpected endpoint from wasm: ${endpoint}`);
}

export function parseDownstreamModel(
	provider: ProviderType,
	path: string,
	body: Record<string, unknown> | null,
): string | null {
	return parseDownstreamModelViaWasm(provider, path, body);
}

export function parseDownstreamStream(
	provider: ProviderType,
	path: string,
	body: Record<string, unknown> | null,
): boolean {
	return parseDownstreamStreamViaWasm(provider, path, body);
}

export function normalizeChatRequest(
	provider: ProviderType,
	endpoint: EndpointType,
	body: Record<string, unknown> | null,
	model: string | null,
	isStream: boolean,
): NormalizedChatRequest | null {
	const normalizedBody =
		provider === "openai" ? normalizeOpenAiBodyForWasm(body) : body;
	return normalizeChatRequestViaWasm<NormalizedChatRequest>(
		normalizedBody,
		provider,
		endpoint,
		model,
		isStream,
	);
}

export function normalizeEmbeddingRequest(
	provider: ProviderType,
	body: Record<string, unknown> | null,
	model: string | null,
): NormalizedEmbeddingRequest | null {
	if (!body) {
		return null;
	}
	if (provider === "gemini") {
		if (Array.isArray(body.requests)) {
			const inputs = body.requests
				.map((req) => {
					if (!req || typeof req !== "object") {
						return "";
					}
					const record = req as Record<string, unknown>;
					return toTextContent(record.content);
				})
				.filter((item) => item.length > 0);
			return { model, inputs };
		}
		const content = body.content ?? body.input;
		return { model, inputs: [toTextContent(content)] };
	}
	const input = body.input ?? body.inputs;
	if (Array.isArray(input)) {
		return {
			model,
			inputs: input.map((item) => toTextContent(item)),
		};
	}
	return { model, inputs: [toTextContent(input)] };
}

export function normalizeImageRequest(
	provider: ProviderType,
	body: Record<string, unknown> | null,
	model: string | null,
): NormalizedImageRequest | null {
	if (!body) {
		return null;
	}
	if (provider === "openai") {
		return {
			model,
			prompt: toTextContent(body.prompt),
			n: toNumber(body.n),
			size: body.size ? String(body.size) : null,
			quality: body.quality ? String(body.quality) : null,
			style: body.style ? String(body.style) : null,
			responseFormat: body.response_format
				? String(body.response_format)
				: null,
		};
	}
	return {
		model,
		prompt: toTextContent(body.prompt ?? body.text ?? body.input),
		n: null,
		size: null,
		quality: null,
		style: null,
		responseFormat: null,
	};
}

function resolveOverride(
	override: string | null | undefined,
	model: string | null,
): { absolute?: string; path?: string } | null {
	if (!override) {
		return null;
	}
	const resolved = model ? override.replace("{model}", model) : override;
	if (resolved.startsWith("http://") || resolved.startsWith("https://")) {
		return { absolute: resolved };
	}
	return { path: resolved };
}

export function buildUpstreamChatRequest(
	provider: ProviderType,
	normalized: NormalizedChatRequest,
	model: string | null,
	endpoint: EndpointType,
	isStream: boolean,
	endpointOverrides: EndpointOverrides,
): UpstreamRequest | null {
	return buildUpstreamChatRequestViaWasm<UpstreamRequest>(
		normalized as unknown as Record<string, unknown>,
		provider,
		model,
		endpoint,
		isStream,
		endpointOverrides as unknown as Record<string, unknown>,
	);
}

export function buildUpstreamEmbeddingRequest(
	provider: ProviderType,
	normalized: NormalizedEmbeddingRequest,
	model: string | null,
	endpointOverrides: EndpointOverrides,
): UpstreamRequest | null {
	if (provider === "openai") {
		const override = resolveOverride(endpointOverrides.embedding_url, model);
		return {
			path: override?.path ?? "/v1/embeddings",
			absoluteUrl: override?.absolute,
			body: {
				model,
				input:
					normalized.inputs.length === 1
						? normalized.inputs[0]
						: normalized.inputs,
			},
		};
	}
	if (provider === "anthropic") {
		return null;
	}
	const override = resolveOverride(endpointOverrides.embedding_url, model);
	const isBatch = normalized.inputs.length > 1;
	const defaultPath = isBatch
		? `/v1beta/models/${model}:batchEmbedContents`
		: `/v1beta/models/${model}:embedContent`;
	const body = isBatch
		? {
				requests: normalized.inputs.map((input) => ({
					content: { parts: [{ text: input }] },
				})),
			}
		: {
				content: { parts: [{ text: normalized.inputs[0] ?? "" }] },
			};
	return {
		path: override?.path ?? defaultPath,
		absoluteUrl: override?.absolute,
		body,
	};
}

export function buildUpstreamImageRequest(
	provider: ProviderType,
	normalized: NormalizedImageRequest,
	model: string | null,
	endpointOverrides: EndpointOverrides,
): UpstreamRequest | null {
	if (provider === "openai") {
		const override = resolveOverride(endpointOverrides.image_url, model);
		const body: Record<string, unknown> = {
			model,
			prompt: normalized.prompt,
		};
		if (normalized.n !== null) {
			body.n = normalized.n;
		}
		if (normalized.size !== null) {
			body.size = normalized.size;
		}
		if (normalized.quality !== null) {
			body.quality = normalized.quality;
		}
		if (normalized.style !== null) {
			body.style = normalized.style;
		}
		if (normalized.responseFormat !== null) {
			body.response_format = normalized.responseFormat;
		}
		return {
			path: override?.path ?? "/v1/images/generations",
			absoluteUrl: override?.absolute,
			body,
		};
	}
	if (provider === "anthropic") {
		return null;
	}
	const override = resolveOverride(endpointOverrides.image_url, model);
	return {
		path: override?.path ?? `/v1beta/models/${model}:generateImage`,
		absoluteUrl: override?.absolute,
		body: {
			prompt: normalized.prompt,
		},
	};
}

export function applyGeminiModelToPath(
	path: string,
	model: string | null,
): string {
	return applyGeminiModelToPathViaWasm(path, model);
}
