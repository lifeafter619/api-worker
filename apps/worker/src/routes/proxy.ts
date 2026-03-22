import { Hono } from "hono";
import type { AppEnv } from "../env";
import { type TokenRecord, tokenAuth } from "../middleware/tokenAuth";
import type { CallTokenItem } from "../services/call-token-selector";
import { listCallTokens } from "../services/channel-call-token-repo";
import {
	type ChannelMetadata,
	parseChannelMetadata,
	resolveMappedModel,
	resolveProvider,
} from "../services/channel-metadata";
import {
	listCoolingDownChannelsForModel,
	listVerifiedModelsByChannel,
} from "../services/channel-model-capabilities";
import {
	type ChannelRecord,
	createWeightedOrder,
	extractModels,
} from "../services/channels";
import { adaptChatResponse } from "../services/chat-response-adapter";
import {
	applyGeminiModelToPath,
	buildUpstreamChatRequest,
	buildUpstreamEmbeddingRequest,
	buildUpstreamImageRequest,
	detectDownstreamProvider,
	detectEndpointType,
	type EndpointType,
	type NormalizedChatRequest,
	type NormalizedEmbeddingRequest,
	type NormalizedImageRequest,
	normalizeChatRequest,
	normalizeEmbeddingRequest,
	normalizeImageRequest,
	type ProviderType,
	parseDownstreamModel,
	parseDownstreamStream,
} from "../services/provider-transform";
import {
	buildActiveChannelsKey,
	buildCallTokensIndexKey,
	readHotJson,
	writeHotJson,
} from "../services/hot-kv";
import { getCacheConfig, getProxyRuntimeSettings } from "../services/settings";
import {
	getUsageLimiterStub,
	reserveUsageQueue,
	trackUsageQueue,
	type UsageQueueTrackKind,
} from "../services/usage-limiter";
import {
	processUsageQueueEvent,
	type UsageQueueEvent,
} from "../services/usage-queue";
import { jsonError } from "../utils/http";
import { safeJsonParse } from "../utils/json";
import { extractReasoningEffort } from "../utils/reasoning";
import { normalizeBaseUrl } from "../utils/url";
import {
	type NormalizedUsage,
	parseUsageFromHeaders,
	parseUsageFromJson,
	parseUsageFromSse,
	type StreamUsageMode,
	type StreamUsageOptions,
	StreamUsageParseError,
} from "../utils/usage";

const proxy = new Hono<AppEnv>();

type ExecutionContextLike = {
	waitUntil: (promise: Promise<unknown>) => void;
};

function scheduleDbWrite(
	c: { executionCtx?: ExecutionContextLike },
	task: Promise<void>,
): void {
	if (c.executionCtx?.waitUntil) {
		c.executionCtx.waitUntil(task);
	} else {
		task.catch(() => undefined);
	}
}

type ErrorDetails = {
	upstreamStatus: number | null;
	errorCode: string | null;
	errorMessage: string | null;
};

const STREAM_USAGE_UNKNOWN_PARSE_ERROR_CODE =
	"usage_stream_parse_unknown_error";
const STREAM_USAGE_NON_ERROR_THROWN_CODE =
	"usage_stream_parse_non_error_thrown";
const PROXY_UPSTREAM_TIMEOUT_ERROR_CODE = "proxy_upstream_timeout";
const PROXY_UPSTREAM_FETCH_ERROR_CODE = "proxy_upstream_fetch_exception";
const INTERNAL_USAGE_RESERVE_TIMEOUT_MS = 600;
const INTERNAL_USAGE_QUEUE_SEND_TIMEOUT_MS = 1500;
const INTERNAL_COOLDOWN_HTTP_STATUSES = [408, 429];
const INTERNAL_COOLDOWN_MIN_STATUS = 500;
const HOT_KV_ACTIVE_CHANNELS_TTL_SECONDS = 60;
const HOT_KV_CALL_TOKENS_TTL_SECONDS = 60;
const INTERNAL_COOLDOWN_ERROR_CODES = [
	"timeout",
	"exception",
	PROXY_UPSTREAM_TIMEOUT_ERROR_CODE,
	PROXY_UPSTREAM_FETCH_ERROR_CODE,
];

let activeStreamUsageParsers = 0;

function normalizeMessage(value: string | null): string | null {
	if (!value) {
		return null;
	}
	const trimmed = value.trim();
	if (!trimmed) {
		return null;
	}
	return trimmed;
}

function formatUsageErrorMessage(
	code: string,
	detail: string | null,
	maxLength: number,
): string {
	const safeMaxLength = Math.max(1, Math.floor(maxLength));
	const normalized = normalizeMessage(detail);
	if (!normalized) {
		return code;
	}
	const combined = `${code}: ${normalized}`;
	if (combined.length <= safeMaxLength) {
		return combined;
	}
	return `${combined.slice(0, safeMaxLength - 1)}…`;
}

function classifyStreamUsageParseError(
	error: unknown,
	maxLength: number,
): {
	errorCode: string;
	errorMessage: string;
} {
	if (error instanceof StreamUsageParseError) {
		return {
			errorCode: error.code,
			errorMessage: formatUsageErrorMessage(
				error.code,
				error.detail,
				maxLength,
			),
		};
	}
	if (error instanceof Error) {
		const errorCode = STREAM_USAGE_UNKNOWN_PARSE_ERROR_CODE;
		return {
			errorCode,
			errorMessage: formatUsageErrorMessage(
				errorCode,
				normalizeMessage(error.message) ?? error.name,
				maxLength,
			),
		};
	}
	const errorCode = STREAM_USAGE_NON_ERROR_THROWN_CODE;
	return {
		errorCode,
		errorMessage: errorCode,
	};
}

function normalizeUpstreamErrorCode(
	errorCode: string | null,
	status: number,
): string {
	const normalized = normalizeMessage(errorCode);
	if (normalized) {
		return normalized;
	}
	return `upstream_http_${status}`;
}

function getStreamUsageOptions(settings: {
	stream_usage_mode: string;
	stream_usage_max_bytes: number;
}): StreamUsageOptions {
	return {
		mode: settings.stream_usage_mode as StreamUsageMode,
		maxBytes: Math.max(0, Math.floor(settings.stream_usage_max_bytes)),
	};
}

function getStreamUsageMaxParsers(settings: {
	stream_usage_max_parsers: number;
}): number {
	const maxParsers = Math.max(0, Math.floor(settings.stream_usage_max_parsers));
	if (maxParsers === 0) {
		return Number.POSITIVE_INFINITY;
	}
	return maxParsers;
}

function withTimeout<T>(
	task: Promise<T>,
	timeoutMs: number,
	timeoutCode: string,
): Promise<T> {
	if (timeoutMs <= 0) {
		return task;
	}
	return new Promise<T>((resolve, reject) => {
		const timer = setTimeout(() => {
			reject(new Error(timeoutCode));
		}, timeoutMs);
		task.then(
			(value) => {
				clearTimeout(timer);
				resolve(value);
			},
			(error) => {
				clearTimeout(timer);
				reject(error);
			},
		);
	});
}

function hashFNV1a32(input: string): number {
	let hash = 0x811c9dc5;
	for (let i = 0; i < input.length; i += 1) {
		hash ^= input.charCodeAt(i);
		hash = Math.imul(hash, 0x01000193);
	}
	return hash >>> 0;
}

function shouldDirectWriteByRatio(
	dispatchKey: string,
	directRatio: number,
): boolean {
	if (directRatio <= 0) {
		return false;
	}
	if (directRatio >= 1) {
		return true;
	}
	const bucket = hashFNV1a32(dispatchKey) / 0x1_0000_0000;
	return bucket < directRatio;
}

function createUsageEventScheduler(
	c: { env: AppEnv["Bindings"]; executionCtx?: ExecutionContextLike },
	settings: {
		usage_queue_enabled: boolean;
		usage_queue_daily_limit: number;
		usage_queue_direct_write_ratio: number;
		usage_reserve_breaker_ms: number;
	},
	diagnostics: {
		requestPath?: string | null;
		method?: string | null;
		tokenId?: string | null;
		model?: string | null;
		requestSeed?: string | null;
	},
): (event: UsageQueueEvent) => void {
	const queue = c.env.USAGE_QUEUE;
	const queueBound = Boolean(queue);
	const queueEnabled = settings.usage_queue_enabled && queueBound;
	const limiter = c.env.USAGE_LIMITER
		? getUsageLimiterStub(c.env.USAGE_LIMITER)
		: null;
	const directRatio = settings.usage_queue_direct_write_ratio;
	const dailyLimit = settings.usage_queue_daily_limit;
	const reserveTimeoutMs = INTERNAL_USAGE_RESERVE_TIMEOUT_MS;
	const queueSendTimeoutMs = INTERNAL_USAGE_QUEUE_SEND_TIMEOUT_MS;
	const reserveBreakerMs = Math.max(
		0,
		Math.floor(settings.usage_reserve_breaker_ms),
	);
	const requestSeed = diagnostics.requestSeed ?? String(Date.now());
	let overLimit = false;
	let reserveBreakerUntil = 0;
	let dispatchSequence = 0;
	const emitRuntimeEvent = (
		_level: "info" | "warning" | "error",
		_code: string,
		_message: string,
		_context: Record<string, unknown>,
	) => {
		// runtime events intentionally disabled
	};

	const trackQueueMetric = (kind: UsageQueueTrackKind): void => {
		if (!limiter) {
			return;
		}
		const task = withTimeout(
			trackUsageQueue(limiter, { kind }),
			reserveTimeoutMs,
			"usage_track_timeout",
		)
			.then(() => undefined)
			.catch(() => undefined);
		scheduleDbWrite(c, task);
	};

	const buildDispatchKey = (event: UsageQueueEvent): string => {
		dispatchSequence += 1;
		return [
			requestSeed,
			diagnostics.tokenId ?? "anonymous",
			diagnostics.requestPath ?? "-",
			diagnostics.model ?? "-",
			event.type,
			String(dispatchSequence),
		].join(":");
	};

	type QueueDecision = {
		useQueue: boolean;
		reason:
			| "queue_disabled"
			| "over_limit"
			| "reserve_breaker"
			| "direct_ratio"
			| "reserve_failed"
			| "allowed";
	};

	const shouldUseQueue = async (
		dispatchKey: string,
	): Promise<QueueDecision> => {
		if (!queueEnabled) {
			return {
				useQueue: false,
				reason: "queue_disabled",
			};
		}
		if (overLimit) {
			return {
				useQueue: false,
				reason: "over_limit",
			};
		}
		if (Date.now() < reserveBreakerUntil) {
			return {
				useQueue: false,
				reason: "reserve_breaker",
			};
		}
		if (shouldDirectWriteByRatio(dispatchKey, directRatio)) {
			return {
				useQueue: false,
				reason: "direct_ratio",
			};
		}
		if (!limiter || dailyLimit <= 0) {
			return {
				useQueue: true,
				reason: "allowed",
			};
		}
		try {
			const result = await withTimeout(
				reserveUsageQueue(limiter, {
					limit: dailyLimit,
					amount: 1,
				}),
				reserveTimeoutMs,
				"usage_reserve_timeout",
			);
			if (!result.allowed) {
				overLimit = true;
				emitRuntimeEvent(
					"warning",
					"usage_limiter_reserve_over_limit",
					"usage_limiter_reserve_over_limit",
					{ limit: dailyLimit },
				);
			}
			return {
				useQueue: result.allowed,
				reason: result.allowed ? "allowed" : "over_limit",
			};
		} catch (error) {
			reserveBreakerUntil = Date.now() + reserveBreakerMs;
			emitRuntimeEvent(
				"warning",
				"usage_limiter_reserve_failed",
				"usage_limiter_reserve_failed",
				{
					error: error instanceof Error ? error.message : String(error),
					breaker_ms: reserveBreakerMs,
				},
			);
			return {
				useQueue: false,
				reason: "reserve_failed",
			};
		}
	};

	return (event: UsageQueueEvent) => {
		const task = (async () => {
			const dispatchKey = buildDispatchKey(event);
			const decision = await shouldUseQueue(dispatchKey);
			if (decision.useQueue && queue) {
				try {
					await withTimeout(
						queue.send(event),
						queueSendTimeoutMs,
						"usage_queue_send_timeout",
					);
					trackQueueMetric("enqueue_success");
					return;
				} catch (error) {
					trackQueueMetric("queue_send_failed");
					trackQueueMetric("fallback_direct");
					emitRuntimeEvent(
						"warning",
						"usage_queue_send_failed",
						"usage_queue_send_failed",
						{
							error: error instanceof Error ? error.message : String(error),
							fallback: "direct_write",
						},
					);
				}
			} else {
				if (decision.reason === "reserve_failed") {
					trackQueueMetric("reserve_failed");
				} else if (decision.reason === "over_limit") {
					trackQueueMetric("reserve_over_limit");
				} else {
					trackQueueMetric("direct");
				}
			}
			await processUsageQueueEvent(
				c.env.DB,
				event,
			);
		})().catch((error) => {
			emitRuntimeEvent(
				"error",
				"usage_event_schedule_failed",
				"usage_event_schedule_failed",
				{
					event_type: event.type,
					error: error instanceof Error ? error.message : String(error),
				},
			);
		});
		scheduleDbWrite(c, task);
	};
}

async function extractErrorDetails(
	response: Response,
): Promise<{ errorCode: string | null; errorMessage: string | null }> {
	const contentType = response.headers.get("content-type") ?? "";
	if (contentType.includes("application/json")) {
		const payload = await response
			.clone()
			.json()
			.catch(() => null);
		if (payload && typeof payload === "object") {
			const raw = payload as Record<string, unknown>;
			const error = (raw.error ?? raw) as Record<string, unknown>;
			const errorCode =
				typeof error.code === "string"
					? error.code
					: typeof error.type === "string"
						? error.type
						: null;
			const errorMessage =
				typeof error.message === "string"
					? error.message
					: typeof raw.message === "string"
						? raw.message
						: null;
			return {
				errorCode,
				errorMessage: normalizeMessage(errorMessage),
			};
		}
	}
	const text = await response
		.clone()
		.text()
		.catch(() => "");
	return { errorCode: null, errorMessage: normalizeMessage(text) };
}

export function shouldCooldown(
	upstreamStatus: number | null,
	errorCode: string | null,
): boolean {
	const normalizedCode = normalizeMessage(errorCode)?.toLowerCase() ?? "";
	const cooldownErrorCodes = new Set(
		INTERNAL_COOLDOWN_ERROR_CODES.map((item) => item.trim().toLowerCase()),
	);
	if (normalizedCode && cooldownErrorCodes.has(normalizedCode)) {
		return true;
	}
	const cooldownStatuses = new Set(
		INTERNAL_COOLDOWN_HTTP_STATUSES.map((item) =>
			Math.floor(Number(item)),
		).filter((item) => Number.isInteger(item) && item >= 0),
	);
	if (upstreamStatus !== null && cooldownStatuses.has(upstreamStatus)) {
		return true;
	}
	const minStatus = Math.max(0, Math.floor(INTERNAL_COOLDOWN_MIN_STATUS));
	if (upstreamStatus !== null && minStatus > 0 && upstreamStatus >= minStatus) {
		return true;
	}
	return false;
}

function channelSupportsModel(
	channel: ChannelRecord,
	model: string | null,
	verifiedModelsByChannel: Map<string, Set<string>>,
): boolean {
	if (!model) {
		return true;
	}
	const verified = verifiedModelsByChannel.get(channel.id);
	const declaredModels = extractModels(channel).map((entry) => entry.id);
	const metadata = parseChannelMetadata(channel.metadata_json);
	const mapped = resolveMappedModel(metadata.model_mapping, model);
	const hasExplicitMapping = hasExplicitModelMapping(metadata, model);
	const declaredAllows =
		declaredModels.length > 0
			? (mapped ? declaredModels.includes(mapped) : false) ||
				declaredModels.includes(model)
			: null;
	const verifiedAllows =
		verified && verified.size > 0
			? (mapped ? verified.has(mapped) : false) || verified.has(model)
			: null;
	if (hasExplicitMapping) {
		if (verified && verified.size > 0) {
			return Boolean(verifiedAllows);
		}
		if (declaredModels.length > 0) {
			return Boolean(declaredAllows);
		}
		return true;
	}
	if (declaredModels.length > 0 && !declaredAllows) {
		return false;
	}
	if (verified && verified.size > 0) {
		return Boolean(verifiedAllows);
	}
	if (declaredModels.length > 0) {
		return true;
	}
	return false;
}

export function selectCandidateChannels(
	allowedChannels: ChannelRecord[],
	downstreamModel: string | null,
	verifiedModelsByChannel: Map<string, Set<string>> = new Map(),
): ChannelRecord[] {
	const modelChannels = allowedChannels.filter((channel) =>
		channelSupportsModel(channel, downstreamModel, verifiedModelsByChannel),
	);
	return modelChannels;
}

function hasExplicitModelMapping(
	metadata: ChannelMetadata,
	downstreamModel: string | null,
): boolean {
	if (downstreamModel) {
		return (
			metadata.model_mapping[downstreamModel] !== undefined ||
			metadata.model_mapping["*"] !== undefined
		);
	}
	return metadata.model_mapping["*"] !== undefined;
}

export function resolveUpstreamModelForChannel(
	channel: ChannelRecord,
	metadata: ChannelMetadata,
	downstreamModel: string | null,
	verifiedModelsByChannel: Map<string, Set<string>> = new Map(),
): { model: string | null; autoMapped: boolean } {
	const mapped = resolveMappedModel(metadata.model_mapping, downstreamModel);
	if (!downstreamModel || hasExplicitModelMapping(metadata, downstreamModel)) {
		return { model: mapped, autoMapped: false };
	}

	const verified = verifiedModelsByChannel.get(channel.id);
	const declaredModels = verified
		? Array.from(verified)
		: extractModels(channel).map((entry) => entry.id);
	if (declaredModels.length === 0) {
		return { model: mapped, autoMapped: false };
	}
	if (declaredModels.includes(downstreamModel)) {
		return { model: downstreamModel, autoMapped: false };
	}
	return { model: declaredModels[0] ?? mapped, autoMapped: true };
}

function filterAllowedChannels(
	channels: ChannelRecord[],
	tokenRecord: TokenRecord,
): ChannelRecord[] {
	const allowed = safeJsonParse<string[] | null>(
		tokenRecord.allowed_channels,
		null,
	);
	if (!allowed || allowed.length === 0) {
		return channels;
	}
	const allowedSet = new Set(allowed);
	return channels.filter((channel) => allowedSet.has(channel.id));
}

const normalizeTokenModels = (raw?: string | null): string[] | null => {
	const parsed = safeJsonParse<string[] | null>(raw ?? null, null);
	if (!parsed || !Array.isArray(parsed) || parsed.length === 0) {
		return null;
	}
	return parsed.map((item) => String(item));
};

const selectTokenForModel = (
	tokens: CallTokenItem[],
	model: string | null,
): { token: CallTokenItem | null; hasModelList: boolean } => {
	if (tokens.length === 0) {
		return { token: null, hasModelList: false };
	}
	if (!model) {
		return { token: tokens[0], hasModelList: false };
	}
	const tokensWithModels = tokens.map((token) => ({
		token,
		models: normalizeTokenModels(token.models_json),
	}));
	const hasModelList = tokensWithModels.some((entry) => entry.models);
	if (!hasModelList) {
		return { token: tokens[0], hasModelList: false };
	}
	const match = tokensWithModels.find((entry) => entry.models?.includes(model));
	return { token: match?.token ?? null, hasModelList };
};

function resolveChannelBaseUrl(channel: ChannelRecord): string {
	return normalizeBaseUrl(channel.base_url);
}

function mergeQuery(
	base: string,
	querySuffix: string,
	overrides: Record<string, string>,
): string {
	const [path, rawQuery] = base.split("?");
	const params = new URLSearchParams(rawQuery ?? "");
	if (querySuffix) {
		const suffix = querySuffix.startsWith("?")
			? querySuffix.slice(1)
			: querySuffix;
		const suffixParams = new URLSearchParams(suffix);
		suffixParams.forEach((value, key) => {
			params.set(key, value);
		});
	}
	for (const [key, value] of Object.entries(overrides)) {
		params.set(key, value);
	}
	const query = params.toString();
	return query ? `${path}?${query}` : path;
}

function buildUpstreamHeaders(
	baseHeaders: Headers,
	provider: ProviderType,
	apiKey: string,
	overrides: Record<string, string>,
): Headers {
	const headers = new Headers(baseHeaders);
	headers.delete("x-admin-token");
	headers.delete("x-api-key");
	if (provider === "openai") {
		headers.set("Authorization", `Bearer ${apiKey}`);
		headers.set("x-api-key", apiKey);
	} else if (provider === "anthropic") {
		headers.delete("Authorization");
		headers.set("x-api-key", apiKey);
		headers.set("anthropic-version", "2023-06-01");
	} else {
		headers.delete("Authorization");
		headers.set("x-goog-api-key", apiKey);
	}
	for (const [key, value] of Object.entries(overrides)) {
		headers.set(key, value);
	}
	return headers;
}

async function fetchWithTimeout(
	url: string,
	init: RequestInit,
	timeoutMs: number,
): Promise<Response> {
	if (timeoutMs <= 0) {
		return fetch(url, init);
	}
	const controller = new AbortController();
	const timer = setTimeout(() => controller.abort(), timeoutMs);
	try {
		return await fetch(url, {
			...init,
			signal: controller.signal,
		});
	} finally {
		clearTimeout(timer);
	}
}

/**
 * Multi-provider proxy handler.
 */
proxy.all("/*", tokenAuth, async (c) => {
	const db = c.env.DB;
	const tokenRecord = c.get("tokenRecord") as TokenRecord;
	const requestStart = Date.now();
	const [cacheConfig, runtimeSettings] = await Promise.all([
		getCacheConfig(db, c.env.CACHE_VERSION_STORE),
		getProxyRuntimeSettings(db),
	]);
	let requestText = await c.req.text();
	const parsedBody = requestText
		? safeJsonParse<Record<string, unknown> | null>(requestText, null)
		: null;
	const downstreamProvider = detectDownstreamProvider(c.req.path);
	const endpointType = detectEndpointType(downstreamProvider, c.req.path);
	const downstreamModel = parseDownstreamModel(
		downstreamProvider,
		c.req.path,
		parsedBody,
	);
	const isStream = parseDownstreamStream(
		downstreamProvider,
		c.req.path,
		parsedBody,
	);
	const reasoningEffort = extractReasoningEffort(parsedBody);
	const scheduleRuntimeEvent = (
		_level: "info" | "warning" | "error",
		_code: string,
		_message: string,
		_context: Record<string, unknown>,
	) => {
		// runtime events intentionally disabled
	};
	const scheduleUsageEvent = createUsageEventScheduler(c, runtimeSettings, {
		requestPath: c.req.path,
		method: c.req.method,
		tokenId: tokenRecord.id,
		model: downstreamModel,
		requestSeed: String(requestStart),
	});
	if (
		downstreamProvider === "openai" &&
		isStream &&
		parsedBody &&
		typeof parsedBody === "object"
	) {
		const streamOptions = (parsedBody as Record<string, unknown>)
			.stream_options;
		if (!streamOptions || typeof streamOptions !== "object") {
			(parsedBody as Record<string, unknown>).stream_options = {
				include_usage: true,
			};
		} else if (
			(streamOptions as Record<string, unknown>).include_usage !== true
		) {
			(streamOptions as Record<string, unknown>).include_usage = true;
		}
		requestText = JSON.stringify(parsedBody);
	}

	let normalizedChat: NormalizedChatRequest | null = null;
	let normalizedEmbedding: NormalizedEmbeddingRequest | null = null;
	let normalizedImage: NormalizedImageRequest | null = null;
	if (endpointType === "chat" || endpointType === "responses") {
		normalizedChat = normalizeChatRequest(
			downstreamProvider,
			endpointType,
			parsedBody,
			downstreamModel,
			isStream,
		);
	}
	if (endpointType === "embeddings") {
		normalizedEmbedding = normalizeEmbeddingRequest(
			downstreamProvider,
			parsedBody,
			downstreamModel,
		);
	}
	if (endpointType === "images") {
		normalizedImage = normalizeImageRequest(
			downstreamProvider,
			parsedBody,
			downstreamModel,
		);
	}

	const recordEarlyUsage = (options: {
		status: number;
		code: string;
		message?: string | null;
	}) => {
		const latencyMs = Date.now() - requestStart;
		const errorMessage = options.message ?? options.code;
		scheduleUsageEvent({
			type: "usage",
			payload: {
				tokenId: tokenRecord.id,
				channelId: null,
				model: downstreamModel,
				requestPath: c.req.path,
				totalTokens: 0,
				latencyMs,
				firstTokenLatencyMs: isStream ? null : latencyMs,
				stream: isStream,
				reasoningEffort,
				status: "error",
				upstreamStatus: options.status,
				errorCode: options.code,
				errorMessage,
			},
		});
	};
	const recordAttemptUsage = (options: {
		channelId: string | null;
		requestPath: string;
		latencyMs: number;
		firstTokenLatencyMs: number | null;
		usage: NormalizedUsage | null;
		status: "ok" | "error";
		upstreamStatus: number | null;
		errorCode?: string | null;
		errorMessage?: string | null;
	}) => {
		const normalized = options.usage ?? {
			totalTokens: 0,
			promptTokens: 0,
			completionTokens: 0,
		};
		scheduleUsageEvent({
			type: "usage",
			payload: {
				tokenId: tokenRecord.id,
				channelId: options.channelId,
				model: downstreamModel,
				requestPath: options.requestPath,
				totalTokens: normalized.totalTokens,
				promptTokens: normalized.promptTokens,
				completionTokens: normalized.completionTokens,
				cost: 0,
				latencyMs: options.latencyMs,
				firstTokenLatencyMs: options.firstTokenLatencyMs,
				stream: isStream,
				reasoningEffort,
				status: options.status,
				upstreamStatus: options.upstreamStatus,
				errorCode: options.errorCode ?? null,
				errorMessage: options.errorMessage ?? null,
			},
		});
	};

	const activeChannelsCacheKey = buildActiveChannelsKey(
		cacheConfig.version_channels,
	);
	let activeChannelRows = await readHotJson<ChannelRecord[]>(
		c.env.KV_HOT,
		activeChannelsCacheKey,
	);
	if (!Array.isArray(activeChannelRows)) {
		const activeChannels = await db
			.prepare("SELECT * FROM channels WHERE status = ?")
			.bind("active")
			.all<ChannelRecord>();
		activeChannelRows = (activeChannels.results ?? []) as ChannelRecord[];
		void writeHotJson(
			c.env.KV_HOT,
			activeChannelsCacheKey,
			activeChannelRows,
			HOT_KV_ACTIVE_CHANNELS_TTL_SECONDS,
		);
	}
	const channelIds = activeChannelRows.map((channel) => channel.id);
	const callTokensCacheKey = buildCallTokensIndexKey(
		cacheConfig.version_call_tokens,
		cacheConfig.version_channels,
	);
	const cachedCallTokenRows = await readHotJson<
		Array<{
			id: string;
			channel_id: string;
			name: string;
			api_key: string;
			models_json?: string | null;
		}>
	>(c.env.KV_HOT, callTokensCacheKey);
	let callTokenRows: Array<{
		id: string;
		channel_id: string;
		name: string;
		api_key: string;
		models_json?: string | null;
	}> = [];
	if (Array.isArray(cachedCallTokenRows)) {
		callTokenRows = cachedCallTokenRows;
	} else {
		callTokenRows = await listCallTokens(db, {
			channelIds,
		});
		void writeHotJson(
			c.env.KV_HOT,
			callTokensCacheKey,
			callTokenRows,
			HOT_KV_CALL_TOKENS_TTL_SECONDS,
		);
	}
	const callTokenMap = new Map<string, CallTokenItem[]>();
	for (const row of callTokenRows) {
		const entry: CallTokenItem = {
			id: row.id,
			channel_id: row.channel_id,
			name: row.name,
			api_key: row.api_key,
			models_json: row.models_json ?? null,
		};
		const list = callTokenMap.get(row.channel_id) ?? [];
		list.push(entry);
		callTokenMap.set(row.channel_id, list);
	}
	const allowedChannels = filterAllowedChannels(activeChannelRows, tokenRecord);
	const verifiedModelsByChannel = await listVerifiedModelsByChannel(
		db,
		allowedChannels.map((channel) => channel.id),
	);
	let candidates = selectCandidateChannels(
		allowedChannels,
		downstreamModel,
		verifiedModelsByChannel,
	);
	const cooldownMinutes = Math.max(
		0,
		Math.floor(runtimeSettings.model_failure_cooldown_minutes),
	);
	const cooldownSeconds = cooldownMinutes * 60;
	const cooldownFailureThreshold = Math.max(
		1,
		Math.floor(runtimeSettings.model_failure_cooldown_threshold),
	);
	const usageErrorMessageMaxLength = Math.max(
		1,
		Math.floor(runtimeSettings.usage_error_message_max_length),
	);
	const streamUsageParseTimeoutMs = Math.max(
		0,
		Math.floor(runtimeSettings.stream_usage_parse_timeout_ms),
	);
	if (downstreamModel && cooldownSeconds > 0 && candidates.length > 0) {
		const coolingChannels = await listCoolingDownChannelsForModel(
			db,
			candidates.map((channel) => channel.id),
			downstreamModel,
			cooldownSeconds,
			cooldownFailureThreshold,
		);
		if (coolingChannels.size > 0) {
			candidates = candidates.filter(
				(channel) => !coolingChannels.has(channel.id),
			);
			if (candidates.length === 0) {
				scheduleRuntimeEvent(
					"warning",
					"proxy_model_cooldown",
					"proxy_model_cooldown",
					{
						path: c.req.path,
						model: downstreamModel,
						cooldown_minutes: cooldownMinutes,
						cooldown_threshold: cooldownFailureThreshold,
						blocked_channels: coolingChannels.size,
					},
				);
				recordEarlyUsage({
					status: 503,
					code: "upstream_cooldown",
					message: "upstream_cooldown",
				});
				return jsonError(c, 503, "upstream_cooldown", "upstream_cooldown");
			}
		}
	}

	if (candidates.length === 0 && allowedChannels.length > 0) {
		scheduleRuntimeEvent(
			"warning",
			"proxy_no_compatible_channels",
			"proxy_no_compatible_channels",
			{
				path: c.req.path,
				model: downstreamModel,
				downstream_provider: downstreamProvider,
				allowed_channels: allowedChannels.length,
			},
		);
	}
	if (candidates.length === 0) {
		recordEarlyUsage({
			status: 503,
			code: "no_available_channels",
			message: "no_available_channels",
		});
		return jsonError(c, 503, "no_available_channels", "no_available_channels");
	}
	const targetPath = c.req.path;
	const querySuffix = c.req.url.includes("?")
		? `?${c.req.url.split("?")[1]}`
		: "";

	const maxRetries = Math.max(
		0,
		Math.floor(Number(runtimeSettings.retry_max_retries ?? 3)),
	);
	const ordered = createWeightedOrder(candidates).slice(0, maxRetries + 1);
	const upstreamTimeoutMs = Math.max(
		0,
		Math.floor(Number(runtimeSettings.upstream_timeout_ms ?? 30000)),
	);
	const nowSeconds = Math.floor(Date.now() / 1000);
	let selectedResponse: Response | null = null;
	let selectedChannel: ChannelRecord | null = null;
	let selectedUpstreamProvider: ProviderType | null = null;
	let selectedUpstreamEndpoint: EndpointType | null = null;
	let selectedUpstreamModel: string | null = null;
	let selectedRequestPath = targetPath;
	let selectedImmediateUsage: NormalizedUsage | null = null;
	let lastResponse: Response | null = null;
	let lastChannel: ChannelRecord | null = null;
	let lastRequestPath = targetPath;
	let lastErrorDetails: ErrorDetails | null = null;
	for (const channel of ordered) {
		lastChannel = channel;
		const attemptStart = Date.now();
		const metadata = parseChannelMetadata(channel.metadata_json);
		const upstreamProvider = resolveProvider(metadata.site_type);
		const resolvedModel = resolveUpstreamModelForChannel(
			channel,
			metadata,
			downstreamModel,
			verifiedModelsByChannel,
		);
		const upstreamModel = resolvedModel.model;
		const recordModel = upstreamModel ?? downstreamModel;
		if (
			upstreamProvider === "gemini" &&
			!upstreamModel &&
			endpointType !== "passthrough"
		) {
			continue;
		}
		const baseUrl = resolveChannelBaseUrl(channel);
		const tokens = callTokenMap.get(channel.id) ?? [];
		const selection = selectTokenForModel(tokens, recordModel);
		if (!selection.token && selection.hasModelList && recordModel) {
			continue;
		}
		const apiKey = selection.token?.api_key ?? channel.api_key;
		const headers = buildUpstreamHeaders(
			new Headers(c.req.header()),
			upstreamProvider,
			String(apiKey),
			metadata.header_overrides,
		);
		headers.delete("host");
		headers.delete("content-length");
		let upstreamRequestPath = targetPath;
		let upstreamFallbackPath: string | undefined;
		let upstreamBodyText = requestText || undefined;
		let absoluteUrl: string | undefined;
		const sameProvider = upstreamProvider === downstreamProvider;
		if (endpointType === "passthrough") {
			if (!sameProvider) {
				continue;
			}
			if (upstreamProvider === "gemini") {
				upstreamRequestPath = applyGeminiModelToPath(
					upstreamRequestPath,
					upstreamModel,
				);
			} else if (upstreamModel && parsedBody) {
				upstreamBodyText = JSON.stringify({
					...parsedBody,
					model: upstreamModel,
				});
			}
		} else if (sameProvider && parsedBody) {
			if (upstreamProvider === "gemini") {
				upstreamRequestPath = applyGeminiModelToPath(
					upstreamRequestPath,
					upstreamModel,
				);
			} else if (upstreamModel) {
				upstreamBodyText = JSON.stringify({
					...parsedBody,
					model: upstreamModel,
				});
			}
			if (endpointType === "chat" || endpointType === "responses") {
				if (metadata.endpoint_overrides.chat_url && normalizedChat) {
					const request = buildUpstreamChatRequest(
						upstreamProvider,
						normalizedChat,
						upstreamModel,
						endpointType,
						isStream,
						metadata.endpoint_overrides,
					);
					if (request) {
						upstreamRequestPath = request.path;
						absoluteUrl = request.absoluteUrl;
						upstreamFallbackPath = request.absoluteUrl
							? undefined
							: request.fallbackPath;
					}
				} else if (
					endpointType === "responses" &&
					upstreamProvider === "openai"
				) {
					upstreamFallbackPath = "/responses";
				}
			}
			if (
				endpointType === "embeddings" &&
				metadata.endpoint_overrides.embedding_url
			) {
				if (normalizedEmbedding) {
					const request = buildUpstreamEmbeddingRequest(
						upstreamProvider,
						normalizedEmbedding,
						upstreamModel,
						metadata.endpoint_overrides,
					);
					if (request) {
						upstreamRequestPath = request.path;
						absoluteUrl = request.absoluteUrl;
					}
				}
			}
			if (endpointType === "images" && metadata.endpoint_overrides.image_url) {
				if (normalizedImage) {
					const request = buildUpstreamImageRequest(
						upstreamProvider,
						normalizedImage,
						upstreamModel,
						metadata.endpoint_overrides,
					);
					if (request) {
						upstreamRequestPath = request.path;
						absoluteUrl = request.absoluteUrl;
					}
				}
			}
		} else {
			let built: {
				request: {
					path: string;
					fallbackPath?: string;
					absoluteUrl?: string;
					body: Record<string, unknown> | null;
				};
				bodyText?: string;
			} | null = null;
			if (endpointType === "chat" || endpointType === "responses") {
				if (!normalizedChat) {
					recordEarlyUsage({
						status: 400,
						code: "invalid_body",
						message: "invalid_body",
					});
					return jsonError(c, 400, "invalid_body", "invalid_body");
				}
				const request = buildUpstreamChatRequest(
					upstreamProvider,
					normalizedChat,
					upstreamModel,
					endpointType,
					isStream,
					metadata.endpoint_overrides,
				);
				if (!request) {
					continue;
				}
				built = {
					request,
					bodyText: request.body ? JSON.stringify(request.body) : undefined,
				};
			} else if (endpointType === "embeddings") {
				if (!normalizedEmbedding) {
					recordEarlyUsage({
						status: 400,
						code: "invalid_body",
						message: "invalid_body",
					});
					return jsonError(c, 400, "invalid_body", "invalid_body");
				}
				const request = buildUpstreamEmbeddingRequest(
					upstreamProvider,
					normalizedEmbedding,
					upstreamModel,
					metadata.endpoint_overrides,
				);
				if (!request) {
					continue;
				}
				built = {
					request,
					bodyText: request.body ? JSON.stringify(request.body) : undefined,
				};
			} else if (endpointType === "images") {
				if (!normalizedImage) {
					recordEarlyUsage({
						status: 400,
						code: "invalid_body",
						message: "invalid_body",
					});
					return jsonError(c, 400, "invalid_body", "invalid_body");
				}
				const request = buildUpstreamImageRequest(
					upstreamProvider,
					normalizedImage,
					upstreamModel,
					metadata.endpoint_overrides,
				);
				if (!request) {
					continue;
				}
				built = {
					request,
					bodyText: request.body ? JSON.stringify(request.body) : undefined,
				};
			}
			if (!built) {
				continue;
			}
			upstreamRequestPath = built.request.path;
			absoluteUrl = built.request.absoluteUrl;
			upstreamFallbackPath = built.request.absoluteUrl
				? undefined
				: built.request.fallbackPath;
			upstreamBodyText = built.bodyText;
		}
		const targetBase = absoluteUrl ?? `${baseUrl}${upstreamRequestPath}`;
		const target = mergeQuery(
			targetBase,
			querySuffix,
			metadata.query_overrides,
		);

		try {
			let response = await fetchWithTimeout(
				target,
				{
					method: c.req.method,
					headers,
					body: upstreamBodyText || undefined,
				},
				upstreamTimeoutMs,
			);
			let responsePath = upstreamRequestPath;
			if (
				(response.status === 400 || response.status === 404) &&
				upstreamFallbackPath
			) {
				const fallbackTargetBase = absoluteUrl
					? absoluteUrl
					: `${baseUrl}${upstreamFallbackPath}`;
				const fallbackTarget = mergeQuery(
					fallbackTargetBase,
					querySuffix,
					metadata.query_overrides,
				);
				response = await fetchWithTimeout(
					fallbackTarget,
					{
						method: c.req.method,
						headers,
						body: upstreamBodyText || undefined,
					},
					upstreamTimeoutMs,
				);
				responsePath = upstreamFallbackPath;
			}

			const attemptLatencyMs = Date.now() - attemptStart;
			lastResponse = response;
			lastRequestPath = responsePath;
			if (response.ok) {
				const headerUsage = parseUsageFromHeaders(response.headers);
				let jsonUsage: NormalizedUsage | null = null;
				if (
					!isStream &&
					response.headers.get("content-type")?.includes("application/json")
				) {
					const data = await response
						.clone()
						.json()
						.catch(() => null);
					jsonUsage = parseUsageFromJson(data);
				}
				const immediateUsage = jsonUsage ?? headerUsage;
				if (!isStream && !immediateUsage) {
					const usageMissingMessage =
						"usage_missing: non-stream response does not include usage in json or headers";
					lastErrorDetails = {
						upstreamStatus: response.status,
						errorCode: "usage_missing",
						errorMessage: usageMissingMessage,
					};
					recordAttemptUsage({
						channelId: channel.id,
						requestPath: responsePath,
						latencyMs: attemptLatencyMs,
						firstTokenLatencyMs: attemptLatencyMs,
						usage: null,
						status: "error",
						upstreamStatus: response.status,
						errorCode: "usage_missing",
						errorMessage: usageMissingMessage,
					});
					continue;
				}
				if (!isStream) {
					recordAttemptUsage({
						channelId: channel.id,
						requestPath: responsePath,
						latencyMs: attemptLatencyMs,
						firstTokenLatencyMs: attemptLatencyMs,
						usage: immediateUsage,
						status: "ok",
						upstreamStatus: response.status,
					});
				}
				selectedChannel = channel;
				selectedUpstreamProvider = upstreamProvider;
				try {
					selectedUpstreamEndpoint = detectEndpointType(
						upstreamProvider,
						responsePath,
					);
				} catch {
					selectedUpstreamEndpoint = endpointType;
				}
				selectedUpstreamModel = upstreamModel;
				selectedResponse = response;
				selectedRequestPath = responsePath;
				selectedImmediateUsage = immediateUsage;
				lastErrorDetails = null;
				if (recordModel) {
					scheduleUsageEvent({
						type: "capability_upsert",
						payload: {
							channelId: channel.id,
							models: [recordModel],
							nowSeconds,
						},
					});
				}
				break;
			}
			const errorInfo = await extractErrorDetails(response);
			const normalizedErrorCode = normalizeUpstreamErrorCode(
				errorInfo.errorCode,
				response.status,
			);
			const normalizedErrorMessage =
				normalizeMessage(errorInfo.errorMessage) ?? normalizedErrorCode;
			lastErrorDetails = {
				upstreamStatus: response.status,
				errorCode: normalizedErrorCode,
				errorMessage: normalizedErrorMessage,
			};
			recordAttemptUsage({
				channelId: channel.id,
				requestPath: responsePath,
				latencyMs: attemptLatencyMs,
				firstTokenLatencyMs: isStream ? null : attemptLatencyMs,
				usage: null,
				status: "error",
				upstreamStatus: response.status,
				errorCode: normalizedErrorCode,
				errorMessage: normalizedErrorMessage,
			});
			const cooldownEligible = shouldCooldown(
				response.status,
				normalizedErrorCode,
			);
			if (recordModel && cooldownSeconds > 0 && cooldownEligible) {
				scheduleUsageEvent({
					type: "model_error",
					payload: {
						channelId: channel.id,
						model: recordModel,
						errorCode: String(response.status),
						nowSeconds,
					},
				});
			}
			if (
				downstreamModel &&
				downstreamModel !== recordModel &&
				cooldownSeconds > 0 &&
				cooldownEligible
			) {
				scheduleUsageEvent({
					type: "model_error",
					payload: {
						channelId: channel.id,
						model: downstreamModel,
						errorCode: String(response.status),
						nowSeconds,
					},
				});
			}
		} catch (error) {
			const isTimeout =
				error instanceof Error &&
				(error.name === "AbortError" ||
					error.message.includes("upstream_timeout"));
			const usageErrorCode = isTimeout
				? PROXY_UPSTREAM_TIMEOUT_ERROR_CODE
				: PROXY_UPSTREAM_FETCH_ERROR_CODE;
			const usageErrorDetail = normalizeMessage(
				error instanceof Error ? error.message : String(error),
			);
			const usageErrorMessage = formatUsageErrorMessage(
				usageErrorCode,
				usageErrorDetail,
				usageErrorMessageMaxLength,
			);
			scheduleRuntimeEvent(
				"error",
				"proxy_upstream_exception",
				"proxy_upstream_exception",
				{
					channel_id: channel.id,
					upstream_provider: upstreamProvider,
					path: upstreamRequestPath,
					model: downstreamModel,
					upstream_model: upstreamModel,
					timeout_ms: upstreamTimeoutMs,
					reason: isTimeout ? "timeout" : "exception",
					error: error instanceof Error ? error.message : String(error),
				},
			);
			const attemptLatencyMs = Date.now() - attemptStart;
			lastErrorDetails = {
				upstreamStatus: null,
				errorCode: usageErrorCode,
				errorMessage: usageErrorMessage,
			};
			recordAttemptUsage({
				channelId: channel.id,
				requestPath: upstreamRequestPath,
				latencyMs: attemptLatencyMs,
				firstTokenLatencyMs: null,
				usage: null,
				status: "error",
				upstreamStatus: null,
				errorCode: lastErrorDetails.errorCode,
				errorMessage: lastErrorDetails.errorMessage,
			});
			const cooldownEligible = shouldCooldown(null, usageErrorCode);
			if (recordModel && cooldownSeconds > 0 && cooldownEligible) {
				scheduleUsageEvent({
					type: "model_error",
					payload: {
						channelId: channel.id,
						model: recordModel,
						errorCode: usageErrorCode,
						nowSeconds,
					},
				});
			}
			if (
				downstreamModel &&
				downstreamModel !== recordModel &&
				cooldownSeconds > 0 &&
				cooldownEligible
			) {
				scheduleUsageEvent({
					type: "model_error",
					payload: {
						channelId: channel.id,
						model: downstreamModel,
						errorCode: usageErrorCode,
						nowSeconds,
					},
				});
			}
			lastResponse = null;
		}
	}

	if (!selectedResponse && lastResponse && !lastResponse.ok) {
		scheduleRuntimeEvent(
			"warning",
			"proxy_upstream_exhausted",
			"proxy_upstream_exhausted",
			{
				path: targetPath,
				model: downstreamModel,
				status: lastResponse.status,
				last_channel_id: lastChannel?.id ?? null,
				last_request_path: lastRequestPath,
			},
		);
	}

	if (!selectedResponse) {
		if (lastResponse && !lastResponse.ok) {
			return lastResponse;
		}
		if (lastErrorDetails) {
			const errorCode = lastErrorDetails.errorCode ?? "upstream_unavailable";
			return jsonError(c, 502, errorCode, errorCode);
		}
		scheduleRuntimeEvent("error", "proxy_unavailable", "proxy_unavailable", {
			path: targetPath,
			model: downstreamModel,
			latency_ms: Date.now() - requestStart,
			last_channel_id: lastChannel?.id ?? null,
		});
		recordEarlyUsage({
			status: 502,
			code: "upstream_unavailable",
			message: "upstream_unavailable",
		});
		return jsonError(c, 502, "upstream_unavailable", "upstream_unavailable");
	}

	if (selectedChannel && isStream) {
		const selectedLatencyMs = Date.now() - requestStart;
		const executionCtx = (c as { executionCtx?: ExecutionContextLike })
			.executionCtx;
		const streamUsageOptions = getStreamUsageOptions(runtimeSettings);
		const streamUsageMaxParsers = getStreamUsageMaxParsers(runtimeSettings);
		let usageFinalized = false;
		const finalizeUsage = (
			options: Parameters<typeof recordAttemptUsage>[0],
		) => {
			if (usageFinalized) {
				return;
			}
			usageFinalized = true;
			recordAttemptUsage(options);
		};
		const canParseStream =
			streamUsageOptions.mode !== "off" &&
			activeStreamUsageParsers < streamUsageMaxParsers;
		if (!canParseStream) {
			finalizeUsage({
				channelId: selectedChannel.id,
				requestPath: selectedRequestPath,
				latencyMs: selectedLatencyMs,
				firstTokenLatencyMs: null,
				usage: selectedImmediateUsage,
				status: "ok",
				upstreamStatus: selectedResponse.status,
			});
		} else {
			activeStreamUsageParsers += 1;
			const task = parseUsageFromSse(selectedResponse.clone(), {
				...streamUsageOptions,
				timeoutMs: streamUsageParseTimeoutMs,
			})
				.then((streamUsage) => {
					const usageValue = selectedImmediateUsage ?? streamUsage.usage;
					if (streamUsage.timedOut) {
						scheduleRuntimeEvent(
							"warning",
							"usage_stream_parse_timeout",
							"usage_stream_parse_timeout",
							{
								path: selectedRequestPath,
								timeout_ms: streamUsageParseTimeoutMs,
							},
						);
						const timeoutMessage = `usage_parse_timeout: stream usage parsing timed out after ${streamUsageParseTimeoutMs}ms`;
						finalizeUsage({
							channelId: selectedChannel.id,
							requestPath: selectedRequestPath,
							latencyMs: selectedLatencyMs,
							firstTokenLatencyMs: streamUsage.firstTokenLatencyMs,
							usage: usageValue,
							status: usageValue ? "ok" : "error",
							upstreamStatus: selectedResponse.status,
							errorCode: usageValue ? null : "usage_parse_timeout",
							errorMessage: usageValue ? null : timeoutMessage,
						});
						return;
					}
					if (!usageValue) {
						const streamUsageMissingMessage =
							"usage_missing: stream completed without usage payload or header usage";
						finalizeUsage({
							channelId: selectedChannel.id,
							requestPath: selectedRequestPath,
							latencyMs: selectedLatencyMs,
							firstTokenLatencyMs: streamUsage.firstTokenLatencyMs,
							usage: null,
							status: "error",
							upstreamStatus: selectedResponse.status,
							errorCode: "usage_missing",
							errorMessage: streamUsageMissingMessage,
						});
						return;
					}
					finalizeUsage({
						channelId: selectedChannel.id,
						requestPath: selectedRequestPath,
						latencyMs: selectedLatencyMs,
						firstTokenLatencyMs: streamUsage.firstTokenLatencyMs,
						usage: usageValue,
						status: "ok",
						upstreamStatus: selectedResponse.status,
					});
				})
				.catch((error) => {
					const parseFailure = classifyStreamUsageParseError(
						error,
						usageErrorMessageMaxLength,
					);
					scheduleRuntimeEvent(
						"warning",
						"usage_stream_parse_failed",
						"usage_stream_parse_failed",
						{
							path: selectedRequestPath,
							error_code: parseFailure.errorCode,
							error_message: parseFailure.errorMessage,
						},
					);
					finalizeUsage({
						channelId: selectedChannel.id,
						requestPath: selectedRequestPath,
						latencyMs: selectedLatencyMs,
						firstTokenLatencyMs: null,
						usage: selectedImmediateUsage,
						status: selectedImmediateUsage ? "ok" : "error",
						upstreamStatus: selectedResponse.status,
						errorCode: selectedImmediateUsage ? null : parseFailure.errorCode,
						errorMessage: selectedImmediateUsage
							? null
							: parseFailure.errorMessage,
					});
				})
				.finally(() => {
					activeStreamUsageParsers = Math.max(0, activeStreamUsageParsers - 1);
				});
			if (executionCtx?.waitUntil) {
				executionCtx.waitUntil(task);
			} else {
				task.catch(() => undefined);
			}
		}
	}

	if (
		selectedUpstreamProvider &&
		selectedUpstreamEndpoint &&
		(endpointType === "chat" || endpointType === "responses")
	) {
		const transformed = await adaptChatResponse({
			response: selectedResponse,
			upstreamProvider: selectedUpstreamProvider,
			downstreamProvider,
			upstreamEndpoint: selectedUpstreamEndpoint,
			downstreamEndpoint: endpointType,
			model: selectedUpstreamModel ?? downstreamModel,
			isStream,
		});
		if (transformed !== selectedResponse) {
			return transformed;
		}
	}

	return selectedResponse;
});

export default proxy;
