import type { KVNamespace } from "@cloudflare/workers-types";

const HOT_PREFIX = "hot";
const ACTIVE_CHANNELS_KEY = `${HOT_PREFIX}:active_channels`;
const MODELS_INDEX_KEY = `${HOT_PREFIX}:models_index`;
const CALL_TOKENS_INDEX_KEY = `${HOT_PREFIX}:call_tokens_index`;

function buildKey(parts: Array<string | number>): string {
	return parts.map((part) => String(part)).join(":");
}

export function buildActiveChannelsKey(): string {
	return ACTIVE_CHANNELS_KEY;
}

export function buildModelsIndexKey(): string {
	return MODELS_INDEX_KEY;
}

export function buildCallTokensIndexKey(): string {
	return CALL_TOKENS_INDEX_KEY;
}

export function buildResponsesAffinityKey(responseId: string): string {
	return `${HOT_PREFIX}:responses_affinity:${buildKey([
		"id",
		responseId.trim(),
	])}`;
}

export function buildStreamOptionsCapabilityKey(channelId: string): string {
	return `${HOT_PREFIX}:stream_options_capability:${buildKey([
		"channel",
		channelId.trim(),
	])}`;
}

export async function readHotJson<T>(
	kv: KVNamespace | undefined,
	key: string,
): Promise<T | null> {
	if (!kv) {
		return null;
	}
	try {
		const value = await kv.get(key, "json");
		return value === null ? null : (value as T);
	} catch {
		return null;
	}
}

export async function writeHotJson<T>(
	kv: KVNamespace | undefined,
	key: string,
	value: T,
	ttlSeconds: number,
): Promise<void> {
	if (!kv) {
		return;
	}
	const safeTtl = Math.max(60, Math.floor(ttlSeconds));
	try {
		await kv.put(key, JSON.stringify(value), {
			expirationTtl: safeTtl,
		});
	} catch {
		// ignore KV failures and fall back to D1
	}
}

export async function deleteHotKey(
	kv: KVNamespace | undefined,
	key: string,
): Promise<void> {
	if (!kv) {
		return;
	}
	try {
		await kv.delete(key);
	} catch {
		// ignore KV failures and fall back to D1
	}
}

export async function invalidateSelectionHotCache(
	kv: KVNamespace | undefined,
): Promise<void> {
	await Promise.all([
		deleteHotKey(kv, buildActiveChannelsKey()),
		deleteHotKey(kv, buildModelsIndexKey()),
		deleteHotKey(kv, buildCallTokensIndexKey()),
	]);
}
