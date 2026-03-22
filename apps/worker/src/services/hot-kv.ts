import type { KVNamespace } from "@cloudflare/workers-types";

const HOT_PREFIX = "hot";

function buildVersionKey(parts: Array<string | number>): string {
	return parts.map((part) => String(part)).join(":");
}

export function buildActiveChannelsKey(versionChannels: number): string {
	return `${HOT_PREFIX}:active_channels:${buildVersionKey(["v", versionChannels])}`;
}

export function buildModelsIndexKey(versionModels: number): string {
	return `${HOT_PREFIX}:models_index:${buildVersionKey(["v", versionModels])}`;
}

export function buildCallTokensIndexKey(
	versionCallTokens: number,
	versionChannels: number,
): string {
	return `${HOT_PREFIX}:call_tokens_index:${buildVersionKey([
		"v",
		versionCallTokens,
		"cv",
		versionChannels,
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
