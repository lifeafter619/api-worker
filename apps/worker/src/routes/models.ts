import { Hono } from "hono";
import type { AppEnv } from "../env";
import { listModelEntriesWithFallback } from "../services/channel-model-capabilities";
import { listActiveChannels } from "../services/channel-repo";
import {
	buildModelsIndexKey,
	readHotJson,
	writeHotJson,
} from "../services/hot-kv";

const models = new Hono<AppEnv>();

/**
 * Returns aggregated models from all channels.
 */
models.get("/", async (c) => {
	const db = c.env.DB;
	const cacheKey = buildModelsIndexKey();
	const cached = await readHotJson<{
		models: Array<{
			id: string;
			channels: Array<{ id: string; name: string }>;
		}>;
	}>(c.env.KV_HOT, cacheKey);
	if (cached && Array.isArray(cached.models)) {
		return c.json(cached);
	}

	const channels = await listActiveChannels(db);
	const entries = await listModelEntriesWithFallback(
		db,
		channels.map((channel) => ({
			id: channel.id,
			name: channel.name,
			models_json: channel.models_json,
		})),
	);

	const map = new Map<
		string,
		{ id: string; channels: { id: string; name: string }[] }
	>();
	for (const entry of entries) {
		const existing = map.get(entry.id) ?? { id: entry.id, channels: [] };
		existing.channels.push({
			id: entry.channelId,
			name: entry.channelName,
		});
		map.set(entry.id, existing);
	}

	const payload = {
		models: Array.from(map.values()),
	};
	void writeHotJson(c.env.KV_HOT, cacheKey, payload, 120);
	return c.json(payload);
});

export default models;
