import { Hono } from "hono";
import type { AppEnv } from "../env";

const dashboard = new Hono<AppEnv>();

function splitCsv(value?: string): string[] {
	if (!value) {
		return [];
	}
	return String(value)
		.split(",")
		.map((item) => item.trim())
		.filter(Boolean);
}

function buildDateFilters(query: Record<string, string>) {
	let sql = " WHERE 1=1";
	const params: Array<string> = [];
	const channelIds = splitCsv(query.channel_ids);
	const tokenIds = splitCsv(query.token_ids);
	if (query.from) {
		sql += " AND created_at >= ?";
		params.push(query.from);
	}
	if (query.to) {
		sql += " AND created_at <= ?";
		params.push(query.to);
	}
	if (query.model) {
		sql += " AND model LIKE ? COLLATE NOCASE";
		params.push(`%${query.model}%`);
	}
	if (channelIds.length > 0) {
		sql += ` AND channel_id IN (${channelIds.map(() => "?").join(",")})`;
		params.push(...channelIds);
	}
	if (tokenIds.length > 0) {
		sql += ` AND token_id IN (${tokenIds.map(() => "?").join(",")})`;
		params.push(...tokenIds);
	}
	return { sql, params };
}

async function buildNameMap(
	metaDb: AppEnv["Bindings"]["DB"],
	table: "channels" | "tokens",
	ids: string[],
): Promise<Map<string, string>> {
	const map = new Map<string, string>();
	if (ids.length === 0) {
		return map;
	}
	const placeholders = ids.map(() => "?").join(", ");
	const result = await metaDb
		.prepare(`SELECT id, name FROM ${table} WHERE id IN (${placeholders})`)
		.bind(...ids)
		.all<{ id: string; name: string }>();
	for (const row of result.results ?? []) {
		map.set(String(row.id), String(row.name ?? ""));
	}
	return map;
}

/**
 * Returns aggregated usage metrics.
 */
dashboard.get("/", async (c) => {
	const db = c.env.DB;
	const query = c.req.query();
	const interval =
		query.interval === "week" || query.interval === "month"
			? query.interval
			: "day";
	const rawLimit = Number(query.limit ?? 30);
	const normalizedLimit = Number.isNaN(rawLimit) ? 30 : Math.floor(rawLimit);
	const limit = Math.min(Math.max(normalizedLimit, 1), 366);
	const { sql, params } = buildDateFilters(query);
	const bucketExpression =
		interval === "week"
			? "strftime('%Y-W%W', created_at)"
			: interval === "month"
				? "substr(created_at, 1, 7)"
				: "substr(created_at, 1, 10)";

	const summary = await db
		.prepare(
			`SELECT COUNT(*) as total_requests, COALESCE(SUM(total_tokens), 0) as total_tokens, COALESCE(AVG(latency_ms), 0) as avg_latency, COALESCE(SUM(CASE WHEN status != 'ok' THEN 1 ELSE 0 END), 0) as total_errors FROM usage_logs${sql}`,
		)
		.bind(...params)
		.first();

	const trend = await db
		.prepare(
			`SELECT ${bucketExpression} as bucket, COUNT(*) as requests, COALESCE(SUM(total_tokens), 0) as tokens FROM usage_logs${sql} GROUP BY bucket ORDER BY bucket ASC LIMIT ?`,
		)
		.bind(...params, limit)
		.all();

	const byModel = await db
		.prepare(
			`SELECT model, COUNT(*) as requests, COALESCE(SUM(total_tokens), 0) as tokens FROM usage_logs${sql} GROUP BY model ORDER BY requests DESC LIMIT 20`,
		)
		.bind(...params)
		.all();

	const byChannelAgg = await db
		.prepare(
			`SELECT channel_id, COUNT(*) as requests, COALESCE(SUM(total_tokens), 0) as tokens FROM usage_logs${sql} GROUP BY channel_id ORDER BY requests DESC LIMIT 20`,
		)
		.bind(...params)
		.all<{ channel_id: string | null; requests: number; tokens: number }>();

	const byTokenAgg = await db
		.prepare(
			`SELECT token_id, COUNT(*) as requests, COALESCE(SUM(total_tokens), 0) as tokens FROM usage_logs${sql} GROUP BY token_id ORDER BY requests DESC LIMIT 20`,
		)
		.bind(...params)
		.all<{ token_id: string | null; requests: number; tokens: number }>();

	const channelIds = Array.from(
		new Set(
			(byChannelAgg.results ?? [])
				.map((row) => String(row.channel_id ?? "").trim())
				.filter(Boolean),
		),
	);
	const tokenIds = Array.from(
		new Set(
			(byTokenAgg.results ?? [])
				.map((row) => String(row.token_id ?? "").trim())
				.filter(Boolean),
		),
	);

	const [channelNameMap, tokenNameMap] = await Promise.all([
		buildNameMap(db, "channels", channelIds),
		buildNameMap(db, "tokens", tokenIds),
	]);

	const byChannel = (byChannelAgg.results ?? []).map((row) => {
		const channelId = String(row.channel_id ?? "").trim();
		return {
			channel_name: channelId
				? (channelNameMap.get(channelId) ?? channelId)
				: "未归属",
			requests: Number(row.requests ?? 0),
			tokens: Number(row.tokens ?? 0),
		};
	});

	const byToken = (byTokenAgg.results ?? []).map((row) => {
		const tokenId = String(row.token_id ?? "").trim();
		return {
			token_name: tokenId ? (tokenNameMap.get(tokenId) ?? tokenId) : "未归属",
			requests: Number(row.requests ?? 0),
			tokens: Number(row.tokens ?? 0),
		};
	});

	return c.json({
		summary: summary ?? {
			total_requests: 0,
			total_tokens: 0,
			avg_latency: 0,
			total_errors: 0,
		},
		trend: trend.results ?? [],
		interval,
		byModel: byModel.results ?? [],
		byChannel,
		byToken,
	});
});

export default dashboard;
