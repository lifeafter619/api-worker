import { Hono } from "hono";
import type { AppEnv } from "../env";
import { getRetentionDays } from "../services/settings";
import { pruneUsageLogs } from "../services/usage";

const usage = new Hono<AppEnv>();

function splitCsv(value?: string): string[] {
	if (!value) {
		return [];
	}
	return String(value)
		.split(",")
		.map((item) => item.trim())
		.filter(Boolean);
}

function normalizeLimit(value: unknown): number {
	const raw = Number(value ?? 50);
	const normalized = Number.isNaN(raw) ? 50 : Math.floor(raw);
	return Math.min(Math.max(normalized, 1), 200);
}

function normalizeOffset(value: unknown): number {
	const raw = Number(value ?? 0);
	return Number.isNaN(raw) ? 0 : Math.max(0, Math.floor(raw));
}

function normalizeErrorCodeLimit(value: unknown): number {
	const raw = Number(value ?? 200);
	const normalized = Number.isNaN(raw) ? 200 : Math.floor(raw);
	return Math.min(Math.max(normalized, 1), 1000);
}

function applyInFilter(
	field: string,
	values: string[],
	filters: string[],
	params: Array<string | number>,
): void {
	if (values.length === 0) {
		return;
	}
	const placeholders = values.map(() => "?").join(", ");
	filters.push(`${field} IN (${placeholders})`);
	params.push(...values);
}

async function findIdsByName(
	table: "channels" | "tokens",
	metaDb: AppEnv["Bindings"]["DB"],
	name: string,
): Promise<string[]> {
	const result = await metaDb
		.prepare(`SELECT id FROM ${table} WHERE name LIKE ? COLLATE NOCASE`)
		.bind(`%${name}%`)
		.all<{ id: string }>();
	return (result.results ?? [])
		.map((row) => String(row.id ?? "").trim())
		.filter(Boolean);
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
 * Returns distinct error codes from usage logs.
 */
usage.get("/error-codes", async (c) => {
	const db = c.env.DB;
	const limit = normalizeErrorCodeLimit(c.req.query("limit"));
	const retention = await getRetentionDays(db);
	await pruneUsageLogs(db, retention);

	const rows = await db
		.prepare(
			[
				"SELECT error_code, COUNT(*) AS trace_count",
				"FROM usage_logs",
				"WHERE error_code IS NOT NULL AND TRIM(error_code) != ''",
				"GROUP BY error_code",
				"ORDER BY trace_count DESC, error_code ASC",
				"LIMIT ?",
			].join(" "),
		)
		.bind(limit)
		.all<{ error_code: string; trace_count: number }>();

	const items = (rows.results ?? []).map((row) => ({
		error_code: String(row.error_code ?? "").trim(),
		trace_count: Number(row.trace_count ?? 0),
	}));

	return c.json({ items });
});

/**
 * Lists usage logs with filters.
 */
usage.get("/", async (c) => {
	const db = c.env.DB;
	const query = c.req.query();
	const filters: string[] = [];
	const params: Array<string | number> = [];

	if (query.from) {
		filters.push("created_at >= ?");
		params.push(query.from);
	}
	if (query.to) {
		filters.push("created_at <= ?");
		params.push(query.to);
	}
	if (query.model) {
		const model = String(query.model).trim();
		if (model) {
			filters.push("model LIKE ? COLLATE NOCASE");
			params.push(`%${model}%`);
		}
	}
	if (query.channel_id) {
		filters.push("channel_id = ?");
		params.push(query.channel_id);
	}
	if (query.token_id) {
		filters.push("token_id = ?");
		params.push(query.token_id);
	}
	const channelIds = splitCsv(query.channel_ids);
	const tokenIds = splitCsv(query.token_ids);
	const models = splitCsv(query.models);
	applyInFilter("channel_id", channelIds, filters, params);
	applyInFilter("token_id", tokenIds, filters, params);
	applyInFilter("model", models, filters, params);

	if (query.channel) {
		const channel = String(query.channel).trim();
		if (channel) {
			const ids = await findIdsByName("channels", db, channel);
			if (ids.length === 0) {
				filters.push("1 = 0");
			} else {
				applyInFilter("channel_id", ids, filters, params);
			}
		}
	}

	if (query.token) {
		const token = String(query.token).trim();
		if (token) {
			const ids = await findIdsByName("tokens", db, token);
			if (ids.length === 0) {
				filters.push("1 = 0");
			} else {
				applyInFilter("token_id", ids, filters, params);
			}
		}
	}

	if (query.statuses) {
		const statuses = splitCsv(query.statuses);
		if (statuses.length > 0) {
			const numericStatuses = statuses
				.map((item) => Number(item))
				.filter((value) => !Number.isNaN(value));
			const textStatuses = statuses.filter((item) =>
				Number.isNaN(Number(item)),
			);
			const statusFilters: string[] = [];
			if (numericStatuses.length > 0) {
				const placeholders = numericStatuses.map(() => "?").join(", ");
				statusFilters.push(`upstream_status IN (${placeholders})`);
				params.push(...numericStatuses);
			}
			if (textStatuses.length > 0) {
				const placeholders = textStatuses.map(() => "?").join(", ");
				statusFilters.push(`status IN (${placeholders})`);
				params.push(...textStatuses);
			}
			if (statusFilters.length > 0) {
				filters.push(`(${statusFilters.join(" OR ")})`);
			}
		}
	}

	if (query.status) {
		const rawStatus = String(query.status).trim();
		if (rawStatus) {
			const numericStatus = Number(rawStatus);
			if (Number.isNaN(numericStatus)) {
				filters.push("status LIKE ? COLLATE NOCASE");
				params.push(`%${rawStatus}%`);
			} else {
				filters.push("upstream_status = ?");
				params.push(numericStatus);
			}
		}
	}

	const limit = normalizeLimit(query.limit);
	const offset = normalizeOffset(query.offset);
	const whereSql = filters.length > 0 ? `WHERE ${filters.join(" AND ")}` : "";

	const retention = await getRetentionDays(db);
	await pruneUsageLogs(db, retention);

	const countRow = await db
		.prepare(`SELECT COUNT(*) AS total FROM usage_logs ${whereSql}`)
		.bind(...params)
		.first<{ total: number }>();
	const total = Number(countRow?.total ?? 0);

	const result = await db
		.prepare(
			`SELECT * FROM usage_logs ${whereSql} ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		)
		.bind(...params, limit, offset)
		.all<Record<string, unknown>>();
	const logs = (result.results ?? []) as Array<Record<string, unknown>>;

	const pageChannelIds = Array.from(
		new Set(
			logs.map((row) => String(row.channel_id ?? "").trim()).filter(Boolean),
		),
	);
	const pageTokenIds = Array.from(
		new Set(
			logs.map((row) => String(row.token_id ?? "").trim()).filter(Boolean),
		),
	);

	const [channelNameMap, tokenNameMap] = await Promise.all([
		buildNameMap(db, "channels", pageChannelIds),
		buildNameMap(db, "tokens", pageTokenIds),
	]);

	const enriched = logs.map((row) => {
		const channelId = String(row.channel_id ?? "").trim();
		const tokenId = String(row.token_id ?? "").trim();
		return {
			...row,
			channel_name: channelId ? (channelNameMap.get(channelId) ?? null) : null,
			token_name: tokenId ? (tokenNameMap.get(tokenId) ?? null) : null,
		};
	});

	return c.json({
		logs: enriched,
		total,
		limit,
		offset,
	});
});

export default usage;
