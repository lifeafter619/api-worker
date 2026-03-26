import type { D1Database } from "@cloudflare/workers-types";

export type AttemptLogInput = {
	traceId: string;
	attemptIndex: number;
	channelId: string | null;
	provider: string | null;
	model: string | null;
	status: "ok" | "error";
	errorClass: string | null;
	errorCode: string | null;
	httpStatus: number | null;
	latencyMs: number;
	upstreamRequestId: string | null;
	startedAt: string;
	endedAt: string;
	rawSizeBytes: number | null;
	rawHash: string | null;
	createdAt?: string | null;
};

export type AttemptEventRecord = {
	id: string;
	trace_id: string;
	attempt_index: number;
	channel_id: string | null;
	provider: string | null;
	model: string | null;
	status: string;
	error_class: string | null;
	error_code: string | null;
	http_status: number | null;
	latency_ms: number;
	upstream_request_id: string | null;
	started_at: string;
	ended_at: string;
	raw_size_bytes: number | null;
	raw_hash: string | null;
	created_at: string;
};

export async function insertAttemptEvent(
	db: D1Database,
	input: AttemptLogInput,
): Promise<void> {
	const createdAt = input.createdAt ?? new Date().toISOString();
	await db
		.prepare(
			"INSERT INTO attempt_events (id, trace_id, attempt_index, channel_id, provider, model, status, error_class, error_code, http_status, latency_ms, upstream_request_id, started_at, ended_at, raw_size_bytes, raw_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		)
		.bind(
			crypto.randomUUID(),
			input.traceId,
			Math.max(0, Math.floor(input.attemptIndex)),
			input.channelId ?? null,
			input.provider ?? null,
			input.model ?? null,
			input.status,
			input.errorClass ?? null,
			input.errorCode ?? null,
			input.httpStatus ?? null,
			Math.max(0, Math.floor(input.latencyMs)),
			input.upstreamRequestId ?? null,
			input.startedAt,
			input.endedAt,
			input.rawSizeBytes ?? null,
			input.rawHash ?? null,
			createdAt,
		)
		.run();
}

export async function listAttemptEventsByTrace(
	db: D1Database,
	traceId: string,
): Promise<AttemptEventRecord[]> {
	const result = await db
		.prepare(
			"SELECT * FROM attempt_events WHERE trace_id = ? ORDER BY attempt_index ASC, created_at ASC",
		)
		.bind(traceId)
		.all<AttemptEventRecord>();
	return (result.results ?? []) as AttemptEventRecord[];
}

export async function pruneAttemptEvents(
	db: D1Database,
	retentionDays: number,
): Promise<void> {
	const cutoff = new Date();
	cutoff.setDate(cutoff.getDate() - Math.max(1, Math.floor(retentionDays)));
	await db
		.prepare("DELETE FROM attempt_events WHERE created_at < ?")
		.bind(cutoff.toISOString())
		.run();
}
