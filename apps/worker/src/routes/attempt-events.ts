import { Hono } from "hono";
import type { AppEnv } from "../env";
import {
	listAttemptEventsByTrace,
	pruneAttemptEvents,
} from "../services/attempt-events";
import { getAttemptLogRetentionDays } from "../services/settings";
import { jsonError } from "../utils/http";

const attemptEvents = new Hono<AppEnv>();

attemptEvents.get("/", async (c) => {
	const traceId = String(c.req.query("trace_id") ?? "").trim();
	if (!traceId) {
		return jsonError(c, 400, "trace_id_required", "trace_id_required");
	}
	const retentionDays = await getAttemptLogRetentionDays(c.env.DB);
	await pruneAttemptEvents(c.env.DB, retentionDays);
	const events = await listAttemptEventsByTrace(c.env.DB, traceId);
	return c.json({
		trace_id: traceId,
		total: events.length,
		events,
	});
});

export default attemptEvents;
