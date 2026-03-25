import { Hono } from "hono";
import type { AppEnv } from "../env";
import {
	getCheckinSchedulerStub,
	shouldResetLastRun,
} from "../services/checkin-scheduler";
import {
	getCheckinScheduleTime,
	getProxyRuntimeSettings,
	getRetentionDays,
	getRuntimeProxyConfig,
	getSessionTtlHours,
	isAdminPasswordSet,
	setAdminPasswordHash,
	setCheckinScheduleTime,
	setProxyRuntimeSettings,
	setRetentionDays,
	setSessionTtlHours,
} from "../services/settings";
import { sha256Hex } from "../utils/crypto";
import { jsonError } from "../utils/http";

const settings = new Hono<AppEnv>();

type UsageQueueStatusView = {
	count: number | null;
	date: string | null;
	limit: number;
	enabled: boolean;
	bound: boolean;
	active: boolean;
	reserved_count: number | null;
	enqueue_success_count: number | null;
	direct_count: number | null;
	fallback_direct_count: number | null;
	reserve_failed_count: number | null;
	reserve_over_limit_count: number | null;
	queue_send_failed_count: number | null;
	target_queue_ratio: number;
	target_direct_ratio: number;
	effective_queue_ratio: number | null;
	effective_direct_ratio: number | null;
	effective_total_count: number | null;
};

/**
 * Returns settings values.
 */
settings.get("/", async (c) => {
	const db = c.env.DB;
	const retention = await getRetentionDays(db);
	const sessionTtlHours = await getSessionTtlHours(db);
	const adminPasswordSet = await isAdminPasswordSet(db);
	const checkinScheduleTime = await getCheckinScheduleTime(db);
	const runtimeSettings = await getProxyRuntimeSettings(db);
	const runtimeConfig = getRuntimeProxyConfig(c.env, runtimeSettings);

	const emptyUsageQueueStatus: UsageQueueStatusView = {
		count: null,
		date: null,
		limit: 0,
		enabled: false,
		bound: false,
		active: false,
		reserved_count: null,
		enqueue_success_count: null,
		direct_count: null,
		fallback_direct_count: null,
		reserve_failed_count: null,
		reserve_over_limit_count: null,
		queue_send_failed_count: null,
		target_queue_ratio: 0,
		target_direct_ratio: 1,
		effective_queue_ratio: null,
		effective_direct_ratio: null,
		effective_total_count: null,
	};

	const usageQueueStatus: UsageQueueStatusView = emptyUsageQueueStatus;

	return c.json({
		log_retention_days: retention,
		session_ttl_hours: sessionTtlHours,
		admin_password_set: adminPasswordSet,
		checkin_schedule_time: checkinScheduleTime,
		proxy_model_failure_cooldown_minutes:
			runtimeSettings.model_failure_cooldown_minutes,
		proxy_model_failure_cooldown_threshold:
			runtimeSettings.model_failure_cooldown_threshold,
		runtime_config: runtimeConfig,
		runtime_settings: runtimeSettings,
		usage_queue_status: usageQueueStatus,
	});
});

/**
 * Updates settings values.
 */
settings.put("/", async (c) => {
	const db = c.env.DB;
	const body = await c.req.json().catch(() => null);
	if (!body) {
		return jsonError(c, 400, "settings_required", "settings_required");
	}

	let touched = false;
	let runtimeTouched = false;
	let scheduleTouched = false;
	let scheduleReset = false;

	const runtimePatch: {
		upstream_timeout_ms?: number;
		retry_max_retries?: number;
		zero_completion_as_error_enabled?: boolean;
		model_failure_cooldown_minutes?: number;
		model_failure_cooldown_threshold?: number;
		stream_usage_mode?: string;
		stream_usage_max_bytes?: number;
		stream_usage_max_parsers?: number;
		stream_usage_parse_timeout_ms?: number;
		responses_affinity_ttl_seconds?: number;
		stream_options_capability_ttl_seconds?: number;
		attempt_worker_fallback_enabled?: boolean;
		attempt_worker_fallback_threshold?: number;
		large_request_offload_threshold_bytes?: number;
		attempt_log_enabled?: boolean;
		attempt_log_retention_days?: number;
	} = {};

	if (body.log_retention_days !== undefined) {
		const days = Number(body.log_retention_days);
		if (Number.isNaN(days) || days < 1) {
			return jsonError(
				c,
				400,
				"invalid_log_retention_days",
				"invalid_log_retention_days",
			);
		}
		await setRetentionDays(db, days);
		touched = true;
	}

	if (body.session_ttl_hours !== undefined) {
		const hours = Number(body.session_ttl_hours);
		if (Number.isNaN(hours) || hours < 1) {
			return jsonError(
				c,
				400,
				"invalid_session_ttl_hours",
				"invalid_session_ttl_hours",
			);
		}
		await setSessionTtlHours(db, hours);
		touched = true;
	}

	if (body.proxy_upstream_timeout_ms !== undefined) {
		const timeoutMs = Number(body.proxy_upstream_timeout_ms);
		if (Number.isNaN(timeoutMs) || timeoutMs < 0) {
			return jsonError(
				c,
				400,
				"invalid_proxy_upstream_timeout_ms",
				"invalid_proxy_upstream_timeout_ms",
			);
		}
		runtimePatch.upstream_timeout_ms = Math.floor(timeoutMs);
		runtimeTouched = true;
	}

	if (body.proxy_retry_max_retries !== undefined) {
		const retryMaxRetries = Number(body.proxy_retry_max_retries);
		if (
			Number.isNaN(retryMaxRetries) ||
			retryMaxRetries < 0 ||
			!Number.isInteger(retryMaxRetries)
		) {
			return jsonError(
				c,
				400,
				"invalid_proxy_retry_max_retries",
				"invalid_proxy_retry_max_retries",
			);
		}
		runtimePatch.retry_max_retries = retryMaxRetries;
		runtimeTouched = true;
	}

	if (body.proxy_zero_completion_as_error_enabled !== undefined) {
		const raw = body.proxy_zero_completion_as_error_enabled;
		let enabled: boolean | null = null;
		if (typeof raw === "boolean") {
			enabled = raw;
		} else if (typeof raw === "number") {
			enabled = raw !== 0;
		} else if (typeof raw === "string") {
			const normalized = raw.trim().toLowerCase();
			if (["1", "true", "yes", "on"].includes(normalized)) {
				enabled = true;
			} else if (["0", "false", "no", "off"].includes(normalized)) {
				enabled = false;
			}
		}
		if (enabled === null) {
			return jsonError(
				c,
				400,
				"invalid_proxy_zero_completion_as_error_enabled",
				"invalid_proxy_zero_completion_as_error_enabled",
			);
		}
		runtimePatch.zero_completion_as_error_enabled = enabled;
		runtimeTouched = true;
	}

	if (body.proxy_model_failure_cooldown_minutes !== undefined) {
		const minutes = Number(body.proxy_model_failure_cooldown_minutes);
		if (Number.isNaN(minutes) || minutes < 0) {
			return jsonError(
				c,
				400,
				"invalid_proxy_model_failure_cooldown_minutes",
				"invalid_proxy_model_failure_cooldown_minutes",
			);
		}
		runtimePatch.model_failure_cooldown_minutes = Math.floor(minutes);
		runtimeTouched = true;
	}

	if (body.proxy_model_failure_cooldown_threshold !== undefined) {
		const threshold = Number(body.proxy_model_failure_cooldown_threshold);
		if (
			Number.isNaN(threshold) ||
			threshold < 1 ||
			!Number.isInteger(threshold)
		) {
			return jsonError(
				c,
				400,
				"invalid_proxy_model_failure_cooldown_threshold",
				"invalid_proxy_model_failure_cooldown_threshold",
			);
		}
		runtimePatch.model_failure_cooldown_threshold = threshold;
		runtimeTouched = true;
	}

	if (body.proxy_stream_usage_mode !== undefined) {
		const mode = String(body.proxy_stream_usage_mode).trim().toLowerCase();
		if (!["full", "lite", "off"].includes(mode)) {
			return jsonError(
				c,
				400,
				"invalid_proxy_stream_usage_mode",
				"invalid_proxy_stream_usage_mode",
			);
		}
		runtimePatch.stream_usage_mode = mode;
		runtimeTouched = true;
	}

	if (body.proxy_stream_usage_max_bytes !== undefined) {
		const maxBytes = Number(body.proxy_stream_usage_max_bytes);
		if (Number.isNaN(maxBytes) || maxBytes < 0) {
			return jsonError(
				c,
				400,
				"invalid_proxy_stream_usage_max_bytes",
				"invalid_proxy_stream_usage_max_bytes",
			);
		}
		runtimePatch.stream_usage_max_bytes = Math.floor(maxBytes);
		runtimeTouched = true;
	}

	if (body.proxy_stream_usage_max_parsers !== undefined) {
		const maxParsers = Number(body.proxy_stream_usage_max_parsers);
		if (Number.isNaN(maxParsers) || maxParsers < 0) {
			return jsonError(
				c,
				400,
				"invalid_proxy_stream_usage_max_parsers",
				"invalid_proxy_stream_usage_max_parsers",
			);
		}
		runtimePatch.stream_usage_max_parsers = Math.floor(maxParsers);
		runtimeTouched = true;
	}

	if (body.proxy_stream_usage_parse_timeout_ms !== undefined) {
		const timeoutMs = Number(body.proxy_stream_usage_parse_timeout_ms);
		if (Number.isNaN(timeoutMs) || timeoutMs < 0) {
			return jsonError(
				c,
				400,
				"invalid_proxy_stream_usage_parse_timeout_ms",
				"invalid_proxy_stream_usage_parse_timeout_ms",
			);
		}
		runtimePatch.stream_usage_parse_timeout_ms = Math.floor(timeoutMs);
		runtimeTouched = true;
	}

	if (body.proxy_responses_affinity_ttl_seconds !== undefined) {
		const ttlSeconds = Number(body.proxy_responses_affinity_ttl_seconds);
		if (
			Number.isNaN(ttlSeconds) ||
			ttlSeconds < 60 ||
			!Number.isInteger(ttlSeconds)
		) {
			return jsonError(
				c,
				400,
				"invalid_proxy_responses_affinity_ttl_seconds",
				"invalid_proxy_responses_affinity_ttl_seconds",
			);
		}
		runtimePatch.responses_affinity_ttl_seconds = ttlSeconds;
		runtimeTouched = true;
	}

	if (body.proxy_stream_options_capability_ttl_seconds !== undefined) {
		const ttlSeconds = Number(body.proxy_stream_options_capability_ttl_seconds);
		if (
			Number.isNaN(ttlSeconds) ||
			ttlSeconds < 60 ||
			!Number.isInteger(ttlSeconds)
		) {
			return jsonError(
				c,
				400,
				"invalid_proxy_stream_options_capability_ttl_seconds",
				"invalid_proxy_stream_options_capability_ttl_seconds",
			);
		}
		runtimePatch.stream_options_capability_ttl_seconds = ttlSeconds;
		runtimeTouched = true;
	}

	if (body.proxy_attempt_worker_fallback_enabled !== undefined) {
		const raw = body.proxy_attempt_worker_fallback_enabled;
		let enabled: boolean | null = null;
		if (typeof raw === "boolean") {
			enabled = raw;
		} else if (typeof raw === "number") {
			enabled = raw !== 0;
		} else if (typeof raw === "string") {
			const normalized = raw.trim().toLowerCase();
			if (["1", "true", "yes", "on"].includes(normalized)) {
				enabled = true;
			} else if (["0", "false", "no", "off"].includes(normalized)) {
				enabled = false;
			}
		}
		if (enabled === null) {
			return jsonError(
				c,
				400,
				"invalid_proxy_attempt_worker_fallback_enabled",
				"invalid_proxy_attempt_worker_fallback_enabled",
			);
		}
		runtimePatch.attempt_worker_fallback_enabled = enabled;
		runtimeTouched = true;
	}

	if (body.proxy_attempt_worker_fallback_threshold !== undefined) {
		const threshold = Number(body.proxy_attempt_worker_fallback_threshold);
		if (
			Number.isNaN(threshold) ||
			threshold < 1 ||
			!Number.isInteger(threshold)
		) {
			return jsonError(
				c,
				400,
				"invalid_proxy_attempt_worker_fallback_threshold",
				"invalid_proxy_attempt_worker_fallback_threshold",
			);
		}
		runtimePatch.attempt_worker_fallback_threshold = threshold;
		runtimeTouched = true;
	}

	if (body.proxy_large_request_offload_threshold_bytes !== undefined) {
		const thresholdBytes = Number(
			body.proxy_large_request_offload_threshold_bytes,
		);
		if (
			Number.isNaN(thresholdBytes) ||
			thresholdBytes < 0 ||
			!Number.isInteger(thresholdBytes)
		) {
			return jsonError(
				c,
				400,
				"invalid_proxy_large_request_offload_threshold_bytes",
				"invalid_proxy_large_request_offload_threshold_bytes",
			);
		}
		runtimePatch.large_request_offload_threshold_bytes = thresholdBytes;
		runtimeTouched = true;
	}

	if (body.attempt_log_enabled !== undefined) {
		const raw = body.attempt_log_enabled;
		let enabled: boolean | null = null;
		if (typeof raw === "boolean") {
			enabled = raw;
		} else if (typeof raw === "number") {
			enabled = raw !== 0;
		} else if (typeof raw === "string") {
			const normalized = raw.trim().toLowerCase();
			if (["1", "true", "yes", "on"].includes(normalized)) {
				enabled = true;
			} else if (["0", "false", "no", "off"].includes(normalized)) {
				enabled = false;
			}
		}
		if (enabled === null) {
			return jsonError(
				c,
				400,
				"invalid_attempt_log_enabled",
				"invalid_attempt_log_enabled",
			);
		}
		runtimePatch.attempt_log_enabled = enabled;
		runtimeTouched = true;
	}

	if (body.attempt_log_retention_days !== undefined) {
		const days = Number(body.attempt_log_retention_days);
		if (Number.isNaN(days) || days < 1 || !Number.isInteger(days)) {
			return jsonError(
				c,
				400,
				"invalid_attempt_log_retention_days",
				"invalid_attempt_log_retention_days",
			);
		}
		runtimePatch.attempt_log_retention_days = days;
		runtimeTouched = true;
	}

	if (typeof body.admin_password === "string" && body.admin_password.trim()) {
		const hash = await sha256Hex(body.admin_password.trim());
		await setAdminPasswordHash(db, hash);
		touched = true;
	}

	if (body.checkin_schedule_time !== undefined) {
		const currentTime = await getCheckinScheduleTime(db);
		const timeValue = String(body.checkin_schedule_time).trim();
		if (!/^\d{2}:\d{2}$/.test(timeValue)) {
			return jsonError(
				c,
				400,
				"invalid_checkin_schedule_time",
				"invalid_checkin_schedule_time",
			);
		}
		const [hour, minute] = timeValue.split(":").map((value) => Number(value));
		if (
			Number.isNaN(hour) ||
			Number.isNaN(minute) ||
			hour < 0 ||
			hour > 23 ||
			minute < 0 ||
			minute > 59
		) {
			return jsonError(
				c,
				400,
				"invalid_checkin_schedule_time",
				"invalid_checkin_schedule_time",
			);
		}
		await setCheckinScheduleTime(db, timeValue);
		touched = true;
		scheduleTouched = true;
		scheduleReset = shouldResetLastRun(currentTime, timeValue);
	}

	if (runtimeTouched) {
		await setProxyRuntimeSettings(db, runtimePatch, c.env.CACHE_VERSION_STORE);
		touched = true;
	}

	if (!touched) {
		return jsonError(c, 400, "settings_empty", "settings_empty");
	}

	if (scheduleTouched) {
		const scheduler = getCheckinSchedulerStub(c.env.CHECKIN_SCHEDULER);
		await scheduler.fetch("https://checkin-scheduler/reschedule", {
			method: "POST",
			...(scheduleReset ? { body: JSON.stringify({ reset: true }) } : {}),
		});
	}

	return c.json({ ok: true });
});

export default settings;
