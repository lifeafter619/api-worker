import type { D1Database } from "@cloudflare/workers-types";
import { nowIso } from "../utils/time";
import { normalizeBaseUrl } from "../utils/url";
import { listCallTokens } from "./channel-call-token-repo";
import { modelsToJson } from "./channel-models";
import { listChannels } from "./channel-repo";
import { fetchChannelModels, updateChannelTestResult } from "./channel-testing";

const RECOVERY_PROBE_PROMPT = "Reply with a short health-check message.";
const RECOVERY_PROBE_MAX_TOKENS = 32;

type RecoveryToken = {
	id: string;
	name: string;
	api_key: string;
};

export type DisabledChannelRecoveryResult = {
	attempted: boolean;
	recovered: boolean;
	reason:
		| "recovered"
		| "already_active"
		| "no_disabled_channel"
		| "missing_token"
		| "token_model_test_failed"
		| "completion_probe_failed";
	channel_id?: string;
	channel_name?: string;
	model?: string;
};

export type DisabledChannelRecoveryBatchResult = {
	total: number;
	attempted: number;
	recovered: number;
	failed: number;
	items: DisabledChannelRecoveryResult[];
};

type DisabledChannelItem = {
	id: string;
	name: string;
	base_url: string;
	api_key: string;
};

/**
 * Picks a random item from an array.
 *
 * Args:
 *   items: Candidate items.
 *   random: Random number generator.
 *
 * Returns:
 *   Picked item or null when input is empty.
 */
export function pickRandomItem<T>(
	items: readonly T[],
	random: () => number = Math.random,
): T | null {
	if (items.length === 0) {
		return null;
	}
	const index = Math.floor(random() * items.length);
	const safeIndex = Math.max(0, Math.min(items.length - 1, index));
	return items[safeIndex] ?? null;
}

function shuffleItems<T>(
	items: readonly T[],
	random: () => number = Math.random,
): T[] {
	const cloned = [...items];
	for (let i = cloned.length - 1; i > 0; i -= 1) {
		const j = Math.floor(random() * (i + 1));
		[cloned[i], cloned[j]] = [cloned[j], cloned[i]];
	}
	return cloned;
}

/**
 * Extracts text output from a chat completion style payload.
 *
 * Args:
 *   payload: Upstream JSON payload.
 *
 * Returns:
 *   Trimmed text content; empty string when missing.
 */
export function extractProbeText(payload: unknown): string {
	if (!payload || typeof payload !== "object") {
		return "";
	}
	const record = payload as Record<string, unknown>;
	if (typeof record.output_text === "string") {
		return record.output_text.trim();
	}
	const choices = record.choices;
	if (!Array.isArray(choices) || choices.length === 0) {
		return "";
	}
	const firstChoice =
		choices[0] && typeof choices[0] === "object"
			? (choices[0] as Record<string, unknown>)
			: null;
	if (!firstChoice) {
		return "";
	}
	if (typeof firstChoice.text === "string") {
		return firstChoice.text.trim();
	}
	const message =
		firstChoice.message && typeof firstChoice.message === "object"
			? (firstChoice.message as Record<string, unknown>)
			: null;
	if (!message) {
		return "";
	}
	const content = message.content;
	if (typeof content === "string") {
		return content.trim();
	}
	if (!Array.isArray(content)) {
		return "";
	}
	for (const item of content) {
		if (!item || typeof item !== "object") {
			continue;
		}
		const textValue = (item as Record<string, unknown>).text;
		if (typeof textValue === "string" && textValue.trim().length > 0) {
			return textValue.trim();
		}
	}
	return "";
}

async function sendCompletionProbe(options: {
	baseUrl: string;
	apiKey: string;
	model: string;
	fetcher?: typeof fetch;
}): Promise<boolean> {
	const fetcher = options.fetcher ?? fetch;
	const target = `${normalizeBaseUrl(options.baseUrl)}/v1/chat/completions`;
	let response: Response;
	try {
		response = await fetcher(target, {
			method: "POST",
			headers: {
				Authorization: `Bearer ${options.apiKey}`,
				"x-api-key": options.apiKey,
				"Content-Type": "application/json",
			},
			body: JSON.stringify({
				model: options.model,
				messages: [{ role: "user", content: RECOVERY_PROBE_PROMPT }],
				max_tokens: RECOVERY_PROBE_MAX_TOKENS,
				temperature: 0,
			}),
		});
	} catch {
		return false;
	}
	if (!response.ok) {
		return false;
	}
	const payload = await response.json().catch(() => null);
	return extractProbeText(payload).length > 0;
}

async function recoverDisabledChannel(
	db: D1Database,
	channel: DisabledChannelItem,
	options: {
		random?: () => number;
		fetcher?: typeof fetch;
	} = {},
): Promise<DisabledChannelRecoveryResult> {
	const random = options.random ?? Math.random;
	const callTokenRows = await listCallTokens(db, {
		channelIds: [channel.id],
	});
	const fallbackApiKey = String(channel.api_key ?? "").trim();
	const tokens: RecoveryToken[] =
		callTokenRows.length > 0
			? callTokenRows.map((row) => ({
					id: row.id,
					name: row.name,
					api_key: row.api_key,
				}))
			: fallbackApiKey
				? [
						{
							id: "primary",
							name: "primary",
							api_key: fallbackApiKey,
						},
					]
				: [];
	if (tokens.length === 0) {
		return {
			attempted: true,
			recovered: false,
			reason: "missing_token",
			channel_id: channel.id,
			channel_name: channel.name,
		};
	}

	let selectedToken: RecoveryToken | null = null;
	let selectedModels: string[] = [];
	let selectedElapsed = 0;
	let elapsedSum = 0;
	let elapsedCount = 0;

	for (const token of shuffleItems(tokens, random)) {
		const result = await fetchChannelModels(
			String(channel.base_url),
			token.api_key,
		);
		elapsedSum += result.elapsed;
		elapsedCount += 1;
		if (result.ok && result.models.length > 0) {
			selectedToken = token;
			selectedModels = result.models;
			selectedElapsed = result.elapsed;
			break;
		}
	}

	if (!selectedToken || selectedModels.length === 0) {
		const elapsed =
			elapsedCount > 0 ? Math.round(elapsedSum / elapsedCount) : 0;
		await updateChannelTestResult(db, channel.id, {
			ok: false,
			elapsed,
		});
		return {
			attempted: true,
			recovered: false,
			reason: "token_model_test_failed",
			channel_id: channel.id,
			channel_name: channel.name,
		};
	}

	await updateChannelTestResult(db, channel.id, {
		ok: true,
		elapsed: selectedElapsed,
		models: selectedModels,
		modelsJson: modelsToJson(selectedModels),
	});

	const model = pickRandomItem(selectedModels, random);
	if (!model) {
		return {
			attempted: true,
			recovered: false,
			reason: "completion_probe_failed",
			channel_id: channel.id,
			channel_name: channel.name,
		};
	}

	const probeOk = await sendCompletionProbe({
		baseUrl: String(channel.base_url),
		apiKey: selectedToken.api_key,
		model,
		fetcher: options.fetcher,
	});
	if (!probeOk) {
		return {
			attempted: true,
			recovered: false,
			reason: "completion_probe_failed",
			channel_id: channel.id,
			channel_name: channel.name,
			model,
		};
	}

	const updatedAt = nowIso();
	const updateResult = await db
		.prepare(
			"UPDATE channels SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
		)
		.bind("active", updatedAt, channel.id, "disabled")
		.run();
	const recovered = Number(updateResult.meta?.changes ?? 0) > 0;
	return {
		attempted: true,
		recovered,
		reason: recovered ? "recovered" : "already_active",
		channel_id: channel.id,
		channel_name: channel.name,
		model,
	};
}

/**
 * Probes all disabled channels and restores those that pass recovery checks.
 *
 * Args:
 *   db: D1 database.
 *   options: Optional test overrides.
 *
 * Returns:
 *   Batch recovery result.
 */
export async function recoverDisabledChannels(
	db: D1Database,
	options: {
		random?: () => number;
		fetcher?: typeof fetch;
	} = {},
): Promise<DisabledChannelRecoveryBatchResult> {
	const disabledChannels = await listChannels(db, {
		filters: { status: "disabled" },
		orderBy: "created_at",
		order: "DESC",
	});
	const probeTargets = disabledChannels.filter(
		(channel) => Number(channel.auto_disabled_permanent ?? 0) <= 0,
	);
	if (probeTargets.length === 0) {
		return {
			total: 0,
			attempted: 0,
			recovered: 0,
			failed: 0,
			items: [],
		};
	}
	const items: DisabledChannelRecoveryResult[] = [];
	for (const channel of probeTargets) {
		const result = await recoverDisabledChannel(db, channel, options);
		items.push(result);
	}
	const attempted = items.filter((item) => item.attempted).length;
	const recovered = items.filter((item) => item.recovered).length;
	return {
		total: items.length,
		attempted,
		recovered,
		failed: attempted - recovered,
		items,
	};
}
