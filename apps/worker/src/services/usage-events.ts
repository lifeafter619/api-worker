import type { D1Database } from "@cloudflare/workers-types";
import { type AttemptLogInput, insertAttemptEvent } from "./attempt-events";
import {
	recordChannelDisableHit,
	recordChannelModelError,
	upsertChannelModelCapabilities,
} from "./channel-model-capabilities";
import type { UsageInput } from "./usage";
import { recordUsage } from "./usage";

export type UsageEvent =
	| {
			type: "usage";
			payload: UsageInput;
	  }
	| {
			type: "capability_upsert";
			payload: {
				channelId: string;
				models: string[];
				nowSeconds?: number;
			};
	  }
	| {
			type: "model_error";
			payload: {
				channelId: string;
				model: string | null;
				errorCode: string;
				cooldownSeconds: number;
				cooldownFailureThreshold: number;
				channelDisableMatched: boolean;
				channelDisableDurationSeconds: number;
				channelDisableThreshold: number;
				nowSeconds?: number;
			};
	  }
	| {
			type: "attempt_log";
			payload: AttemptLogInput;
	  };

function resolveNowSeconds(value?: number): number {
	if (typeof value === "number" && Number.isFinite(value) && value > 0) {
		return Math.floor(value);
	}
	return Math.floor(Date.now() / 1000);
}

export type UsageEventProcessResult = {
	channelDisabled: boolean;
};

export async function processUsageEvent(
	db: D1Database,
	event: UsageEvent,
): Promise<UsageEventProcessResult> {
	if (event.type === "usage") {
		await recordUsage(db, event.payload);
		return { channelDisabled: false };
	}
	if (event.type === "capability_upsert") {
		const nowSeconds = resolveNowSeconds(event.payload.nowSeconds);
		await upsertChannelModelCapabilities(
			db,
			event.payload.channelId,
			event.payload.models,
			nowSeconds,
		);
		return { channelDisabled: false };
	}
	if (event.type === "model_error") {
		const nowSeconds = resolveNowSeconds(event.payload.nowSeconds);
		let channelDisabled = false;
		if (event.payload.model && event.payload.cooldownSeconds > 0) {
			const result = await recordChannelModelError(
				db,
				event.payload.channelId,
				event.payload.model,
				event.payload.errorCode,
				{
					cooldownSeconds: event.payload.cooldownSeconds,
					cooldownFailureThreshold: event.payload.cooldownFailureThreshold,
				},
				nowSeconds,
			);
			channelDisabled = result.channelDisabled;
		}
		if (event.payload.channelDisableMatched) {
			const disableResult = await recordChannelDisableHit(
				db,
				event.payload.channelId,
				event.payload.errorCode,
				{
					disableDurationSeconds: event.payload.channelDisableDurationSeconds,
					disableThreshold: event.payload.channelDisableThreshold,
				},
				nowSeconds,
			);
			channelDisabled =
				channelDisabled ||
				disableResult.channelTempDisabled ||
				disableResult.channelPermanentlyDisabled;
		}
		return {
			channelDisabled,
		};
	}
	if (event.type === "attempt_log") {
		await insertAttemptEvent(db, event.payload);
	}
	return { channelDisabled: false };
}
