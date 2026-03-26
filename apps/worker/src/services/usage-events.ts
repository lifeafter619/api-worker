import type { D1Database } from "@cloudflare/workers-types";
import { type AttemptLogInput, insertAttemptEvent } from "./attempt-events";
import {
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

export async function processUsageEvent(
	db: D1Database,
	event: UsageEvent,
): Promise<void> {
	if (event.type === "usage") {
		await recordUsage(db, event.payload);
		return;
	}
	if (event.type === "capability_upsert") {
		const nowSeconds = resolveNowSeconds(event.payload.nowSeconds);
		await upsertChannelModelCapabilities(
			db,
			event.payload.channelId,
			event.payload.models,
			nowSeconds,
		);
		return;
	}
	if (event.type === "model_error") {
		const nowSeconds = resolveNowSeconds(event.payload.nowSeconds);
		await recordChannelModelError(
			db,
			event.payload.channelId,
			event.payload.model,
			event.payload.errorCode,
			nowSeconds,
		);
		return;
	}
	if (event.type === "attempt_log") {
		await insertAttemptEvent(db, event.payload);
	}
}
