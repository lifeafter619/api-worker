export type ResponsesRequestHints = {
	previousResponseId: string | null;
	functionCallOutputIds: string[];
	hasFunctionCallOutput: boolean;
};

export type ToolCallChainRepairReport = {
	patchedAssistantCalls: number;
	droppedToolMessages: number;
	droppedFunctionOutputs: number;
	droppedToolMessageIndexes: number[];
	droppedFunctionOutputIndexes: number[];
};

export type ToolCallChainValidationIssue = {
	code: "tool_call_chain_invalid_local";
	message: string;
	errorMetaJson: string;
};

const RESPONSES_TOOL_CALL_NOT_FOUND_SNIPPET =
	"no tool call found for function call output";

export function normalizeMessage(value: string | null): string | null {
	if (!value) {
		return null;
	}
	const trimmed = value.trim();
	if (!trimmed) {
		return null;
	}
	return trimmed;
}

export function normalizeStringField(value: unknown): string | null {
	if (typeof value !== "string") {
		return null;
	}
	const trimmed = value.trim();
	return trimmed.length > 0 ? trimmed : null;
}

export function extractResponsesRequestHints(
	body: Record<string, unknown> | null,
): ResponsesRequestHints | null {
	if (!body) {
		return null;
	}
	const previousResponseId = normalizeStringField(
		body.previous_response_id ?? body.previousResponseId,
	);
	const outputIds: string[] = [];
	const scanInputItem = (item: unknown): void => {
		if (!item || typeof item !== "object") {
			return;
		}
		const itemRecord = item as Record<string, unknown>;
		const itemType = normalizeStringField(itemRecord.type)?.toLowerCase();
		if (itemType !== "function_call_output") {
			return;
		}
		const callId = normalizeStringField(
			itemRecord.call_id ?? itemRecord.tool_call_id ?? itemRecord.toolCallId,
		);
		if (!callId) {
			return;
		}
		outputIds.push(callId);
	};
	const rawInput = body.input;
	if (Array.isArray(rawInput)) {
		for (const item of rawInput) {
			scanInputItem(item);
		}
	} else {
		scanInputItem(rawInput);
	}
	return {
		previousResponseId,
		functionCallOutputIds: outputIds,
		hasFunctionCallOutput: outputIds.length > 0,
	};
}

export function hasChatToolOutputHint(
	body: Record<string, unknown> | null,
): boolean {
	if (!body || !Array.isArray(body.messages)) {
		return false;
	}
	for (const message of body.messages) {
		if (!message || typeof message !== "object" || Array.isArray(message)) {
			continue;
		}
		const record = message as Record<string, unknown>;
		const role = normalizeStringField(record.role)?.toLowerCase();
		if (role !== "tool") {
			continue;
		}
		const toolCallId = normalizeStringField(
			record.tool_call_id ??
				record.toolCallId ??
				record.call_id ??
				record.callId,
		);
		if (toolCallId) {
			return true;
		}
	}
	return false;
}

function hasAssistantToolCallHint(
	body: Record<string, unknown> | null,
): boolean {
	if (!body || !Array.isArray(body.messages)) {
		return false;
	}
	for (const message of body.messages) {
		if (!message || typeof message !== "object" || Array.isArray(message)) {
			continue;
		}
		const record = message as Record<string, unknown>;
		const role = normalizeStringField(record.role)?.toLowerCase();
		if (role !== "assistant") {
			continue;
		}
		if (Array.isArray(record.tool_calls) || Array.isArray(record.toolCalls)) {
			return true;
		}
		const functionCall = record.function_call ?? record.functionCall;
		if (
			functionCall &&
			typeof functionCall === "object" &&
			!Array.isArray(functionCall)
		) {
			return true;
		}
	}
	return false;
}

function emptyToolCallChainRepairReport(): ToolCallChainRepairReport {
	return {
		patchedAssistantCalls: 0,
		droppedToolMessages: 0,
		droppedFunctionOutputs: 0,
		droppedToolMessageIndexes: [],
		droppedFunctionOutputIndexes: [],
	};
}

function collectAssistantToolCallIds(
	message: Record<string, unknown>,
): string[] {
	const ids: string[] = [];
	const toolCalls = Array.isArray(message.tool_calls)
		? message.tool_calls
		: Array.isArray(message.toolCalls)
			? message.toolCalls
			: [];
	for (const call of toolCalls) {
		if (!call || typeof call !== "object" || Array.isArray(call)) {
			continue;
		}
		const record = call as Record<string, unknown>;
		const id = normalizeStringField(
			record.id ?? record.call_id ?? record.callId,
		);
		if (id) {
			ids.push(id);
		}
	}
	const functionCall =
		message.function_call &&
		typeof message.function_call === "object" &&
		!Array.isArray(message.function_call)
			? (message.function_call as Record<string, unknown>)
			: message.functionCall &&
					typeof message.functionCall === "object" &&
					!Array.isArray(message.functionCall)
				? (message.functionCall as Record<string, unknown>)
				: null;
	if (functionCall) {
		const id = normalizeStringField(
			functionCall.id ?? functionCall.call_id ?? functionCall.callId,
		);
		if (id) {
			ids.push(id);
		}
	}
	return ids;
}

function patchNearestAssistantCallId(
	messages: Array<Record<string, unknown>>,
	toolCallId: string,
): boolean {
	for (let index = messages.length - 1; index >= 0; index -= 1) {
		const candidate = messages[index];
		const role = normalizeStringField(candidate.role)?.toLowerCase();
		if (role !== "assistant") {
			continue;
		}
		const toolCalls = Array.isArray(candidate.tool_calls)
			? candidate.tool_calls
			: Array.isArray(candidate.toolCalls)
				? candidate.toolCalls
				: [];
		const missingCalls = toolCalls.filter((call) => {
			if (!call || typeof call !== "object" || Array.isArray(call)) {
				return false;
			}
			const record = call as Record<string, unknown>;
			return !normalizeStringField(
				record.id ?? record.call_id ?? record.callId,
			);
		});
		if (missingCalls.length === 1) {
			const missingRecord = missingCalls[0] as Record<string, unknown>;
			missingRecord.id = toolCallId;
			return true;
		}
		const functionCall =
			candidate.function_call &&
			typeof candidate.function_call === "object" &&
			!Array.isArray(candidate.function_call)
				? (candidate.function_call as Record<string, unknown>)
				: candidate.functionCall &&
						typeof candidate.functionCall === "object" &&
						!Array.isArray(candidate.functionCall)
					? (candidate.functionCall as Record<string, unknown>)
					: null;
		if (functionCall) {
			const hasId = normalizeStringField(
				functionCall.id ?? functionCall.call_id ?? functionCall.callId,
			);
			if (!hasId) {
				functionCall.call_id = toolCallId;
				return true;
			}
		}
	}
	return false;
}

function repairOpenAiChatToolCallChain(
	body: Record<string, unknown> | null,
	report: ToolCallChainRepairReport,
): void {
	if (!body || !Array.isArray(body.messages)) {
		return;
	}
	const repairedMessages: Array<Record<string, unknown>> = [];
	const seenIds = new Set<string>();
	for (let index = 0; index < body.messages.length; index += 1) {
		const rawMessage = body.messages[index];
		if (
			!rawMessage ||
			typeof rawMessage !== "object" ||
			Array.isArray(rawMessage)
		) {
			continue;
		}
		const message = rawMessage as Record<string, unknown>;
		const role = normalizeStringField(message.role)?.toLowerCase();
		if (role === "assistant") {
			for (const id of collectAssistantToolCallIds(message)) {
				seenIds.add(id);
			}
			repairedMessages.push(message);
			continue;
		}
		if (role !== "tool") {
			repairedMessages.push(message);
			continue;
		}
		const toolCallId = normalizeStringField(
			message.tool_call_id ??
				message.toolCallId ??
				message.call_id ??
				message.callId,
		);
		if (!toolCallId) {
			report.droppedToolMessages += 1;
			report.droppedToolMessageIndexes.push(index);
			continue;
		}
		if (!seenIds.has(toolCallId)) {
			const patched = patchNearestAssistantCallId(repairedMessages, toolCallId);
			if (patched) {
				report.patchedAssistantCalls += 1;
				seenIds.add(toolCallId);
			}
		}
		if (!seenIds.has(toolCallId)) {
			report.droppedToolMessages += 1;
			report.droppedToolMessageIndexes.push(index);
			continue;
		}
		repairedMessages.push(message);
	}
	body.messages = repairedMessages;
}

function repairOpenAiResponsesToolCallChain(
	body: Record<string, unknown> | null,
	report: ToolCallChainRepairReport,
): void {
	if (!body || !Array.isArray(body.input)) {
		return;
	}
	const seenFunctionCallIds = new Set<string>();
	const pendingWithoutId: Array<Record<string, unknown>> = [];
	const repairedInput: Array<unknown> = [];
	for (let index = 0; index < body.input.length; index += 1) {
		const rawItem = body.input[index];
		if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
			repairedInput.push(rawItem);
			continue;
		}
		const item = rawItem as Record<string, unknown>;
		const itemType = normalizeStringField(item.type)?.toLowerCase();
		if (itemType === "function_call") {
			const callId = normalizeStringField(
				item.call_id ?? item.callId ?? item.id,
			);
			if (callId) {
				seenFunctionCallIds.add(callId);
			} else {
				pendingWithoutId.push(item);
			}
			repairedInput.push(item);
			continue;
		}
		if (itemType !== "function_call_output") {
			repairedInput.push(item);
			continue;
		}
		const outputCallId = normalizeStringField(
			item.call_id ?? item.callId ?? item.tool_call_id ?? item.toolCallId,
		);
		if (!outputCallId) {
			report.droppedFunctionOutputs += 1;
			report.droppedFunctionOutputIndexes.push(index);
			continue;
		}
		if (!seenFunctionCallIds.has(outputCallId) && pendingWithoutId.length > 0) {
			const candidate = pendingWithoutId.pop() as Record<string, unknown>;
			candidate.call_id = outputCallId;
			seenFunctionCallIds.add(outputCallId);
			report.patchedAssistantCalls += 1;
		}
		if (!seenFunctionCallIds.has(outputCallId)) {
			report.droppedFunctionOutputs += 1;
			report.droppedFunctionOutputIndexes.push(index);
			continue;
		}
		repairedInput.push(item);
	}
	body.input = repairedInput;
}

export function repairOpenAiToolCallChain(
	body: Record<string, unknown> | null,
	endpointType: string,
): ToolCallChainRepairReport {
	const report = emptyToolCallChainRepairReport();
	if (!body) {
		return report;
	}
	repairOpenAiChatToolCallChain(body, report);
	if (endpointType === "responses") {
		repairOpenAiResponsesToolCallChain(body, report);
	}
	return report;
}

function validateOpenAiChatToolCallChain(
	body: Record<string, unknown> | null,
): ToolCallChainValidationIssue | null {
	if (!body || !Array.isArray(body.messages)) {
		return null;
	}
	const seenToolCallIds = new Set<string>();
	const missingRefs: Array<{ id: string; index: number }> = [];
	const missingIdIndexes: number[] = [];
	for (let index = 0; index < body.messages.length; index += 1) {
		const rawMessage = body.messages[index];
		if (
			!rawMessage ||
			typeof rawMessage !== "object" ||
			Array.isArray(rawMessage)
		) {
			continue;
		}
		const message = rawMessage as Record<string, unknown>;
		const role = normalizeStringField(message.role)?.toLowerCase();
		if (role === "assistant") {
			const toolCalls = Array.isArray(message.tool_calls)
				? message.tool_calls
				: Array.isArray(message.toolCalls)
					? message.toolCalls
					: [];
			for (const call of toolCalls) {
				if (!call || typeof call !== "object" || Array.isArray(call)) {
					continue;
				}
				const callRecord = call as Record<string, unknown>;
				const callId = normalizeStringField(
					callRecord.id ?? callRecord.call_id ?? callRecord.callId,
				);
				if (callId) {
					seenToolCallIds.add(callId);
				}
			}
			const legacyFunctionCall =
				(message.function_call &&
				typeof message.function_call === "object" &&
				!Array.isArray(message.function_call)
					? (message.function_call as Record<string, unknown>)
					: null) ??
				(message.functionCall &&
				typeof message.functionCall === "object" &&
				!Array.isArray(message.functionCall)
					? (message.functionCall as Record<string, unknown>)
					: null);
			const legacyCallId = normalizeStringField(
				legacyFunctionCall?.id ??
					legacyFunctionCall?.call_id ??
					legacyFunctionCall?.callId,
			);
			if (legacyCallId) {
				seenToolCallIds.add(legacyCallId);
			}
			continue;
		}
		if (role !== "tool") {
			continue;
		}
		const toolCallId = normalizeStringField(
			message.tool_call_id ?? message.toolCallId ?? message.call_id,
		);
		if (!toolCallId) {
			missingIdIndexes.push(index);
			continue;
		}
		if (!seenToolCallIds.has(toolCallId)) {
			missingRefs.push({ id: toolCallId, index });
		}
	}
	if (missingRefs.length === 0 && missingIdIndexes.length === 0) {
		return null;
	}
	const missingIds = Array.from(new Set(missingRefs.map((item) => item.id)));
	const message = [
		"tool_call_chain_invalid_local: chat_messages contain tool output without matching assistant tool_calls",
		`missing_ids=${missingIds.length > 0 ? missingIds.join(",") : "-"}`,
		`missing_id_message_indexes=${missingIdIndexes.length > 0 ? missingIdIndexes.join(",") : "-"}`,
		`unmatched_message_indexes=${missingRefs.length > 0 ? missingRefs.map((item) => item.index).join(",") : "-"}`,
	].join(", ");
	return {
		code: "tool_call_chain_invalid_local",
		message,
		errorMetaJson: JSON.stringify({
			type: "local_validation",
			source: "chat_messages",
			status: 409,
			missing_ids: missingIds,
			missing_id_message_indexes: missingIdIndexes,
			unmatched_message_indexes: missingRefs.map((item) => item.index),
		}),
	};
}

function validateOpenAiResponsesToolCallChain(
	body: Record<string, unknown> | null,
	previousResponseId: string | null,
): ToolCallChainValidationIssue | null {
	if (!body || previousResponseId) {
		return null;
	}
	const rawInput = body.input;
	const inputItems = Array.isArray(rawInput)
		? rawInput
		: rawInput
			? [rawInput]
			: [];
	if (inputItems.length === 0) {
		return null;
	}
	const seenFunctionCallIds = new Set<string>();
	const missingOutputs: Array<{ id: string; index: number }> = [];
	for (let index = 0; index < inputItems.length; index += 1) {
		const rawItem = inputItems[index];
		if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
			continue;
		}
		const item = rawItem as Record<string, unknown>;
		const itemType = normalizeStringField(item.type)?.toLowerCase();
		if (itemType === "function_call") {
			const callId = normalizeStringField(item.call_id ?? item.id);
			const callIdAlt = normalizeStringField(item.callId);
			if (callId) {
				seenFunctionCallIds.add(callId);
			}
			if (callIdAlt) {
				seenFunctionCallIds.add(callIdAlt);
			}
			continue;
		}
		if (itemType !== "function_call_output") {
			continue;
		}
		const outputCallId = normalizeStringField(
			item.call_id ?? item.callId ?? item.tool_call_id ?? item.toolCallId,
		);
		if (!outputCallId || seenFunctionCallIds.has(outputCallId)) {
			continue;
		}
		missingOutputs.push({ id: outputCallId, index });
	}
	if (missingOutputs.length === 0) {
		return null;
	}
	const missingIds = Array.from(new Set(missingOutputs.map((item) => item.id)));
	const message = [
		"tool_call_chain_invalid_local: responses input contains function_call_output without previous_response_id or in-request function_call",
		`missing_ids=${missingIds.join(",")}`,
		`unmatched_item_indexes=${missingOutputs.map((item) => item.index).join(",")}`,
	].join(", ");
	return {
		code: "tool_call_chain_invalid_local",
		message,
		errorMetaJson: JSON.stringify({
			type: "local_validation",
			source: "responses_input",
			status: 409,
			missing_ids: missingIds,
			unmatched_item_indexes: missingOutputs.map((item) => item.index),
		}),
	};
}

function validateOpenAiResponsesChatMessageChain(
	body: Record<string, unknown> | null,
	hints: ResponsesRequestHints | null,
): ToolCallChainValidationIssue | null {
	if (!body) {
		return null;
	}
	const hasChatToolOutput = hasChatToolOutputHint(body);
	if (!hasChatToolOutput) {
		return null;
	}
	const hasAssistantCalls = hasAssistantToolCallHint(body);
	if (!hasAssistantCalls) {
		return {
			code: "tool_call_chain_invalid_local",
			message:
				"tool_call_chain_invalid_local: responses request contains tool messages but assistant tool_calls are missing in messages",
			errorMetaJson: JSON.stringify({
				type: "local_validation",
				source: "responses_chat_messages",
				status: 409,
				reason: "assistant_tool_calls_missing",
			}),
		};
	}
	if (!hints?.previousResponseId && !hints?.hasFunctionCallOutput) {
		return {
			code: "tool_call_chain_invalid_local",
			message:
				"tool_call_chain_invalid_local: responses request carries chat-style tool messages without previous_response_id",
			errorMetaJson: JSON.stringify({
				type: "local_validation",
				source: "responses_chat_messages",
				status: 409,
				reason: "missing_previous_response_id",
			}),
		};
	}
	return null;
}

export function validateOpenAiToolCallChain(
	body: Record<string, unknown> | null,
	endpointType: string,
	hints: ResponsesRequestHints | null,
): ToolCallChainValidationIssue | null {
	const chatIssue = validateOpenAiChatToolCallChain(body);
	if (chatIssue) {
		return chatIssue;
	}
	if (endpointType === "chat") {
		return null;
	}
	if (endpointType === "responses") {
		const responsesChatIssue = validateOpenAiResponsesChatMessageChain(
			body,
			hints,
		);
		if (responsesChatIssue) {
			return responsesChatIssue;
		}
		return validateOpenAiResponsesToolCallChain(
			body,
			hints?.previousResponseId ?? null,
		);
	}
	return null;
}

export function hasUnresolvedResponsesFunctionCallOutput(
	body: Record<string, unknown> | null,
	hints: ResponsesRequestHints | null,
): boolean {
	if (!body || !hints?.hasFunctionCallOutput) {
		return false;
	}
	const rawInput = body.input;
	const inputItems = Array.isArray(rawInput)
		? rawInput
		: rawInput
			? [rawInput]
			: [];
	if (inputItems.length === 0) {
		return false;
	}
	const inRequestFunctionCallIds = new Set<string>();
	for (const rawItem of inputItems) {
		if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
			continue;
		}
		const item = rawItem as Record<string, unknown>;
		const itemType = normalizeStringField(item.type)?.toLowerCase();
		if (itemType !== "function_call") {
			continue;
		}
		const callId = normalizeStringField(item.call_id ?? item.callId ?? item.id);
		if (callId) {
			inRequestFunctionCallIds.add(callId);
		}
	}
	for (const outputCallId of hints.functionCallOutputIds) {
		if (!inRequestFunctionCallIds.has(outputCallId)) {
			return true;
		}
	}
	return false;
}

export function isResponsesToolCallNotFoundMessage(
	message: string | null,
): boolean {
	const normalized = normalizeMessage(message)?.toLowerCase();
	if (!normalized) {
		return false;
	}
	return normalized.includes(RESPONSES_TOOL_CALL_NOT_FOUND_SNIPPET);
}
