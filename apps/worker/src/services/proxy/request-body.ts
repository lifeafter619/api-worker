import { safeJsonParse } from "../../utils/json";

export type ResponsesRequestHints = {
	previousResponseId: string | null;
	functionCallOutputIds: string[];
	hasFunctionCallOutput: boolean;
};

export function extractModelFromRawJsonRequest(rawText: string): string | null {
	if (!rawText) {
		return null;
	}
	const match = rawText.match(/"model"\s*:\s*"((?:\\.|[^"\\])*)"/);
	if (!match?.[1]) {
		return null;
	}
	return decodeRawJsonStringLiteral(match[1]);
}

function decodeRawJsonStringLiteral(value: string): string | null {
	try {
		return JSON.parse(`"${value}"`);
	} catch {
		return null;
	}
}

function extractStringFieldFromRawJsonRequest(
	rawText: string,
	fieldName: string,
): string | null {
	const escapedName = fieldName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
	const matcher = new RegExp(
		`"${escapedName}"\\s*:\\s*"((?:\\\\.|[^"\\\\])*)"`,
	);
	const match = rawText.match(matcher);
	if (!match?.[1]) {
		return null;
	}
	return decodeRawJsonStringLiteral(match[1]);
}

export function extractResponsesRequestHintsFromRawJsonRequest(
	rawText: string,
): ResponsesRequestHints | null {
	if (!rawText) {
		return null;
	}
	const previousResponseId =
		extractStringFieldFromRawJsonRequest(rawText, "previous_response_id") ??
		extractStringFieldFromRawJsonRequest(rawText, "previousResponseId");
	const hasFunctionCallOutput = /"type"\s*:\s*"function_call_output"/.test(
		rawText,
	);
	if (!previousResponseId && !hasFunctionCallOutput) {
		return null;
	}
	return {
		previousResponseId,
		functionCallOutputIds: [],
		hasFunctionCallOutput,
	};
}

export function rewriteModelInRawJsonRequest(
	rawText: string | undefined,
	model: string,
): string | undefined {
	if (!rawText) {
		return rawText;
	}
	const matcher = /"model"\s*:\s*"(?:\\.|[^"\\])*"/;
	if (!matcher.test(rawText)) {
		return rawText;
	}
	return rawText.replace(matcher, `"model":${JSON.stringify(model)}`);
}

function normalizeInputImagePart(part: Record<string, unknown>): boolean {
	if (
		String(part.type ?? "")
			.trim()
			.toLowerCase() !== "input_image"
	) {
		return false;
	}
	let changed = false;
	if (
		part.image_url &&
		typeof part.image_url === "object" &&
		!Array.isArray(part.image_url)
	) {
		const imageUrlRecord = part.image_url as Record<string, unknown>;
		const nestedUrl =
			typeof imageUrlRecord.url === "string" ? imageUrlRecord.url.trim() : "";
		if (nestedUrl) {
			part.image_url = nestedUrl;
			changed = true;
		}
	}
	const directUrl = typeof part.url === "string" ? part.url.trim() : "";
	if (directUrl) {
		if (part.image_url === undefined) {
			part.image_url = directUrl;
		}
		delete part.url;
		changed = true;
	}
	return changed;
}

function normalizeResponsesInputItem(item: unknown): boolean {
	if (!item || typeof item !== "object" || Array.isArray(item)) {
		return false;
	}
	const record = item as Record<string, unknown>;
	let changed = false;
	if (Array.isArray(record.content)) {
		for (const rawPart of record.content) {
			if (rawPart && typeof rawPart === "object" && !Array.isArray(rawPart)) {
				changed =
					normalizeInputImagePart(rawPart as Record<string, unknown>) ||
					changed;
			}
		}
		return changed;
	}
	if (
		record.content &&
		typeof record.content === "object" &&
		!Array.isArray(record.content)
	) {
		return normalizeInputImagePart(record.content as Record<string, unknown>);
	}
	return false;
}

export function sanitizeOpenAiResponsesBodyInPlace(
	body: Record<string, unknown> | null,
): boolean {
	if (!body) {
		return false;
	}
	const rawInput = body.input;
	if (Array.isArray(rawInput)) {
		let changed = false;
		for (const item of rawInput) {
			changed = normalizeResponsesInputItem(item) || changed;
		}
		return changed;
	}
	return normalizeResponsesInputItem(rawInput);
}

export function maybeParseAndSanitizeOpenAiRequestText(rawText: string): {
	body: Record<string, unknown>;
	bodyText: string;
} | null {
	if (
		!rawText ||
		!rawText.includes('"input"') ||
		(!rawText.includes('"url"') && !rawText.includes('"image_url"'))
	) {
		return null;
	}
	const parsed = safeJsonParse<Record<string, unknown> | null>(rawText, null);
	if (!parsed) {
		return null;
	}
	if (!sanitizeOpenAiResponsesBodyInPlace(parsed)) {
		return null;
	}
	return {
		body: parsed,
		bodyText: JSON.stringify(parsed),
	};
}
