export type OffloadDecisionInput = {
	attemptWorkerAvailable: boolean;
	thresholdBytes: number;
	contentLengthHeader: string | null;
};

export type OffloadDecision = {
	shouldOffload: boolean;
	requestSizeKnown: boolean;
	requestSizeBytes: number | null;
};

export function resolveLargeRequestOffload(
	input: OffloadDecisionInput,
): OffloadDecision {
	const thresholdBytes = Math.max(
		0,
		Math.floor(Number(input.thresholdBytes || 0)),
	);
	// threshold <= 0 means offload is disabled.
	const offloadEnabled = thresholdBytes > 0;
	const contentLengthHeader = input.contentLengthHeader?.trim() ?? "";
	const contentLength = Number(contentLengthHeader);
	const requestSizeKnown =
		contentLengthHeader.length > 0 &&
		Number.isFinite(contentLength) &&
		contentLength >= 0;
	const requestSizeBytes = requestSizeKnown ? Math.floor(contentLength) : null;
	if (!input.attemptWorkerAvailable || !offloadEnabled) {
		return {
			shouldOffload: false,
			requestSizeKnown,
			requestSizeBytes,
		};
	}
	if (!requestSizeKnown) {
		return {
			// Unknown size defaults to local handling; caller may use exact body size.
			shouldOffload: false,
			requestSizeKnown,
			requestSizeBytes,
		};
	}
	return {
		shouldOffload:
			requestSizeBytes !== null && requestSizeBytes >= thresholdBytes,
		requestSizeKnown,
		requestSizeBytes,
	};
}
