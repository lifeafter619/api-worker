export function shouldHandleOpenAiStreamOptions(options: {
	upstreamProvider: string;
	isStream: boolean;
	endpointType: string;
	shouldSkipHeavyBodyParsing: boolean;
}): boolean {
	return (
		options.upstreamProvider === "openai" &&
		options.isStream &&
		options.endpointType === "chat" &&
		!options.shouldSkipHeavyBodyParsing
	);
}
