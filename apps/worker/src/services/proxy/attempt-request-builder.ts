import {
	buildUpstreamHeaders,
	mergeQuery,
	resolveChannelBaseUrl,
} from "./request-planning";
import { transformOpenAiStreamOptions } from "./usage-observe";
import { getProviderAdapter } from "../providers";
import { buildProviderChatRequest } from "../providers/chat-request";
import {
	buildProviderEmbeddingRequest,
	buildProviderImageRequest,
} from "../providers/requests";
import { rewriteModelInRawJsonRequest } from "./request-body";
import { shouldHandleOpenAiStreamOptions } from "./stream-options";

export type PreparedAttemptRequest = {
	upstreamProvider: string;
	upstreamModel: string | null;
	recordModel: string | null;
	tokenSelection: any;
	headers: Headers;
	target: string;
	fallbackTarget?: string;
	responsePath: string;
	fallbackPath?: string;
	bodyText?: string;
	streamOptionsHandled: boolean;
	streamOptionsInjected: boolean;
	strippedBodyText?: string;
};

/**
 * Builds a normalized upstream request for one candidate channel attempt.
 *
 * Returns `null` when the attempt should be skipped for this channel.
 */
export async function prepareAttemptRequest(options: {
	channel: any;
	attemptTarget: any;
	requestHeaders: Headers;
	targetPath: string;
	effectiveRequestText: string;
	parsedBody: Record<string, unknown> | null;
	downstreamProvider: string;
	endpointType: string;
	isStream: boolean;
	shouldSkipHeavyBodyParsing: boolean;
	querySuffix: string;
	upstreamTimeoutMs: number;
	streamUsageOptions: any;
	ensureNormalizedChat: () => any;
	ensureNormalizedEmbedding: () => any;
	ensureNormalizedImage: () => any;
	loadStreamOptionsCapability: (
		channelId: string,
	) => Promise<"supported" | "unsupported" | "unknown">;
}): Promise<PreparedAttemptRequest | null> {
	const metadata = options.attemptTarget.metadata;
	const upstreamProvider = options.attemptTarget.upstreamProvider;
	const providerAdapter = getProviderAdapter(upstreamProvider as any);
	const upstreamModel = options.attemptTarget.upstreamModel;
	const recordModel = options.attemptTarget.recordModel;
	const tokenSelection = options.attemptTarget.tokenSelection;

	const baseUrl = resolveChannelBaseUrl(options.channel);
	const apiKey = tokenSelection.token?.api_key ?? options.channel.api_key;
	const headers = buildUpstreamHeaders(
		new Headers(options.requestHeaders),
		upstreamProvider as any,
		String(apiKey),
		metadata.header_overrides,
	);
	headers.delete("host");
	headers.delete("content-length");

	let upstreamRequestPath = options.targetPath;
	let upstreamFallbackPath: string | undefined;
	let upstreamBodyText = options.effectiveRequestText || undefined;
	let absoluteUrl: string | undefined;
	const sameProvider = upstreamProvider === options.downstreamProvider;

	if (options.endpointType === "passthrough") {
		if (!sameProvider) {
			return null;
		}
		upstreamRequestPath = providerAdapter.applyModelToPath(
			upstreamRequestPath,
			upstreamModel,
		);
		if (upstreamRequestPath === options.targetPath && upstreamModel) {
			upstreamBodyText = options.parsedBody
				? JSON.stringify({
						...options.parsedBody,
						model: upstreamModel,
					})
				: rewriteModelInRawJsonRequest(upstreamBodyText, upstreamModel);
		}
	} else if (sameProvider) {
		upstreamRequestPath = providerAdapter.applyModelToPath(
			upstreamRequestPath,
			upstreamModel,
		);
		if (upstreamRequestPath === options.targetPath && upstreamModel) {
			upstreamBodyText = options.parsedBody
				? JSON.stringify({
						...options.parsedBody,
						model: upstreamModel,
					})
				: rewriteModelInRawJsonRequest(upstreamBodyText, upstreamModel);
		}
	} else {
		let built: { request: any; bodyText?: string } | null = null;

		if (
			options.endpointType === "chat" ||
			options.endpointType === "responses"
		) {
			const chatPayload = options.ensureNormalizedChat();
			if (!chatPayload) {
				return null;
			}
			const request = buildProviderChatRequest(
				upstreamProvider as any,
				chatPayload,
				upstreamModel,
				options.endpointType as any,
				options.isStream,
				metadata.endpoint_overrides,
			);
			if (!request) {
				return null;
			}
			built = {
				request,
				bodyText: request.body ? JSON.stringify(request.body) : undefined,
			};
		} else if (options.endpointType === "embeddings") {
			const embeddingPayload = options.ensureNormalizedEmbedding();
			if (!embeddingPayload) {
				return null;
			}
			const request = buildProviderEmbeddingRequest(
				upstreamProvider as any,
				embeddingPayload,
				upstreamModel,
				metadata.endpoint_overrides,
			);
			if (!request) {
				return null;
			}
			built = {
				request,
				bodyText: request.body ? JSON.stringify(request.body) : undefined,
			};
		} else if (options.endpointType === "images") {
			const imagePayload = options.ensureNormalizedImage();
			if (!imagePayload) {
				return null;
			}
			const request = buildProviderImageRequest(
				upstreamProvider as any,
				imagePayload,
				upstreamModel,
				metadata.endpoint_overrides,
			);
			if (!request) {
				return null;
			}
			built = {
				request,
				bodyText: request.body ? JSON.stringify(request.body) : undefined,
			};
		}

		if (!built) {
			return null;
		}

		upstreamRequestPath = built.request.path;
		absoluteUrl = built.request.absoluteUrl;
		upstreamFallbackPath = built.request.absoluteUrl
			? undefined
			: built.request.fallbackPath;
		upstreamBodyText = built.bodyText;
	}

	const shouldHandleStreamOptions = shouldHandleOpenAiStreamOptions({
		upstreamProvider,
		isStream: options.isStream,
		endpointType: options.endpointType,
		shouldSkipHeavyBodyParsing: options.shouldSkipHeavyBodyParsing,
	});
	let streamOptionsInjected = false;
	let strippedStreamOptionsBodyText: string | undefined = upstreamBodyText;

	if (shouldHandleStreamOptions) {
		const capability = await options.loadStreamOptionsCapability(
			options.channel.id,
		);
		if (capability !== "unsupported") {
			const injected = transformOpenAiStreamOptions(upstreamBodyText, "inject");
			upstreamBodyText = injected.bodyText;
			streamOptionsInjected = injected.injected;
			const stripped = transformOpenAiStreamOptions(upstreamBodyText, "strip");
			strippedStreamOptionsBodyText = stripped.bodyText;
		} else {
			const stripped = transformOpenAiStreamOptions(upstreamBodyText, "strip");
			upstreamBodyText = stripped.bodyText;
			strippedStreamOptionsBodyText = stripped.bodyText;
		}
	}

	const targetBase = absoluteUrl ?? `${baseUrl}${upstreamRequestPath}`;
	const target = mergeQuery(
		targetBase,
		options.querySuffix,
		metadata.query_overrides,
	);
	const fallbackTarget =
		upstreamFallbackPath && !absoluteUrl
			? mergeQuery(
					`${baseUrl}${upstreamFallbackPath}`,
					options.querySuffix,
					metadata.query_overrides,
				)
			: undefined;

	return {
		upstreamProvider,
		upstreamModel,
		recordModel,
		tokenSelection,
		headers,
		target,
		fallbackTarget,
		responsePath: upstreamRequestPath,
		fallbackPath: upstreamFallbackPath,
		bodyText: upstreamBodyText,
		streamOptionsHandled: shouldHandleStreamOptions,
		streamOptionsInjected,
		strippedBodyText: strippedStreamOptionsBodyText,
	};
}
