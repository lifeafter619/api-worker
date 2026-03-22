import type {
	D1Database,
	DurableObjectNamespace,
	KVNamespace,
	Queue,
} from "@cloudflare/workers-types";
import type { UsageQueueEvent } from "./services/usage-queue";

export type Bindings = {
	DB: D1Database;
	KV_HOT?: KVNamespace;
	CORS_ORIGIN?: string;
	PROXY_UPSTREAM_TIMEOUT_MS?: string;
	PROXY_STREAM_USAGE_MODE?: string;
	PROXY_STREAM_USAGE_MAX_BYTES?: string;
	PROXY_STREAM_USAGE_MAX_PARSERS?: string;
	PROXY_USAGE_QUEUE_ENABLED?: string;
	USAGE_QUEUE?: Queue<UsageQueueEvent>;
	USAGE_LIMITER?: DurableObjectNamespace;
	CACHE_VERSION_STORE?: DurableObjectNamespace;
	CHECKIN_SCHEDULER: DurableObjectNamespace;
};

export type Variables = {
	adminSessionId?: string;
	newApiUserId?: string | null;
	tokenRecord?: unknown;
};

export type AppEnv = {
	Bindings: Bindings;
	Variables: Variables;
};
