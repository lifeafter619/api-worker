import type {
	D1Database,
	DurableObjectNamespace,
	Fetcher,
	KVNamespace,
} from "@cloudflare/workers-types";

export type Bindings = {
	DB: D1Database;
	KV_HOT?: KVNamespace;
	ATTEMPT_WORKER?: Fetcher;
	CORS_ORIGIN?: string;
	PROXY_UPSTREAM_TIMEOUT_MS?: string;
	PROXY_STREAM_USAGE_MODE?: string;
	PROXY_STREAM_USAGE_MAX_BYTES?: string;
	PROXY_STREAM_USAGE_MAX_PARSERS?: string;
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
