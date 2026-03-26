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
