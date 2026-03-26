import { Hono } from "hono";
import { warmupWasmCore } from "../../worker/src/wasm/core";
import type { AppEnv } from "./env";
import attemptRoutes from "./routes/attempt";
import proxyRoutes from "./routes/proxy";

const app = new Hono<AppEnv>({ strict: false });
warmupWasmCore();

app.get("/health", (c) => c.json({ ok: true }));
app.route("/internal/attempt", attemptRoutes);
app.route("/v1", proxyRoutes);
app.route("/v1beta", proxyRoutes);

app.notFound((c) => c.json({ error: "Not Found" }, 404));
app.onError((error, c) => {
	console.error(error);
	return c.json({ error: "Internal Server Error" }, 500);
});

export default { fetch: app.fetch };
