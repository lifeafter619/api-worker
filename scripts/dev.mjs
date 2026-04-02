#!/usr/bin/env node
import { spawn } from "node:child_process";
import {
	closeSync,
	existsSync,
	mkdirSync,
	openSync,
	readFileSync,
	unlinkSync,
	writeFileSync,
	writeSync,
} from "node:fs";
import path from "node:path";
import { createInterface } from "node:readline/promises";
import { fileURLToPath } from "node:url";

const BUN_CMD = (() => {
	if (process.env.BUN_BIN && existsSync(process.env.BUN_BIN)) {
		return process.env.BUN_BIN;
	}
	const npmExec = process.env.npm_execpath;
	if (npmExec && existsSync(npmExec)) {
		const npmExecBaseName = path.basename(npmExec).toLowerCase();
		if (npmExecBaseName === "bun" || npmExecBaseName === "bun.exe") {
			return npmExec;
		}
	}
	if (process.env.BUN_INSTALL) {
		const candidate = path.join(
			process.env.BUN_INSTALL,
			"bin",
			process.platform === "win32" ? "bun.exe" : "bun",
		);
		if (existsSync(candidate)) {
			return candidate;
		}
	}
	return process.platform === "win32" ? "bun.exe" : "bun";
})();

const scriptPath = fileURLToPath(import.meta.url);
const stateDir = path.join(process.cwd(), ".dev");
const statePath = path.join(stateDir, "dev-runner.json");
const logPath = path.join(stateDir, "dev-runner.log");

const rawArgs = process.argv.slice(2);
const interactiveDelegatedMode = rawArgs.includes("--_interactive-run");
const runtimeArgs = rawArgs.filter((arg) => arg !== "--_interactive-run");
const daemonMode = runtimeArgs.includes("--_daemon");
const backgroundMode = runtimeArgs.includes("--bg");
const statusMode = runtimeArgs.includes("--status");
const stopMode = runtimeArgs.includes("--stop");

const isRemote = runtimeArgs.includes("--cloud-db");
const disableHotCache = runtimeArgs.includes("--no-hot-cache");
const skipAttemptWorker = runtimeArgs.includes("--no-attempt-worker");
const skipUi = runtimeArgs.includes("--no-ui");
const buildUi = runtimeArgs.includes("--build-ui");
const skipUiBuild = runtimeArgs.includes("--skip-ui-build");
const isInteractiveTerminal = Boolean(
	process.stdin.isTTY && process.stdout.isTTY,
);

const devInteractiveBaseOptions = [
	{ flag: "--no-attempt-worker", label: "不启动调用执行器 attempt-worker" },
	{ flag: "--no-ui", label: "不启动 UI dev server" },
	{ flag: "--no-hot-cache", label: "禁用热缓存 KV_HOT" },
	{ flag: "--cloud-db", label: "连接云端 D1/KV" },
];

const devInteractiveUiBuildOptions = [
	{ mode: "1", label: "构建 UI（--build-ui）", flags: ["--build-ui"] },
	{
		mode: "2",
		label: "跳过 UI 预构建（--skip-ui-build）",
		flags: ["--skip-ui-build"],
	},
];

const parsePortFromEnv = (name, fallback) => {
	const raw = process.env[name];
	if (!raw || raw.trim().length === 0) {
		return fallback;
	}
	const value = Number(raw);
	if (!Number.isInteger(value) || value < 1 || value > 65535) {
		throw new Error(
			`环境变量 ${name} 端口非法（${raw}），需为 1-65535 的整数。`,
		);
	}
	return value;
};

const workerPort = parsePortFromEnv(
	"DEV_WORKER_PORT",
	parsePortFromEnv("DEV_PORT", 8787),
);
const attemptWorkerPort = parsePortFromEnv("DEV_ATTEMPT_WORKER_PORT", 8788);
const uiPort = parsePortFromEnv("DEV_UI_PORT", 4173);
const workerInspectorPort = parsePortFromEnv("DEV_WORKER_INSPECTOR_PORT", 9229);
const attemptInspectorPort = parsePortFromEnv(
	"DEV_ATTEMPT_INSPECTOR_PORT",
	9230,
);

const children = new Map();
let shuttingDown = false;

const printSync = (message) => {
	writeSync(1, `${message}\n`);
};

const parseInteractiveSelection = (raw, maxIndex) => {
	const text = String(raw ?? "").trim();
	if (text.length === 0) {
		return [];
	}
	const parts = text
		.split(/[\s,，、]+/u)
		.map((item) => item.trim())
		.filter(Boolean);
	const indexes = [];
	for (const part of parts) {
		const value = Number(part);
		if (!Number.isInteger(value) || value < 1 || value > maxIndex) {
			throw new Error(
				`无效编号 "${part}"，请输入 1-${maxIndex} 之间的数字，可用空格分隔。`,
			);
		}
		if (!indexes.includes(value)) {
			indexes.push(value);
		}
	}
	return indexes;
};

const parseUiBuildModeArgs = (selection) => {
	const mode = String(selection ?? "").trim();
	if (mode.length === 0) {
		return ["--skip-ui-build"];
	}
	const matched = devInteractiveUiBuildOptions.find(
		(item) => item.mode === mode,
	);
	if (!matched) {
		throw new Error("UI 预构建策略无效，请输入 1 / 2。");
	}
	return matched.flags;
};

const promptInteractiveRunArgs = async () => {
	const rl = createInterface({
		input: process.stdin,
		output: process.stdout,
	});
	try {
		while (true) {
			console.log("交互模式：开发服务");
			console.log("1. 开始");
			console.log("2. 查看后台状态");
			console.log("3. 停止后台实例");
			console.log("0. 退出");
			const action = (await rl.question("请选择操作编号: ")).trim();
			if (action === "0") {
				return null;
			}
			if (action === "2") {
				return ["--status"];
			}
			if (action === "3") {
				return ["--stop"];
			}
			if (action === "1") {
				console.log("");
				console.log("开始开发服务：请选择附加参数（可多选）");
				for (let i = 0; i < devInteractiveBaseOptions.length; i += 1) {
					const option = devInteractiveBaseOptions[i];
					console.log(`${i + 1}. ${option.label}: ${option.flag}`);
				}
				const selection = await rl.question(
					"输入编号（示例: 1 4；直接回车=不附加参数）: ",
				);
				const selectedIndexes = parseInteractiveSelection(
					selection,
					devInteractiveBaseOptions.length,
				);
				const args = selectedIndexes.map(
					(index) => devInteractiveBaseOptions[index - 1].flag,
				);
				console.log("");
				console.log("UI 预构建策略（单选）:");
				for (const option of devInteractiveUiBuildOptions) {
					console.log(`${option.mode}. ${option.label}`);
				}
				const uiBuildMode = await rl.question(
					"请选择 UI 预构建策略（默认 2）: ",
				);
				args.push(...parseUiBuildModeArgs(uiBuildMode));
				const runMode = (
					await rl.question("是否静默启动（1=否，2=是，默认 1）: ")
				)
					.trim()
					.toLowerCase();
				if (runMode === "2") {
					args.push("--bg");
				} else if (runMode.length > 0 && runMode !== "1") {
					throw new Error("启动方式无效，请输入 1 / 2。");
				}
				return args;
			}
			console.log("输入无效，请输入 0 / 1 / 2 / 3。");
		}
	} finally {
		rl.close();
	}
};

const runSelf = (args) =>
	new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [scriptPath, ...args], {
			stdio: "inherit",
			cwd: process.cwd(),
			env: process.env,
		});
		child.on("error", reject);
		child.on("exit", (code) => {
			if (code === 0) {
				resolve();
				return;
			}
			reject(new Error(`交互执行失败，退出码 ${code ?? 1}`));
		});
	});

const ensureStateDir = () => {
	mkdirSync(stateDir, { recursive: true });
};

const readState = () => {
	if (!existsSync(statePath)) {
		return null;
	}
	try {
		return JSON.parse(readFileSync(statePath, "utf8"));
	} catch {
		return null;
	}
};

const removeState = () => {
	if (!existsSync(statePath)) {
		return;
	}
	try {
		unlinkSync(statePath);
	} catch {
		// ignore stale state cleanup errors
	}
};

const isPidRunning = (pid) => {
	if (typeof pid !== "number" || Number.isNaN(pid)) {
		return false;
	}
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
};

const readLiveState = () => {
	const state = readState();
	if (!state) {
		return null;
	}
	if (!isPidRunning(state.pid)) {
		removeState();
		return null;
	}
	return state;
};

const writeState = (state) => {
	ensureStateDir();
	writeFileSync(statePath, `${JSON.stringify(state, null, 2)}\n`, "utf8");
};

const killTree = async (pid) =>
	new Promise((resolve, reject) => {
		if (process.platform === "win32") {
			const child = spawn("taskkill", ["/PID", String(pid), "/T", "/F"], {
				stdio: "ignore",
			});
			child.on("error", reject);
			child.on("exit", (code) => {
				if (code === 0 || code === 128) {
					resolve();
					return;
				}
				reject(new Error(`taskkill 退出码 ${code ?? 1}`));
			});
			return;
		}
		try {
			process.kill(-pid, "SIGTERM");
			resolve();
		} catch {
			try {
				process.kill(pid, "SIGTERM");
				resolve();
			} catch (error) {
				reject(error);
			}
		}
	});

const shutdown = (code = 0) => {
	if (shuttingDown) {
		return;
	}
	shuttingDown = true;
	for (const child of children.values()) {
		if (!child.killed) {
			child.kill("SIGINT");
		}
	}
	if (daemonMode) {
		removeState();
	}
	process.exit(code);
};

const runOnce = (command, args, name) =>
	new Promise((resolve, reject) => {
		const child = spawn(command, args, { stdio: "inherit" });
		child.on("error", (error) => {
			if (error.code === "ENOENT") {
				reject(
					new Error(
						"未找到 Bun，请确认已安装并配置 PATH，或设置 BUN_BIN 指向 bun 可执行文件。",
					),
				);
				return;
			}
			reject(new Error(`执行 ${name} 失败: ${error.message}`));
		});
		child.on("exit", (code) => {
			if (code === 0) {
				resolve();
				return;
			}
			reject(new Error(`执行 ${name} 失败，退出码 ${code ?? 1}`));
		});
	});

const runBunScript = (name, args) =>
	runOnce(BUN_CMD, ["run", name, ...args], name);

const prepareConfigs = async () => {
	if (isRemote) {
		await runBunScript("prepare:remote-config", ["--", "--only", "worker"]);
		if (!skipAttemptWorker) {
			await runBunScript("prepare:remote-config", [
				"--",
				"--only",
				"attempt-worker",
			]);
		}
	}
	if (disableHotCache) {
		const baseArgs = isRemote ? ["--", "--remote"] : ["--"];
		await runBunScript("prepare:no-hot-cache-config", [
			...baseArgs,
			"--only",
			"worker",
		]);
		if (!skipAttemptWorker) {
			await runBunScript("prepare:no-hot-cache-config", [
				...baseArgs,
				"--only",
				"attempt-worker",
			]);
		}
	}
};

const prepareUiBuild = async () => {
	if (!buildUi || skipUiBuild) {
		return;
	}
	await runBunScript("build:ui", []);
};

const buildCommands = () => {
	const commands = [];
	if (!skipAttemptWorker) {
		const attemptWranglerArgs = ["dev", "--port", String(attemptWorkerPort)];
		if (isRemote) {
			attemptWranglerArgs.push("--remote", "--config", ".wrangler.remote.toml");
		}
		attemptWranglerArgs.push("--inspector-port", String(attemptInspectorPort));
		commands.push({
			name: "attempt-worker",
			cmd: BUN_CMD,
			args: ["x", "wrangler", ...attemptWranglerArgs],
			cwd: path.join(process.cwd(), "apps/attempt-worker"),
		});
	}
	const workerWranglerArgs = ["dev", "--port", String(workerPort)];
	if (isRemote) {
		workerWranglerArgs.push("--remote");
		workerWranglerArgs.push(
			"--config",
			disableHotCache
				? ".wrangler.remote.no-hot-cache.toml"
				: ".wrangler.remote.toml",
		);
	} else if (disableHotCache) {
		workerWranglerArgs.push("--config", ".wrangler.local.no-hot-cache.toml");
	}
	workerWranglerArgs.push("--inspector-port", String(workerInspectorPort));
	commands.push({
		name: "worker",
		cmd: BUN_CMD,
		args: ["x", "wrangler", ...workerWranglerArgs],
		cwd: path.join(process.cwd(), "apps/worker"),
	});
	if (!skipUi) {
		commands.push({
			name: "ui",
			cmd: BUN_CMD,
			args: [
				"--filter",
				"api-worker-ui",
				"dev",
				"--",
				"--port",
				String(uiPort),
			],
		});
	}
	return commands;
};

const startLongRunningCommands = (commands) => {
	for (const command of commands) {
		const child = spawn(command.cmd, command.args, {
			stdio: "inherit",
			cwd: command.cwd ?? process.cwd(),
		});
		children.set(command.name, child);
		child.on("error", (error) => {
			if (error.code === "ENOENT") {
				console.error(
					"❌ 未找到 Bun，请确认已安装并配置 PATH，或设置 BUN_BIN 指向 bun 可执行文件。",
				);
				shutdown(1);
				return;
			}
			console.error(`❌ 启动 ${command.name} 失败: ${error.message}`);
			shutdown(1);
		});
		child.on("exit", (code) => {
			if (shuttingDown) {
				return;
			}
			if (code && code !== 0) {
				shutdown(code);
				return;
			}
			const allExited = Array.from(children.values()).every(
				(item) => item.exitCode !== null,
			);
			if (allExited) {
				shutdown(0);
			}
		});
	}
};

const printStatus = () => {
	const state = readLiveState();
	if (!state) {
		console.log("ℹ️ 后台 dev 未运行。");
		console.log(`日志文件: ${logPath}`);
		return;
	}
	console.log("✅ 后台 dev 正在运行。");
	console.log(`PID: ${state.pid}`);
	console.log(`启动时间: ${state.startedAt}`);
	console.log(`参数: ${state.args.join(" ") || "(无)"}`);
	console.log(`日志文件: ${state.logPath}`);
};

const stopBackground = async () => {
	const state = readLiveState();
	if (!state) {
		console.log("ℹ️ 后台 dev 未运行，无需停止。");
		return;
	}
	await killTree(state.pid);
	removeState();
	console.log(`✅ 已停止后台 dev（PID ${state.pid}）。`);
};

const startBackground = () => {
	const current = readLiveState();
	if (current) {
		printSync(`ℹ️ 后台 dev 已在运行（PID ${current.pid}）。`);
		printSync(`日志文件: ${current.logPath}`);
		return;
	}

	ensureStateDir();
	const cleanArgs = runtimeArgs.filter(
		(arg) => arg !== "--bg" && arg !== "--_daemon",
	);
	const stdoutFd = openSync(logPath, "a");
	const stderrFd = openSync(logPath, "a");
	const child = spawn(
		process.execPath,
		[scriptPath, ...cleanArgs, "--_daemon"],
		{
			detached: true,
			stdio: ["ignore", stdoutFd, stderrFd],
			windowsHide: true,
			cwd: process.cwd(),
			env: process.env,
		},
	);
	closeSync(stdoutFd);
	closeSync(stderrFd);
	child.unref();

	writeState({
		pid: child.pid,
		args: cleanArgs,
		startedAt: new Date().toISOString(),
		logPath,
	});
	printSync(`✅ 已后台启动 dev（PID ${child.pid}）。`);
	printSync(`日志文件: ${logPath}`);
	printSync(`查看状态: bun run dev -- --status`);
	printSync(`停止服务: bun run dev -- --stop`);
};

process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));
process.on("exit", () => {
	if (daemonMode) {
		removeState();
	}
});

const main = async () => {
	if (
		!daemonMode &&
		!interactiveDelegatedMode &&
		runtimeArgs.length === 0 &&
		isInteractiveTerminal
	) {
		const interactiveArgs = await promptInteractiveRunArgs();
		if (!interactiveArgs) {
			console.log("已退出交互模式。");
			return;
		}
		await runSelf(["--_interactive-run", ...interactiveArgs]);
		return;
	}

	const actionCount = [backgroundMode, statusMode, stopMode].filter(
		Boolean,
	).length;
	if (actionCount > 1) {
		throw new Error("--bg / --status / --stop 只能三选一");
	}

	if (statusMode) {
		printStatus();
		return;
	}

	if (stopMode) {
		await stopBackground();
		return;
	}

	if (backgroundMode && !daemonMode) {
		startBackground();
		return;
	}

	if (daemonMode) {
		writeState({
			pid: process.pid,
			args: runtimeArgs.filter((arg) => arg !== "--_daemon"),
			startedAt: new Date().toISOString(),
			logPath,
		});
	}

	await prepareUiBuild();
	await prepareConfigs();
	const commands = buildCommands();
	startLongRunningCommands(commands);
};

main().catch((error) => {
	console.error(`❌ 启动前准备失败: ${error.message}`);
	process.exit(1);
});
