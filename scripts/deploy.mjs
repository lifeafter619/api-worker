#!/usr/bin/env node
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { access } from "node:fs/promises";
import path from "node:path";
import { createInterface } from "node:readline/promises";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DEFAULT_CONFIG = "apps/worker/wrangler.toml";
const BUN_CMD = (() => {
	if (process.env.BUN_BIN && existsSync(process.env.BUN_BIN)) {
		return process.env.BUN_BIN;
	}
	const npmExec = process.env.npm_execpath;
	if (npmExec?.toLowerCase().includes("bun")) {
		return npmExec;
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
	return "bun";
})();

const parseArgs = () => {
	const args = process.argv.slice(2);
	const options = {
		action: null,
		target: "auto",
		migrate: "true",
		config: DEFAULT_CONFIG,
		skipInstall: false,
	};

	const positionals = [];
	for (let i = 0; i < args.length; i += 1) {
		const arg = args[i];
		if (arg.startsWith("--")) {
			const key = arg.slice(2);
			const value = args[i + 1];
			if (key === "action" && value) {
				options.action = value;
				i += 1;
			} else if (key === "target" && value) {
				options.target = value;
				i += 1;
			} else if (key === "migrate" && value) {
				options.migrate = value;
				i += 1;
			} else if (key === "config" && value) {
				options.config = value;
				i += 1;
			} else if (key === "skip-install") {
				options.skipInstall = true;
			} else if (key === "help") {
				printHelp();
				process.exit(0);
			}
		} else {
			positionals.push(arg);
		}
	}

	if (positionals[0] === "init" || positionals[0] === "update") {
		options.action = positionals[0];
	}
	if (!options.action && args.length > 0) {
		options.action = "update";
	}

	return options;
};

const printHelp = () => {
	console.log("Usage:");
	console.log(
		"  node scripts/deploy.mjs init [--target both] [--migrate true]",
	);
	console.log(
		"  node scripts/deploy.mjs update [--target auto] [--migrate true]",
	);
	console.log("");
	console.log("Options:");
	console.log("  --action init|update    部署动作（可用位置参数替代）");
	console.log("  --target frontend|backend|both|auto");
	console.log("  --migrate true|false|auto (默认 true)");
	console.log("  --config <path>         wrangler.toml 路径");
	console.log("  --skip-install          跳过 bun install");
	console.log("");
	console.log(
		"说明: 本脚本仅执行本地流程（构建 + 本地迁移），不会触发远程部署。",
	);
};

const run = (command, args, options = {}) =>
	new Promise((resolve, reject) => {
		const child = spawn(command, args, {
			cwd: ROOT,
			env: { ...process.env, ...options.env },
			stdio: "inherit",
		});
		child.on("error", (error) => {
			if (error.code === "ENOENT") {
				reject(
					new Error(
						`无法执行 ${command}，请确认已安装 Bun 并配置 PATH，或设置 BUN_BIN 指向 bun 可执行文件。`,
					),
				);
				return;
			}
			reject(error);
		});
		child.on("close", (code) => {
			if (code === 0) {
				resolve();
			} else {
				reject(new Error(`${command} failed with code ${code}`));
			}
		});
	});

const runCapture = (command, args, options = {}) =>
	new Promise((resolve, reject) => {
		const child = spawn(command, args, {
			cwd: ROOT,
			env: { ...process.env, ...options.env },
			stdio: ["ignore", "pipe", "pipe"],
		});
		let stdout = "";
		let stderr = "";
		child.stdout.on("data", (chunk) => {
			stdout += String(chunk);
		});
		child.stderr.on("data", (chunk) => {
			stderr += String(chunk);
		});
		child.on("error", (error) => {
			if (error.code === "ENOENT") {
				reject(
					new Error(
						`无法执行 ${command}，请确认已安装 Bun 并配置 PATH，或设置 BUN_BIN 指向 bun 可执行文件。`,
					),
				);
				return;
			}
			reject(error);
		});
		child.on("close", (code) => {
			if (code === 0) {
				resolve(stdout.trim());
			} else {
				reject(
					new Error(`${command} failed: ${stderr.trim() || stdout.trim()}`),
				);
			}
		});
	});

const runBun = (args, options) => run(BUN_CMD, args, options);
const runBunx = (args, options) => run(BUN_CMD, ["x", ...args], options);

const promptMenuChoice = async (rl, title, options, defaultValue) => {
	console.log(title);
	for (let i = 0; i < options.length; i += 1) {
		console.log(`${i + 1}. ${options[i].label}`);
	}
	const defaultIndex = options.findIndex((item) => item.value === defaultValue);
	const defaultHint = defaultIndex >= 0 ? `（默认 ${defaultIndex + 1}）` : "";
	while (true) {
		const answer = (await rl.question(`请选择编号${defaultHint}: `)).trim();
		if (!answer && defaultIndex >= 0) {
			return options[defaultIndex].value;
		}
		const numeric = Number(answer);
		if (
			Number.isInteger(numeric) &&
			numeric >= 1 &&
			numeric <= options.length
		) {
			return options[numeric - 1].value;
		}
		console.log(`输入无效，请输入 1-${options.length} 的编号。`);
	}
};

const targetFlagsToKey = (flags) => {
	if (flags.deployUi && flags.deployWorker) {
		return "both";
	}
	if (flags.deployUi) {
		return "frontend";
	}
	if (flags.deployWorker) {
		return "backend";
	}
	return "both";
};

const promptOptions = async (options, defaults) => {
	const rl = createInterface({ input: process.stdin, output: process.stdout });
	try {
		const action = await promptMenuChoice(
			rl,
			"请选择部署动作",
			[
				{ value: "init", label: "init（全量初始化）" },
				{ value: "update", label: "update（增量更新）" },
			],
			"update",
		);
		options.action = action;
		options.target = await promptMenuChoice(
			rl,
			"请选择部署目标",
			[
				{ value: "frontend", label: "frontend（仅前端）" },
				{ value: "backend", label: "backend（仅后端）" },
				{ value: "both", label: "both（前后端）" },
			],
			defaults.target,
		);
		options.migrate = await promptMenuChoice(
			rl,
			"请选择迁移策略",
			[
				{ value: "true", label: "true（执行迁移）" },
				{ value: "false", label: "false（跳过迁移）" },
			],
			defaults.migrate,
		);
		return options;
	} finally {
		rl.close();
	}
};

const ensureUiBuild = async () => {
	await runBun(["run", "--filter", "api-worker-ui", "build"]);
	const distDir = path.join(ROOT, "apps/ui/dist");
	const indexFile = path.join(distDir, "index.html");
	if (!existsSync(distDir) || !existsSync(indexFile)) {
		throw new Error("UI 构建失败：dist 或 index.html 不存在");
	}
};

const getChangedFiles = async () => {
	try {
		const insideRepo = await runCapture("git", [
			"rev-parse",
			"--is-inside-work-tree",
		]);
		if (insideRepo !== "true") {
			return null;
		}
		await runCapture("git", ["rev-parse", "HEAD~1"]);
		const diff = await runCapture("git", [
			"diff",
			"--name-only",
			"HEAD~1",
			"HEAD",
		]);
		return diff.split("\n").filter(Boolean);
	} catch {
		return null;
	}
};

const resolveTarget = (target, changedFiles) => {
	if (target !== "auto") {
		if (target === "backend") {
			return { deployUi: false, deployWorker: true };
		}
		if (target === "frontend") {
			return { deployUi: true, deployWorker: false };
		}
		return { deployUi: true, deployWorker: true };
	}
	if (!changedFiles) {
		return { deployUi: true, deployWorker: true };
	}
	const deployUi = changedFiles.some((file) => file.startsWith("apps/ui/"));
	const deployWorker = changedFiles.some(
		(file) =>
			file.startsWith("apps/worker/") ||
			file.startsWith("apps/attempt-worker/"),
	);
	if (!deployUi && !deployWorker) {
		return { deployUi: true, deployWorker: true };
	}
	return { deployUi, deployWorker };
};

const resolveMigrate = (migrate, changedFiles) => {
	if (migrate === "true") {
		return true;
	}
	if (migrate === "false") {
		return false;
	}
	if (!changedFiles) {
		return true;
	}
	return changedFiles.some((file) =>
		file.startsWith("apps/worker/migrations/"),
	);
};

const main = async () => {
	const options = parseArgs();
	const changedFiles = await getChangedFiles();
	const interactiveDefaults = {
		target: targetFlagsToKey(resolveTarget("auto", changedFiles)),
		migrate: "true",
	};
	if (!options.action) {
		const prompted = await promptOptions(options, interactiveDefaults);
		if (!prompted) {
			console.log("已退出交互模式。");
			return;
		}
	}
	const configPath = path.resolve(ROOT, options.config);

	await access(configPath);

	if (options.action === "init" && process.argv.slice(2).length > 0) {
		options.target = "both";
		options.migrate = "true";
	}
	const targets = resolveTarget(options.target, changedFiles);
	const shouldMigrate = resolveMigrate(options.migrate, changedFiles);

	console.log("本地部署计划:");
	console.log(`- action: ${options.action}`);
	console.log(`- target: ${options.target}`);
	console.log(`- migrate: ${options.migrate} => ${shouldMigrate}`);
	console.log(`- build UI: ${targets.deployUi}`);
	console.log(`- prepare backend: ${targets.deployWorker}`);
	console.log("- mode: local (no remote deploy)");

	if (!options.skipInstall) {
		await runBun(["install"]);
	}

	if (targets.deployUi) {
		await ensureUiBuild();
	}

	await runBunx(["wrangler", "telemetry", "disable"]);

	if (shouldMigrate) {
		await runBunx([
			"wrangler",
			"d1",
			"migrations",
			"apply",
			"DB",
			"--local",
			"--config",
			configPath,
		]);
	}

	console.log("✅ 本地部署完成");
};

main().catch((error) => {
	console.error(`❌ 部署失败: ${error.message}`);
	process.exit(1);
});
