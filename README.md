> cpu limit 我没辙了 本地部署吧

# api-worker

Cloudflare Workers + D1 的 API 网关与管理台一体化项目。

- 后端编排：`apps/worker`（主 Worker，负责鉴权、路由、重试编排）
- 后端执行：`apps/attempt-worker`（调用执行器 Worker，负责单次上游调用）
- 前端：`apps/ui`（Vite 管理控制台）
- 部署：Worker 静态资源模式，`apps/ui/dist` 与 Worker 一起发布

## 适用场景

- 统一管理多上游 AI 渠道（OpenAI / Anthropic / Gemini 等）
- 基于 Token 的访问控制、配额与用量统计
- 提供 OpenAI 兼容代理入口（`/v1/*`、`/v1beta/*`）
- 提供管理台用于渠道、模型、令牌、日志和系统设置维护
- 支持按日定时探测禁用渠道（每个渠道随机选模型），测试通过后自动恢复
- 支持按日定时更新启用渠道的模型列表

## 部署

### 部署前环境变量

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`

说明：

- 创建 `CLOUDFLARE_API_TOKEN` 时，建议使用 **Account Token**，资源范围限制到目标账号（`CLOUDFLARE_ACCOUNT_ID` 对应账号）。
- 建议最小权限（Edit）：`Workers Scripts`、`D1`。
- 可选权限：`Workers Routes`（仅需要管理路由时）、`Workers Tail`（仅需要日志 tail 调试时）、`Workers Observability`（仅需要通过 Observability 能力查询/分析日志与追踪时）。

### GitHub Actions 自动部署

工作流：`.github/workflows/deploy.yml`（`Deploy SPA CF Workers[Worker一体化部署]`）

触发方式：

- `push` 到 `main/master` 且命中 `apps/ui/**`、`apps/worker/**` 或 `apps/attempt-worker/**`
- `workflow_dispatch` 手动触发
- `repository_dispatch`（`deploy-spa-button`）

需要配置的 Secrets：

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`

可选变量：

- `SPA_DEPLOY`：自动部署开关（`true` / `false`）

### 本地部署脚本（不执行远程 deploy）

```bash
node scripts/deploy.mjs init
node scripts/deploy.mjs update
```

等价脚本：

```bash
bun run deploy
bun run deploy:init
bun run deploy:update
```

说明：

- `bun run deploy`：交互式本地部署入口（引导选择 init/update 与参数）
- `init`：全量初始化流程（包含本地迁移）
- `update`：按参数执行构建与本地迁移判断
- 该脚本用于本地复刻流程，不会执行远程 `wrangler deploy`

## 技术栈

- Runtime: Cloudflare Workers
- API Framework: Hono
- Database: Cloudflare D1（SQLite）
- Stateful: Durable Objects
- WASM: Rust + wasm-bindgen（`apps/worker/wasm`）
- Frontend: Vite + TypeScript
- Monorepo: Bun workspaces

## 项目结构

```text
.
├─ apps/
│  ├─ worker/               # Worker API、路由、D1 迁移、wrangler 配置
│  │  ├─ src/
│  │  ├─ migrations/
│  │  ├─ wasm/
│  │  └─ wrangler.toml
│  ├─ attempt-worker/       # 调用执行器（单次上游调用）
│  └─ ui/                   # 管理台（Vite）
├─ scripts/
│  ├─ dev.mjs               # 本地并行启动 worker + attempt-worker + ui
│  └─ deploy.mjs            # 本地部署流程脚本（构建 + 本地迁移）
├─ package.json
└─ README.md
```

## 快速开始

### 1) 前置要求

- Bun `1.3.9`（见根 `package.json` 的 `packageManager`）
- Node.js（运行 `scripts/*.mjs`）
- Cloudflare Wrangler（通过 `bunx wrangler` 调用）

### 2) 安装依赖

```bash
bun install
```

### 3) 启动本地开发

统一启动命令：

```bash
bun run dev
bun run dev -- [可选参数]
```

说明：

- `bun run dev`：交互式启动入口（开始/状态/停止 + 参数选择）
- `bun run dev -- ...`：命令行参数直连模式（适合脚本和自启动）

可选参数：

- `--no-attempt-worker`：不启动调用执行器（attempt-worker）
- `--no-ui`：不启动 UI dev server
- `--remote-d1`：连接云端 D1/KV，但主 worker 与 attempt-worker 仍在本地执行
- `--remote-worker`：主 worker / attempt-worker 都切到远端预览执行（隐含 `--remote-d1`）
- `--no-hot-cache`：仅禁用 `KV_HOT` 热缓存（不影响其他内存级缓存）
- `--build-ui`：启动前先构建 UI（`bun run build:ui`）
- `--skip-ui-build`：跳过 UI 预构建（用于覆盖 `--build-ui` 默认行为）
- `--bg`：后台启动
- `--log-mode file|none`：后台日志策略（默认 `file`，写入 `.dev/dev-runner.log`；`none` 表示不写后台日志）
- `--status`：查看后台运行状态
- `--stop`：停止后台运行实例

常用示例：

- 默认启动（主 Worker + 本地 attempt-worker + UI）：`bun run dev`
- 主 Worker + UI（不启调用执行器）：`bun run dev -- --no-attempt-worker`
- 仅主 Worker：`bun run dev -- --no-attempt-worker --no-ui`
- 默认启动 + 云端 D1/KV：`bun run dev -- --remote-d1`
- 仅主 Worker + 云端 D1/KV + 禁用热缓存：`bun run dev -- --no-attempt-worker --no-ui --remote-d1 --no-hot-cache`
- 后台启动（仅主 Worker + 云端 D1/KV + 禁用热缓存）：`bun run dev -- --no-attempt-worker --no-ui --remote-d1 --no-hot-cache --bg`
- 后台启动且不写日志：`bun run dev -- --no-attempt-worker --no-ui --remote-d1 --bg --log-mode none`
- 远端预览执行：`bun run dev -- --remote-worker`
- 查看后台状态：`bun run dev -- --status`
- 停止后台实例：`bun run dev -- --stop`

自启动脚本（Windows / Linux）：

- 交互式配置：`bun run autostart`
- 开启自启动：`bun run autostart -- enable [dev 参数，空格分隔]`
- 关闭自启动：`bun run autostart -- disable`
- 查看状态：`bun run autostart -- status`
- 示例：`bun run autostart -- enable --no-attempt-worker --remote-d1 --no-ui --log-mode none`
- Windows：使用计划任务，不再写入 Startup `.cmd`
- Windows：为避免 `bun.exe` 作为控制台程序拉起窗口，计划任务会先通过隐藏的 PowerShell 启动器再执行 `bun run dev -- --bg`
- Linux：使用 `systemd --user`，会生成 `~/.config/systemd/user/api-worker-dev-autostart.service`
- Linux：要求当前发行版启用了 user session 的 systemd；脚本会把仓库绝对路径写入 `WorkingDirectory`
- Linux：默认属于 user service，通常在用户登录后启动；若需要开机后未登录也自动启动，请额外执行 `sudo loginctl enable-linger $USER`
- Linux：service 会直接托管 `scripts/dev.mjs` 的守护进程分支
- `bun run autostart -- status` 会同时显示“是否已启用”“当前是否正在运行”以及 Linux 旧版 `--bg` 配置提示
- 登录后真正拉起 Worker / Wrangler / UI 的后台守护链路同样继续沿用 `scripts/dev.mjs` 的日志与运行时配置策略

快捷命令（仅主 Worker + 禁用热缓存）：

- 默认先构建 UI 后启动：`bun run dev:worker`
- 可选跳过 UI 构建：`bun run dev:worker -- --skip-ui-build`
- 同样支持叠加参数（例如云端 D1 + 后台）：`bun run dev:worker -- --remote-d1 --bg`

仅主 Worker时是否受 UI 影响：

- 如果 `apps/ui/dist` 已存在（或之前构建过），Worker 的静态资源绑定可继续使用该构建产物
- 即使不启动 UI dev server，后端 API `/api/*`、`/v1*` 不受影响
- 若你需要实时改前端，仍建议单独启动 UI：`bun --filter api-worker-ui dev -- --port 4173`

或分别启动：

```bash
bun --cwd apps/attempt-worker run dev
bun --cwd apps/worker run dev
bun --filter api-worker-ui dev -- --port 4173
```

默认端口：

- Worker: `8787`（wrangler dev 默认）
- Attempt Worker: `8788`
- UI: `4173`

支持环境变量覆盖（`bun run dev` 生效）：

- `DEV_WORKER_PORT`：主 Worker 端口（默认 `8787`）
- `DEV_ATTEMPT_WORKER_PORT`：调用执行器端口（默认 `8788`）
- `DEV_UI_PORT`：UI 端口（默认 `4173`）

### 4) 首次本地迁移（推荐）

```bash
bun run --filter api-worker db:migrate
```

### 5) 本地运行并连接云端 D1/KV（可选）

在项目根目录 `.env` 或当前 shell 环境中提供以下 Cloudflare 资源 ID：

- `CLOUDFLARE_D1_DATABASE_ID`（D1 UUID）
- `CLOUDFLARE_KV_HOT_ID`（KV namespace id，32 位十六进制）

可先复制模板：

```bash
cp .env.example .env
```

然后按顺序执行：

```bash
bun run prepare:remote-config
bun run db:migrate:remote
bun run dev -- --remote-d1
bun run autostart -- status
```

说明：

- `prepare:remote-config` 会在本地生成 `apps/worker/.wrangler.remote.toml` 与 `apps/attempt-worker/.wrangler.remote.toml`
- `--remote-d1` 会使用本地执行 + remote bindings，不再默认切到 `wrangler dev --remote`
- 默认情况下主 worker 会优先走本地 `attempt-worker`（`http://127.0.0.1:<DEV_ATTEMPT_WORKER_PORT>`）
- 只有显式传入 `--remote-worker` 时，主 worker / attempt-worker 才会改走远端预览执行
- `--no-hot-cache` 会额外生成 no-hot 配置并移除 `KV_HOT` 绑定，仅影响热缓存路径
- 通过根目录 `bun run dev` / `bun run autostart` 触发的运行时配置会统一写入 `.dev/generated/wrangler/`
- 直接执行 `prepare:remote-config` / `prepare:no-hot-cache-config` 时，默认仍写入各应用目录；如需统一输出，可追加 `--output-root .dev/generated/wrangler`
- 这些临时文件已加入 `.gitignore`，不会入库
- 如需单独启动服务，可用 `bun --cwd apps/worker run dev:remote` 与 `bun --cwd apps/attempt-worker run dev:remote`
- 想切回本地数据库时，继续使用 `bun run dev` + `bun run --filter api-worker db:migrate`

## 常用命令

```bash
bun run test
bun run typecheck
bun run lint
bun run format
bun run check
bun run prepare:remote-config
bun run db:migrate:remote
bun run dev -- --remote-d1
```

说明：

- 当前仓库保留 Playwright E2E 验证入口；本地 unit/service 测试文件已移除，因此 `bun run test` 会在空测试集场景下正常通过。

## Agent 协作规范

- 统一执行规范：`AGENTS.md`
- Agent 本地环境变量文件：`.env.agent`（本地创建，不入库）
- Agent 环境变量参考模板：`.env.agent.example`

约定：

- 任务完成后必须执行格式化与检测。
- 前端改动必须使用自动化工具回归关键交互。
- 后端改动必须通过直接请求验证接口行为。

## Worker 绑定与运行配置

关键配置位于 `apps/worker/wrangler.toml`：

- D1: `DB`
- KV（热点只读数据）: `KV_HOT`
- Static Assets: `ASSETS`（目录 `../ui/dist`）
- Durable Objects: `CHECKIN_SCHEDULER`
- Service Binding: `ATTEMPT_WORKER`（绑定到 `attempt-worker`）
- CORS 默认值：`*`（如需限制来源，可配置可选绑定 `CORS_ORIGIN`）

注意：

- 仓库中的 `database_id` / `KV namespace id` 使用占位值，避免提交个人账号资源 ID。
- GitHub Actions 部署流程会自动“检查/创建资源并回填真实 ID”后再部署，因此不影响他人一键初始化。

### 前端开发代理

`apps/ui/vite.config.ts` 默认将以下路径代理到 `VITE_API_TARGET`（默认 `http://localhost:8787`）：

- `/api`
- `/v1`

`apps/ui/src/core/constants.ts` 支持 `VITE_API_BASE`，用于覆盖前端请求基址（默认同源）。

## API 概览

### 健康检查

- `GET /health`

### 管理台 API（`/api/*`）

- 认证
- `POST /api/auth/login`
- `POST /api/auth/logout`

- 渠道（兼容接口）
- `GET /api/channels`
- `POST /api/channels`
- `PATCH /api/channels/:id`
- `DELETE /api/channels/:id`
- `POST /api/channels/:id/test`

- 站点（管理台主用）
- `GET /api/sites`
- `POST /api/sites`
- `PATCH /api/sites/:id`
- `DELETE /api/sites/:id`
- `POST /api/sites/checkin-all`
- `POST /api/sites/:id/checkin`

- 模型
- `GET /api/models`

- 令牌
- `GET /api/tokens`
- `POST /api/tokens`
- `PATCH /api/tokens/:id`
- `GET /api/tokens/:id/reveal`
- `DELETE /api/tokens/:id`

- 用量与看板
- `GET /api/usage`
- `GET /api/dashboard`

- 系统设置
- `GET /api/settings`
- `PUT /api/settings`
- `POST /api/settings/cache/refresh`

### New API 兼容（`/api/channel` / `/api/group` / `/api/user`）

- 渠道
- `GET /api/channel`
- `GET /api/channel/search`
- `GET /api/channel/:id`
- `POST /api/channel`
- `PUT /api/channel`
- `DELETE /api/channel/:id`
- `GET /api/channel/test/:id`
- `POST /api/channel/test`
- `GET /api/channel/fetch_models/:id`
- `POST /api/channel/fetch_models`
- `GET /api/channel/models`
- `GET /api/channel/models_enabled`
- `PUT /api/channel/tag`
- `POST /api/channel/tag/enabled`
- `POST /api/channel/tag/disabled`

- 分组与用户
- `GET /api/group`
- `GET /api/user/models`

### OpenAI 兼容代理

- `GET /v1/models`：返回本站 active 渠道聚合出的 OpenAI 兼容模型列表，不请求上游；会按调用令牌的 `allowed_channels` 过滤可见模型
- `ALL /v1/*`：除 `GET /v1/models` 外，其余请求继续走多上游代理
- `ALL /v1beta/*`
- Responses 兼容补充：代理会在转发前规范化 `input_image` 内容块中的图片字段（将不兼容的 `url` 纠正为 `image_url`），并在流式 `responses` 成功返回时提取 `response_id` 写入亲和缓存，减少后续 `invalid_encrypted_content` 跨渠道重试
- `stream_options` 自动注入目前仅用于 `/v1/chat/completions` 流式请求；`/v1/responses` 会保持更接近直连的请求形状以提升兼容性

鉴权与细节请以 `apps/worker/src/middleware/*` 与对应 route 实现为准。

## 验收与排障建议

在提交前建议至少执行：

```bash
bun run typecheck
bun run check
bun run test
```

补充：

- 当前 `bun run test` 主要用于保证测试入口可执行；仓库仅保留 Playwright E2E 测试文件，不再保留本地 unit/service 测试文件。

若本地接口异常，优先检查：

- Worker 是否运行（`bun run dev` 或 `bun --cwd apps/worker run dev`）
- UI 代理目标是否正确（`VITE_API_TARGET`）
- D1 本地迁移是否完成（`db:migrate`）

## 维护说明

- 文档以代码为准；若行为变更，请同步更新本 README。
- API 全量定义请参考 `apps/worker/src/routes`。
