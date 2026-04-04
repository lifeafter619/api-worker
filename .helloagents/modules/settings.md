# settings 模块

## 职责
- 维护系统级配置（如日志保留天数）
- 为使用日志与面板提供统一配置读取
- 维护定时签到执行时间
- 维护模型失败冷却时间（运行时失败的渠道/模型冷却窗口）
- 维护已禁用站点自动恢复评估的开关、时间与站点任务并发

## 接口定义
- `GET /api/settings` 读取设置
- `PUT /api/settings` 更新设置

## 行为规范
- 配置写入后即时生效
- 未配置时使用默认值回退
- 定时签到时间使用 `HH:mm` 格式（默认 `00:10`，中国时间）
- 更新签到时间后会触发 `CheckinScheduler` 重新调度
- 若更新后当前时间已超过新签到时间，当日可再次触发自动签到
- `channel_recovery_probe_enabled/channel_recovery_probe_schedule_time` 用于驱动“已禁用站点恢复评估”任务
- 站点任务并发上限同时作用于批量签到、批量验证与恢复评估
- 模型失败冷却以分钟计（默认 10 分钟），用于跳过最近失败的渠道/模型
- 运行时配置可写入 settings（不再使用环境变量回退）
- `GET /api/settings` 返回：
  - `runtime_settings`（可写配置值）
  - `runtime_config`（生效值 + 队列绑定/生效状态）
  - `usage_queue_status`（队列使用数量与状态：count/date/limit/enabled/bound/active）
- 运行时配置字段：
  - `proxy_upstream_timeout_ms`
  - `proxy_stream_usage_mode`
  - `proxy_stream_usage_max_bytes`
  - `proxy_stream_usage_max_parsers`
  - `proxy_usage_queue_enabled`
  - `usage_queue_daily_limit`
  - `usage_queue_direct_write_ratio`
- 运行时配置默认值：
  - `proxy_stream_usage_mode=full`
  - `proxy_stream_usage_max_bytes=0`
  - `proxy_stream_usage_max_parsers=0`
  - `proxy_upstream_timeout_ms=30000`
  - `proxy_usage_queue_enabled=true`
  - `usage_queue_daily_limit=10000`
  - `usage_queue_direct_write_ratio=0.5`
- `PROXY_STREAM_USAGE_MAX_BYTES/PROXY_STREAM_USAGE_MAX_PARSERS` 设为 `0` 表示无限制
- 缓存配置字段：
  - `cache_enabled`
  - `cache_ttl_dashboard_seconds` / `cache_ttl_usage_seconds` / `cache_ttl_models_seconds`
  - `cache_ttl_tokens_seconds` / `cache_ttl_channels_seconds` / `cache_ttl_call_tokens_seconds`
  - `cache_ttl_settings_seconds`
- `cache_config` 中包含分组版本号（dashboard/usage/models/tokens/channels/call_tokens/settings），用于精准失效
- `POST /api/settings/cache/refresh` 会 bump 所有缓存版本（用于紧急刷新）
- 队列使用数量来自 UsageLimiter DO 的 `/status`，未绑定时返回空值

## 依赖关系
- `settings` 表
- `checkin-scheduler` / `sites` 模块
