import type {
	AdminData,
	DashboardQuery,
	SettingsForm,
	SiteForm,
	TabItem,
	TokenForm,
} from "./types";

export const apiBase = import.meta.env.VITE_API_BASE ?? "";

export const tabs: TabItem[] = [
	{ id: "dashboard", label: "数据面板" },
	{ id: "channels", label: "站点管理" },
	{ id: "models", label: "模型广场" },
	{ id: "tokens", label: "令牌管理" },
	{ id: "usage", label: "使用日志" },
	{ id: "settings", label: "系统设置" },
];

export const initialData: AdminData = {
	sites: [],
	tokens: [],
	models: [],
	usage: [],
	dashboard: null,
	settings: null,
};

export const initialSiteForm: SiteForm = {
	name: "",
	base_url: "",
	weight: 1,
	status: "active",
	site_type: "new-api",
	checkin_url: "",
	system_token: "",
	system_userid: "",
	checkin_enabled: false,
	call_tokens: [
		{
			name: "主调用令牌",
			api_key: "",
		},
	],
};

export const initialSettingsForm: SettingsForm = {
	log_retention_days: "30",
	session_ttl_hours: "12",
	admin_password: "",
	checkin_schedule_time: "00:10",
	proxy_model_failure_cooldown_minutes: "10",
	proxy_model_failure_cooldown_threshold: "2",
	proxy_upstream_timeout_ms: "30000",
	proxy_retry_max_retries: "3",
	proxy_stream_usage_mode: "full",
	proxy_stream_usage_max_bytes: "0",
	proxy_stream_usage_max_parsers: "0",
	proxy_usage_reserve_breaker_ms: "60000",
	proxy_stream_usage_parse_timeout_ms: "20000",
	proxy_usage_error_message_max_length: "320",
	proxy_usage_queue_enabled: true,
	usage_queue_daily_limit: "10000",
	usage_queue_direct_write_ratio: "0.5",
};

export const initialDashboardQuery: DashboardQuery = {
	preset: "all",
	interval: "month",
	from: "",
	to: "",
	channel_ids: [],
	token_ids: [],
	model: "",
};

export const initialTokenForm: TokenForm = {
	name: "",
	quota_total: "",
	status: "active",
	expires_at: "",
	allowed_channels: [],
};
