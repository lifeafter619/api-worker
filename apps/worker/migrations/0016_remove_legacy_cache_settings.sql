DELETE FROM settings
WHERE key IN (
	'cache_enabled',
	'cache_ttl_dashboard_seconds',
	'cache_ttl_usage_seconds',
	'cache_ttl_models_seconds',
	'cache_ttl_tokens_seconds',
	'cache_ttl_channels_seconds',
	'cache_ttl_call_tokens_seconds',
	'cache_ttl_settings_seconds',
	'cache_v_dashboard',
	'cache_v_usage',
	'cache_v_models',
	'cache_v_tokens',
	'cache_v_channels',
	'cache_v_call_tokens',
	'cache_v_settings'
);
