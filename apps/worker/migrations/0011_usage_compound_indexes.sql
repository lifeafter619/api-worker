CREATE INDEX IF NOT EXISTS idx_usage_logs_token_created_at
  ON usage_logs (token_id, created_at);

CREATE INDEX IF NOT EXISTS idx_usage_logs_channel_created_at
  ON usage_logs (channel_id, created_at);

CREATE INDEX IF NOT EXISTS idx_usage_logs_model_created_at
  ON usage_logs (model, created_at);

CREATE INDEX IF NOT EXISTS idx_usage_logs_upstream_status_created_at
  ON usage_logs (upstream_status, created_at);

CREATE INDEX IF NOT EXISTS idx_usage_logs_status_created_at
  ON usage_logs (status, created_at);
