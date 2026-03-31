ALTER TABLE channels
ADD COLUMN auto_disable_hit_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE channels
ADD COLUMN auto_disabled_until INTEGER;

ALTER TABLE channels
ADD COLUMN auto_disabled_reason_code TEXT;

ALTER TABLE channels
ADD COLUMN auto_disabled_permanent INTEGER NOT NULL DEFAULT 0;

DELETE FROM settings
WHERE key IN (
	'proxy_retry_skip_error_codes',
	'model_failure_auto_disable_threshold'
);
