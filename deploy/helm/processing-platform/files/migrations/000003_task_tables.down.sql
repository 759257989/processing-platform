ALTER TABLE devices DROP COLUMN IF EXISTS firmware_version;
ALTER TABLE devices DROP COLUMN IF EXISTS last_seen_at;
DROP TABLE IF EXISTS alerts;
DROP TABLE IF EXISTS command_audit;
DROP TABLE IF EXISTS firmware_history;
DROP TABLE IF EXISTS device_metrics;
