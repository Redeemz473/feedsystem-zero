-- Outbox dispatcher final version:
-- 1. claim pending/failed events first and publish Kafka outside the DB transaction;
-- 2. recover processing events whose worker crashed or timed out;
-- 3. use lock_token to ensure only the owner can mark sent/failed/dead.
ALTER TABLE outbox_events
  ADD COLUMN lock_token VARCHAR(64) NOT NULL DEFAULT '' AFTER status,
  ADD COLUMN locked_by VARCHAR(128) NOT NULL DEFAULT '' AFTER lock_token,
  ADD COLUMN locked_at DATETIME NULL AFTER locked_by,
  ADD INDEX idx_status_locked (status, locked_at, id);
