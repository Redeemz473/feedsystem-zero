-- 为互动统计增加版本号，并补齐事件表分批清理所需索引。
-- 本迁移可重复执行，便于多台开发机对齐数据库结构。
SET @schema_name = DATABASE();

SET @ddl = (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE videos ADD COLUMN stats_version BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER popularity',
    'SELECT ''videos.stats_version already exists'''
  )
  FROM information_schema.columns
  WHERE table_schema = @schema_name
    AND table_name = 'videos'
    AND column_name = 'stats_version'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl = (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE outbox_events ADD INDEX idx_status_sent_id (status, sent_at, id)',
    'SELECT ''idx_status_sent_id already exists'''
  )
  FROM information_schema.statistics
  WHERE table_schema = @schema_name
    AND table_name = 'outbox_events'
    AND index_name = 'idx_status_sent_id'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl = (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE processed_events ADD INDEX idx_expire_id (expire_at, id)',
    'SELECT ''idx_expire_id already exists'''
  )
  FROM information_schema.statistics
  WHERE table_schema = @schema_name
    AND table_name = 'processed_events'
    AND index_name = 'idx_expire_id'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl = (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE dead_letter_events ADD INDEX idx_created_id (created_at, id)',
    'SELECT ''idx_created_id already exists'''
  )
  FROM information_schema.statistics
  WHERE table_schema = @schema_name
    AND table_name = 'dead_letter_events'
    AND index_name = 'idx_created_id'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
