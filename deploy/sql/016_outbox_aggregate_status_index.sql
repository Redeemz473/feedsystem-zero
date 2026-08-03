-- 加速 Outbox 对同一业务聚合“最早未完成事件”的查找。
-- dispatcher 只允许前序 pending/failed/dead/processing 全部结束后投递下一条，
-- 该覆盖索引避免每轮扫描同一聚合已经发送的全部历史事件。
SET @outbox_schema = DATABASE();
SET @outbox_sql = (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE outbox_events ADD INDEX idx_aggregate_status_id (aggregate_type, aggregate_id, status, id)',
    'SELECT ''idx_aggregate_status_id already exists'''
  )
  FROM information_schema.statistics
  WHERE table_schema = @outbox_schema
    AND table_name = 'outbox_events'
    AND index_name = 'idx_aggregate_status_id'
);
PREPARE outbox_stmt FROM @outbox_sql;
EXECUTE outbox_stmt;
DEALLOCATE PREPARE outbox_stmt;
