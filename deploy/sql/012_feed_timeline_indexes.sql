-- Feed Timeline 冷启动和关注回填索引。
-- 按作者获取最近正常视频时使用 author_id + status 过滤，并按 created_at/id 倒序。
-- 001_schema.sql 的新建库已经包含该索引；这里按需补齐，保证旧库迁移和重复执行都安全。
SET @feed_schema = DATABASE();
SET @feed_sql = (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE videos ADD INDEX idx_author_status_created (author_id, status, created_at, id)',
    'SELECT ''idx_author_status_created already exists'''
  )
  FROM information_schema.statistics
  WHERE table_schema = @feed_schema
    AND table_name = 'videos'
    AND index_name = 'idx_author_status_created'
);
PREPARE feed_stmt FROM @feed_sql;
EXECUTE feed_stmt;
DEALLOCATE PREPARE feed_stmt;

-- 视频发布/删除 fanout 按被关注者分页扫描活跃粉丝。
SET @feed_sql = (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE follows ADD INDEX idx_following_status_id (following_id, status, id)',
    'SELECT ''idx_following_status_id already exists'''
  )
  FROM information_schema.statistics
  WHERE table_schema = @feed_schema
    AND table_name = 'follows'
    AND index_name = 'idx_following_status_id'
);
PREPARE feed_stmt FROM @feed_sql;
EXECUTE feed_stmt;
DEALLOCATE PREPARE feed_stmt;
