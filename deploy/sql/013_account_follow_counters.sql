-- 013_account_follow_counters.sql
-- 在 accounts 表冗余粉丝数(follower_count)与关注数(following_count)，
-- 由 social 模块在关注/取关事务中维护，与 follows 表保持最终一致。
-- 读取直接走 accounts 表，避免 COUNT 查询，前端经 GetProfile/BatchGetProfiles 获取。

-- 每个字段分别判断后再添加，使迁移可以在执行中断后安全重跑。
SET @account_schema = DATABASE();
SET @account_sql = (
    SELECT IF(
        COUNT(*) = 0,
        'ALTER TABLE accounts ADD COLUMN follower_count BIGINT NOT NULL DEFAULT 0 COMMENT ''粉丝数，冗余自 follows 表，由 social 模块维护''',
        'SELECT ''follower_count already exists'''
    )
    FROM information_schema.columns
    WHERE table_schema = @account_schema
      AND table_name = 'accounts'
      AND column_name = 'follower_count'
);
PREPARE account_stmt FROM @account_sql;
EXECUTE account_stmt;
DEALLOCATE PREPARE account_stmt;

SET @account_sql = (
    SELECT IF(
        COUNT(*) = 0,
        'ALTER TABLE accounts ADD COLUMN following_count BIGINT NOT NULL DEFAULT 0 COMMENT ''关注数，冗余自 follows 表，由 social 模块维护''',
        'SELECT ''following_count already exists'''
    )
    FROM information_schema.columns
    WHERE table_schema = @account_schema
      AND table_name = 'accounts'
      AND column_name = 'following_count'
);
PREPARE account_stmt FROM @account_sql;
EXECUTE account_stmt;
DEALLOCATE PREPARE account_stmt;

-- 回填存量数据：follows.status=1 表示有效关注，status=2 表示已取关。
-- 注意：following_id 对应"被关注者"（粉丝数），follower_id 对应"主动关注者"（关注数）。
UPDATE accounts a
SET follower_count = (
        SELECT COUNT(*)
        FROM follows f
        WHERE f.following_id = a.id
          AND f.status = 1
          AND f.deleted_at IS NULL
    ),
    following_count = (
        SELECT COUNT(*)
        FROM follows f
        WHERE f.follower_id = a.id
          AND f.status = 1
          AND f.deleted_at IS NULL
    );
