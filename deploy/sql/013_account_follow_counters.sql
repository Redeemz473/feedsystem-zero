-- 013_account_follow_counters.sql
-- 在 accounts 表冗余粉丝数(follower_count)与关注数(following_count)，
-- 由 social 模块在关注/取关事务中维护，与 follows 表保持最终一致。
-- 读取直接走 accounts 表，避免 COUNT 查询，前端经 GetProfile/BatchGetProfiles 获取。

ALTER TABLE accounts
    ADD COLUMN follower_count  BIGINT NOT NULL DEFAULT 0 COMMENT '粉丝数，冗余自 follows 表，由 social 模块维护',
    ADD COLUMN following_count BIGINT NOT NULL DEFAULT 0 COMMENT '关注数，冗余自 follows 表，由 social 模块维护';

-- 回填存量数据：以 follows 表中 status='active' 且未软删除的关系为准。
-- 注意：following_id 对应"被关注者"（粉丝数），follower_id 对应"主动关注者"（关注数）。
UPDATE accounts a
SET follower_count = (
        SELECT COUNT(*)
        FROM follows f
        WHERE f.following_id = a.id
          AND f.status = 'active'
          AND f.deleted_at IS NULL
    ),
    following_count = (
        SELECT COUNT(*)
        FROM follows f
        WHERE f.follower_id = a.id
          AND f.status = 'active'
          AND f.deleted_at IS NULL
    );
