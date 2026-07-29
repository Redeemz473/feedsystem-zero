-- 015_account_big_v_flag.sql
-- 为 accounts 表增加"是否已升级为大 V"的持久化标记位 is_big_v，用于解决
-- Feed 推拉分离中大 V 阈值反向穿越（follower_count 从 >=阈值 掉回 <阈值）
-- 导致的历史视频从关注流消失问题。
--
-- 语义约定（只升不降，Monotonic）：
--   1) 首次 follower_count 达到 feedx.BigCreatorFollowerThreshold 时，由 social 模块
--      在关注事务内同事务 UPDATE is_big_v = 1；
--   2) 一旦置为 1 就永久保留，即使粉丝掉回阈值以下也不回滚，
--      保证已经写入 author outbox 的历史视频永远能被读侧 union 到；
--   3) 写侧 fanout 决策与读侧 union 决策全部改看 is_big_v，
--      不再直接比较 follower_count 与阈值。

SET @account_schema = DATABASE();

-- 幂等新增 is_big_v 列，允许迁移中断后安全重跑。
SET @account_sql = (
    SELECT IF(
        COUNT(*) = 0,
        'ALTER TABLE accounts ADD COLUMN is_big_v TINYINT(1) NOT NULL DEFAULT 0 COMMENT ''是否已升级为大V，只升不降，由 social 模块在关注事务内维护''',
        'SELECT ''is_big_v already exists'''
    )
    FROM information_schema.columns
    WHERE table_schema = @account_schema
      AND table_name = 'accounts'
      AND column_name = 'is_big_v'
);
PREPARE account_stmt FROM @account_sql;
EXECUTE account_stmt;
DEALLOCATE PREPARE account_stmt;

-- 回填存量数据：把当前 follower_count 已经 >= 5000 的账号一次性标记为大 V。
-- 阈值需与 common/feedx/bigv.go 中的 BigCreatorFollowerThreshold 保持一致；
-- 未来若调大阈值，历史已标记为大 V 的账号仍保留大 V 身份（只升不降原则）。
UPDATE accounts
SET is_big_v = 1
WHERE follower_count >= 5000
  AND is_big_v = 0;
