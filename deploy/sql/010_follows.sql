-- 单向关注关系表（类似微博/抖音：A 关注 B 不要求 B 反向关注 A）。
-- follower_id = 主动关注者；following_id = 被关注者。
-- 采用软删除：取关将 status 置为 2，不物理删除，保留审计并方便统计。
-- 关注事件通过 outbox_events(topic=social.follow.events) 异步分发，
-- 下游 feed-timeline-job / notification-job 消费，使用 processed_events 做幂等。
CREATE TABLE IF NOT EXISTS follows (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  follower_id BIGINT UNSIGNED NOT NULL,
  following_id BIGINT UNSIGNED NOT NULL,
  status TINYINT NOT NULL DEFAULT 1,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  deleted_at DATETIME NULL,
  UNIQUE KEY uk_follower_following (follower_id, following_id),
  KEY idx_following (following_id, status, id),
  KEY idx_follower (follower_id, status, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
