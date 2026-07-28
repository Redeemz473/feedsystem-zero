-- 通知中心。
-- status: 1=未读，2=已读，3=已撤回。
-- business_key 表示稳定业务关系，例如：
--   like:{receiver_id}:{actor_id}:{video_id}
--   comment:{receiver_id}:{actor_id}:{comment_id}
--   follow:{receiver_id}:{actor_id}
-- 同一关系取消后再次发生时复用原行，避免通知无限重复堆积。
CREATE TABLE IF NOT EXISTS notifications (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  business_key VARCHAR(191) NOT NULL,
  source_event_id VARCHAR(128) NOT NULL,
  receiver_id BIGINT UNSIGNED NOT NULL,
  actor_id BIGINT UNSIGNED NOT NULL,
  notification_type VARCHAR(32) NOT NULL,
  video_id BIGINT UNSIGNED NULL,
  comment_id BIGINT UNSIGNED NULL,
  status TINYINT NOT NULL DEFAULT 1,
  occurred_at DATETIME(3) NOT NULL,
  read_at DATETIME(3) NULL,
  deleted_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_notification_business (business_key),
  KEY idx_receiver_status_created (receiver_id, status, occurred_at, id),
  KEY idx_receiver_created (receiver_id, occurred_at, id),
  KEY idx_actor_created (actor_id, occurred_at, id),
  KEY idx_source_event (source_event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
