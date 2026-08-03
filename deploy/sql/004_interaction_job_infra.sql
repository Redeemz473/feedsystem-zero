ALTER TABLE likes
  ADD COLUMN status TINYINT NOT NULL DEFAULT 1 AFTER user_id,
  ADD COLUMN deleted_at DATETIME NULL AFTER status,
  ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at,
  DROP INDEX idx_user_created,
  DROP INDEX idx_video_created,
  ADD INDEX idx_user_status_created (user_id, status, created_at, video_id),
  ADD INDEX idx_video_status_created (video_id, status, created_at);

ALTER TABLE comments
  ADD COLUMN request_id VARCHAR(128) NULL AFTER content,
  ADD COLUMN status TINYINT NOT NULL DEFAULT 1 AFTER request_id,
  ADD COLUMN deleted_at DATETIME NULL AFTER status,
  DROP INDEX idx_video_created,
  DROP INDEX idx_user_created,
  ADD UNIQUE KEY uk_comment_request (user_id, request_id),
  ADD INDEX idx_video_status_created (video_id, status, created_at, id),
  ADD INDEX idx_user_status_created (user_id, status, created_at, id);

CREATE TABLE IF NOT EXISTS interaction_events (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  event_id VARCHAR(128) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  video_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
  user_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
  comment_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
  action VARCHAR(32) NOT NULL DEFAULT '',
  delta BIGINT NOT NULL DEFAULT 0,
  request_id VARCHAR(128) NULL,
  payload JSON NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_event_id (event_id),
  KEY idx_video_created (video_id, created_at, id),
  KEY idx_user_created (user_id, created_at, id),
  KEY idx_type_created (event_type, created_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS outbox_events (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  event_id VARCHAR(128) NOT NULL,
  topic VARCHAR(128) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  aggregate_type VARCHAR(64) NOT NULL,
  aggregate_id VARCHAR(128) NOT NULL,
  payload JSON NOT NULL,
  status TINYINT NOT NULL DEFAULT 1,
  lock_token VARCHAR(64) NOT NULL DEFAULT '',
  locked_by VARCHAR(128) NOT NULL DEFAULT '',
  locked_at DATETIME NULL,
  retry_count INT NOT NULL DEFAULT 0,
  next_retry_at DATETIME NULL,
  sent_at DATETIME NULL,
  last_error VARCHAR(1024) NOT NULL DEFAULT '',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_event_id (event_id),
  KEY idx_status_next_retry (status, next_retry_at, id),
  KEY idx_status_locked (status, locked_at, id),
  KEY idx_topic_status (topic, status, id),
  KEY idx_aggregate (aggregate_type, aggregate_id, id),
  KEY idx_aggregate_status_id (aggregate_type, aggregate_id, status, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS processed_events (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  event_id VARCHAR(128) NOT NULL,
  consumer_name VARCHAR(128) NOT NULL,
  topic VARCHAR(128) NOT NULL DEFAULT '',
  partition_no INT NOT NULL DEFAULT 0,
  offset_no BIGINT NOT NULL DEFAULT 0,
  processed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expire_at DATETIME NULL,
  UNIQUE KEY uk_event_consumer (event_id, consumer_name),
  KEY idx_consumer_processed (consumer_name, processed_at),
  KEY idx_expire_at (expire_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dead_letter_events (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  consumer_name VARCHAR(128) NOT NULL,
  topic VARCHAR(128) NOT NULL,
  partition_no INT NOT NULL,
  offset_no BIGINT NOT NULL,
  event_id VARCHAR(128) NOT NULL DEFAULT '',
  event_type VARCHAR(64) NOT NULL DEFAULT '',
  aggregate_type VARCHAR(64) NOT NULL DEFAULT '',
  aggregate_id VARCHAR(128) NOT NULL DEFAULT '',
  reason VARCHAR(1024) NOT NULL DEFAULT '',
  payload MEDIUMTEXT NOT NULL,
  headers JSON NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_consumer_message (consumer_name, topic, partition_no, offset_no),
  KEY idx_event_id (event_id),
  KEY idx_topic_created (topic, created_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
