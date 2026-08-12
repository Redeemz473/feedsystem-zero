CREATE TABLE IF NOT EXISTS accounts (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  username VARCHAR(64) NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  email VARCHAR(128) NOT NULL,
  refresh_token VARCHAR(255) NULL,
  avatar_url VARCHAR(512) DEFAULT '',
  bio VARCHAR(512) DEFAULT '',
  follower_count BIGINT NOT NULL DEFAULT 0 COMMENT '粉丝数，冗余自 follows 表，由 social 模块维护',
  following_count BIGINT NOT NULL DEFAULT 0 COMMENT '关注数，冗余自 follows 表，由 social 模块维护',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_username (username),
  UNIQUE KEY uk_email (email),
  UNIQUE KEY uk_refresh_token (refresh_token)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS videos (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  author_id BIGINT UNSIGNED NOT NULL,
  author_username VARCHAR(64) NOT NULL DEFAULT '',
  title VARCHAR(255) NOT NULL,
  description VARCHAR(1024) NOT NULL DEFAULT '',
  play_url VARCHAR(512) NOT NULL,
  cover_url VARCHAR(512) NOT NULL,
  request_id VARCHAR(128) NULL,
  likes_count BIGINT NOT NULL DEFAULT 0,
  comments_count BIGINT NOT NULL DEFAULT 0,
  popularity BIGINT NOT NULL DEFAULT 0,
  stats_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  status TINYINT NOT NULL DEFAULT 1,
  deleted_at DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_video_request (author_id, request_id),
  KEY idx_author_play_url (author_id, play_url),
  KEY idx_play_url_status (play_url, status, deleted_at),
  KEY idx_cover_url_status (cover_url, status, deleted_at),
  KEY idx_author_status_created (author_id, status, created_at, id),
  KEY idx_status_created_id (status, created_at, id),
  KEY idx_likes_id (likes_count, id),
  KEY idx_popularity_created_id (popularity, created_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS file_assets (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  file_hash VARCHAR(128) NOT NULL,
  file_type VARCHAR(32) NOT NULL DEFAULT 'video',
  url VARCHAR(512) NOT NULL,
  storage_path VARCHAR(1024) NOT NULL,
  size BIGINT NOT NULL DEFAULT 0,
  ref_count BIGINT NOT NULL DEFAULT 0,
  status TINYINT NOT NULL DEFAULT 1,
  deleted_at DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_file_hash (file_hash),
  UNIQUE KEY uk_url (url),
  KEY idx_status_ref (status, ref_count, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tags (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS video_tags (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  video_id BIGINT UNSIGNED NOT NULL,
  tag_id BIGINT UNSIGNED NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_video_tag (video_id, tag_id),
  KEY idx_tag_video (tag_id, video_id),
  KEY idx_video (video_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS likes (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  video_id BIGINT UNSIGNED NOT NULL,
  user_id BIGINT UNSIGNED NOT NULL,
  status TINYINT NOT NULL DEFAULT 1,
  deleted_at DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_video_user (video_id, user_id),
  KEY idx_user_status_created (user_id, status, created_at, video_id),
  KEY idx_user_status_updated (user_id, status, updated_at, id),
  KEY idx_video_status_created (video_id, status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS comments (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  video_id BIGINT UNSIGNED NOT NULL,
  user_id BIGINT UNSIGNED NOT NULL,
  username VARCHAR(64) NOT NULL DEFAULT '',
  content TEXT NOT NULL,
  request_id VARCHAR(128) NULL,
  status TINYINT NOT NULL DEFAULT 1,
  deleted_at DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_comment_request (user_id, request_id),
  KEY idx_video_status_created (video_id, status, created_at, id),
  KEY idx_video_status_deleted_created (video_id, status, deleted_at, created_at, id),
  KEY idx_user_status_created (user_id, status, created_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
  KEY idx_aggregate_status_id (aggregate_type, aggregate_id, status, id),
  KEY idx_status_sent_id (status, sent_at, id)
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
  KEY idx_expire_at (expire_at),
  KEY idx_expire_id (expire_at, id)
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
  KEY idx_topic_created (topic, created_at, id),
  KEY idx_created_id (created_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
