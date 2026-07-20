-- interaction_sync 死信表：
-- 1. 单条 Kafka 脏消息写入死信后允许提交 offset，避免阻塞整个分区；
-- 2. Flush RPC 多次部分失败后写入死信，后续可人工排查或做补偿重放。
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
