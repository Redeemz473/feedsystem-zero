ALTER TABLE videos
  ADD INDEX idx_play_url_status (play_url, status, deleted_at),
  ADD INDEX idx_cover_url_status (cover_url, status, deleted_at);
