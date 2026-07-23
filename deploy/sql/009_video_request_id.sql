ALTER TABLE videos
  ADD COLUMN request_id VARCHAR(128) NULL AFTER cover_url,
  ADD UNIQUE KEY uk_video_request (author_id, request_id);
