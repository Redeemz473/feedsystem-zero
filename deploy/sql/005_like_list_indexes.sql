-- 为“我的喜欢列表”补充游标分页索引。
-- ListMyLikedVideos 使用 user_id + status 过滤，并按 updated_at DESC, id DESC 翻页。
ALTER TABLE likes
  ADD INDEX idx_user_status_updated (user_id, status, updated_at, id);
