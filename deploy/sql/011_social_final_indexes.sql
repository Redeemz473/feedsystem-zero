-- Social 最终版分页索引。
-- 关注关系采用软删除并允许重新关注；重新关注会更新 updated_at，
-- 因此粉丝/关注列表按 updated_at DESC, id DESC 做稳定游标分页。
ALTER TABLE follows
  DROP INDEX idx_following,
  DROP INDEX idx_follower,
  ADD INDEX idx_following_status_updated (following_id, status, updated_at, id),
  ADD INDEX idx_follower_status_updated (follower_id, status, updated_at, id);
