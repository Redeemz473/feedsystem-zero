-- 为评论列表游标分页补充更贴近查询条件的联合索引。
-- ListComments 使用 video_id + status + deleted_at 过滤，并按 created_at DESC, id DESC 翻页。
ALTER TABLE comments
  ADD INDEX idx_video_status_deleted_created (video_id, status, deleted_at, created_at, id);
