# Interaction + Job Final Design

本项目的 interaction 模块按最终版设计，不走临时同步方案。

核心原则：

- gateway 只调用 interaction-rpc，不直接访问 Redis/MySQL/Kafka。
- interaction-rpc 是在线高并发入口，负责用户请求语义、Redis 实时状态、MySQL 核心记录、outbox 事件。
- job/worker 不被 gateway 同步调用，只消费 Kafka 或扫描 outbox，负责批处理、补偿、热榜、feed、通知。
- 核心关系表保证可恢复，派生状态允许最终一致。

## Runtime Flow

```text
Client
  |
  | HTTP + JWT
  v
gateway
  |
  | zrpc
  v
interaction-rpc
  |
  | Redis: 状态缓存/计数缓冲/限流/幂等
  | MySQL: likes/comments/interaction_events/outbox_events
  v
outbox_events
  |
  | job/outbox 投递
  v
Kafka topics
  |
  +--> like-sync-job       -> likes 表补偿 / processed_events
  +--> comment-sync-job    -> comments 补偿 / processed_events
  +--> video-stat-sync-job -> videos.likes_count/comments_count/popularity
  +--> hotrank-job         -> Redis hot rank
  +--> feed-timeline-job   -> Redis feed timeline
  +--> notification-job    -> notification 派生数据
```

## Like Write Path

```text
LikeVideo(user_id, video_id)
  1. 参数校验，确认 user_id/video_id 非 0
  2. 获取 rediskey.LikeActionLockKey(video_id,user_id) 短锁
  3. 读取 Redis like state
     - 已点赞：幂等返回 liked=true
     - 未命中：兜底查 MySQL likes(status=1)
  4. 更新 Redis:
     - SADD LikeVideoUsersKey(video_id) user_id
     - SADD LikeUserVideosKey(user_id) video_id
     - SET LikeStateKey(video_id,user_id)=1
     - HINCRBY VideoLikeDeltaKey video_id +1
     - HINCRBY VideoPopularityDeltaKey video_id +like_weight
     - ZINCRBY HotVideoRealtimeKey
  5. MySQL 事务:
     - upsert likes(status=1, deleted_at=NULL)
     - insert interaction_events
     - insert outbox_events(topic=interaction.like.events)
  6. 返回 liked=true 和实时 likes_count
```

UnlikeVideo 与 LikeVideo 对称：

```text
Redis:
  SREM like sets
  SET LikeStateKey=0
  HINCRBY like_delta -1
  HINCRBY popularity_delta -like_weight
MySQL:
  update likes set status=2, deleted_at=now()
  insert interaction_events
  insert outbox_events
```

## Comment Write Path

```text
PublishComment(user_id, video_id, content, request_id)
  1. 参数校验和评论限流 CommentRateLimitKey
  2. request_id 非空时先查 CommentIdempotencyKey
  3. MySQL 事务:
     - insert comments(status=1)
     - insert interaction_events
     - insert outbox_events(topic=interaction.comment.events)
  4. Redis:
     - DEL/版本递增 CommentListVersionKey(video_id)
     - HINCRBY VideoCommentDeltaKey video_id +1
     - HINCRBY VideoPopularityDeltaKey video_id +comment_weight
     - SET CommentIdempotencyKey=user comment_id
  5. 返回 comment
```

DeleteComment:

```text
1. 查 comment，校验 user_id 是评论作者
2. 软删除 comments(status=2, deleted_at=now)
3. 写 interaction_events/outbox_events
4. Redis 评论数和热度做负增量，评论列表版本递增
```

## Job Responsibility

job 不应该被 interaction 同步调用。

推荐 job：

- job/outbox：扫描 outbox_events(status=pending)，投递 Kafka，成功后 status=sent。
- job/stat-sync：消费 like/comment/stat delta，批量更新 videos 计数和热度。
- job/hotrank：消费互动事件，维护 Redis 热榜窗口。
- job/timeline：消费 video published/deleted，维护 feed timeline。
- job/notification：消费 like/comment/follow，生成通知。

每个 consumer 都必须写 processed_events：

```text
BEGIN
  INSERT processed_events(event_id, consumer_name)
  如果唯一键冲突：说明处理过，直接跳过
  执行业务副作用
COMMIT
提交 Kafka offset
```

## Kafka Topics

```text
interaction.like.events
interaction.comment.events
video.stat.delta.events
feed.video.events
notification.events
```

本地创建 topic：

```bash
cd ~/feedsystem-zero
./deploy/kafka/create_topics.sh
```

## Redis Keys

关键 key 在 `common/rediskey` 中统一维护，业务代码不要手写字符串。

```text
LikeVideoUsersKey
LikeUserVideosKey
LikeStateKey
LikeActionLockKey
VideoLikeDeltaKey
VideoCommentDeltaKey
VideoPopularityDeltaKey
VideoStatsCacheKey
HotVideoRealtimeKey
CommentRateLimitKey
CommentIdempotencyKey
CommentListCacheKey
CommentListVersionKey
OutboxDispatchLockKey
JobLockKey
ProcessedEventKey
```
