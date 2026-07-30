package rediskey

import (
	"fmt"
	"strings"
	"time"
)

const (
	// VideoEntityCacheTTL 是正常视频实体缓存的基础有效期，写入时会增加少量抖动。
	VideoEntityCacheTTL = 10 * time.Minute
	// VideoEntityMissingTTL 是不存在、已删除或已下架视频的短期负缓存有效期。
	VideoEntityMissingTTL = 30 * time.Second
)

// ========================================
// 视频实体缓存
// ========================================

// VideoEntityKey 视频实体缓存。
// 数据结构: STRING (JSON)
// key: fsz:video:entity:{videoID}  value: 视频实体 JSON
// 用途: video-rpc 读侧惰性回源，命中即返回；不存在/已下架视频写入负缓存防穿透。
func VideoEntityKey(videoID uint64) string {
	return fmt.Sprintf("%s:video:entity:%d", prefix, videoID)
}

// VideoEntityVersionKey 视频实体缓存版本号。
// 数据结构: STRING (int64)
// key: fsz:video:entity:{videoID}:version  value: 单调递增版本号
// 用途: 发布或删除/下架事务提交后 INCR；缓存值只有携带相同版本才允许命中或回填，
// 避免并发写入下旧快照覆盖新数据。
func VideoEntityVersionKey(videoID uint64) string {
	return fmt.Sprintf("%s:video:entity:%d:version", prefix, videoID)
}

// VideoDetailKey 视频详情缓存（含作者信息和统计数据）。
// 数据结构: STRING (JSON)
// key: fsz:video:detail:{videoID}  value: 视频详情 JSON（含 author、stats 快照）
// 用途: 详情接口聚合缓存，需要与 VideoStatsCacheKey 共同保证读一致性。
func VideoDetailKey(videoID uint64) string {
	return fmt.Sprintf("%s:video:detail:%d", prefix, videoID)
}

// ========================================
// 视频互动统计增量缓冲
// ========================================

// VideoLikeDeltaKey 视频点赞数增量缓冲。
// 数据结构: HASH
// key: fsz:video:like_delta  field=videoID, value=delta
// 用途: 在线点赞/取消点赞先原子更新此 delta，interaction_sync job 定期 flush 到 MySQL 后清零。
func VideoLikeDeltaKey() string {
	return fmt.Sprintf("%s:video:like_delta", prefix)
}

// VideoCommentDeltaKey 视频评论数增量缓冲。
// 数据结构: HASH
// key: fsz:video:comment_delta  field=videoID, value=delta
// 用途: 在线发/删评论先原子更新此 delta，interaction_sync job 定期 flush 到 MySQL 后清零。
func VideoCommentDeltaKey() string {
	return fmt.Sprintf("%s:video:comment_delta", prefix)
}

// VideoPopularityDeltaKey 视频热度增量缓冲。
// 数据结构: HASH
// key: fsz:video:popularity_delta  field=videoID, value=delta
// 用途: 在线互动按权重叠加热度分，hotrank job 消费后累加到分钟窗口 ZSET 并清零。
func VideoPopularityDeltaKey() string {
	return fmt.Sprintf("%s:video:popularity_delta", prefix)
}

// VideoStatsCacheKey 视频互动统计缓存。
// 数据结构: HASH
// key: fsz:video:stats:{videoID}  fields: likes_count / comments_count / popularity / updated_at
// 用途: 读侧展示实时统计；写侧由 interaction_sync 落库后回填，保证与 MySQL 最终一致。
func VideoStatsCacheKey(videoID uint64) string {
	return fmt.Sprintf("%s:video:stats:%d", prefix, videoID)
}

// InteractionDeltaPendingKey 标记某个互动事件的实时增量已写入 Redis、尚未确认落库。
// 数据结构: STRING
// key: fsz:interaction:delta:pending:{eventID}  value: 1
// 用途: consumer 只有看到该标记时才扣减对应增量；避免"Kafka 先消费 → 在线请求后写 Redis"
// 造成计数被永久重复计算。TTL 过期后 consumer 直接跳过扣减，保持逻辑闭环。
func InteractionDeltaPendingKey(eventID string) string {
	return fmt.Sprintf("%s:interaction:delta:pending:%s", prefix, eventID)
}

// InteractionDeltaAckKey 标记某个互动事件已完成 MySQL 聚合并处理过 Redis 增量。
// 数据结构: STRING
// key: fsz:interaction:delta:acked:{eventID}  value: 1
// 用途: 在线请求发现此标记时不再写实时增量，consumer 重试时也不会重复扣减，
// 与 pending 一起构成双标记幂等闭环。
func InteractionDeltaAckKey(eventID string) string {
	return fmt.Sprintf("%s:interaction:delta:acked:%s", prefix, eventID)
}

// HotVideoRealtimeKey 实时热榜。
// 数据结构: ZSET
// key: fsz:hot:video:realtime  score=实时热度, member=videoID
// 用途: interaction 在线更新，hotrank job 读取快照参与聚合。
func HotVideoRealtimeKey() string {
	return fmt.Sprintf("%s:hot:video:realtime", prefix)
}

// ========================================
// 点赞
// ========================================

// LikeVideoUsersKey 视频点赞用户集合。
// 数据结构: SET
// key: fsz:like:video:{videoID}:users  member=userID
// 用途: 判断用户是否对该视频点过赞；interaction_sync 落库时也用它做差集/幂等。
func LikeVideoUsersKey(videoID uint64) string {
	return fmt.Sprintf("%s:like:video:%d:users", prefix, videoID)
}

// LikeUserVideosKey 用户点赞视频集合。
// 数据结构: SET
// key: fsz:like:user:{userID}:videos  member=videoID
// 用途: 用户"我的点赞"列表构建来源；分页列表另有惰性版本号缓存。
func LikeUserVideosKey(userID uint64) string {
	return fmt.Sprintf("%s:like:user:%d:videos", prefix, userID)
}

// LikeUserVideosListVersionKey 用户点赞列表版本号。
// 数据结构: STRING (int64)
// key: fsz:like:user:{userID}:videos:list:version  value: 单调递增版本号
// 用途: 点赞/取消点赞成功后 INCR，分页缓存 key 带上版本号避免返回旧列表。
func LikeUserVideosListVersionKey(userID uint64) string {
	return fmt.Sprintf("%s:like:user:%d:videos:list:version", prefix, userID)
}

// LikeUserVideosPageCacheKey 用户点赞视频分页结果缓存。
// 数据结构: STRING (JSON)
// key: fsz:like:user:{userID}:videos:page:{version}:{cursorCreatedAt}:{cursorLikeID}:{pageSize}
// value: 该分页页数据 JSON
// 用途: cursorCreatedAt 实际表示上一页最后一条 liked_at，沿用 proto 字段名 cursor_created_at；
// 版本变化后旧 key 自然失效。
func LikeUserVideosPageCacheKey(userID uint64, version int64, cursorCreatedAt int64, cursorLikeID uint64, pageSize int64) string {
	return fmt.Sprintf("%s:like:user:%d:videos:page:%d:%d:%d:%d", prefix, userID, version, cursorCreatedAt, cursorLikeID, pageSize)
}

// LikeUserVideosPageCacheBuildLockKey 用户点赞视频分页缓存构建锁。
// 数据结构: STRING
// key: fsz:like:user:videos:page:lock:{cacheKey}  value: 持锁实例标识
// 用途: 防止分页缓存 miss 时多副本并发回源 MySQL。
func LikeUserVideosPageCacheBuildLockKey(cacheKey string) string {
	return fmt.Sprintf("%s:like:user:videos:page:lock:%s", prefix, cacheKey)
}

// LikeStateKey 点赞状态覆盖缓存。
// 数据结构: STRING
// key: fsz:like:state:{videoID}:{userID}  value: 1/0
// 用途: 解决 Redis 异步刷库期间，MySQL 旧状态与用户最新操作不一致；读侧优先看此覆盖值。
func LikeStateKey(videoID uint64, userID uint64) string {
	return fmt.Sprintf("%s:like:state:%d:%d", prefix, videoID, userID)
}

// LikeActionLockKey 单用户对单视频的点赞/取消点赞短锁。
// 数据结构: STRING
// key: fsz:like:lock:{videoID}:{userID}  value: 持锁实例标识
// 用途: 避免用户快速连点导致同一时刻并发改状态。
func LikeActionLockKey(videoID uint64, userID uint64) string {
	return fmt.Sprintf("%s:like:lock:%d:%d", prefix, videoID, userID)
}

// LikeEventStreamKey 点赞关系异步刷库日志。
// 数据结构: STREAM
// key: fsz:like:events  fields: action=like/unlike, video_id, user_id, ts
// 用途: 在线操作先入 Stream，interaction_sync job 消费后批量落 MySQL + 投递 Kafka。
func LikeEventStreamKey() string {
	return fmt.Sprintf("%s:like:events", prefix)
}

// LikeEventFailStreamKey 点赞事件本地失败回退 Stream。
// 数据结构: STREAM
// key: fsz:like:events:fail  fields: 同 LikeEventStreamKey
// 用途: Kafka 短暂不可用时的本地兜底，job 定时扫描重投 Kafka 或直接补偿处理。
func LikeEventFailStreamKey() string {
	return fmt.Sprintf("%s:like:events:fail", prefix)
}

// ========================================
// 评论
// ========================================

// CommentRateLimitKey 评论发布限流标记。
// 数据结构: STRING
// key: fsz:comment:rate:{userID}:{videoID}  value: 1
// 用途: 短 TTL 防刷，同一用户同一视频短时间内不允许连发。
func CommentRateLimitKey(userID uint64, videoID uint64) string {
	return fmt.Sprintf("%s:comment:rate:%d:%d", prefix, userID, videoID)
}

// CommentIdempotencyKey 评论发布幂等 Key。
// 数据结构: STRING
// key: fsz:comment:idempotency:{userID}:{requestID}  value: commentID
// 用途: requestID 由 gateway 或前端生成，同一 requestID 重试应返回同一条评论。
func CommentIdempotencyKey(userID uint64, requestID string) string {
	requestID = strings.TrimSpace(requestID)
	return fmt.Sprintf("%s:comment:idempotency:%d:%s", prefix, userID, requestID)
}

// CommentEventStreamKey 评论事件本地 Stream。
// 数据结构: STREAM
// key: fsz:comment:events  fields: action, comment_id, video_id, user_id, ts
// 用途: 用于 Kafka 失败回退或本地开发环境，逻辑同 LikeEventStreamKey。
func CommentEventStreamKey() string {
	return fmt.Sprintf("%s:comment:events", prefix)
}

// CommentEventFailStreamKey 评论事件失败回退 Stream。
// 数据结构: STREAM
// key: fsz:comment:events:fail  fields: 同 CommentEventStreamKey
// 用途: Kafka 短暂不可用时的本地兜底，job 定时扫描重投。
func CommentEventFailStreamKey() string {
	return fmt.Sprintf("%s:comment:events:fail", prefix)
}

// CommentFirstPageCacheKey 评论首页固定窗口缓存。
// 数据结构: STRING (JSON)
// key: fsz:comment:first:{videoID}:{version}  value: 前 N 条基础评论 JSON
// 用途: 只保存基础评论，不含访问者权限、请求 pageSize 对应的游标和 has_more，
// 这些需要在读取后动态计算。
func CommentFirstPageCacheKey(videoID uint64, version int64) string {
	return fmt.Sprintf("%s:comment:first:%d:%d", prefix, videoID, version)
}

// CommentFirstPageCacheBuildLockKey 评论首页缓存构建锁。
// 数据结构: STRING
// key: fsz:comment:first:lock:{cacheKey}  value: 持锁实例标识
// 用途: 防止首页缓存 miss 时多副本并发回源。
func CommentFirstPageCacheBuildLockKey(cacheKey string) string {
	return fmt.Sprintf("%s:comment:first:lock:%s", prefix, cacheKey)
}

// CommentListVersionKey 评论列表版本号。
// 数据结构: STRING (int64)
// key: fsz:comment:list:version:{videoID}  value: 单调递增版本号
// 用途: 写评论/删评论成功后 INCR；首页缓存 key 和缓存值都携带该版本号。
func CommentListVersionKey(videoID uint64) string {
	return fmt.Sprintf("%s:comment:list:version:%d", prefix, videoID)
}
