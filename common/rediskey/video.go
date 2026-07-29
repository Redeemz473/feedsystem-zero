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

// VideoEntityKey 视频实体缓存，value 是 JSON 序列化的视频信息
// 格式: fsz:video:entity:{videoID}
func VideoEntityKey(videoID uint64) string {
	return fmt.Sprintf("%s:video:entity:%d", prefix, videoID)
}

// VideoEntityVersionKey 视频实体缓存版本号。
// 发布或删除状态成功提交后递增；缓存值只有携带相同版本才允许命中或回填。
// 格式: fsz:video:entity:{videoID}:version
func VideoEntityVersionKey(videoID uint64) string {
	return fmt.Sprintf("%s:video:entity:%d:version", prefix, videoID)
}

// VideoDetailKey 视频详情缓存（含作者信息和统计数据）
// 格式: fsz:video:detail:{videoID}
func VideoDetailKey(videoID uint64) string {
	return fmt.Sprintf("%s:video:detail:%d", prefix, videoID)
}

// ========================================
// 视频互动统计增量缓冲
// ========================================

// VideoLikeDeltaKey 视频点赞数增量缓冲，HASH，field=videoID，value=delta
// 格式: fsz:video:like_delta
func VideoLikeDeltaKey() string {
	return fmt.Sprintf("%s:video:like_delta", prefix)
}

// VideoCommentDeltaKey 视频评论数增量缓冲，HASH，field=videoID，value=delta
// 格式: fsz:video:comment_delta
func VideoCommentDeltaKey() string {
	return fmt.Sprintf("%s:video:comment_delta", prefix)
}

// VideoPopularityDeltaKey 视频热度增量缓冲，HASH，field=videoID，value=delta
// 格式: fsz:video:popularity_delta
func VideoPopularityDeltaKey() string {
	return fmt.Sprintf("%s:video:popularity_delta", prefix)
}

// VideoStatsCacheKey 视频互动统计缓存，HASH。
// fields: likes_count/comments_count/popularity/updated_at
// 格式: fsz:video:stats:{videoID}
func VideoStatsCacheKey(videoID uint64) string {
	return fmt.Sprintf("%s:video:stats:%d", prefix, videoID)
}

// InteractionDeltaPendingKey 标记某个互动事件的实时增量已经写入 Redis、尚未确认落库。
// consumer 只有看到该标记时才扣减对应增量，避免 Kafka 先消费、在线请求后写 Redis
// 导致计数被永久重复计算。
// 格式: fsz:interaction:delta:pending:{eventID}
func InteractionDeltaPendingKey(eventID string) string {
	return fmt.Sprintf("%s:interaction:delta:pending:%s", prefix, eventID)
}

// InteractionDeltaAckKey 标记某个互动事件已经完成 MySQL 聚合并处理过 Redis 增量。
// 在线请求若发现该标记，不再写入实时增量；consumer 重试时也不会重复扣减。
// 格式: fsz:interaction:delta:acked:{eventID}
func InteractionDeltaAckKey(eventID string) string {
	return fmt.Sprintf("%s:interaction:delta:acked:%s", prefix, eventID)
}

// HotVideoRealtimeKey 实时热榜，ZSET，score=实时热度，member=videoID
// 格式: fsz:hot:video:realtime
func HotVideoRealtimeKey() string {
	return fmt.Sprintf("%s:hot:video:realtime", prefix)
}

// ========================================
// 点赞
// ========================================

// LikeVideoUsersKey 视频点赞用户集合，SET，member=userID
// 格式: fsz:like:video:{videoID}:users
func LikeVideoUsersKey(videoID uint64) string {
	return fmt.Sprintf("%s:like:video:%d:users", prefix, videoID)
}

// LikeUserVideosKey 用户点赞视频集合，SET，member=videoID
// 格式: fsz:like:user:{userID}:videos
func LikeUserVideosKey(userID uint64) string {
	return fmt.Sprintf("%s:like:user:%d:videos", prefix, userID)
}

// LikeUserVideosListVersionKey 用户点赞列表版本号。
// 点赞/取消点赞成功后递增，分页缓存 key 带上版本号，避免返回旧列表。
// 格式: fsz:like:user:{userID}:videos:list:version
func LikeUserVideosListVersionKey(userID uint64) string {
	return fmt.Sprintf("%s:like:user:%d:videos:list:version", prefix, userID)
}

// LikeUserVideosPageCacheKey 用户点赞视频分页结果缓存。
// cursorCreatedAt 实际表示上一页最后一条 liked_at，沿用 proto 字段名 cursor_created_at。
// 格式: fsz:like:user:{userID}:videos:page:{version}:{cursorCreatedAt}:{cursorLikeID}:{pageSize}
func LikeUserVideosPageCacheKey(userID uint64, version int64, cursorCreatedAt int64, cursorLikeID uint64, pageSize int64) string {
	return fmt.Sprintf("%s:like:user:%d:videos:page:%d:%d:%d:%d", prefix, userID, version, cursorCreatedAt, cursorLikeID, pageSize)
}

// LikeUserVideosPageCacheBuildLockKey 用户点赞视频分页缓存构建锁。
// 格式: fsz:like:user:videos:page:lock:{cacheKey}
func LikeUserVideosPageCacheBuildLockKey(cacheKey string) string {
	return fmt.Sprintf("%s:like:user:videos:page:lock:%s", prefix, cacheKey)
}

// LikeStateKey 点赞状态覆盖缓存，value=1/0
// 用于解决 Redis 异步刷库期间，MySQL 旧状态和用户最新操作不一致的问题。
// 格式: fsz:like:state:{videoID}:{userID}
func LikeStateKey(videoID uint64, userID uint64) string {
	return fmt.Sprintf("%s:like:state:%d:%d", prefix, videoID, userID)
}

// LikeActionLockKey 单用户对单视频点赞/取消点赞的短锁。
// 用于避免用户快速连点导致同一时刻并发改状态。
// 格式: fsz:like:lock:{videoID}:{userID}
func LikeActionLockKey(videoID uint64, userID uint64) string {
	return fmt.Sprintf("%s:like:lock:%d:%d", prefix, videoID, userID)
}

// LikeEventStreamKey 点赞关系异步刷库日志，Redis Stream
// 字段建议: action=like/unlike, video_id, user_id, ts
// 格式: fsz:like:events
func LikeEventStreamKey() string {
	return fmt.Sprintf("%s:like:events", prefix)
}

// LikeEventFailStreamKey Kafka 短暂不可用时的本地失败回退 Stream。
// job 可以定时扫描该 Stream 重新投递 Kafka 或直接补偿处理。
// 格式: fsz:like:events:fail
func LikeEventFailStreamKey() string {
	return fmt.Sprintf("%s:like:events:fail", prefix)
}

// ========================================
// 评论
// ========================================

// CommentRateLimitKey 评论发布限流，value=1，短 TTL
// 格式: fsz:comment:rate:{userID}:{videoID}
func CommentRateLimitKey(userID uint64, videoID uint64) string {
	return fmt.Sprintf("%s:comment:rate:%d:%d", prefix, userID, videoID)
}

// CommentIdempotencyKey 评论发布幂等 Key，value=commentID。
// requestID 由 gateway 或前端生成，同一个 requestID 重试应返回同一条评论。
// 格式: fsz:comment:idempotency:{userID}:{requestID}
func CommentIdempotencyKey(userID uint64, requestID string) string {
	requestID = strings.TrimSpace(requestID)
	return fmt.Sprintf("%s:comment:idempotency:%d:%s", prefix, userID, requestID)
}

// CommentEventStreamKey 评论事件 Redis Stream，用于 Kafka 失败回退或本地开发。
// 格式: fsz:comment:events
func CommentEventStreamKey() string {
	return fmt.Sprintf("%s:comment:events", prefix)
}

// CommentEventFailStreamKey 评论事件失败回退 Stream。
// 格式: fsz:comment:events:fail
func CommentEventFailStreamKey() string {
	return fmt.Sprintf("%s:comment:events:fail", prefix)
}

// CommentFirstPageCacheKey 评论首页固定窗口缓存，value=JSON。
// version 来自 CommentListVersionKey；缓存固定保存前 N 条基础评论，
// 不包含访问者权限、请求 pageSize 对应的游标和 has_more。
// 格式: fsz:comment:first:{videoID}:{version}
func CommentFirstPageCacheKey(videoID uint64, version int64) string {
	return fmt.Sprintf("%s:comment:first:%d:%d", prefix, videoID, version)
}

// CommentFirstPageCacheBuildLockKey 评论首页缓存构建锁。
// 格式: fsz:comment:first:lock:{cacheKey}
func CommentFirstPageCacheBuildLockKey(cacheKey string) string {
	return fmt.Sprintf("%s:comment:first:lock:%s", prefix, cacheKey)
}

// CommentListVersionKey 评论列表版本号。
// 写评论/删评论成功后递增；首页缓存 key 和缓存值都携带该版本。
// 格式: fsz:comment:list:version:{videoID}
func CommentListVersionKey(videoID uint64) string {
	return fmt.Sprintf("%s:comment:list:version:%d", prefix, videoID)
}
