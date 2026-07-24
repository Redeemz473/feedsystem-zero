package rediskey

import (
	"fmt"
	"strings"
	"time"
)

// ========================================
// 通用前缀，所有 key 统一命名空间，防止和其他项目冲突
// ========================================
const prefix = "fsz"

const VerificationCodeTTL = 5 * time.Minute

const (
	// SocialFollowingStateTTL 是单条关注状态缓存的有效期。
	// value 使用 "1"/"0"，必须同时缓存未关注状态，避免不存在关系反复穿透 MySQL。
	SocialFollowingStateTTL = 10 * time.Minute
	// SocialFollowStatsTTL 是粉丝数/关注数缓存的有效期。
	SocialFollowStatsTTL = 5 * time.Minute
)

// 账号模块
// TokenKey 当前有效 access token，value 是 JWT 字符串。
// 当前采用单设备/单会话模型：同一用户新登录会覆盖旧 token。
// 格式: fsz:token:{userID}
func TokenKey(userID uint64) string {
	return fmt.Sprintf("%s:token:%d", prefix, userID)
}

// VerificationCodeKey 邮箱验证码，value 是 6 位验证码
// TTL 通常设 5 分钟
// 格式: fsz:verify:{email}
func VerificationCodeKey(email string) string {
	email = strings.ToLower(strings.TrimSpace(email))
	return fmt.Sprintf("%s:verify:%s", prefix, email)
}

// ========================================
// 视频模块
// VideoEntityKey 视频实体缓存，value 是 JSON 序列化的视频信息
// 格式: fsz:video:entity:{videoID}
func VideoEntityKey(videoID uint64) string {
	return fmt.Sprintf("%s:video:entity:%d", prefix, videoID)
}

// VideoDetailKey 视频详情缓存（含作者信息和统计数据）
// 格式: fsz:video:detail:{videoID}
func VideoDetailKey(videoID uint64) string {
	return fmt.Sprintf("%s:video:detail:%d", prefix, videoID)
}

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

// HotVideoRealtimeKey 实时热榜，ZSET，score=实时热度，member=videoID
// 格式: fsz:hot:video:realtime
func HotVideoRealtimeKey() string {
	return fmt.Sprintf("%s:hot:video:realtime", prefix)
}

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

// CommentListCacheKey 评论列表分页缓存，value=JSON。
// version 来自 CommentListVersionKey，写评论/删评论时只递增版本号，旧分页缓存靠 TTL 自动淘汰。
// cursor 通常是 "created_at:{cursorCreatedAt}:comment_id:{cursorCommentID}"，首页 cursor 两项均为 0。
// 格式: fsz:comment:list:{videoID}:{version}:{cursor}:{limit}
func CommentListCacheKey(videoID uint64, version int64, cursor string, limit int64) string {
	return fmt.Sprintf("%s:comment:list:%d:%d:%s:%d", prefix, videoID, version, cursor, limit)
}

// CommentListCacheBuildLockKey 评论列表缓存构建锁，防止热点 key 失效时大量请求同时回源 MySQL。
// 格式: fsz:comment:list:lock:{cacheKey}
func CommentListCacheBuildLockKey(cacheKey string) string {
	return fmt.Sprintf("%s:comment:list:lock:%s", prefix, cacheKey)
}

// CommentListVersionKey 评论列表版本号。
// 写评论/删评论时递增版本，列表缓存可把版本放进 cursor key 或响应里。
// 格式: fsz:comment:list:version:{videoID}
func CommentListVersionKey(videoID uint64) string {
	return fmt.Sprintf("%s:comment:list:version:%d", prefix, videoID)
}

// ========================================
// 社交关系模块
// SocialFollowingStateKey 单向关注状态缓存，value=1/0。
// 格式: fsz:social:following:{followerID}:{followingID}
func SocialFollowingStateKey(followerID, followingID uint64) string {
	return fmt.Sprintf("%s:social:following:%d:%d", prefix, followerID, followingID)
}

// SocialFollowStatsKey 用户粉丝数与关注数缓存，HASH。
// fields: followers_count/followings_count
// 格式: fsz:social:stats:{userID}
func SocialFollowStatsKey(userID uint64) string {
	return fmt.Sprintf("%s:social:stats:%d", prefix, userID)
}

// ========================================
// Feed 模块
// FeedGlobalTimelineKey 全局最新视频时间线，ZSet，score=发布时间，member=videoID
// 格式: fsz:feed:global_timeline
func FeedGlobalTimelineKey() string {
	return fmt.Sprintf("%s:feed:global_timeline", prefix)
}

// FeedInboxKey 用户收件箱时间线（关注流），ZSet，score=发布时间，member=videoID
// 格式: fsz:feed:inbox:{userID}
func FeedInboxKey(userID uint64) string {
	return fmt.Sprintf("%s:feed:inbox:%d", prefix, userID)
}

// ========================================
// 热榜模块
// HotVideoWindowKey 单分钟热度窗口，ZSet，score=热度分，member=videoID
// minute 格式: yyyyMMddHHmm
// 格式: fsz:hot:window:{minute}
func HotVideoWindowKey(minute string) string {
	return fmt.Sprintf("%s:hot:window:%s", prefix, minute)
}

// HotVideoMergeKey 合并最近 N 分钟热度后的聚合 ZSet
// 格式: fsz:hot:merge:{asOf}
func HotVideoMergeKey(asOf string) string {
	return fmt.Sprintf("%s:hot:merge:%s", prefix, asOf)
}

// ========================================
// 幂等 / 去重
// ProcessedEventKey Kafka 消费幂等标记，value=1
// TTL 建议 7 天，防止同一条消息被重复消费
// 格式: fsz:processed:{eventID}:{consumerName}
func ProcessedEventKey(eventID string, consumerName string) string {
	return fmt.Sprintf("%s:processed:%s:%s", prefix, eventID, consumerName)
}

// OutboxDispatchLockKey outbox dispatcher 全局短锁。
// 防止多个 dispatcher 同时抢同一批 pending 事件。
// 格式: fsz:outbox:dispatch:lock
func OutboxDispatchLockKey() string {
	return fmt.Sprintf("%s:outbox:dispatch:lock", prefix)
}

// JobLockKey 后台 job 分布式锁，name 建议使用 hotrank/timeline/stat-sync 等。
// 格式: fsz:job:lock:{name}
func JobLockKey(name string) string {
	name = strings.TrimSpace(name)
	return fmt.Sprintf("%s:job:lock:%s", prefix, name)
}

// ========================================
// 分片上传（预留，后面会用到）
// ChunkUploadKey 分片上传会话状态，建议用 SET 保存已上传分片索引。
// 格式: fsz:chunk:upload:{uploadID}
func ChunkUploadKey(uploadID string) string {
	return fmt.Sprintf("%s:chunk:upload:%s", prefix, uploadID)
}

// ChunkUploadMetaKey 分片上传元数据，建议用 HASH 保存 user_id/file_hash/file_size/chunk_size/total_chunks/final_ext。
// 格式: fsz:chunk:meta:{uploadID}
func ChunkUploadMetaKey(uploadID string) string {
	return fmt.Sprintf("%s:chunk:meta:%s", prefix, uploadID)
}

// ChunkUploadLockKey 分片合并锁，防止 complete 接口被重复并发调用。
// 格式: fsz:chunk:lock:{uploadID}
func ChunkUploadLockKey(uploadID string) string {
	return fmt.Sprintf("%s:chunk:lock:%s", prefix, uploadID)
}

// ChunkUploadSessionKey 用户未完成上传会话索引，value=uploadID。
// 用于前端刷新页面后，只凭 userID + fileHash 找回未完成上传，支持断点续传。
// 格式: fsz:chunk:session:{userID}:{fileHash}
func ChunkUploadSessionKey(userID uint64, fileHash string) string {
	return fmt.Sprintf("%s:chunk:session:%d:%s", prefix, userID, fileHash)
}

// ChunkUploadHashKey 文件秒传标记，value 建议保存 play_url。
// 格式: fsz:chunk:hash:{userID}:{fileHash}
func ChunkUploadHashKey(userID uint64, fileHash string) string {
	return fmt.Sprintf("%s:chunk:hash:%d:%s", prefix, userID, fileHash)
}

// ChunkUploadGlobalHashKey 全局文件秒传标记，value 建议保存 play_url。
// 如果希望不同用户上传同一文件也能秒传，可用这个 key；如果只允许用户维度秒传，用 ChunkUploadHashKey。
// 格式: fsz:chunk:global_hash:{fileHash}
func ChunkUploadGlobalHashKey(fileHash string) string {
	return fmt.Sprintf("%s:chunk:global_hash:%s", prefix, fileHash)
}
