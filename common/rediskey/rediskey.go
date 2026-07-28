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
	// AccountPublicProfileCacheTTL 是公开用户资料缓存的基础有效期。
	// 写入时会按 userID 增加少量抖动，降低热点资料同时过期造成的缓存雪崩。
	AccountPublicProfileCacheTTL = 15 * time.Minute
	// AccountPublicProfileMissingTTL 是不存在用户的短期负缓存有效期，用于防止无效 ID 穿透 MySQL。
	AccountPublicProfileMissingTTL = time.Minute
	// VideoEntityCacheTTL 是正常视频实体缓存的基础有效期，写入时会增加少量抖动。
	VideoEntityCacheTTL = 10 * time.Minute
	// VideoEntityMissingTTL 是不存在、已删除或已下架视频的短期负缓存有效期。
	VideoEntityMissingTTL = 30 * time.Second
)

const (
	// SocialFollowingStateTTL 是单条关注状态缓存的有效期。
	// value 使用 "1"/"0"，必须同时缓存未关注状态，避免不存在关系反复穿透 MySQL。
	SocialFollowingStateTTL = 10 * time.Minute
	// SocialListFirstPageCacheTTL 是粉丝/关注列表首页基础数据缓存的有效期。
	// 写缓存时建议在此基础上增加少量随机抖动，避免大量热点 key 同时失效。
	SocialListFirstPageCacheTTL = time.Minute
	// SocialListCacheBuildLockTTL 是列表首页缓存构建锁的有效期。
	// 锁只用于削峰防击穿，业务正确性仍由 MySQL 保证。
	SocialListCacheBuildLockTTL = 5 * time.Second
)

const (
	// FeedTimelineMaxLen 是单个用户关注流 Timeline ZSet 的最大保留条数。
	// 超过该长度会通过 ZREMRANGEBYRANK 从旧到新裁剪，防止单 key 无限膨胀。
	FeedTimelineMaxLen = 1000
	// FeedTimelineTTL 是单个用户关注流 Timeline ZSet 的有效期。
	// 活跃用户每次写入会自动续期，长期不活跃用户 TTL 到期后自动淘汰。
	FeedTimelineTTL = 7 * 24 * time.Hour
	// FeedTimelineBackfillLimit 是新关注一位作者时，回填该作者最近视频的最大条数。
	FeedTimelineBackfillLimit = 200
	// FeedTimelineBuildLockTTL 是 Timeline 首次冷启动构建锁的有效期。
	FeedTimelineBuildLockTTL = 10 * time.Second
)

// 账号模块
// AccountPublicProfileVersionKey 公开用户资料缓存版本号。
// 用户资料更新成功后递增版本；旧版本缓存无需扫描删除，等待 TTL 自动淘汰。
// 格式: fsz:account:profile:{userID}:version
func AccountPublicProfileVersionKey(userID uint64) string {
	return fmt.Sprintf("%s:account:profile:%d:version", prefix, userID)
}

// AccountPublicProfileKey 公开用户资料缓存，value 是 JSON。
// 缓存内容只能包含 user_id、username、avatar_url、bio，禁止存放邮箱等敏感字段。
// 格式: fsz:account:profile:{userID}:v:{version}
func AccountPublicProfileKey(userID uint64, version int64) string {
	return fmt.Sprintf("%s:account:profile:%d:v:%d", prefix, userID, version)
}

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

// ========================================
// 社交关系模块
// SocialFollowingStateKey 单向关注状态缓存，value=1/0。
// 格式: fsz:social:following:{followerID}:{followingID}
func SocialFollowingStateKey(followerID, followingID uint64) string {
	return fmt.Sprintf("%s:social:following:%d:%d", prefix, followerID, followingID)
}

// SocialFollowersListVersionKey 用户粉丝列表版本号。
// 关注/取关事务提交后，对被关注者 followingID 对应的版本执行 INCR。
// 固定首页缓存 key 带上版本号，旧版本缓存不主动扫描删除，等待 TTL 自动淘汰。
// 格式: fsz:social:user:{userID}:followers:list:version
func SocialFollowersListVersionKey(userID uint64) string {
	return fmt.Sprintf("%s:social:user:%d:followers:list:version", prefix, userID)
}

// SocialFollowersFirstPageCacheKey 用户粉丝列表固定首页窗口缓存，value=JSON。
// 同一版本只保存一份最多 50 条的基础关系，不区分访问者和请求 page_size。
// viewer_is_following、响应游标和请求级 has_more 必须在读取后动态计算。
// 格式: fsz:social:user:{userID}:followers:first:{version}
func SocialFollowersFirstPageCacheKey(userID uint64, version int64) string {
	return fmt.Sprintf("%s:social:user:%d:followers:first:%d", prefix, userID, version)
}

// SocialFollowersFirstPageCacheBuildLockKey 用户粉丝列表固定首页缓存构建锁。
// 格式: fsz:social:followers:first:lock:{cacheKey}
func SocialFollowersFirstPageCacheBuildLockKey(cacheKey string) string {
	return fmt.Sprintf("%s:social:followers:first:lock:%s", prefix, cacheKey)
}

// SocialFollowingsListVersionKey 用户关注列表版本号。
// 关注/取关事务提交后，对发起者 followerID 对应的版本执行 INCR。
// 固定首页缓存 key 带上版本号，旧版本缓存不主动扫描删除，等待 TTL 自动淘汰。
// 格式: fsz:social:user:{userID}:followings:list:version
func SocialFollowingsListVersionKey(userID uint64) string {
	return fmt.Sprintf("%s:social:user:%d:followings:list:version", prefix, userID)
}

// SocialFollowingsFirstPageCacheKey 用户关注列表固定首页窗口缓存，value=JSON。
// 同一版本只保存一份最多 50 条的基础关系，不区分访问者和请求 page_size。
// viewer_is_following、响应游标和请求级 has_more 必须在读取后动态计算。
// 格式: fsz:social:user:{userID}:followings:first:{version}
func SocialFollowingsFirstPageCacheKey(userID uint64, version int64) string {
	return fmt.Sprintf("%s:social:user:%d:followings:first:%d", prefix, userID, version)
}

// SocialFollowingsFirstPageCacheBuildLockKey 用户关注列表固定首页缓存构建锁。
// 格式: fsz:social:followings:first:lock:{cacheKey}
func SocialFollowingsFirstPageCacheBuildLockKey(cacheKey string) string {
	return fmt.Sprintf("%s:social:followings:first:lock:%s", prefix, cacheKey)
}

// ========================================
// Feed 模块
// FeedGlobalTimelineKey 全局最新视频时间线，ZSet。
// 所有元素 score=0，member=固定宽度的 publishedAt:videoID，按字典序倒序读取。
// 用于推荐流/未登录首页；写入方为视频发布事件消费者，读取方为 feed-rpc。
// 格式: fsz:feed:global_timeline
func FeedGlobalTimelineKey() string {
	return fmt.Sprintf("%s:feed:global_timeline", prefix)
}

// FeedGlobalTimelineVersionKey 全局 Timeline 版本号。
// 每次发布、删除或完整重建都会递增，防止冷启动快照覆盖并发事件。
func FeedGlobalTimelineVersionKey() string {
	return fmt.Sprintf("%s:feed:global_timeline:version", prefix)
}

// FeedGlobalTimelineReadyKey 表示全局 Timeline 已完成一次完整构建。
func FeedGlobalTimelineReadyKey() string {
	return fmt.Sprintf("%s:feed:global_timeline:ready", prefix)
}

// FeedGlobalTimelineBuildLockKey 全局 Timeline 冷启动构建锁。
func FeedGlobalTimelineBuildLockKey() string {
	return fmt.Sprintf("%s:feed:global_timeline:build:lock", prefix)
}

// FeedGlobalTimelineTempKey 全局 Timeline 原子重建时使用的临时 ZSet。
func FeedGlobalTimelineTempKey(token string) string {
	return fmt.Sprintf("%s:feed:global_timeline:tmp:%s", prefix, token)
}

// FeedVideoTimelineMemberKey 保存 videoID 对应的复合 Timeline member。
// 删除事件优先从 MySQL created_at 重建 member；该映射用于数据被意外物理删除时兜底清理。
func FeedVideoTimelineMemberKey(videoID uint64) string {
	return fmt.Sprintf("%s:feed:video:%d:member", prefix, videoID)
}

// FeedTimelineKey 单用户关注流 Timeline，ZSet。
// 所有元素 score=0，member=固定宽度的 publishedAt:videoID，按字典序倒序读取。
// 写入方为 feed_fanout job（视频发布扇出 / 新关注回填），读取方为 feed-rpc。
// 单 key 最多保留 FeedTimelineMaxLen 条，超出走 ZREMRANGEBYRANK 裁剪。
// 格式: fsz:feed:timeline:user:{userID}
func FeedTimelineKey(userID uint64) string {
	return fmt.Sprintf("%s:feed:timeline:user:%d", prefix, userID)
}

// FeedTimelineVersionKey 用户 Timeline 版本号。
// 发布、删除、关注、取关等任何可能改变该用户 Timeline 的事件都会递增。
func FeedTimelineVersionKey(userID uint64) string {
	return fmt.Sprintf("%s:feed:timeline:user:%d:version", prefix, userID)
}

// FeedTimelineReadyKey 表示用户 Timeline 已完成一次完整冷启动构建。
// 空 Timeline 也必须保留该标记，否则每次读取都会反复回源 MySQL。
func FeedTimelineReadyKey(userID uint64) string {
	return fmt.Sprintf("%s:feed:timeline:user:%d:ready", prefix, userID)
}

// FeedTimelineBuildLockKey 用户 Timeline 首次冷启动构建锁。
// 用户从未有过 Timeline（首次登录或长时间不活跃 TTL 到期）时，避免多实例并发回源 MySQL 拼装。
// 格式: fsz:feed:timeline:user:{userID}:lock
func FeedTimelineBuildLockKey(userID uint64) string {
	return fmt.Sprintf("%s:feed:timeline:user:%d:lock", prefix, userID)
}

// FeedTimelineTempKey 用户 Timeline 原子重建时使用的临时 ZSet。
func FeedTimelineTempKey(userID uint64, token string) string {
	return fmt.Sprintf("%s:feed:timeline:user:%d:tmp:%s", prefix, userID, token)
}

// FeedBigVMarkKey 大 V 作者标记，SET，member=authorID。
// 关注数超过阈值的作者标记为大 V，发布视频时不走 push 扇出，读接口按需 pull 合并。
// 起步阶段可以先不使用；预留常量方便后续切换到推拉结合模式。
// 格式: fsz:feed:bigv:authors
func FeedBigVMarkKey() string {
	return fmt.Sprintf("%s:feed:bigv:authors", prefix)
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
// 分片上传
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
