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
	// VideoStatsAuthTTL 是 Redis 权威互动统计的滑动过期时间。
	// 每次读写都会 EXPIRE 续期，热视频常驻内存；冷视频过期后由下次访问从 MySQL 冷备重建。
	VideoStatsAuthTTL = 7 * 24 * time.Hour
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
// 视频互动统计权威缓存（Redis 权威 + MySQL 冷备架构）
// ========================================

// VideoStatsAuthKey 视频互动统计的 Redis 权威计数。
// 数据结构: HASH
// key: fsz:video:stats:auth:{videoID}  fields: likes_count / comments_count / popularity
// 用途: 在线路径直接 HINCRBY 该 Hash 得到用户可见的最终计数；
// 读侧 HGetAll 直接返回；miss 时从 MySQL videos 冷备读取基准值 HSetNX 建立后再 HINCRBY 累加，
// 通过一段 Lua 脚本保证"冷启动 + 增量"原子执行，避免并发下基准值覆盖新增量。
// MySQL videos.{likes_count,comments_count,popularity} 由 interaction_sync job 异步维护，
// 仅作为 Redis 冷启动兜底，不再是读侧权威。
func VideoStatsAuthKey(videoID uint64) string {
	return fmt.Sprintf("%s:video:stats:auth:%d", prefix, videoID)
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

// LikeUserVideosFirstPageCacheKey 用户点赞视频固定首页窗口缓存。
// 数据结构: STRING (JSON)
// key: fsz:like:user:{userID}:videos:first:{version}
// value: 最多 20 条点赞关系及窗口外是否还有数据。
// 用途: 同一版本下所有 page_size<=20 的首页请求共用一份缓存；历史页和大页直接查询 MySQL。
func LikeUserVideosFirstPageCacheKey(userID uint64, version int64) string {
	return fmt.Sprintf("%s:like:user:%d:videos:first:%d", prefix, userID, version)
}

// LikeUserVideosFirstPageCacheBuildLockKey 用户点赞视频首页缓存构建锁。
// 数据结构: STRING
// key: fsz:like:user:videos:first:lock:{cacheKey}  value: 持锁实例标识
// 用途: 防止首页缓存 miss 时多副本并发回源 MySQL。
func LikeUserVideosFirstPageCacheBuildLockKey(cacheKey string) string {
	return fmt.Sprintf("%s:like:user:videos:first:lock:%s", prefix, cacheKey)
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
