package rediskey

import (
	"fmt"
	"time"
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
	// FeedAuthorOutboxMaxLen 是大 V 作者 outbox ZSet 的最大保留条数。
	// 读侧需要 union 多份 outbox，因此单份长度取小于用户 Timeline 的值以控制归并成本。
	FeedAuthorOutboxMaxLen = 500
	// FeedAuthorOutboxTTL 是大 V outbox ZSet 的有效期。
	// 有事件到达或读侧访问时自动续期，长期无人访问的大 V outbox 会被 TTL 淘汰。
	FeedAuthorOutboxTTL = 30 * 24 * time.Hour
)

// ========================================
// 全局最新流
// ========================================

// FeedGlobalTimelineKey 全局最新视频时间线。
// 数据结构: ZSET
// key: fsz:feed:global_timeline  score=0, member=固定宽度的 publishedAt:videoID（字典序倒序读取）
// 用途: 推荐流 / 未登录首页数据源；写入方为视频发布事件消费者，读取方为 feed-rpc。
func FeedGlobalTimelineKey() string {
	return fmt.Sprintf("%s:feed:global_timeline", prefix)
}

// FeedGlobalTimelineVersionKey 全局 Timeline 版本号。
// 数据结构: STRING (int64)
// key: fsz:feed:global_timeline:version  value: 单调递增版本号
// 用途: 发布、删除或完整重建时 INCR，防止冷启动快照覆盖并发事件。
func FeedGlobalTimelineVersionKey() string {
	return fmt.Sprintf("%s:feed:global_timeline:version", prefix)
}

// FeedGlobalTimelineReadyKey 全局 Timeline 就绪标记。
// 数据结构: STRING
// key: fsz:feed:global_timeline:ready  value: 1
// 用途: 有此 key 才认为全局 Timeline 已完成一次完整构建，读侧可信任；
// 空 Timeline 也必须保留此标记，避免每次读都触发冷启动回源。
func FeedGlobalTimelineReadyKey() string {
	return fmt.Sprintf("%s:feed:global_timeline:ready", prefix)
}

// FeedGlobalTimelineBuildLockKey 全局 Timeline 冷启动构建锁。
// 数据结构: STRING
// key: fsz:feed:global_timeline:build:lock  value: 持锁实例标识
// 用途: 多副本同时冷启动时串行执行 MySQL 回源拼装，防击穿。
func FeedGlobalTimelineBuildLockKey() string {
	return fmt.Sprintf("%s:feed:global_timeline:build:lock", prefix)
}

// FeedGlobalTimelineTempKey 全局 Timeline 原子重建临时 ZSet。
// 数据结构: ZSET
// key: fsz:feed:global_timeline:tmp:{token}  score=0, member=publishedAt:videoID
// 用途: 冷启动先写临时 key，完成后 RENAME 为正式 key，实现原子替换。
func FeedGlobalTimelineTempKey(token string) string {
	return fmt.Sprintf("%s:feed:global_timeline:tmp:%s", prefix, token)
}

// FeedVideoTimelineMemberKey videoID 到复合 Timeline member 的映射。
// 数据结构: STRING
// key: fsz:feed:video:{videoID}:member  value: 定长的 publishedAt:videoID member 字符串
// 用途: 删除事件优先从 MySQL created_at 重建 member；
// 该映射用于数据被意外物理删除时兜底清理。
func FeedVideoTimelineMemberKey(videoID uint64) string {
	return fmt.Sprintf("%s:feed:video:%d:member", prefix, videoID)
}

// ========================================
// 用户关注流 Timeline（小 V 走 fanout 写入）
// ========================================

// FeedTimelineKey 单用户关注流 Timeline。
// 数据结构: ZSET
// key: fsz:feed:timeline:user:{userID}  score=0, member=固定宽度 publishedAt:videoID（字典序倒序读取）
// 用途: 写入方为 feed_fanout job（视频发布扇出 / 新关注回填），读取方为 feed-rpc；
// 单 key 最多保留 FeedTimelineMaxLen 条，超出走 ZREMRANGEBYRANK 裁剪。
func FeedTimelineKey(userID uint64) string {
	return fmt.Sprintf("%s:feed:timeline:user:%d", prefix, userID)
}

// FeedTimelineVersionKey 用户 Timeline 版本号。
// 数据结构: STRING (int64)
// key: fsz:feed:timeline:user:{userID}:version  value: 单调递增版本号
// 用途: 发布、删除、关注、取关等任何可能改变该用户 Timeline 的事件都会 INCR。
func FeedTimelineVersionKey(userID uint64) string {
	return fmt.Sprintf("%s:feed:timeline:user:%d:version", prefix, userID)
}

// FeedTimelineReadyKey 用户 Timeline 就绪标记。
// 数据结构: STRING
// key: fsz:feed:timeline:user:{userID}:ready  value: 1
// 用途: 表示已完成一次完整冷启动构建；空 Timeline 也必须保留该标记，
// 否则每次读取都会反复回源 MySQL。
func FeedTimelineReadyKey(userID uint64) string {
	return fmt.Sprintf("%s:feed:timeline:user:%d:ready", prefix, userID)
}

// FeedTimelineBuildLockKey 用户 Timeline 首次冷启动构建锁。
// 数据结构: STRING
// key: fsz:feed:timeline:user:{userID}:lock  value: 持锁实例标识
// 用途: 用户从未有过 Timeline（首次登录或长时间不活跃 TTL 到期）时，
// 避免多实例并发回源 MySQL 拼装。
func FeedTimelineBuildLockKey(userID uint64) string {
	return fmt.Sprintf("%s:feed:timeline:user:%d:lock", prefix, userID)
}

// FeedTimelineTempKey 用户 Timeline 原子重建临时 ZSet。
// 数据结构: ZSET
// key: fsz:feed:timeline:user:{userID}:tmp:{token}  score=0, member=publishedAt:videoID
// 用途: 冷启动先写临时 key，完成后 RENAME 为正式 key，实现原子替换。
func FeedTimelineTempKey(userID uint64, token string) string {
	return fmt.Sprintf("%s:feed:timeline:user:%d:tmp:%s", prefix, userID, token)
}

// ========================================
// 大 V outbox（推拉分离的"拉"侧）
// ========================================

// FeedBigVMarkKey 大 V 作者标记。
// 数据结构: SET
// key: fsz:feed:bigv:authors  member=authorID
// 用途: 关注数超过阈值的作者标记为大 V，发布视频时不走 push 扇出，读接口按需 pull 合并；
// 起步阶段可以先不使用，预留常量方便后续切换到推拉结合模式。
func FeedBigVMarkKey() string {
	return fmt.Sprintf("%s:feed:bigv:authors", prefix)
}

// FeedAuthorOutboxKey 大 V 作者发件箱 Timeline。
// 数据结构: ZSET
// key: fsz:feed:author:{authorID}:outbox  score=0, member=固定宽度 publishedAt:videoID（字典序倒序读取）
// 用途: 结构与用户 inbox 完全同构；只对大 V（accounts.is_big_v=1）维护，
// 标记位在 social 模块首次达到 BigCreatorFollowerThreshold 时置 1、只升不降，
// 天然避免阈值反向穿越导致历史视频消失。写入方为视频发布事件消费者，
// 读取方为 feed-rpc（把粉丝 inbox 与其关注的每个大 V outbox 归并）；
// 单 key 最多保留 FeedAuthorOutboxMaxLen 条，超出走 ZREMRANGEBYRANK 裁剪。
func FeedAuthorOutboxKey(authorID uint64) string {
	return fmt.Sprintf("%s:feed:author:%d:outbox", prefix, authorID)
}

// FeedAuthorOutboxVersionKey 大 V outbox 版本号。
// 数据结构: STRING (int64)
// key: fsz:feed:author:{authorID}:outbox:version  value: 单调递增版本号
// 用途: 发布、删除、状态变更等任何可能改变该作者 outbox 的事件都会 INCR；
// 冷启动构建时使用版本号避免覆盖并发写入。
func FeedAuthorOutboxVersionKey(authorID uint64) string {
	return fmt.Sprintf("%s:feed:author:%d:outbox:version", prefix, authorID)
}

// FeedAuthorOutboxReadyKey 大 V outbox 就绪标记。
// 数据结构: STRING
// key: fsz:feed:author:{authorID}:outbox:ready  value: 1
// 用途: 表示已完成一次完整冷启动构建；空 outbox 也必须保留该标记，
// 否则每次读取都会反复回源 MySQL。
func FeedAuthorOutboxReadyKey(authorID uint64) string {
	return fmt.Sprintf("%s:feed:author:%d:outbox:ready", prefix, authorID)
}

// FeedAuthorOutboxBuildLockKey 大 V outbox 首次冷启动构建锁。
// 数据结构: STRING
// key: fsz:feed:author:{authorID}:outbox:lock  value: 持锁实例标识
// 用途: 懒加载模式下，读侧首次访问该大 V 时可能有多实例并发触发构建，用此锁串行化。
func FeedAuthorOutboxBuildLockKey(authorID uint64) string {
	return fmt.Sprintf("%s:feed:author:%d:outbox:lock", prefix, authorID)
}

// FeedAuthorOutboxTempKey 大 V outbox 原子重建临时 ZSet。
// 数据结构: ZSET
// key: fsz:feed:author:{authorID}:outbox:tmp:{token}  score=0, member=publishedAt:videoID
// 用途: 冷启动先写临时 key，完成后 RENAME 为正式 key，实现原子替换。
func FeedAuthorOutboxTempKey(authorID uint64, token string) string {
	return fmt.Sprintf("%s:feed:author:%d:outbox:tmp:%s", prefix, authorID, token)
}
