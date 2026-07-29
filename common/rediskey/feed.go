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

// ========================================
// 用户关注流 Timeline（小 V 走 fanout 写入）
// ========================================

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

// ========================================
// 大 V outbox（推拉分离的"拉"侧）
// ========================================

// FeedBigVMarkKey 大 V 作者标记，SET，member=authorID。
// 关注数超过阈值的作者标记为大 V，发布视频时不走 push 扇出，读接口按需 pull 合并。
// 起步阶段可以先不使用；预留常量方便后续切换到推拉结合模式。
// 格式: fsz:feed:bigv:authors
func FeedBigVMarkKey() string {
	return fmt.Sprintf("%s:feed:bigv:authors", prefix)
}

// FeedAuthorOutboxKey 大 V 作者发件箱 Timeline，ZSet。
// 结构与用户 inbox 完全同构：score=0，member 为固定宽度的 publishedAt:videoID，按字典序倒序读取。
// 只对大 V（accounts.is_big_v = 1）维护；该标记位在 social 模块首次达到
// BigCreatorFollowerThreshold 时置 1，只升不降，能天然避免阈值反向穿越导致的历史视频消失。
// 写入方：视频发布事件消费者判定为大 V 时写入自己的 outbox，跳过对粉丝的 fanout；
// 读取方：feed-rpc 在关注流中把粉丝的 inbox 与其关注的每个大 V 的 outbox 归并合并。
// 单 key 最多保留 FeedAuthorOutboxMaxLen 条，超出走 ZREMRANGEBYRANK 裁剪。
// 格式: fsz:feed:author:{authorID}:outbox
func FeedAuthorOutboxKey(authorID uint64) string {
	return fmt.Sprintf("%s:feed:author:%d:outbox", prefix, authorID)
}

// FeedAuthorOutboxVersionKey 大 V outbox 版本号。
// 发布、删除、状态变更等任何可能改变该作者 outbox 的事件都会递增；
// 冷启动构建时使用版本号避免覆盖并发写入。
// 格式: fsz:feed:author:{authorID}:outbox:version
func FeedAuthorOutboxVersionKey(authorID uint64) string {
	return fmt.Sprintf("%s:feed:author:%d:outbox:version", prefix, authorID)
}

// FeedAuthorOutboxReadyKey 表示大 V outbox 已完成一次完整冷启动构建。
// 空 outbox 也必须保留该标记，否则每次读取都会反复回源 MySQL。
// 格式: fsz:feed:author:{authorID}:outbox:ready
func FeedAuthorOutboxReadyKey(authorID uint64) string {
	return fmt.Sprintf("%s:feed:author:%d:outbox:ready", prefix, authorID)
}

// FeedAuthorOutboxBuildLockKey 大 V outbox 首次冷启动构建锁。
// 懒加载模式下，读侧首次访问该大 V 时可能有多实例并发触发构建，用此锁串行化。
// 格式: fsz:feed:author:{authorID}:outbox:lock
func FeedAuthorOutboxBuildLockKey(authorID uint64) string {
	return fmt.Sprintf("%s:feed:author:%d:outbox:lock", prefix, authorID)
}

// FeedAuthorOutboxTempKey 大 V outbox 原子重建时使用的临时 ZSet。
// 格式: fsz:feed:author:{authorID}:outbox:tmp:{token}
func FeedAuthorOutboxTempKey(authorID uint64, token string) string {
	return fmt.Sprintf("%s:feed:author:%d:outbox:tmp:%s", prefix, authorID, token)
}
