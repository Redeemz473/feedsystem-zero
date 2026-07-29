// Package notificationcache 集中封装通知模块的 Redis 缓存原语。
//
// 采用"版本号 + 惰性重算"方案（参考 rediskey.go 中 AccountPublicProfileVersionKey、
// FeedTimelineVersionKey 等既有实现）：任何可能改变用户未读数的入口（新增通知、
// 撤回通知、单条已读、全部已读）都只需要 INCR 该用户的 version key，
// 旧版本对应的缓存 key 自然失效，下一次读会触发 MySQL COUNT 并回写新版本的缓存。
//
// 相比"精确 DECR/INCR"，这种方案的好处是：
//  1. 写侧只有一种动作（INCR version），4 个入口写法一致，几乎不可能漏；
//  2. 天然并发安全，不存在旧快照覆盖新增量的竞态；
//  3. Redis 短暂不可用只会导致下一次读 miss 走 MySQL，不会污染缓存。
package notificationcache

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

// UnreadCounter 抽象出 GetUnreadCount / ListNotifications 侧真正回源计算未读数的能力。
// 通常由调用方注入一个走 MySQL COUNT 的闭包，避免本包直接依赖 gorm.DB。
type UnreadCounter func(ctx context.Context, userID uint64) (int64, error)

// bumpUnreadVersionScript 原子递增未读数缓存版本号，并顺带清理"上一版本"的缓存 key。
//
// KEYS[1] = version key（NotificationUnreadCountVersionKey）
// KEYS[2] = cache key 前缀（NotificationUnreadCountKey 去掉版本号后缀，含冒号）
//
// 步骤：
//  1. INCR version → 得到 newVersion；
//  2. 计算 oldVersion = newVersion - 1；若 oldVersion >= 0，DEL 掉旧版本对应的 cache key。
//     （version 从 0 起递增，oldVersion = 0 也对应真实存在过的缓存 key，需要删除。）
//
// 只删"上一版本"这一个 key 就够了：
//   - 由于每次 bump 都会 DEL，任何时刻最多只会残留"当前正在使用的版本"这一份缓存；
//   - 更早的历史版本在上一次 bump 时已经被删过，不会堆积。
const bumpUnreadVersionScript = `
local newVersion = redis.call("INCR", KEYS[1])
local oldVersion = newVersion - 1
if oldVersion >= 0 then
    redis.call("DEL", KEYS[2] .. tostring(oldVersion))
end
return newVersion
`

// BumpUnreadVersion 递增用户未读数缓存版本号，并同步删除上一版本的 unread cache key。
//
// 调用方约定：
//   - 必须在 MySQL 事务提交成功之后再调用；
//   - Redis 报错只打日志、绝不向上传播（MySQL 已经生效，不能因缓存失败回滚业务）；
//   - 只有当"未读数实际发生变化"时才应该调用，避免无谓的缓存失效。
//
// 说明：
//   - 高频写入场景（例如短时间内被大量点赞/关注）如果只 INCR 不 DEL，同一用户在 Redis 中会
//     残留多个仅靠 TTL 才能过期的 stale cache key，占用内存。
//   - Lua 脚本保证 "INCR + DEL 旧 cache" 原子执行，避免多个 bump 之间的竞态导致漏删。
//   - version key 本身无 TTL（极小 int64），成本可忽略；cache key 仍由 SET EX 兜底过期，
//     以应对本函数偶发失败或 Redis 主从切换时数据丢失的场景。
func BumpUnreadVersion(ctx context.Context, rdb *redis.Client, userID uint64) {
	if rdb == nil || userID == 0 {
		return
	}
	if _, err := rdb.Eval(
		ctx,
		bumpUnreadVersionScript,
		[]string{
			rediskey.NotificationUnreadCountVersionKey(userID),
			rediskey.NotificationUnreadCountKeyPrefix(userID),
		},
	).Result(); err != nil {
		logx.WithContext(ctx).Errorf(
			"bump notification unread version failed, user_id:%d error:%v",
			userID, err,
		)
	}
}

// LoadUnreadCount 读取用户未读通知数量，命中缓存直接返回，未命中走 counter 回源并回写。
//
// 流程：
//  1. GET version key（不存在按 0 处理）；
//  2. GET unread:{uid}:v:{version} 缓存 key，命中直接返回；
//  3. miss 时调用 counter 走 MySQL COUNT，然后 SET 缓存并加 TTL；
//     未读=0 使用较短的负缓存 TTL，长期无通知的用户不会每次读都回源。
//
// Redis 完全不可用时（version 读失败或 SET 失败）不会阻塞主流程，
// 直接透传 counter 的结果，保证功能可用性。
func LoadUnreadCount(
	ctx context.Context,
	rdb *redis.Client,
	userID uint64,
	counter UnreadCounter,
) (int64, error) {
	if counter == nil {
		return 0, errors.New("notificationcache: counter must not be nil")
	}
	if userID == 0 {
		return 0, nil
	}

	// Redis 缺失时退化为直接查 DB，避免通知模块对 Redis 形成硬依赖。
	if rdb == nil {
		return counter(ctx, userID)
	}

	versionKey := rediskey.NotificationUnreadCountVersionKey(userID)
	version, err := rdb.Get(ctx, versionKey).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		logx.WithContext(ctx).Errorf(
			"load unread version failed, user_id:%d error:%v",
			userID, err,
		)
		return counter(ctx, userID)
	}
	// version key 不存在时视为 0；bump 时 INCR 会自然把它变成 1，缓存 key 也会跟着换。

	cacheKey := rediskey.NotificationUnreadCountKey(userID, version)
	cached, err := rdb.Get(ctx, cacheKey).Int64()
	if err == nil {
		return cached, nil
	}
	if !errors.Is(err, redis.Nil) {
		logx.WithContext(ctx).Errorf(
			"load unread cache failed, user_id:%d version:%d error:%v",
			userID, version, err,
		)
		// 缓存读失败不影响正确性，继续走 DB 回源。
	}

	count, err := counter(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("count unread notifications failed, user_id:%d: %w", userID, err)
	}

	// 未读=0 使用短 TTL，避免长期无通知的用户占用 Redis 内存；
	// 有未读时用较长 TTL，命中率更高。
	ttl := rediskey.NotificationUnreadCountCacheTTL
	if count == 0 {
		ttl = rediskey.NotificationUnreadCountMissingTTL
	}
	if setErr := rdb.Set(ctx, cacheKey, strconv.FormatInt(count, 10), ttl).Err(); setErr != nil {
		// 回写失败仅打日志，本次结果照常返回；下次读会再次尝试回写。
		logx.WithContext(ctx).Errorf(
			"set unread cache failed, user_id:%d version:%d error:%v",
			userID, version, setErr,
		)
	}
	return count, nil
}
