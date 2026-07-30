// Package rediskey 通知模块未读数缓存（版本号 + 惰性重算）。
//
// 通知模块采用与用户资料 / Feed Timeline 完全一致的"版本号 + 缓存"范式：
//   - Version key 保存单调递增的 int64，任何可能改变未读数的入口（新增/撤回/单条已读/全部已读）
//     都只需要 INCR 它；旧 version 对应的缓存 key 永远不会再被读到，天然失效。
//   - Cache key 的名字里带 version，SET 时用 TTL 兜底回收；即使 Redis 短暂不可用，
//     下一次读会 miss 后走 MySQL COUNT 回源，功能不受影响。
package rediskey

import (
	"fmt"
	"strconv"
	"time"
)

const (
	// NotificationUnreadCountCacheTTL 是"未读数 > 0"时缓存 key 的有效期。
	// 版本号已经保证一致性，TTL 主要用于淘汰版本切换后遗留的旧 key。
	NotificationUnreadCountCacheTTL = 30 * time.Minute
	// NotificationUnreadCountMissingTTL 是"未读数 = 0"时的短 TTL。
	// 长期无通知的用户，缓存快速回收，避免占用 Redis 内存；
	// 有新通知时 INCR version 会立刻让这个空快照失效，不会导致数据滞后。
	NotificationUnreadCountMissingTTL = 5 * time.Minute
)

// NotificationUnreadCountVersionKey 用户未读数缓存版本号。
// 数据结构: STRING (int64)
// key: fsz:notification:unread:{userID}:version  value: 单调递增版本号
// 用途: 任何可能改变未读数的入口都要 INCR 一次——
//   - notification-job 新增未读通知；
//   - notification-job 撤回原本未读的通知；
//   - MarkNotificationRead 成功命中未读行；
//   - MarkAllNotificationsRead 有未读被批量置读。
//
// version key 本身不设 TTL：单条 int64 成本可忽略，且需要跨会话稳定递增。
func NotificationUnreadCountVersionKey(userID uint64) string {
	return fmt.Sprintf("%s:notification:unread:%d:version", prefix, userID)
}

// NotificationUnreadCountKey 用户当前版本下的未读数快照。
// 数据结构: STRING (int64)
// key: fsz:notification:unread:{userID}:v:{version}  value: 未读数
// 用途: 读侧先取 version 再拼此 key 去 GET，miss 时走 MySQL COUNT 并回写；
// bump 版本号时同时 DEL 上一版本 key，避免高频写入下 stale 缓存堆积到 TTL 才过期。
func NotificationUnreadCountKey(userID uint64, version int64) string {
	return NotificationUnreadCountKeyPrefix(userID) + strconv.FormatInt(version, 10)
}

// NotificationUnreadCountKeyPrefix 返回 NotificationUnreadCountKey 去掉版本号后的前缀（含末尾冒号）。
// 数据结构: STRING key 前缀（配合 Lua 脚本使用）
// key 前缀: fsz:notification:unread:{userID}:v:
// 用途: 供 Lua 脚本按 "prefix .. tostring(version)" 拼出旧版本 key 进行清理，
// 保证 Go 侧与 Lua 侧共用同一套命名规则，一处修改两处生效。
func NotificationUnreadCountKeyPrefix(userID uint64) string {
	return fmt.Sprintf("%s:notification:unread:%d:v:", prefix, userID)
}
