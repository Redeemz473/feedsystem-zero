// Package rediskey 集中托管本项目所有 Redis Key 和相关 TTL 常量。
//
// 约定：
//   - 所有 key 统一带 `fsz:` 前缀，避免与其他项目命名冲突；
//   - 每个业务模块拆到独立文件（account.go / video.go / social.go /
//     feed.go / hotrank.go / notification.go / chunkupload.go），
//     这里只保留跨模块共用的原语（登录 token、验证码、幂等标记、分布式锁等）；
//   - 涉及分页 / 列表缓存优先采用"版本号 + 惰性重算"范式，
//     写入方 INCR version、读取方带 version 命中即返回、miss 时回源 MySQL。
package rediskey

import (
	"fmt"
	"strings"
	"time"
)

// prefix 是所有 Redis Key 的统一命名空间前缀，供各模块的 key 函数拼接使用。
const prefix = "fsz"

// VerificationCodeTTL 邮箱验证码有效期，5 分钟。
const VerificationCodeTTL = 5 * time.Minute

// TokenKey 当前有效 access token，value 是 JWT 字符串。
// 当前采用单设备/单会话模型：同一用户新登录会覆盖旧 token。
// 格式: fsz:token:{userID}
func TokenKey(userID uint64) string {
	return fmt.Sprintf("%s:token:%d", prefix, userID)
}

// VerificationCodeKey 邮箱验证码，value 是 6 位验证码。
// TTL 通常设 5 分钟（参见 VerificationCodeTTL）。
// 格式: fsz:verify:{email}
func VerificationCodeKey(email string) string {
	email = strings.ToLower(strings.TrimSpace(email))
	return fmt.Sprintf("%s:verify:%s", prefix, email)
}

// ProcessedEventKey Kafka 消费幂等标记，value=1。
// TTL 建议 7 天，防止同一条消息被重复消费。
// 该 key 是 processed_events 表的补充：MySQL 是权威幂等来源，
// Redis 提供更快的短窗口拦截。
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
