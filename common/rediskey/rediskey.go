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

// TokenKey 当前有效 access token。
// 数据结构: STRING
// key: fsz:token:{userID}  value: JWT 字符串
// 用途: 单设备/单会话模型，同一用户新登录会覆盖旧 token；登出/踢下线时 DEL 即可。
func TokenKey(userID uint64) string {
	return fmt.Sprintf("%s:token:%d", prefix, userID)
}

// VerificationCodeKey 邮箱验证码。
// 数据结构: STRING
// key: fsz:verify:{email}  value: 6 位数字验证码
// 用途: 注册/找回密码流程写入，TTL 由 VerificationCodeTTL 控制（5 分钟）。
func VerificationCodeKey(email string) string {
	email = strings.ToLower(strings.TrimSpace(email))
	return fmt.Sprintf("%s:verify:%s", prefix, email)
}

// ProcessedEventKey Kafka 消费幂等标记。
// 数据结构: STRING
// key: fsz:processed:{eventID}:{consumerName}  value: 1
// 用途: 消费者提交后写入并设 7 天 TTL，作为 MySQL processed_events 表的短窗口快速前置拦截；
// MySQL 仍是权威幂等来源，Redis 只是加速。
func ProcessedEventKey(eventID string, consumerName string) string {
	return fmt.Sprintf("%s:processed:%s:%s", prefix, eventID, consumerName)
}

// OutboxDispatchLockKey outbox dispatcher 全局短锁。
// 数据结构: STRING
// key: fsz:outbox:dispatch:lock  value: 持锁实例标识
// 用途: 防止多个 outbox job 副本同时抢同一批 pending 事件造成重复投递。
func OutboxDispatchLockKey() string {
	return fmt.Sprintf("%s:outbox:dispatch:lock", prefix)
}

// JobLockKey 后台 job 分布式锁。
// 数据结构: STRING
// key: fsz:job:lock:{name}  value: 持锁实例标识
// 用途: 单实例执行的定时任务用它做互斥；name 建议使用 hotrank / timeline / stat-sync 等业务标识。
func JobLockKey(name string) string {
	name = strings.TrimSpace(name)
	return fmt.Sprintf("%s:job:lock:%s", prefix, name)
}

// JobLeaseSetKey 后台任务的并发租约集合。
// 数据结构: ZSET，member=租约 token，score=Redis 时间戳（毫秒）。
// 用途: 允许多个普通任务并发执行，同时让维护任务以互斥方式等待所有普通任务结束。
func JobLeaseSetKey(name string) string {
	name = strings.TrimSpace(name)
	return fmt.Sprintf("%s:job:leases:%s", prefix, name)
}
