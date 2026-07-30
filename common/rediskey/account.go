package rediskey

import (
	"fmt"
	"time"
)

const (
	// AccountPublicProfileCacheTTL 是公开用户资料缓存的基础有效期。
	// 写入时会按 userID 增加少量抖动，降低热点资料同时过期造成的缓存雪崩。
	AccountPublicProfileCacheTTL = 15 * time.Minute
	// AccountPublicProfileMissingTTL 是不存在用户的短期负缓存有效期，用于防止无效 ID 穿透 MySQL。
	AccountPublicProfileMissingTTL = time.Minute
)

// AccountPublicProfileVersionKey 公开用户资料缓存版本号。
// 数据结构: STRING (int64)
// key: fsz:account:profile:{userID}:version  value: 单调递增版本号
// 用途: 资料更新成功后 INCR；旧版本缓存无需扫描删除，等待 TTL 自动淘汰。
func AccountPublicProfileVersionKey(userID uint64) string {
	return fmt.Sprintf("%s:account:profile:%d:version", prefix, userID)
}

// AccountPublicProfileKey 公开用户资料缓存。
// 数据结构: STRING (JSON)
// key: fsz:account:profile:{userID}:v:{version}  value: {user_id, username, avatar_url, bio}
// 用途: account-rpc 读侧惰性回源；只允许存放公开字段，禁止写入邮箱等敏感信息。
func AccountPublicProfileKey(userID uint64, version int64) string {
	return fmt.Sprintf("%s:account:profile:%d:v:%d", prefix, userID, version)
}
