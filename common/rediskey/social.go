package rediskey

import (
	"fmt"
	"time"
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

// SocialFollowingStateKey 单向关注状态缓存。
// 数据结构: STRING
// key: fsz:social:following:{followerID}:{followingID}  value: 1/0
// 用途: IsFollowing / BatchIsFollowing 优先命中此 key；未关注状态也需缓存，防止穿透 MySQL。
func SocialFollowingStateKey(followerID, followingID uint64) string {
	return fmt.Sprintf("%s:social:following:%d:%d", prefix, followerID, followingID)
}

// SocialFollowersListVersionKey 用户粉丝列表版本号。
// 数据结构: STRING (int64)
// key: fsz:social:user:{userID}:followers:list:version  value: 单调递增版本号
// 用途: 关注/取关事务提交后，对被关注者 followingID 对应的版本 INCR；
// 旧版本的首页缓存 key 无需扫描删除，随 TTL 自动淘汰。
func SocialFollowersListVersionKey(userID uint64) string {
	return fmt.Sprintf("%s:social:user:%d:followers:list:version", prefix, userID)
}

// SocialFollowersFirstPageCacheKey 用户粉丝列表固定首页窗口缓存。
// 数据结构: STRING (JSON)
// key: fsz:social:user:{userID}:followers:first:{version}  value: 最多 50 条基础关系数组
// 用途: 同一版本只保存一份基础数据，不区分访问者和请求 page_size；
// viewer_is_following、响应游标和请求级 has_more 必须在读取后动态计算。
func SocialFollowersFirstPageCacheKey(userID uint64, version int64) string {
	return fmt.Sprintf("%s:social:user:%d:followers:first:%d", prefix, userID, version)
}

// SocialFollowersFirstPageCacheBuildLockKey 用户粉丝列表首页缓存构建锁。
// 数据结构: STRING
// key: fsz:social:followers:first:lock:{cacheKey}  value: 持锁实例标识
// 用途: 缓存 miss 时多副本并发构建的削峰锁，防止缓存击穿。
func SocialFollowersFirstPageCacheBuildLockKey(cacheKey string) string {
	return fmt.Sprintf("%s:social:followers:first:lock:%s", prefix, cacheKey)
}

// SocialFollowingsListVersionKey 用户关注列表版本号。
// 数据结构: STRING (int64)
// key: fsz:social:user:{userID}:followings:list:version  value: 单调递增版本号
// 用途: 关注/取关事务提交后，对发起者 followerID 对应的版本 INCR；
// 旧版本的首页缓存 key 无需扫描删除，随 TTL 自动淘汰。
func SocialFollowingsListVersionKey(userID uint64) string {
	return fmt.Sprintf("%s:social:user:%d:followings:list:version", prefix, userID)
}

// SocialFollowingsFirstPageCacheKey 用户关注列表固定首页窗口缓存。
// 数据结构: STRING (JSON)
// key: fsz:social:user:{userID}:followings:first:{version}  value: 最多 50 条基础关系数组
// 用途: 同一版本只保存一份基础数据，不区分访问者和请求 page_size；
// viewer_is_following、响应游标和请求级 has_more 必须在读取后动态计算。
func SocialFollowingsFirstPageCacheKey(userID uint64, version int64) string {
	return fmt.Sprintf("%s:social:user:%d:followings:first:%d", prefix, userID, version)
}

// SocialFollowingsFirstPageCacheBuildLockKey 用户关注列表首页缓存构建锁。
// 数据结构: STRING
// key: fsz:social:followings:first:lock:{cacheKey}  value: 持锁实例标识
// 用途: 缓存 miss 时多副本并发构建的削峰锁，防止缓存击穿。
func SocialFollowingsFirstPageCacheBuildLockKey(cacheKey string) string {
	return fmt.Sprintf("%s:social:followings:first:lock:%s", prefix, cacheKey)
}
