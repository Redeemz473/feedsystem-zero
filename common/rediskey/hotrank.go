package rediskey

import "fmt"

// HotVideoWindowKey 单分钟热度窗口。
// 数据结构: ZSET
// key: fsz:hot:window:{minute}  score=当分钟内累计热度分, member=videoID
// 用途: interaction 每次点赞/评论按 UTC yyyyMMddHHmm 分钟粒度累加；hotrank job 定时聚合最近 N 分钟。
func HotVideoWindowKey(minute string) string {
	return fmt.Sprintf("%s:hot:window:%s", prefix, minute)
}

// HotVideoMergeKey 合并最近 N 分钟热度后的聚合快照。
// 数据结构: ZSET
// key: fsz:hot:merge:{asOf}  score=聚合热度分, member=videoID
// 用途: hotrank job 用 ZUNIONSTORE 生成，feed-rpc GetHotFeed 读取此快照分页。
func HotVideoMergeKey(asOf string) string {
	return fmt.Sprintf("%s:hot:merge:%s", prefix, asOf)
}

// HotVideoMergeReadyKey 热榜快照构建完成标记。
// 数据结构: STRING (int64)
// key: fsz:hot:merge:{asOf}:ready  value: 快照成员数量
// 用途: value=0 表示已构建成功但窗口内无正分视频，不能被误判为缓存未命中；有此 key 就代表本轮快照可读。
func HotVideoMergeReadyKey(asOf string) string {
	return fmt.Sprintf("%s:hot:merge:%s:ready", prefix, asOf)
}

// HotVideoMergeBuildLockKey 热榜快照分布式构建锁。
// 数据结构: STRING
// key: fsz:hot:merge:{asOf}:lock  value: 持锁 job 实例标识
// 用途: 多副本 hotrank job 抢锁，串行执行同一 asOf 的构建，避免重复计算与临时 key 冲突。
func HotVideoMergeBuildLockKey(asOf string) string {
	return fmt.Sprintf("%s:hot:merge:%s:lock", prefix, asOf)
}

// HotVideoMergeTempKey 热榜快照构建临时 ZSet。
// 数据结构: ZSET
// key: fsz:hot:merge:{asOf}:tmp:{token}  score=聚合热度分, member=videoID
// 用途: hotrank job 先写临时 key，完成后 RENAME 为正式快照，实现原子替换。
func HotVideoMergeTempKey(asOf string, token string) string {
	return fmt.Sprintf("%s:hot:merge:%s:tmp:%s", prefix, asOf, token)
}

// GatewayAnonymousHotFeedPageCacheKey 游客热榜完整视频卡片的短 TTL 成品缓存。
// snapshotKey 对首页请求是当前 UTC 分钟 Unix 秒，对历史翻页是客户端携带的 snapshot_at。
// 登录用户包含个性化 is_liked，不允许使用此共享缓存。
func GatewayAnonymousHotFeedPageCacheKey(snapshotKey int64, offset int64, pageSize int64) string {
	return fmt.Sprintf(
		"%s:gateway:hot:anonymous:v1:%d:%d:%d",
		prefix,
		snapshotKey,
		offset,
		pageSize,
	)
}

// GatewayAnonymousHotFeedPageBuildLockKey 跨 Gateway 实例合并同一热榜页的缓存回源。
func GatewayAnonymousHotFeedPageBuildLockKey(cacheKey string) string {
	return cacheKey + ":lock"
}
