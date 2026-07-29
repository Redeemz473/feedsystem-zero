package rediskey

import "fmt"

// HotVideoWindowKey 单分钟热度窗口，ZSet，score=热度分，member=videoID。
// minute 必须使用 UTC 时间，格式: yyyyMMddHHmm。
// 格式: fsz:hot:window:{minute}
func HotVideoWindowKey(minute string) string {
	return fmt.Sprintf("%s:hot:window:%s", prefix, minute)
}

// HotVideoMergeKey 合并最近 N 分钟热度后的聚合 ZSet
// 格式: fsz:hot:merge:{asOf}
func HotVideoMergeKey(asOf string) string {
	return fmt.Sprintf("%s:hot:merge:%s", prefix, asOf)
}

// HotVideoMergeReadyKey 热榜快照构建完成标记，value=快照成员数量。
// value=0 表示已经成功构建，只是窗口内没有正分视频，不能当成缓存未命中。
// 格式: fsz:hot:merge:{asOf}:ready
func HotVideoMergeReadyKey(asOf string) string {
	return fmt.Sprintf("%s:hot:merge:%s:ready", prefix, asOf)
}

// HotVideoMergeBuildLockKey 热榜快照分布式构建锁。
// 格式: fsz:hot:merge:{asOf}:lock
func HotVideoMergeBuildLockKey(asOf string) string {
	return fmt.Sprintf("%s:hot:merge:%s:lock", prefix, asOf)
}

// HotVideoMergeTempKey 热榜快照构建临时 ZSet，完成后原子替换正式快照。
// 格式: fsz:hot:merge:{asOf}:tmp:{token}
func HotVideoMergeTempKey(asOf string, token string) string {
	return fmt.Sprintf("%s:hot:merge:%s:tmp:%s", prefix, asOf, token)
}
