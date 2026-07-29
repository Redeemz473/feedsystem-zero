package feedx

// BigCreatorFollowerThreshold 定义大 V 升级阈值：首次 follower_count 达到该值时，
// social 模块会在关注事务内把 accounts.is_big_v 置为 1，之后永久保留（只升不降）。
//
// 关注流采用"推拉结合"模式：
//   - 小 V（is_big_v = 0）：视频发布时扇出（推）到所有粉丝的 Timeline ZSet；
//   - 大 V（is_big_v = 1）：不扇出，读侧在拉取关注流时再合并该作者的 outbox（拉）。
//
// 阈值只用于"是否需要升级为大 V"这一判定，一旦升级完成，写侧 fanout 决策与读侧
// union 决策都改看 is_big_v 字段而非 follower_count，从而避免大 V 掉粉后阈值
// 反向穿越导致历史 outbox 视频从关注流消失。
const BigCreatorFollowerThreshold int64 = 5000

// IsBigCreator 判断作者当前是否被视为大 V。
// 直接读取 accounts.is_big_v 字段，一次主键查询即可得到；由于该字段只升不降，
// 读侧与写侧对同一作者的判定完全一致。
func IsBigCreator(isBigV bool) bool {
	return isBigV
}

// ShouldPromoteBigCreator 判断 follower_count 增长后是否需要把作者升级为大 V。
// 仅在当前不是大 V 且新粉丝数达到阈值时返回 true；已经是大 V 的账号直接返回 false，
// 避免重复 UPDATE。调用方需在同一个 DB 事务内完成升级 UPDATE。
func ShouldPromoteBigCreator(newFollowerCount int64, currentIsBigV bool) bool {
	if currentIsBigV {
		return false
	}
	return newFollowerCount >= BigCreatorFollowerThreshold
}
