package feedx

// BigCreatorFollowerThreshold 定义大 V 阈值：粉丝数达到该值的作者视为大 V。
//
// 关注流采用"推拉结合"模式：
//   - 小 V（follower_count < 阈值）：视频发布时扇出（推）到所有粉丝的 Timeline ZSet；
//   - 大 V（follower_count >= 阈值）：不扇出，读侧在拉取关注流时再合并该作者最新视频（拉）。
//
// 该阈值直接依赖 accounts.follower_count 冗余字段，判断成本为一次主键查询。
// 调整阈值只需修改此常量，写侧（feed_timeline 扇出）和读侧（feed rpc 合并）会同步生效。
const BigCreatorFollowerThreshold int64 = 5000

// IsBigCreator 根据粉丝数判断是否为大 V。
// 传入 accounts.follower_count 字段即可。
func IsBigCreator(followerCount int64) bool {
	return followerCount >= BigCreatorFollowerThreshold
}
