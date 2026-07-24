package logic

// 本文件用于放 Social 多个 logic 共用的辅助函数。
// 下面只给出建议拆分和职责，不提供实现；你可以按照手写进度逐个添加。
//
// 1. normalizeSocialPage(pageSize int64) (int, error)
//    - 默认 20，最大 50；
//    - 负数返回 InvalidArgument。
//
// 2. validateFollowCursor(cursorUpdatedAt int64, cursorFollowID uint64) (time.Time, bool, error)
//    - 两个游标必须同时为 0 或同时非 0；
//    - cursorUpdatedAt 使用 Unix 毫秒；
//    - 返回 cursorTime、是否有游标、错误。
//
// 3. normalizeUserIDs(ids []uint64, max int) ([]uint64, error)
//    - 限制批量数量、过滤 0、去重并保留输入顺序；
//    - Account.BatchGetProfiles 和 BatchIsFollowing 都可以采用相同思路。
//
// 4. batchLoadFollowingStates(
//      ctx context.Context,
//      svcCtx *svc.ServiceContext,
//      viewerID uint64,
//      targetUserIDs []uint64,
//    ) (map[uint64]bool, error)
//    - viewerID=0 时全部 false；
//    - Redis Pipeline/MGET 批量读 SocialFollowingStateKey；
//    - 只对未命中的 ID 执行一次 MySQL IN 查询；
//    - 用 Pipeline 回填 true 和 false；
//    - ListFollowers、ListFollowings、BatchIsFollowing 共用。
//
// 5. newSocialEventID(prefix string) (string, error)
//    - 使用 crypto/rand 或 UUID 生成全局唯一事件 ID；
//    - 不要使用数据库自增 ID或单独的时间戳作为 event_id。
//
// 6. buildFollowOutboxEvent(
//      eventID string,
//      followerID uint64,
//      followingID uint64,
//      action string,
//      occurredAt time.Time,
//    ) (*model.OutboxEvent, error)
//    - 先调用 model.BuildFollowPayload 构造 FollowEvent；
//    - 再用 eventx.Envelope 包裹业务 payload；
//    - topic 固定为 eventx.TopicFollowEvents；
//    - aggregate_id 使用 "followerID:followingID"；
//    - Follow 和 Unfollow 共用，避免两边事件格式漂移。
//
// 7. applyFollowCacheAfterCommit(...)
//    - 只能在 MySQL 事务提交成功后调用；
//    - 写 SocialFollowingStateKey 的 "1"/"0" 和 TTL；
//    - 删除双方 SocialFollowStatsKey；
//    - Redis 错误只记录日志，不改变已经提交的业务结果。
