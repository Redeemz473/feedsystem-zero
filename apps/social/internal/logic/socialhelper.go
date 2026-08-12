package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"feedsystem-zero/apps/social/internal/model"
	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/rediskey"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	defaultSocialPageSize        = 20
	maxSocialPageSize            = 50
	socialFirstPageWindowSize    = maxSocialPageSize
	defaultSocialIDPrefix        = "social"
	socialRedisOpTimeout         = 500 * time.Millisecond
	socialListCacheRetryDelay    = 50 * time.Millisecond
	socialListCacheRetryAttempts = 3
	maxSocialCursorFutureSkew    = 5 * time.Minute
	socialDBMaxRetries           = 3
	socialDBRetryBase            = 20 * time.Millisecond
	socialDBRetryMax             = 200 * time.Millisecond
)

const saveFollowListFirstPageCacheScript = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return 0
end
redis.call("SET", KEYS[2], ARGV[2], "PX", ARGV[3])
return 1
`

// lockFollowAccounts 按主键升序锁住关注双方的账户行，并返回被关注者快照。
// A 关注 B 与 B 关注 A 必须使用相同的加锁顺序，否则两个事务分别先锁
// 对方账户时会形成经典的锁顺序反转。
func lockFollowAccounts(ctx context.Context, tx *gorm.DB, followerID, followingID uint64) (model.Account, error) {
	firstID, secondID := followerID, followingID
	if firstID > secondID {
		firstID, secondID = secondID, firstID
	}

	var accounts []model.Account
	if err := tx.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "follower_count", "following_count", "is_big_v").
		Where("id IN ?", []uint64{firstID, secondID}).
		Order("id ASC").
		Find(&accounts).Error; err != nil {
		return model.Account{}, err
	}
	if len(accounts) != 2 {
		return model.Account{}, gorm.ErrRecordNotFound
	}
	for _, account := range accounts {
		if account.ID == followingID {
			return account, nil
		}
	}
	return model.Account{}, gorm.ErrRecordNotFound
}

// updateFollowAccountCounters 用一条 UPDATE 同时维护关注双方的冗余计数。
// 两行此前已按主键顺序锁定，因此这里既不会丢更新，也不会形成锁顺序反转。
// promoteBigCreator 由锁内快照计算，只升不降，不依赖同一 UPDATE 的字段赋值顺序。
func updateFollowAccountCounters(
	tx *gorm.DB,
	followerID uint64,
	followingID uint64,
	delta int64,
	promoteBigCreator bool,
) error {
	if delta != 1 && delta != -1 {
		return fmt.Errorf("unsupported follow counter delta: %d", delta)
	}

	promote := 0
	if promoteBigCreator {
		promote = 1
	}

	return tx.Exec(`UPDATE accounts
		SET follower_count = CASE
				WHEN id = ? THEN GREATEST(CAST(follower_count AS SIGNED) + ?, 0)
				ELSE follower_count
			END,
			following_count = CASE
				WHEN id = ? THEN GREATEST(CAST(following_count AS SIGNED) + ?, 0)
				ELSE following_count
			END,
			is_big_v = CASE
				WHEN id = ? AND ? = 1 THEN 1
				ELSE is_big_v
			END
		WHERE id IN (?, ?)`,
		followingID,
		delta,
		followerID,
		delta,
		followingID,
		promote,
		followerID,
		followingID,
	).Error
}

// createSocialOutboxEvents 将同一次关注状态变化产生的领域事件与通知事件
// 合并为一次多值 INSERT，减少事务内数据库往返。复制结构体可避免事务重试时
// 复用 GORM 已回填的自增 ID。
func createSocialOutboxEvents(tx *gorm.DB, events ...*model.OutboxEvent) error {
	rows := socialOutboxRows(events...)
	if len(rows) == 0 {
		return nil
	}
	return tx.Create(&rows).Error
}

func socialOutboxRows(events ...*model.OutboxEvent) []model.OutboxEvent {
	rows := make([]model.OutboxEvent, 0, len(events))
	for _, event := range events {
		if event == nil {
			continue
		}
		row := *event
		row.ID = 0
		rows = append(rows, row)
	}
	return rows
}

// runSocialWriteTransaction 对整个关注写事务做有限重试。MySQL 发生 1213 时
// 已经完整回滚当前事务，因此重新执行关系、计数和 outbox 写入不会留下半状态。
// 其他错误不重试，避免把约束错误或数据问题伪装成瞬时故障。
func runSocialWriteTransaction(ctx context.Context, db *gorm.DB, fn func(tx *gorm.DB) error) error {
	for attempt := 0; ; attempt++ {
		err := db.WithContext(ctx).Transaction(fn)
		if err == nil {
			return nil
		}
		if !isRetryableSocialDBError(err) {
			return err
		}
		if attempt >= socialDBMaxRetries {
			return fmt.Errorf("social事务重试耗尽, retries:%d: %w", socialDBMaxRetries, err)
		}

		retryNumber := attempt + 1
		delay := socialDBRetryDelay(retryNumber)
		logx.WithContext(ctx).Infof(
			"social事务发生可重试锁冲突, retry:%d/%d delay:%s error:%v",
			retryNumber,
			socialDBMaxRetries,
			delay,
			err,
		)
		if err := waitSocialDBRetry(ctx, delay); err != nil {
			return err
		}
	}
}

func isRetryableSocialDBError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 1213 || mysqlErr.Number == 1205
}

func socialDBRetryDelay(retryNumber int) time.Duration {
	delay := socialDBRetryBase
	for i := 1; i < retryNumber && delay < socialDBRetryMax; i++ {
		if delay >= socialDBRetryMax/2 {
			delay = socialDBRetryMax
			break
		}
		delay *= 2
	}
	if delay > socialDBRetryMax {
		delay = socialDBRetryMax
	}
	if delay >= socialDBRetryMax {
		return delay
	}

	// 最多增加 50% 抖动，避免同一批被回滚请求再次同步争锁。
	jitterWindow := delay / 2
	jitter := time.Duration(time.Now().UnixNano() % int64(jitterWindow+1))
	if delay+jitter > socialDBRetryMax {
		return socialDBRetryMax
	}
	return delay + jitter
}

func waitSocialDBRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

const releaseSocialCacheLockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`

// followListCacheItem 是粉丝/关注首页缓存中的访问者无关基础关系。
// FollowedAt 使用 follows.updated_at 的 Unix 毫秒值，便于生成分页游标。
type followListCacheItem struct {
	RelationID  uint64 `json:"relation_id"`
	FollowerID  uint64 `json:"follower_id"`
	FollowingID uint64 `json:"following_id"`
	FollowedAt  int64  `json:"followed_at"`
}

// followListFirstPageCache 保存固定 50 条首页窗口。
// HasMoreAfterWindow 只表示第 50 条之后是否还有数据，不等于某个 page_size 请求的 has_more。
type followListFirstPageCache struct {
	Version            int64                 `json:"version"`
	Relations          []followListCacheItem `json:"relations"`
	HasMoreAfterWindow bool                  `json:"has_more_after_window"`
}

var followListLoadGroup localFollowListLoadGroup

type localFollowListLoadGroup struct {
	mu    sync.Mutex
	calls map[string]*followListLoadCall
}

type followListLoadCall struct {
	done    chan struct{}
	follows []model.Follow
	hasMore bool
	err     error
}

// normalizeSocialPage 统一 Social 列表页大小。
// pageSize=0 使用默认值；pageSize<0 返回 InvalidArgument；pageSize>50 自动截断到 50。
func normalizeSocialPage(pageSize int64) (int, error) {
	if pageSize < 0 {
		return 0, status.Error(codes.InvalidArgument, "page_size不能为负数")
	}
	if pageSize == 0 {
		return defaultSocialPageSize, nil
	}
	if pageSize > maxSocialPageSize {
		return maxSocialPageSize, nil
	}
	return int(pageSize), nil
}

// validateFollowCursor 校验关注/粉丝列表游标。
// 两个游标必须同时为空或同时非空；cursorUpdatedAt 使用 Unix milliseconds。
func validateFollowCursor(cursorUpdatedAt int64, cursorFollowID uint64) (time.Time, bool, error) {
	if cursorUpdatedAt == 0 && cursorFollowID == 0 {
		return time.Time{}, false, nil
	}
	if cursorUpdatedAt <= 0 || cursorFollowID == 0 {
		return time.Time{}, false, status.Error(codes.InvalidArgument, "cursor_updated_at和cursor_follow_id必须同时为空或同时非空")
	}
	if cursorUpdatedAt > time.Now().Add(maxSocialCursorFutureSkew).UnixMilli() {
		return time.Time{}, false, status.Error(codes.InvalidArgument, "cursor_updated_at不能超过当前时间")
	}
	return time.UnixMilli(cursorUpdatedAt).Local(), true, nil
}

// normalizeUserIDs 过滤 0、去重并保留输入顺序，同时限制最大数量。
func normalizeUserIDs(ids []uint64, max int) ([]uint64, error) {
	if max <= 0 {
		return nil, status.Error(codes.InvalidArgument, "max必须大于0")
	}

	result := make([]uint64, 0, len(ids))
	seen := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		if len(result) >= max {
			return nil, status.Errorf(codes.InvalidArgument, "user_ids数量不能超过%d", max)
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}

	return result, nil
}

// batchLoadFollowingStates 批量查询 viewerID 是否关注 targetUserIDs。
// 查询顺序：Redis 覆盖缓存 -> MySQL IN 查询兜底 -> Redis SetNX 回填命中/未命中状态。
func batchLoadFollowingStates(ctx context.Context, svcCtx *svc.ServiceContext, viewerID uint64, targetUserIDs []uint64) (map[uint64]bool, error) {
	result := make(map[uint64]bool, len(targetUserIDs))
	for _, targetUserID := range targetUserIDs {
		result[targetUserID] = false
	}

	if len(targetUserIDs) == 0 || viewerID == 0 {
		return result, nil
	}

	// 用 Pipeline/MGET 批量读取所有 SocialFollowingStateKey：
	// 命中 "1"/"0" 的直接记录，redis.Nil 的 ID 放入 missIDs。
	redisCtx, cancel := context.WithTimeout(ctx, socialRedisOpTimeout)
	defer cancel()

	pipe := svcCtx.RedisCli.Pipeline()
	cmdMap := make(map[uint64]*redis.StringCmd, len(targetUserIDs))
	for _, targetUserID := range targetUserIDs {
		cmdMap[targetUserID] = pipe.Get(redisCtx, rediskey.SocialFollowingStateKey(viewerID, targetUserID))
	}

	if _, err := pipe.Exec(redisCtx); err != nil && !errors.Is(err, redis.Nil) {
		return batchLoadFollowingStatesFromDB(ctx, svcCtx, viewerID, targetUserIDs, result, false, nil)
	}

	missIDs := make([]uint64, 0)
	// invalidIDs 用于记录 Redis 中值格式异常的 key。
	// 这些 key 走 DB 兜底后必须用 Set 覆盖，SetNX 无法修复异常值。
	invalidIDs := make(map[uint64]struct{})
	for targetUserID, cmd := range cmdMap {
		value, err := cmd.Result()
		if err == nil {
			switch value {
			case "1":
				result[targetUserID] = true
			case "0":
				result[targetUserID] = false
			default:
				missIDs = append(missIDs, targetUserID)
				invalidIDs[targetUserID] = struct{}{}
			}
			continue
		}

		if errors.Is(err, redis.Nil) {
			missIDs = append(missIDs, targetUserID)
			continue
		}

		missIDs = append(missIDs, targetUserID)
	}

	if len(missIDs) == 0 {
		return result, nil
	}

	return batchLoadFollowingStatesFromDB(ctx, svcCtx, viewerID, missIDs, result, true, invalidIDs)
}

func batchLoadFollowingStatesFromDB(ctx context.Context, svcCtx *svc.ServiceContext, viewerID uint64, targetUserIDs []uint64, result map[uint64]bool, backfillCache bool, invalidIDs map[uint64]struct{}) (map[uint64]bool, error) {
	var followingIDs []uint64
	//  对 misses 只执行一次 MySQL 查询：
	//  SELECT following_id FROM follows
	//  WHERE follower_id=? AND following_id IN ? AND status=Active AND deleted_at IS NULL。
	if err := svcCtx.GormDB.WithContext(ctx).
		Model(&model.Follow{}).
		Where("follower_id = ? AND following_id IN ? AND status = ? AND deleted_at IS NULL", viewerID, targetUserIDs, model.FollowStatusActive).
		Pluck("following_id", &followingIDs).Error; err != nil {
		return nil, status.Error(codes.Internal, "查询关注状态失败")
	}

	followingSet := make(map[uint64]struct{}, len(followingIDs))
	for _, followingID := range followingIDs {
		followingSet[followingID] = struct{}{}
		result[followingID] = true
	}

	//  用一次 Redis Pipeline 把 misses 的 true/false 全部回填并设置 TTL。
	if backfillCache {
		redisCtx, cancel := context.WithTimeout(ctx, socialRedisOpTimeout)
		defer cancel()

		pipe := svcCtx.RedisCli.Pipeline()
		for _, targetUserID := range targetUserIDs {
			value := "0"
			if _, ok := followingSet[targetUserID]; ok {
				value = "1"
			}
			// 值格式异常的 key 用 Set 强制覆盖，其他 miss 用 SetNX，避免覆盖并发写入的最新状态。
			if _, ok := invalidIDs[targetUserID]; ok {
				pipe.Set(redisCtx, rediskey.SocialFollowingStateKey(viewerID, targetUserID), value, rediskey.SocialFollowingStateTTL)
			} else {
				pipe.SetNX(redisCtx, rediskey.SocialFollowingStateKey(viewerID, targetUserID), value, rediskey.SocialFollowingStateTTL)
			}
		}
		_, _ = pipe.Exec(redisCtx)
	}

	return result, nil
}

// newSocialEventID 生成全局唯一事件 ID。
func newSocialEventID(prefix string) (string, error) {
	if prefix == "" {
		prefix = defaultSocialIDPrefix
	}

	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixNano(), hex.EncodeToString(b)), nil
}

// buildFollowOutboxEvent 构造关注/取关 outbox 事件，payload 使用 eventx.Envelope 包裹业务 FollowEvent。
func buildFollowOutboxEvent(eventID string, followerID uint64, followingID uint64, action string, occurredAt time.Time) (*model.OutboxEvent, error) {
	occurredAtMs := occurredAt.UnixMilli()
	payloadBytes, err := model.BuildFollowPayload(eventID, followerID, followingID, action, occurredAtMs)
	if err != nil {
		return nil, status.Error(codes.Internal, "序列化关注事件失败")
	}

	eventType := eventx.EventTypeFollowCreated
	switch action {
	case eventx.FollowActionFollow:
		eventType = eventx.EventTypeFollowCreated
	case eventx.FollowActionUnfollow:
		eventType = eventx.EventTypeFollowDeleted
	default:
		return nil, status.Error(codes.InvalidArgument, "不支持的关注事件动作")
	}

	aggregateID := fmt.Sprintf("%d:%d", followerID, followingID)
	envelopeBytes, err := json.Marshal(eventx.Envelope{
		EventID:       eventID,
		EventType:     eventType,
		AggregateType: eventx.AggregateFollow,
		AggregateID:   aggregateID,
		Producer:      "social-rpc",
		OccurredAt:    occurredAtMs,
		Payload:       payloadBytes,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "序列化 outbox 事件失败")
	}

	return &model.OutboxEvent{
		EventID:       eventID,
		Topic:         eventx.TopicFollowEvents,
		EventType:     eventType,
		AggregateType: eventx.AggregateFollow,
		AggregateID:   aggregateID,
		Payload:       string(envelopeBytes),
		Status:        model.OutboxStatusPending,
		CreatedAt:     occurredAt,
		UpdatedAt:     occurredAt,
	}, nil
}

// buildFollowNotificationOutbox 为关注关系创建或撤回通知。
// aggregate_id 使用稳定业务键，保证同一对用户的 follow/unfollow 在 Kafka 内有序。
func buildFollowNotificationOutbox(
	notificationEventID string,
	sourceEventID string,
	followerID uint64,
	followingID uint64,
	action string,
	occurredAt time.Time,
) (*model.OutboxEvent, error) {
	notificationAction := eventx.NotificationActionCreate
	eventType := eventx.EventTypeNotificationCreate
	switch action {
	case eventx.FollowActionFollow:
	case eventx.FollowActionUnfollow:
		notificationAction = eventx.NotificationActionDelete
		eventType = eventx.EventTypeNotificationDelete
	default:
		return nil, status.Error(codes.InvalidArgument, "不支持的关注通知动作")
	}

	notificationEvent := eventx.NotificationEvent{
		EventID:          notificationEventID,
		SourceEventID:    sourceEventID,
		ReceiverID:       followingID,
		ActorID:          followerID,
		NotificationType: eventx.NotificationTypeFollow,
		Action:           notificationAction,
		OccurredAt:       occurredAt.UnixMilli(),
	}
	envelope, aggregateID, err := eventx.BuildNotificationEnvelope(notificationEvent, "social-rpc")
	if err != nil {
		return nil, status.Error(codes.Internal, "序列化关注通知事件失败")
	}
	return &model.OutboxEvent{
		EventID:       notificationEventID,
		Topic:         eventx.TopicNotificationEvents,
		EventType:     eventType,
		AggregateType: eventx.AggregateNotification,
		AggregateID:   aggregateID,
		Payload:       string(envelope),
		Status:        model.OutboxStatusPending,
		CreatedAt:     occurredAt,
		UpdatedAt:     occurredAt,
	}, nil
}

// applyFollowCacheAfterCommit 只能在 MySQL 事务提交成功后调用。
// 无论是否发生状态变化，都用 MySQL 已确认的最终状态覆盖单条关系缓存。
// 只有 stateChanged=true 时才递增统计与列表版本，避免幂等重试反复制造缓存失效。
// Redis 操作失败只记录日志，不能回滚已经提交的 MySQL 业务结果。
func applyFollowCacheAfterCommit(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	followerID uint64,
	followingID uint64,
	followed bool,
	stateChanged bool,
) error {
	value := "0"
	if followed {
		value = "1"
	}

	// MySQL 已经提交，即使客户端此时取消请求，也要在独立短超时内尝试失效缓存。
	redisCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), socialRedisOpTimeout)
	defer cancel()

	// 使用普通 Pipeline 而非 TxPipeline：几条命令彼此独立，单条失败不应影响其他命令。
	pipe := svcCtx.RedisCli.Pipeline()
	pipe.Set(redisCtx, rediskey.SocialFollowingStateKey(followerID, followingID), value, rediskey.SocialFollowingStateTTL)
	if stateChanged {
		// 只有真正的状态变化才需要让统计/列表缓存失效，
		// 幂等重试路径直接复用现有缓存，避免不必要的击穿。
		// 通过 AccountPublicProfileVersionKey 失效，使其回源拿到最新计数。
		pipe.Incr(redisCtx, rediskey.AccountPublicProfileVersionKey(followerID))
		pipe.Incr(redisCtx, rediskey.AccountPublicProfileVersionKey(followingID))
		pipe.Incr(redisCtx, rediskey.SocialFollowersListVersionKey(followingID))
		pipe.Incr(redisCtx, rediskey.SocialFollowingsListVersionKey(followerID))
	}
	_, err := pipe.Exec(redisCtx)
	return err
}

// followListPageBounds 从固定窗口中计算当前请求返回数量和 has_more。
func followListPageBounds(itemCount int, pageSize int, hasMoreAfterWindow bool) (int, bool) {
	if itemCount <= 0 || pageSize <= 0 {
		return 0, false
	}

	returnCount := itemCount
	if returnCount > pageSize {
		returnCount = pageSize
	}

	hasMore := returnCount < itemCount
	if returnCount == itemCount {
		hasMore = hasMoreAfterWindow
	}
	return returnCount, hasMore
}

func selectFollowListPage(relations []followListCacheItem, pageSize int, hasMoreAfterWindow bool) ([]followListCacheItem, bool) {
	returnCount, hasMore := followListPageBounds(len(relations), pageSize, hasMoreAfterWindow)
	return relations[:returnCount], hasMore
}

func followRowsToCacheItems(follows []model.Follow) []followListCacheItem {
	relations := make([]followListCacheItem, 0, len(follows))
	for _, follow := range follows {
		relations = append(relations, followListCacheItem{
			RelationID:  follow.ID,
			FollowerID:  follow.FollowerID,
			FollowingID: follow.FollowingID,
			FollowedAt:  follow.UpdatedAt.UnixMilli(),
		})
	}
	return relations
}

func followersDBLoadKey(userID uint64, cursorUpdatedAt int64, cursorFollowID uint64, pageSize int) string {
	return fmt.Sprintf(
		"followers:user:%d:cursor:updated_at:%d:follow_id:%d:size:%d",
		userID,
		cursorUpdatedAt,
		cursorFollowID,
		pageSize,
	)
}

// followingsDBLoadKey 与 followersDBLoadKey 保持独立命名空间，
// 避免 ListFollowers/ListFollowings 在同一 userID 下互相复用 SingleFlight 结果。
func followingsDBLoadKey(userID uint64, cursorUpdatedAt int64, cursorFollowID uint64, pageSize int) string {
	return fmt.Sprintf(
		"followings:user:%d:cursor:updated_at:%d:follow_id:%d:size:%d",
		userID,
		cursorUpdatedAt,
		cursorFollowID,
		pageSize,
	)
}

// Do 在单个 Social 进程内合并相同的关注列表数据库查询。
func (g *localFollowListLoadGroup) Do(
	ctx context.Context,
	key string,
	fn func() ([]model.Follow, bool, error),
) ([]model.Follow, bool, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*followListLoadCall)
	}
	if call, ok := g.calls[key]; ok {
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-call.done:
			return call.follows, call.hasMore, call.err
		}
	}

	call := &followListLoadCall{done: make(chan struct{})}
	g.calls[key] = call
	g.mu.Unlock()

	call.follows, call.hasMore, call.err = fn()
	close(call.done) //通知所有等待这个查询结果的请求

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return call.follows, call.hasMore, call.err
}

// getFollowersListVersion 获取粉丝列表版本号。
func (l *ListFollowersLogic) getFollowersListVersion(userID uint64) (int64, bool) {
	redisCtx, cancel := context.WithTimeout(l.ctx, socialRedisOpTimeout)
	defer cancel()

	key := rediskey.SocialFollowersListVersionKey(userID)
	version, err := l.svcCtx.RedisCli.Get(redisCtx, key).Int64()
	if err == nil {
		return version, true
	}
	if errors.Is(err, redis.Nil) {
		initVersion := newFollowersListVersion()
		// 没有版本号时通过 SetNX 竞争初始化。
		ok, err := l.svcCtx.RedisCli.SetNX(redisCtx, key, initVersion, 0).Result()
		if err != nil {
			l.Errorf("init followers list version failed, user_id: %d, error: %v", userID, err)
			return 0, false
		}
		if ok {
			return initVersion, true
		}
		// 没抢到初始化权时读取获胜请求设置的版本。
		version, err = l.svcCtx.RedisCli.Get(redisCtx, key).Int64()
		if err == nil {
			return version, true
		}
		l.Errorf("get followers list version after init race failed, user_id: %d, error: %v", userID, err)
		return 0, false
	}
	l.Errorf("get followers list version failed, user_id: %d, error: %v", userID, err)
	return 0, false
}

// getFollowingsListVersion 获取用户关注列表版本号。
func (l *ListFollowingsLogic) getFollowingsListVersion(userID uint64) (int64, bool) {
	redisCtx, cancel := context.WithTimeout(l.ctx, socialRedisOpTimeout)
	defer cancel()

	key := rediskey.SocialFollowingsListVersionKey(userID)
	version, err := l.svcCtx.RedisCli.Get(redisCtx, key).Int64()
	if err == nil {
		return version, true
	}
	if errors.Is(err, redis.Nil) {
		initVersion := newFollowersListVersion()
		// 没有版本号时通过 SetNX 竞争初始化。
		ok, err := l.svcCtx.RedisCli.SetNX(redisCtx, key, initVersion, 0).Result()
		if err != nil {
			l.Errorf("init followings list version failed, user_id: %d, error: %v", userID, err)
			return 0, false
		}
		if ok {
			return initVersion, true
		}
		// 没抢到初始化权时读取获胜请求设置的版本。
		version, err = l.svcCtx.RedisCli.Get(redisCtx, key).Int64()
		if err == nil {
			return version, true
		}
		l.Errorf("get followings list version after init race failed, user_id: %d, error: %v", userID, err)
		return 0, false
	}
	l.Errorf("get followings list version failed, user_id: %d, error: %v", userID, err)
	return 0, false
}

func newFollowersListVersion() int64 {
	return time.Now().UnixMilli()
}

// loadFollowersFirstPageCache 读取粉丝列表固定首页窗口缓存。
func (l *ListFollowersLogic) loadFollowersFirstPageCache(
	cacheKey string,
	expectedVersion int64,
	userID uint64,
) (*followListFirstPageCache, bool) {
	redisCtx, cancel := context.WithTimeout(l.ctx, socialRedisOpTimeout)
	defer cancel()

	data, err := l.svcCtx.RedisCli.Get(redisCtx, cacheKey).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			l.Errorf("get followers list cache failed, key: %s, error: %v", cacheKey, err)
		}
		return nil, false
	}

	var cached followListFirstPageCache
	if err := json.Unmarshal(data, &cached); err != nil {
		l.Errorf("unmarshal followers list cache failed, key: %s, error: %v", cacheKey, err)
		return nil, false
	}
	//判断版本号是否相符
	if cached.Version != expectedVersion {
		return nil, false
	}
	if len(cached.Relations) > socialFirstPageWindowSize {
		l.Errorf("unexpected followers first page cache size, key: %s, size: %d", cacheKey, len(cached.Relations))
		return nil, false
	}
	if cached.HasMoreAfterWindow && len(cached.Relations) != socialFirstPageWindowSize {
		l.Errorf("invalid followers first page cache window, key: %s, size: %d", cacheKey, len(cached.Relations))
		return nil, false
	}
	for _, relation := range cached.Relations {
		if relation.RelationID == 0 ||
			relation.FollowerID == 0 ||
			relation.FollowingID != userID {
			l.Errorf("invalid followers first page cache item, key: %s", cacheKey)
			return nil, false
		}
	}

	return &cached, true
}

// loadFollowingsFirstPageCache 读取用户关注列表固定首页窗口缓存。
func (l *ListFollowingsLogic) loadFollowingsFirstPageCache(
	cacheKey string,
	expectedVersion int64,
	userID uint64,
) (*followListFirstPageCache, bool) {
	redisCtx, cancel := context.WithTimeout(l.ctx, socialRedisOpTimeout)
	defer cancel()

	data, err := l.svcCtx.RedisCli.Get(redisCtx, cacheKey).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			l.Errorf("get followings list cache failed, key: %s, error: %v", cacheKey, err)
		}
		return nil, false
	}

	var cached followListFirstPageCache
	if err := json.Unmarshal(data, &cached); err != nil {
		l.Errorf("unmarshal followings list cache failed, key: %s, error: %v", cacheKey, err)
		return nil, false
	}
	//判断版本号是否相符
	if cached.Version != expectedVersion {
		return nil, false
	}
	if len(cached.Relations) > socialFirstPageWindowSize {
		l.Errorf("unexpected followings first page cache size, key: %s, size: %d", cacheKey, len(cached.Relations))
		return nil, false
	}
	if cached.HasMoreAfterWindow && len(cached.Relations) != socialFirstPageWindowSize {
		l.Errorf("invalid followings first page cache window, key: %s, size: %d", cacheKey, len(cached.Relations))
		return nil, false
	}
	for _, relation := range cached.Relations {
		if relation.RelationID == 0 ||
			relation.FollowingID == 0 ||
			relation.FollowerID != userID {
			l.Errorf("invalid followings first page cache item, key: %s", cacheKey)
			return nil, false
		}
	}

	return &cached, true
}

// saveFollowersFirstPageCache 原子校验粉丝列表版本并写入固定首页窗口。
// 查询期间若发生关注或取关，版本会变化，旧数据库快照不会写入 Redis。
func (l *ListFollowersLogic) saveFollowersFirstPageCache(
	cacheKey string,
	version int64,
	userID uint64,
	follows []model.Follow,
	hasMoreAfterWindow bool,
) {
	if len(follows) > socialFirstPageWindowSize {
		follows = follows[:socialFirstPageWindowSize]
	}

	cached := followListFirstPageCache{
		Version:            version,
		Relations:          followRowsToCacheItems(follows),
		HasMoreAfterWindow: hasMoreAfterWindow,
	}
	data, err := json.Marshal(cached)
	if err != nil {
		l.Errorf("marshal followers first page cache failed, user_id: %d, error: %v", userID, err)
		return
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, socialRedisOpTimeout)
	defer cancel()
	ttl := socialListFirstPageCacheTTL(userID)
	written, err := l.svcCtx.RedisCli.Eval(
		redisCtx,
		saveFollowListFirstPageCacheScript,
		[]string{rediskey.SocialFollowersListVersionKey(userID), cacheKey},
		strconv.FormatInt(version, 10),
		data,
		ttl.Milliseconds(),
	).Int64()
	if err != nil {
		l.Errorf("set followers first page cache failed, user_id: %d, error: %v", userID, err)
		return
	}
	if written == 0 {
		l.Infof("skip stale followers first page cache, user_id: %d, version: %d", userID, version)
	}
}

// saveFollowingsFirstPageCache 原子校验关注列表版本并写入固定首页窗口。
// 查询期间若发生关注或取关，版本会变化，旧数据库快照不会写入 Redis。
func (l *ListFollowingsLogic) saveFollowingsFirstPageCache(
	cacheKey string,
	version int64,
	userID uint64,
	follows []model.Follow,
	hasMoreAfterWindow bool,
) {
	if len(follows) > socialFirstPageWindowSize {
		follows = follows[:socialFirstPageWindowSize]
	}

	cached := followListFirstPageCache{
		Version:            version,
		Relations:          followRowsToCacheItems(follows),
		HasMoreAfterWindow: hasMoreAfterWindow,
	}
	data, err := json.Marshal(cached)
	if err != nil {
		l.Errorf("marshal followings first page cache failed, user_id: %d, error: %v", userID, err)
		return
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, socialRedisOpTimeout)
	defer cancel()
	ttl := socialListFirstPageCacheTTL(userID)
	written, err := l.svcCtx.RedisCli.Eval(
		redisCtx,
		saveFollowListFirstPageCacheScript,
		// followings 首页缓存必须用 followings 的版本号做原子校验，
		// 否则 followers 版本没变时会把过期的 followings 快照写回缓存。
		[]string{rediskey.SocialFollowingsListVersionKey(userID), cacheKey},
		strconv.FormatInt(version, 10),
		data,
		ttl.Milliseconds(),
	).Int64()
	if err != nil {
		l.Errorf("set followings first page cache failed, user_id: %d, error: %v", userID, err)
		return
	}
	if written == 0 {
		l.Infof("skip stale followings first page cache, user_id: %d, version: %d", userID, version)
	}
}

// tryLockFollowersFirstPageCache 区分锁竞争失败和 Redis 操作异常。
func (l *ListFollowersLogic) tryLockFollowersFirstPageCache(cacheKey string) (string, string, bool, error) {
	lockToken, err := randomSocialHex(16)
	if err != nil {
		l.Errorf("generate followers cache lock token failed, key: %s, error: %v", cacheKey, err)
		return "", "", false, err
	}

	lockKey := rediskey.SocialFollowersFirstPageCacheBuildLockKey(cacheKey)
	redisCtx, cancel := context.WithTimeout(l.ctx, socialRedisOpTimeout)
	defer cancel()
	locked, err := l.svcCtx.RedisCli.SetNX(
		redisCtx,
		lockKey,
		lockToken,
		rediskey.SocialListCacheBuildLockTTL,
	).Result()
	if err != nil {
		l.Errorf("lock followers first page cache failed, key: %s, error: %v", cacheKey, err)
		return "", "", false, err
	}
	return lockKey, lockToken, locked, nil
}

// tryLockFollowingsFirstPageCache 区分锁竞争失败和 Redis 操作异常。
func (l *ListFollowingsLogic) tryLockFollowingsFirstPageCache(cacheKey string) (string, string, bool, error) {
	lockToken, err := randomSocialHex(16)
	if err != nil {
		l.Errorf("generate followings cache lock token failed, key: %s, error: %v", cacheKey, err)
		return "", "", false, err
	}

	lockKey := rediskey.SocialFollowingsFirstPageCacheBuildLockKey(cacheKey)
	redisCtx, cancel := context.WithTimeout(l.ctx, socialRedisOpTimeout)
	defer cancel()
	locked, err := l.svcCtx.RedisCli.SetNX(
		redisCtx,
		lockKey,
		lockToken,
		rediskey.SocialListCacheBuildLockTTL,
	).Result()
	if err != nil {
		l.Errorf("lock followings first page cache failed, key: %s, error: %v", cacheKey, err)
		return "", "", false, err
	}
	return lockKey, lockToken, locked, nil
}

// waitAndReloadFollowersFirstPageCache 对正在构建的缓存进行有限等待。
func (l *ListFollowersLogic) waitAndReloadFollowersFirstPageCache(
	cacheKey string,
	version int64,
	userID uint64,
) (*followListFirstPageCache, bool) {
	for i := 0; i < socialListCacheRetryAttempts; i++ {
		timer := time.NewTimer(socialListCacheRetryDelay)
		select {
		case <-l.ctx.Done():
			timer.Stop()
			return nil, false
		case <-timer.C:
			if cached, hit := l.loadFollowersFirstPageCache(cacheKey, version, userID); hit {
				return cached, true
			}
		}
	}
	return nil, false
}

// waitAndReloadFollowingsFirstPageCache 对正在构建的缓存进行有限等待。
func (l *ListFollowingsLogic) waitAndReloadFollowingsFirstPageCache(
	cacheKey string,
	version int64,
	userID uint64,
) (*followListFirstPageCache, bool) {
	for i := 0; i < socialListCacheRetryAttempts; i++ {
		timer := time.NewTimer(socialListCacheRetryDelay)
		select {
		case <-l.ctx.Done():
			timer.Stop()
			return nil, false
		case <-timer.C:
			if cached, hit := l.loadFollowingsFirstPageCache(cacheKey, version, userID); hit {
				return cached, true
			}
		}
	}
	return nil, false
}

func (l *ListFollowersLogic) releaseFollowersFirstPageCacheLock(lockKey string, lockToken string) {
	if lockKey == "" || lockToken == "" {
		return
	}

	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(l.ctx), socialRedisOpTimeout)
	defer cancel()
	if err := releaseSocialCacheLock(releaseCtx, l.svcCtx.RedisCli, lockKey, lockToken); err != nil {
		l.Errorf("release followers first page cache lock failed, key: %s, error: %v", lockKey, err)
	}
}

func (l *ListFollowingsLogic) releaseFollowingsFirstPageCacheLock(lockKey string, lockToken string) {
	if lockKey == "" || lockToken == "" {
		return
	}

	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(l.ctx), socialRedisOpTimeout)
	defer cancel()
	if err := releaseSocialCacheLock(releaseCtx, l.svcCtx.RedisCli, lockKey, lockToken); err != nil {
		l.Errorf("release followings first page cache lock failed, key: %s, error: %v", lockKey, err)
	}
}

func socialListFirstPageCacheTTL(userID uint64) time.Duration {
	jitter := time.Duration(userID % 10)
	return rediskey.SocialListFirstPageCacheTTL + jitter*time.Second
}

func randomSocialHex(n int) (string, error) {
	if n <= 0 {
		return "", errors.New("随机字节数必须大于0")
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func releaseSocialCacheLock(ctx context.Context, redisCli *redis.Client, lockKey string, lockToken string) error {
	return redisCli.Eval(ctx, releaseSocialCacheLockScript, []string{lockKey}, lockToken).Err()
}
