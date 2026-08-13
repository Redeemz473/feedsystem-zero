package logic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	likedVideosFirstPageWindowSize    int64 = 20
	defaultListMyLikedVideosPageSize        = likedVideosFirstPageWindowSize
	maxListMyLikedVideosPageSize      int64 = 50
	likedVideosListCacheTTL                 = 30 * time.Second
	likedVideosListCacheLockTTL             = 2 * time.Second
	likedVideosListCacheRetryDelay          = 50 * time.Millisecond
	likedVideosListCacheRetryAttempts       = 3
	maxLikedVideosCursorFutureSkew          = 5 * time.Minute
)

const saveLikedVideosFirstPageCacheScript = `
if redis.call("GET", KEYS[3]) ~= ARGV[4] then
	return -1
end
local current_version = redis.call("GET", KEYS[1])
if not current_version then
	redis.call("SET", KEYS[1], "0")
	current_version = "0"
end
if current_version ~= ARGV[1] then
	return 0
end
redis.call("SET", KEYS[2], ARGV[2], "PX", ARGV[3])
return 1
`

type likedVideosListCache struct {
	Version            int64                 `json:"version"`
	LikedVideos        []likedVideoItemCache `json:"liked_videos"`
	HasMoreAfterWindow bool                  `json:"has_more_after_window"`
}

type likedVideoItemCache struct {
	LikeID  uint64 `json:"like_id"`
	VideoID uint64 `json:"video_id"`
	LikedAt int64  `json:"liked_at"`
}

var likedVideosListLoadGroup localLikedVideosListLoadGroup

type localLikedVideosListLoadGroup struct {
	mu    sync.Mutex
	calls map[string]*likedVideosListLoadCall
}

type likedVideosListLoadCall struct {
	done    chan struct{}
	likes   []model.Like
	hasMore bool
	err     error
}

type ListMyLikedVideosLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListMyLikedVideosLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListMyLikedVideosLogic {
	return &ListMyLikedVideosLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListMyLikedVideosLogic) ListMyLikedVideos(in *interaction.ListMyLikedVideosReq) (*interaction.ListMyLikedVideosResp, error) {
	// 校验 user_id 不能为 0；page_size 设置默认值和上限，例如默认 20、最大 50。
	userID := in.GetUserId()
	if userID == 0 {
		return nil, status.Error(codes.Unauthenticated, "用户未登录")
	}
	cursorCreatedAt := in.GetCursorCreatedAt()
	cursorLikeID := in.GetCursorLikeId()
	if err := validateLikedVideosListCursor(cursorCreatedAt, cursorLikeID); err != nil {
		return nil, err
	}

	pageSize := in.GetPageSize()
	if pageSize <= 0 {
		pageSize = defaultListMyLikedVideosPageSize
	}
	if pageSize > maxListMyLikedVideosPageSize {
		pageSize = maxListMyLikedVideosPageSize
	}

	// Redis 只缓存固定 20 条的首页窗口。小首页从窗口中截取；大页和历史页直接查 MySQL。
	cacheable := isLikedVideosFirstPageCacheable(cursorCreatedAt, cursorLikeID, pageSize)
	cacheKey := ""
	version := int64(0)
	if cacheable {
		if currentVersion, ok := l.getLikedVideosListVersion(userID); ok {
			version = currentVersion
			cacheKey = rediskey.LikeUserVideosFirstPageCacheKey(userID, version)
			if resp, hit := l.loadLikedVideosFirstPageCache(cacheKey, version, pageSize); hit {
				return resp, nil
			}
		}
	}

	var lockKey, lockToken string
	useFixedWindow := false
	cacheWriteAllowed := false
	if cacheKey != "" {
		var lockErr error
		var locked bool
		lockKey, lockToken, locked, lockErr = l.tryLockLikedVideosListCache(cacheKey)
		switch {
		case lockErr != nil:
			// Redis 不可用时直接按请求大小查 MySQL，不再等待或回写缓存。
			cacheKey = ""
		case locked:
			useFixedWindow = true
			cacheWriteAllowed = true
		default:
			if resp, hit := l.waitAndReloadLikedVideosFirstPageCache(cacheKey, version, pageSize); hit {
				return resp, nil
			}
			// 等待超时后允许回源，但此时尚无缓存写入权。
			useFixedWindow = true
		}
	}
	if cacheWriteAllowed {
		defer l.releaseLikedVideosListCacheLock(lockKey, lockToken)
	}

	dbPageSize := pageSize
	if useFixedWindow {
		dbPageSize = likedVideosFirstPageWindowSize
	}
	dbLoadKey := likedVideosListDBLoadKey(userID, cursorCreatedAt, cursorLikeID, dbPageSize)
	if useFixedWindow {
		dbLoadKey = "cache:" + cacheKey
	}
	likes, hasMore, err := likedVideosListLoadGroup.Do(l.ctx, dbLoadKey, func() ([]model.Like, bool, error) {
		return l.loadLikedVideosFromDB(userID, cursorCreatedAt, cursorLikeID, dbPageSize)
	})
	if err != nil {
		l.Errorf("list user liked videos failed, user_id: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "获取点赞视频失败")
	}

	// 首次未抢到锁的请求在回源完成后再尝试一次。原构建者已经完成时优先复用其缓存；
	// 原构建者失败或锁已过期时，只有二次抢锁成功者可以接管缓存写入。
	if useFixedWindow && cacheKey != "" && !cacheWriteAllowed {
		if resp, hit := l.loadLikedVideosFirstPageCache(cacheKey, version, pageSize); hit {
			return resp, nil
		}
		secondLockKey, secondLockToken, locked, lockErr := l.tryLockLikedVideosListCache(cacheKey)
		if lockErr == nil && locked {
			lockKey = secondLockKey
			lockToken = secondLockToken
			cacheWriteAllowed = true
			defer l.releaseLikedVideosListCacheLock(lockKey, lockToken)
		}
	}

	if cacheWriteAllowed {
		l.saveLikedVideosFirstPageCache(userID, cacheKey, version, lockKey, lockToken, likes, hasMore)
	}

	responseLikes, responseHasMore := selectLikedVideosPage(likes, pageSize, hasMore)
	resp := buildListMyLikedVideosResp(responseLikes, responseHasMore)
	return resp, nil
}

func isLikedVideosFirstPageCacheable(cursorCreatedAt int64, cursorLikeID uint64, pageSize int64) bool {
	return cursorCreatedAt == 0 &&
		cursorLikeID == 0 &&
		pageSize > 0 &&
		pageSize <= likedVideosFirstPageWindowSize
}

func validateLikedVideosListCursor(cursorCreatedAt int64, cursorLikeID uint64) error {
	if cursorCreatedAt < 0 {
		return status.Error(codes.InvalidArgument, "游标时间不能小于0")
	}
	if cursorCreatedAt > time.Now().Add(maxLikedVideosCursorFutureSkew).UnixMilli() {
		return status.Error(codes.InvalidArgument, "游标时间不能超过当前时间")
	}
	if cursorCreatedAt == 0 && cursorLikeID == 0 {
		return nil
	}
	if cursorCreatedAt <= 0 || cursorLikeID == 0 {
		return status.Error(codes.InvalidArgument, "游标参数不完整")
	}
	return nil
}

func (l *ListMyLikedVideosLogic) loadLikedVideosFromDB(userID uint64, cursorCreatedAt int64, cursorLikeID uint64, pageSize int64) ([]model.Like, bool, error) {
	// MySQL likes 表是最终事实来源；updated_at 表示最近一次点赞生效时间。
	query := l.svcCtx.GormDB.WithContext(l.ctx).
		Model(&model.Like{}).
		Joins("JOIN videos ON videos.id = likes.video_id AND videos.status = ? AND videos.deleted_at IS NULL", model.VideoStatusNormal).
		Where("likes.user_id = ? AND likes.status = ? AND likes.deleted_at IS NULL", userID, model.LikeStatusActive)
	if cursorCreatedAt > 0 && cursorLikeID > 0 {
		cursorTime := time.UnixMilli(cursorCreatedAt)
		query = query.Where(
			"(likes.updated_at < ? OR (likes.updated_at = ? AND likes.id < ?))",
			cursorTime,
			cursorTime,
			cursorLikeID,
		)
	}

	// 排序使用 updated_at DESC, id DESC，并多查一条判断 has_more。
	likes := make([]model.Like, 0, pageSize+1)
	if err := query.
		Order("likes.updated_at DESC").
		Order("likes.id DESC").
		Limit(int(pageSize + 1)).
		Find(&likes).Error; err != nil {
		return nil, false, err
	}

	hasMore := int64(len(likes)) > pageSize
	if hasMore {
		likes = likes[:pageSize]
	}
	return likes, hasMore, nil
}

func buildListMyLikedVideosResp(likes []model.Like, hasMore bool) *interaction.ListMyLikedVideosResp {
	// 返回 LikedVideoItem，只返回 video_id 和 liked_at；视频详情由 gateway 或 feed/video 服务批量补全。
	likedVideoItems := make([]*interaction.LikedVideoItem, 0, len(likes))
	for _, like := range likes {
		likedVideoItems = append(likedVideoItems, &interaction.LikedVideoItem{
			LikeId:  like.ID,
			VideoId: like.VideoID,
			LikedAt: like.UpdatedAt.UnixMilli(),
		})
	}

	var nextCursorCreatedAt int64
	var nextCursorLikeID uint64
	if hasMore && len(likes) > 0 {
		last := likes[len(likes)-1]
		nextCursorCreatedAt = last.UpdatedAt.UnixMilli()
		nextCursorLikeID = last.ID
	}

	return &interaction.ListMyLikedVideosResp{
		LikedVideos:         likedVideoItems,
		NextCursorCreatedAt: nextCursorCreatedAt,
		NextCursorLikeId:    nextCursorLikeID,
		HasMore:             hasMore,
	}
}

func selectLikedVideosPage(likes []model.Like, pageSize int64, hasMoreAfterWindow bool) ([]model.Like, bool) {
	returnCount, hasMore := likedVideosPageBounds(len(likes), pageSize, hasMoreAfterWindow)
	return likes[:returnCount], hasMore
}

func likedVideosPageBounds(itemCount int, pageSize int64, hasMoreAfterWindow bool) (int, bool) {
	if itemCount <= 0 || pageSize <= 0 {
		return 0, false
	}
	returnCount := itemCount
	if int64(returnCount) > pageSize {
		returnCount = int(pageSize)
	}
	hasMore := returnCount < itemCount
	if returnCount == itemCount {
		hasMore = hasMoreAfterWindow
	}
	return returnCount, hasMore
}

func (l *ListMyLikedVideosLogic) getLikedVideosListVersion(userID uint64) (int64, bool) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()

	version, err := l.svcCtx.RedisCli.Get(redisCtx, rediskey.LikeUserVideosListVersionKey(userID)).Int64()
	if err == nil {
		return version, true
	}
	if errors.Is(err, redis.Nil) {
		return 0, true
	}
	l.Errorf("get liked videos list version failed, user_id: %d, error: %v", userID, err)
	return 0, false
}

func (l *ListMyLikedVideosLogic) loadLikedVideosFirstPageCache(cacheKey string, expectedVersion int64, pageSize int64) (*interaction.ListMyLikedVideosResp, bool) {
	if pageSize <= 0 || pageSize > likedVideosFirstPageWindowSize {
		return nil, false
	}
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()

	data, err := l.svcCtx.RedisCli.Get(redisCtx, cacheKey).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			l.Errorf("get liked videos list cache failed, key: %s, error: %v", cacheKey, err)
		}
		return nil, false
	}

	var cached likedVideosListCache
	if err := json.Unmarshal(data, &cached); err != nil {
		l.Errorf("unmarshal liked videos list cache failed, key: %s, error: %v", cacheKey, err)
		return nil, false
	}
	if cached.Version != expectedVersion {
		return nil, false
	}
	if len(cached.LikedVideos) > int(likedVideosFirstPageWindowSize) {
		l.Errorf("unexpected liked videos first page cache size, key: %s, size: %d", cacheKey, len(cached.LikedVideos))
		return nil, false
	}
	if cached.HasMoreAfterWindow && len(cached.LikedVideos) != int(likedVideosFirstPageWindowSize) {
		l.Errorf("invalid liked videos first page cache window, key: %s, size: %d", cacheKey, len(cached.LikedVideos))
		return nil, false
	}
	for _, item := range cached.LikedVideos {
		if item.LikeID == 0 || item.VideoID == 0 || item.LikedAt <= 0 {
			l.Errorf("invalid liked videos first page cache item, key: %s", cacheKey)
			return nil, false
		}
	}

	returnCount, hasMore := likedVideosPageBounds(len(cached.LikedVideos), pageSize, cached.HasMoreAfterWindow)
	items := make([]*interaction.LikedVideoItem, 0, returnCount)
	for _, item := range cached.LikedVideos[:returnCount] {
		items = append(items, &interaction.LikedVideoItem{
			LikeId:  item.LikeID,
			VideoId: item.VideoID,
			LikedAt: item.LikedAt,
		})
	}
	var nextCursorCreatedAt int64
	var nextCursorLikeID uint64
	if hasMore && len(items) > 0 {
		last := items[len(items)-1]
		nextCursorCreatedAt = last.GetLikedAt()
		nextCursorLikeID = last.GetLikeId()
	}

	return &interaction.ListMyLikedVideosResp{
		LikedVideos:         items,
		NextCursorCreatedAt: nextCursorCreatedAt,
		NextCursorLikeId:    nextCursorLikeID,
		HasMore:             hasMore,
	}, true
}

func (l *ListMyLikedVideosLogic) saveLikedVideosFirstPageCache(userID uint64, cacheKey string, version int64, lockKey string, lockToken string, likes []model.Like, hasMoreAfterWindow bool) {
	if len(likes) > int(likedVideosFirstPageWindowSize) {
		likes = likes[:likedVideosFirstPageWindowSize]
	}
	cached := likedVideosListCache{
		Version:            version,
		LikedVideos:        make([]likedVideoItemCache, 0, len(likes)),
		HasMoreAfterWindow: hasMoreAfterWindow,
	}
	for _, item := range likes {
		cached.LikedVideos = append(cached.LikedVideos, likedVideoItemCache{
			LikeID:  item.ID,
			VideoID: item.VideoID,
			LikedAt: item.UpdatedAt.UnixMilli(),
		})
	}

	data, err := json.Marshal(cached)
	if err != nil {
		l.Errorf("marshal liked videos list cache failed, key: %s, error: %v", cacheKey, err)
		return
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	written, err := l.svcCtx.RedisCli.Eval(
		redisCtx,
		saveLikedVideosFirstPageCacheScript,
		[]string{rediskey.LikeUserVideosListVersionKey(userID), cacheKey, lockKey},
		strconv.FormatInt(version, 10),
		data,
		likedVideosListCacheTTL.Milliseconds(),
		lockToken,
	).Int64()
	if err != nil {
		l.Errorf("set liked videos list cache failed, key: %s, error: %v", cacheKey, err)
		return
	}
	if written == 0 {
		l.Infof("skip stale liked videos first page cache, version: %d", version)
	} else if written < 0 {
		l.Infof("skip liked videos first page cache without lock ownership, key: %s", cacheKey)
	}
}

func likedVideosListDBLoadKey(userID uint64, cursorCreatedAt int64, cursorLikeID uint64, pageSize int64) string {
	return fmt.Sprintf("user:%d:cursor_created_at:%d:cursor_like_id:%d:size:%d", userID, cursorCreatedAt, cursorLikeID, pageSize)
}

func (g *localLikedVideosListLoadGroup) Do(ctx context.Context, key string, fn func() ([]model.Like, bool, error)) ([]model.Like, bool, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*likedVideosListLoadCall)
	}
	if call, ok := g.calls[key]; ok {
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-call.done:
			return call.likes, call.hasMore, call.err
		}
	}

	call := &likedVideosListLoadCall{done: make(chan struct{})}
	g.calls[key] = call
	g.mu.Unlock()

	call.likes, call.hasMore, call.err = fn()
	close(call.done)

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return call.likes, call.hasMore, call.err
}

func (l *ListMyLikedVideosLogic) tryLockLikedVideosListCache(cacheKey string) (string, string, bool, error) {
	lockToken, err := randomHex(8)
	if err != nil {
		l.Errorf("generate liked videos list cache lock token failed, key: %s, error: %v", cacheKey, err)
		return "", "", false, err
	}

	lockKey := rediskey.LikeUserVideosFirstPageCacheBuildLockKey(cacheKey)
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	locked, err := l.svcCtx.RedisCli.SetNX(redisCtx, lockKey, lockToken, likedVideosListCacheLockTTL).Result()
	if err != nil {
		l.Errorf("lock liked videos list cache build failed, key: %s, error: %v", cacheKey, err)
		return "", "", false, err
	}
	return lockKey, lockToken, locked, nil
}

// 短轮询3次，每次50ms，每次都会去读缓存，如果超时则自己去查DB
func (l *ListMyLikedVideosLogic) waitAndReloadLikedVideosFirstPageCache(cacheKey string, version int64, pageSize int64) (*interaction.ListMyLikedVideosResp, bool) {
	for i := 0; i < likedVideosListCacheRetryAttempts; i++ {
		timer := time.NewTimer(likedVideosListCacheRetryDelay)
		select {
		case <-l.ctx.Done():
			timer.Stop()
			return nil, false
		case <-timer.C:
			if resp, hit := l.loadLikedVideosFirstPageCache(cacheKey, version, pageSize); hit {
				return resp, true
			}
		}
	}
	return nil, false
}

func (l *ListMyLikedVideosLogic) releaseLikedVideosListCacheLock(lockKey string, lockToken string) {
	if lockKey == "" || lockToken == "" {
		return
	}

	redisCtx, cancel := context.WithTimeout(context.WithoutCancel(l.ctx), commentRedisOpTimeout)
	defer cancel()
	if err := releaseRedisLock(redisCtx, l.svcCtx.RedisCli, lockKey, lockToken); err != nil {
		l.Errorf("release liked videos list cache lock failed, key: %s, error: %v", lockKey, err)
	}
}
