package logic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	defaultListMyLikedVideosPageSize  int64 = 20
	maxListMyLikedVideosPageSize      int64 = 50
	likedVideosListCacheTTL                 = 30 * time.Second
	likedVideosListCacheLockTTL             = 2 * time.Second
	likedVideosListCacheRetryDelay          = 50 * time.Millisecond
	likedVideosListCacheRetryAttempts       = 3
	maxLikedVideosCursorFutureSkew          = 5 * time.Minute
)

type likedVideosListCache struct {
	LikedVideos         []likedVideoItemCache `json:"liked_videos"`
	NextCursorCreatedAt int64                 `json:"next_cursor_created_at"`
	NextCursorLikeID    uint64                `json:"next_cursor_like_id"`
	HasMore             bool                  `json:"has_more"`
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
	// 1. 校验 user_id 不能为 0；page_size 设置默认值和上限，例如默认 20、最大 50。
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

	// 2. Redis 只缓存分页结果，不直接用 LikeUserVideosKey 做分页来源；普通 Set 没有时间顺序。
	cacheKey := ""
	if version, ok := l.getLikedVideosListVersion(userID); ok {
		cacheKey = rediskey.LikeUserVideosPageCacheKey(userID, version, cursorCreatedAt, cursorLikeID, pageSize)
		if resp, hit := l.loadLikedVideosListCache(cacheKey); hit {
			return resp, nil
		}
	}

	var lockKey, lockToken string
	locked := false
	if cacheKey != "" {
		lockKey, lockToken, locked = l.tryLockLikedVideosListCache(cacheKey)
		if !locked {
			if resp, hit := l.waitAndReloadLikedVideosListCache(cacheKey); hit {
				return resp, nil
			}
		}
	}
	if locked {
		defer l.releaseLikedVideosListCacheLock(lockKey, lockToken)
	}

	dbLoadKey := likedVideosListDBLoadKey(userID, cursorCreatedAt, cursorLikeID, pageSize)
	likes, hasMore, err := likedVideosListLoadGroup.Do(l.ctx, dbLoadKey, func() ([]model.Like, bool, error) {
		return l.loadLikedVideosFromDB(userID, cursorCreatedAt, cursorLikeID, pageSize)
	})
	if err != nil {
		l.Errorf("list user liked videos failed, user_id: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "获取点赞视频失败")
	}

	resp := buildListMyLikedVideosResp(likes, hasMore)
	if cacheKey != "" {
		l.saveLikedVideosListCache(cacheKey, resp)
	}

	return resp, nil
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

	// 4. 排序使用 updated_at DESC, id DESC，并多查一条判断 has_more。
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

func (l *ListMyLikedVideosLogic) loadLikedVideosListCache(cacheKey string) (*interaction.ListMyLikedVideosResp, bool) {
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

	items := make([]*interaction.LikedVideoItem, 0, len(cached.LikedVideos))
	for _, item := range cached.LikedVideos {
		items = append(items, &interaction.LikedVideoItem{
			LikeId:  item.LikeID,
			VideoId: item.VideoID,
			LikedAt: item.LikedAt,
		})
	}

	return &interaction.ListMyLikedVideosResp{
		LikedVideos:         items,
		NextCursorCreatedAt: cached.NextCursorCreatedAt,
		NextCursorLikeId:    cached.NextCursorLikeID,
		HasMore:             cached.HasMore,
	}, true
}

func (l *ListMyLikedVideosLogic) saveLikedVideosListCache(cacheKey string, resp *interaction.ListMyLikedVideosResp) {
	cached := likedVideosListCache{
		LikedVideos:         make([]likedVideoItemCache, 0, len(resp.GetLikedVideos())),
		NextCursorCreatedAt: resp.GetNextCursorCreatedAt(),
		NextCursorLikeID:    resp.GetNextCursorLikeId(),
		HasMore:             resp.GetHasMore(),
	}
	for _, item := range resp.GetLikedVideos() {
		cached.LikedVideos = append(cached.LikedVideos, likedVideoItemCache{
			LikeID:  item.GetLikeId(),
			VideoID: item.GetVideoId(),
			LikedAt: item.GetLikedAt(),
		})
	}

	data, err := json.Marshal(cached)
	if err != nil {
		l.Errorf("marshal liked videos list cache failed, key: %s, error: %v", cacheKey, err)
		return
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	if err := l.svcCtx.RedisCli.Set(redisCtx, cacheKey, data, likedVideosListCacheTTL).Err(); err != nil {
		l.Errorf("set liked videos list cache failed, key: %s, error: %v", cacheKey, err)
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

func (l *ListMyLikedVideosLogic) tryLockLikedVideosListCache(cacheKey string) (string, string, bool) {
	lockToken, err := randomHex(8)
	if err != nil {
		l.Errorf("generate liked videos list cache lock token failed, key: %s, error: %v", cacheKey, err)
		return "", "", false
	}

	lockKey := rediskey.LikeUserVideosPageCacheBuildLockKey(cacheKey)
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	locked, err := l.svcCtx.RedisCli.SetNX(redisCtx, lockKey, lockToken, likedVideosListCacheLockTTL).Result()
	if err != nil {
		l.Errorf("lock liked videos list cache build failed, key: %s, error: %v", cacheKey, err)
		return "", "", false
	}
	return lockKey, lockToken, locked
}

func (l *ListMyLikedVideosLogic) waitAndReloadLikedVideosListCache(cacheKey string) (*interaction.ListMyLikedVideosResp, bool) {
	for i := 0; i < likedVideosListCacheRetryAttempts; i++ {
		timer := time.NewTimer(likedVideosListCacheRetryDelay)
		select {
		case <-l.ctx.Done():
			timer.Stop()
			return nil, false
		case <-timer.C:
			if resp, hit := l.loadLikedVideosListCache(cacheKey); hit {
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

	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	if err := releaseRedisLock(redisCtx, l.svcCtx.RedisCli, lockKey, lockToken); err != nil {
		l.Errorf("release liked videos list cache lock failed, key: %s, error: %v", lockKey, err)
	}
}
