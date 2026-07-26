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
	"gorm.io/gorm"
)

const (
	commentFirstPageWindowSize         int64 = 20
	defaultListCommentsPageSize              = commentFirstPageWindowSize
	maxListCommentsPageSize            int64 = 100
	hotCommentListThreshold                  = 1000
	coldCommentListThreshold                 = 20
	commentListHotFirstPageCacheTTL          = 15 * time.Second
	commentListFirstPageCacheTTL             = 30 * time.Second
	commentListColdFirstPageCacheTTL         = time.Minute
	commentFirstPageCacheLockTTL             = 2 * time.Second
	commentFirstPageCacheRetryDelay          = 50 * time.Millisecond
	commentFirstPageCacheRetryAttempts       = 3
	maxCommentCursorFutureSkew               = 5 * time.Minute
)

const saveCommentFirstPageCacheScript = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return 0
end
redis.call("SET", KEYS[2], ARGV[2], "PX", ARGV[3])
return 1
`

type commentFirstPageCache struct {
	Version            int64              `json:"version"`
	Comments           []commentCacheItem `json:"comments"`
	HasMoreAfterWindow bool               `json:"has_more_after_window"`
}

type commentCacheItem struct {
	CommentID uint64 `json:"comment_id"`
	VideoID   uint64 `json:"video_id"`
	UserID    uint64 `json:"user_id"`
	Username  string `json:"username"`
	Content   string `json:"content"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

var commentListLoadGroup localCommentListLoadGroup

type localCommentListLoadGroup struct {
	mu    sync.Mutex
	calls map[string]*commentListLoadCall //保存当前正在执行的查询
}

type commentListLoadCall struct {
	done     chan struct{}
	comments []model.Comment
	hasMore  bool
	err      error
}

type ListCommentsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListCommentsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListCommentsLogic {
	return &ListCommentsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListCommentsLogic) ListComments(in *interaction.ListCommentsReq) (*interaction.ListCommentsResp, error) {
	videoID := in.GetVideoId()
	if videoID == 0 {
		return nil, status.Error(codes.InvalidArgument, "视频ID不能为空")
	}

	viewerID := in.GetViewerId()
	cursorCreatedAt := in.GetCursorCreatedAt()
	cursorCommentID := in.GetCursorCommentId()
	if err := validateCommentListCursor(cursorCreatedAt, cursorCommentID); err != nil {
		return nil, err
	}

	pageSize, err := normalizeListCommentsPageSize(in.GetPageSize())
	if err != nil {
		return nil, err
	}

	video, err := l.loadNormalVideo(videoID)
	if err != nil {
		return nil, err
	}

	// Redis 只缓存固定 20 条的标准首页窗口：
	// 小于等于 20 条的首页请求从窗口中动态截取；
	// 大于 20 条或带游标的历史请求直接查询 MySQL。
	cacheable := isCommentFirstPageCacheable(cursorCreatedAt, cursorCommentID, pageSize)

	cacheKey := ""
	version := int64(0)
	if cacheable {
		if currentVersion, ok := l.getCommentListVersion(videoID); ok {
			version = currentVersion
			cacheKey = rediskey.CommentFirstPageCacheKey(videoID, version)
			if resp, hit := l.loadCommentFirstPageCache(cacheKey, version, videoID, viewerID, video.AuthorID, pageSize); hit {
				return resp, nil
			}
		}
	}

	// 首页缓存未命中时尝试拿构建锁；Redis 不可用时 cacheKey 为空，直接降级 MySQL。
	var lockKey, lockToken string
	useFixedWindow := false
	cacheWriteAllowed := false
	//前端查询首页时 cacheKey才不会为空
	if cacheKey != "" {
		var lockErr error
		var locked bool
		lockKey, lockToken, locked, lockErr = l.tryLockCommentFirstPageCache(cacheKey)
		switch {
		case lockErr != nil:
			// Redis 已不可用，后续直接按请求大小查询 MySQL，不再等待或回写缓存。
			cacheKey = ""
		case locked:
			useFixedWindow = true
			cacheWriteAllowed = true
		default:
			if resp, hit := l.waitAndReloadCommentFirstPageCache(cacheKey, version, videoID, viewerID, video.AuthorID, pageSize); hit {
				return resp, nil
			}
			// 有界等待后仍未命中时允许回源查mysql，但未持有锁的请求不能写缓存。
			useFixedWindow = true
		}
	}
	if cacheWriteAllowed {
		defer l.releaseCommentFirstPageCacheLock(lockKey, lockToken)
	}

	// 缓存构建固定查询 21 条并保存前 20 条；非缓存请求按自身 pageSize 查询。
	dbPageSize := pageSize
	if useFixedWindow {
		dbPageSize = commentFirstPageWindowSize
	}

	// 本地 SingleFlight 合并相同查询。缓存构建 key 包含版本，避免新版本请求
	// 复用旧版本正在执行的数据库快照。
	//判断两个mysql查询是否相同
	dbLoadKey := commentListDBLoadKey(videoID, cursorCreatedAt, cursorCommentID, dbPageSize)
	if useFixedWindow {
		//如果是首页查询，涉及到版本号
		dbLoadKey = "cache:" + cacheKey
	}
	comments, hasMore, err := commentListLoadGroup.Do(l.ctx, dbLoadKey, func() ([]model.Comment, bool, error) {
		return l.loadCommentsFromDB(videoID, cursorCreatedAt, cursorCommentID, dbPageSize)
	})
	if err != nil {
		if l.ctx.Err() != nil {
			return nil, status.FromContextError(l.ctx.Err()).Err()
		}
		l.Errorf("list comments from db failed, video_id: %d, error: %v", videoID, err)
		return nil, status.Error(codes.Internal, "查询评论列表失败")
	}

	if cacheWriteAllowed {
		l.saveCommentFirstPageCache(cacheKey, version, video.ID, video.CommentsCount, comments, hasMore)
	}

	responseComments, responseHasMore := selectCommentPage(comments, pageSize, hasMore)
	resp := buildListCommentsResp(responseComments, viewerID, video.AuthorID, responseHasMore)
	return resp, nil
}

// 从DB查video信息
func (l *ListCommentsLogic) loadNormalVideo(videoID uint64) (model.Video, error) {
	var video model.Video
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Where("id = ? AND status = ? AND deleted_at IS NULL", videoID, model.VideoStatusNormal).
		First(&video).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.Video{}, status.Error(codes.NotFound, "视频不存在或已删除")
		}
		l.Errorf("get video failed, video_id: %d, error: %v", videoID, err)
		return model.Video{}, status.Error(codes.Internal, "查询视频失败")
	}
	return video, nil
}

// 判断游标时间是否合理
func validateCommentListCursor(cursorCreatedAt int64, cursorCommentID uint64) error {
	if cursorCreatedAt < 0 {
		return status.Error(codes.InvalidArgument, "游标时间不能小于0")
	}
	if cursorCreatedAt > time.Now().Add(maxCommentCursorFutureSkew).UnixMilli() {
		return status.Error(codes.InvalidArgument, "游标时间不能超过当前时间")
	}
	if cursorCreatedAt == 0 && cursorCommentID == 0 {
		return nil
	}
	if cursorCreatedAt <= 0 || cursorCommentID == 0 {
		return status.Error(codes.InvalidArgument, "游标参数不完整")
	}
	return nil
}

// normalizeListCommentsPageSize 统一评论列表页大小。
func normalizeListCommentsPageSize(pageSize int64) (int64, error) {
	if pageSize < 0 {
		return 0, status.Error(codes.InvalidArgument, "page_size不能为负数")
	}
	if pageSize == 0 {
		return defaultListCommentsPageSize, nil
	}
	if pageSize > maxListCommentsPageSize {
		return maxListCommentsPageSize, nil
	}
	return pageSize, nil
}

// 判断是否是缓存的首页查询
func isCommentFirstPageCacheable(cursorCreatedAt int64, cursorCommentID uint64, pageSize int64) bool {
	return cursorCreatedAt == 0 &&
		cursorCommentID == 0 &&
		pageSize > 0 &&
		pageSize <= commentFirstPageWindowSize
}

// 真正从DB中取评论
func (l *ListCommentsLogic) loadCommentsFromDB(videoID uint64, cursorCreatedAt int64, cursorCommentID uint64, pageSize int64) ([]model.Comment, bool, error) {
	query := l.svcCtx.GormDB.WithContext(l.ctx).
		Where("video_id = ? AND status = ? AND deleted_at IS NULL", videoID, model.CommentStatusNormal)
	if cursorCreatedAt > 0 && cursorCommentID > 0 {
		// MySQL DSN 当前使用 loc=Local，游标时间也显式转成本地时区，避免 DATETIME 比较出现时区偏移。
		cursorTime := time.UnixMilli(cursorCreatedAt).Local()
		query = query.Where(
			"(created_at < ? OR (created_at = ? AND id < ?))",
			cursorTime,
			cursorTime,
			cursorCommentID,
		)
	}

	comments := make([]model.Comment, 0, pageSize+1)
	if err := query.
		Order("created_at DESC").
		Order("id DESC").
		Limit(int(pageSize + 1)).
		Find(&comments).Error; err != nil {
		return nil, false, err
	}

	hasMore := int64(len(comments)) > pageSize
	if hasMore {
		comments = comments[:pageSize]
	}
	return comments, hasMore, nil
}

// 组装响应
func buildListCommentsResp(comments []model.Comment, viewerID uint64, videoAuthorID uint64, hasMore bool) *interaction.ListCommentsResp {
	items := make([]*interaction.CommentInfo, 0, len(comments))
	for _, comment := range comments {
		items = append(items, buildCommentInfo(comment, viewerID, videoAuthorID))
	}

	var nextCursorCreatedAt int64
	var nextCursorCommentID uint64
	if hasMore && len(comments) > 0 {
		last := comments[len(comments)-1]
		nextCursorCreatedAt = last.CreatedAt.UnixMilli()
		nextCursorCommentID = last.ID
	}

	return &interaction.ListCommentsResp{
		Comments:            items,
		NextCursorCreatedAt: nextCursorCreatedAt,
		NextCursorCommentId: nextCursorCommentID,
		HasMore:             hasMore,
	}
}

func buildCommentInfo(comment model.Comment, viewerID uint64, videoAuthorID uint64) *interaction.CommentInfo {
	return &interaction.CommentInfo{
		CommentId: comment.ID,
		VideoId:   comment.VideoID,
		UserId:    comment.UserID,
		Username:  comment.Username,
		Content:   comment.Content,
		CreatedAt: comment.CreatedAt.UnixMilli(),
		UpdatedAt: comment.UpdatedAt.UnixMilli(),
		CanDelete: viewerID != 0 && (viewerID == comment.UserID || viewerID == videoAuthorID),
	}
}

// selectCommentPage 从固定窗口或普通 DB 查询结果中截取当前请求需要的数量，
// 并根据窗口内剩余数据和窗口外标记动态计算 has_more。
func selectCommentPage(comments []model.Comment, pageSize int64, hasMoreAfterWindow bool) ([]model.Comment, bool) {
	returnCount, hasMore := commentPageBounds(len(comments), pageSize, hasMoreAfterWindow)
	return comments[:returnCount], hasMore
}

func commentPageBounds(itemCount int, pageSize int64, hasMoreAfterWindow bool) (int, bool) {
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

func commentListDBLoadKey(videoID uint64, cursorCreatedAt int64, cursorCommentID uint64, pageSize int64) string {
	return fmt.Sprintf(
		"video:%d:cursor:created_at:%d:comment_id:%d:size:%d",
		videoID,
		cursorCreatedAt,
		cursorCommentID,
		pageSize,
	)
}

func (g *localCommentListLoadGroup) Do(ctx context.Context, key string, fn func() ([]model.Comment, bool, error)) ([]model.Comment, bool, error) {
	//加锁保证相同的key只有一个请求查询
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*commentListLoadCall)
	}
	//判断是否已经有另一个groutine正在执行同一个查询
	if call, ok := g.calls[key]; ok {
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-call.done:
			return call.comments, call.hasMore, call.err
		}
	}

	call := &commentListLoadCall{done: make(chan struct{})}
	g.calls[key] = call
	g.mu.Unlock()

	call.comments, call.hasMore, call.err = fn()
	close(call.done)

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return call.comments, call.hasMore, call.err
}

// 获取评论的版本号
func (l *ListCommentsLogic) getCommentListVersion(videoID uint64) (int64, bool) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()

	key := rediskey.CommentListVersionKey(videoID)
	version, err := l.svcCtx.RedisCli.Get(redisCtx, key).Int64()
	if err == nil {
		return version, true
	}
	if errors.Is(err, redis.Nil) {
		initVersion := newCommentListVersion()
		//没有版本号则抢锁初始化
		ok, err := l.svcCtx.RedisCli.SetNX(redisCtx, key, initVersion, 0).Result()
		if err != nil {
			l.Errorf("init comment list version failed, video_id: %d, error: %v", videoID, err)
			return 0, false
		}
		if ok {
			return initVersion, true
		}
		//没抢到的返回赢家设置的版本号
		version, err = l.svcCtx.RedisCli.Get(redisCtx, key).Int64()
		if err == nil {
			return version, true
		}
		l.Errorf("get comment list version after init race failed, video_id: %d, error: %v", videoID, err)
		return 0, false
	}
	l.Errorf("get comment list version failed, video_id: %d, error: %v", videoID, err)
	return 0, false
}

// loadCommentFirstPageCache 读取固定首页窗口，并按当前 pageSize 动态截取。
func (l *ListCommentsLogic) loadCommentFirstPageCache(
	cacheKey string,
	expectedVersion int64,
	videoID uint64,
	viewerID uint64,
	videoAuthorID uint64,
	pageSize int64,
) (*interaction.ListCommentsResp, bool) {
	if pageSize <= 0 || pageSize > commentFirstPageWindowSize {
		return nil, false
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()

	data, err := l.svcCtx.RedisCli.Get(redisCtx, cacheKey).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			l.Errorf("get comment list cache failed, key: %s, error: %v", cacheKey, err)
		}
		return nil, false
	}

	var cached commentFirstPageCache
	if err := json.Unmarshal(data, &cached); err != nil {
		l.Errorf("unmarshal comment list cache failed, key: %s, error: %v", cacheKey, err)
		return nil, false
	}
	if cached.Version != expectedVersion {
		return nil, false
	}
	if len(cached.Comments) > int(commentFirstPageWindowSize) {
		l.Errorf("unexpected comment first page cache size, key: %s, size: %d", cacheKey, len(cached.Comments))
		return nil, false
	}
	if cached.HasMoreAfterWindow && len(cached.Comments) != int(commentFirstPageWindowSize) {
		l.Errorf("invalid comment first page cache window, key: %s, size: %d", cacheKey, len(cached.Comments))
		return nil, false
	}
	for _, item := range cached.Comments {
		if item.CommentID == 0 || item.UserID == 0 || item.VideoID != videoID {
			l.Errorf("invalid comment first page cache item, key: %s", cacheKey)
			return nil, false
		}
	}

	returnCount, hasMore := commentPageBounds(len(cached.Comments), pageSize, cached.HasMoreAfterWindow)
	items := make([]*interaction.CommentInfo, 0, returnCount)
	for _, item := range cached.Comments[:returnCount] {
		items = append(items, &interaction.CommentInfo{
			CommentId: item.CommentID,
			VideoId:   item.VideoID,
			UserId:    item.UserID,
			Username:  item.Username,
			Content:   item.Content,
			CreatedAt: item.CreatedAt,
			UpdatedAt: item.UpdatedAt,
			CanDelete: viewerID != 0 && (viewerID == item.UserID || viewerID == videoAuthorID),
		})
	}

	var nextCursorCreatedAt int64
	var nextCursorCommentID uint64
	if hasMore && len(items) > 0 {
		last := items[len(items)-1]
		nextCursorCreatedAt = last.GetCreatedAt()
		nextCursorCommentID = last.GetCommentId()
	}

	return &interaction.ListCommentsResp{
		Comments:            items,
		NextCursorCreatedAt: nextCursorCreatedAt,
		NextCursorCommentId: nextCursorCommentID,
		HasMore:             hasMore,
	}, true
}

// saveCommentFirstPageCache 原子校验版本并写入固定首页窗口。
// 若查询期间发生评论写入导致版本变化，本次旧快照不会写入 Redis。
func (l *ListCommentsLogic) saveCommentFirstPageCache(
	cacheKey string,
	version int64,
	videoID uint64,
	videoCommentsCount int64,
	comments []model.Comment,
	hasMoreAfterWindow bool,
) {
	if len(comments) > int(commentFirstPageWindowSize) {
		comments = comments[:commentFirstPageWindowSize]
	}

	cached := commentFirstPageCache{
		Version:            version,
		Comments:           make([]commentCacheItem, 0, len(comments)),
		HasMoreAfterWindow: hasMoreAfterWindow,
	}
	for _, item := range comments {
		cached.Comments = append(cached.Comments, commentCacheItem{
			CommentID: item.ID,
			VideoID:   item.VideoID,
			UserID:    item.UserID,
			Username:  item.Username,
			Content:   item.Content,
			CreatedAt: item.CreatedAt.UnixMilli(),
			UpdatedAt: item.UpdatedAt.UnixMilli(),
		})
	}

	data, err := json.Marshal(cached)
	if err != nil {
		l.Errorf("marshal comment list cache failed, key: %s, error: %v", cacheKey, err)
		return
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	ttl := commentFirstPageCacheTTL(videoID, videoCommentsCount)
	written, err := l.svcCtx.RedisCli.Eval(
		redisCtx,
		saveCommentFirstPageCacheScript,
		[]string{rediskey.CommentListVersionKey(videoID), cacheKey},
		strconv.FormatInt(version, 10),
		data,
		ttl.Milliseconds(),
	).Int64()
	if err != nil {
		l.Errorf("set comment first page cache failed, key: %s, error: %v", cacheKey, err)
		return
	}
	if written == 0 {
		l.Infof("skip stale comment first page cache, video_id: %d, version: %d", videoID, version)
	}
}

// tryLockCommentFirstPageCache 区分“锁被其他请求持有”和“Redis 操作失败”。
func (l *ListCommentsLogic) tryLockCommentFirstPageCache(cacheKey string) (string, string, bool, error) {
	lockToken, err := randomHex(8)
	if err != nil {
		l.Errorf("generate comment list cache lock token failed, key: %s, error: %v", cacheKey, err)
		return "", "", false, err
	}

	lockKey := rediskey.CommentFirstPageCacheBuildLockKey(cacheKey)
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	//分布式业务锁，控制并发请求，防止都去查DB
	locked, err := l.svcCtx.RedisCli.SetNX(redisCtx, lockKey, lockToken, commentFirstPageCacheLockTTL).Result()
	if err != nil {
		l.Errorf("lock comment list cache build failed, key: %s, error: %v", cacheKey, err)
		return "", "", false, err
	}
	return lockKey, lockToken, locked, nil
}

// 没拿到锁的请求进行有限次数等待；仍未命中时自行回源，不能无限阻塞。
func (l *ListCommentsLogic) waitAndReloadCommentFirstPageCache(
	cacheKey string,
	version int64,
	videoID uint64,
	viewerID uint64,
	videoAuthorID uint64,
	pageSize int64,
) (*interaction.ListCommentsResp, bool) {
	for i := 0; i < commentFirstPageCacheRetryAttempts; i++ {
		timer := time.NewTimer(commentFirstPageCacheRetryDelay)
		select {
		case <-l.ctx.Done():
			timer.Stop()
			return nil, false
		case <-timer.C:
			if resp, hit := l.loadCommentFirstPageCache(cacheKey, version, videoID, viewerID, videoAuthorID, pageSize); hit {
				return resp, true
			}
		}
	}
	return nil, false
}

func (l *ListCommentsLogic) releaseCommentFirstPageCacheLock(lockKey string, lockToken string) {
	if lockKey == "" || lockToken == "" {
		return
	}

	redisCtx, cancel := context.WithTimeout(context.WithoutCancel(l.ctx), commentRedisOpTimeout)
	defer cancel()
	if err := releaseRedisLock(redisCtx, l.svcCtx.RedisCli, lockKey, lockToken); err != nil {
		l.Errorf("release comment list cache lock failed, key: %s, error: %v", lockKey, err)
	}
}

// 按热度分级
func commentFirstPageCacheTTL(videoID uint64, videoCommentsCount int64) time.Duration {
	base := commentListFirstPageCacheTTL
	if videoCommentsCount >= hotCommentListThreshold {
		base = commentListHotFirstPageCacheTTL
	} else if videoCommentsCount <= coldCommentListThreshold {
		base = commentListColdFirstPageCacheTTL
	}

	// 按视频 ID 做稳定抖动，分散不同视频首页缓存的过期时间。
	jitter := time.Duration(videoID % 10)
	return base + jitter*time.Second
}
