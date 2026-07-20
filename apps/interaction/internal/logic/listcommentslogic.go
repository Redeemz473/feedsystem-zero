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
	"gorm.io/gorm"
)

const (
	defaultListCommentsPageSize      int64 = 20
	maxListCommentsPageSize          int64 = 100
	hotCommentListThreshold                = 1000
	coldCommentListThreshold               = 20
	commentListHotFirstPageCacheTTL        = 15 * time.Second
	commentListFirstPageCacheTTL           = 30 * time.Second
	commentListColdFirstPageCacheTTL       = time.Minute
	commentListHistoryPageCacheTTL         = 2 * time.Minute
	commentListCacheLockTTL                = 2 * time.Second
	commentListCacheRetryDelay             = 50 * time.Millisecond
	commentListCacheRetryAttempts          = 3
	maxCommentCursorFutureSkew             = 5 * time.Minute
)

type commentListCache struct {
	Version             int64              `json:"version"`
	Comments            []commentCacheItem `json:"comments"`
	NextCursorCreatedAt int64              `json:"next_cursor_created_at"`
	NextCursorCommentID uint64             `json:"next_cursor_comment_id"`
	HasMore             bool               `json:"has_more"`
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
	calls map[string]*commentListLoadCall
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

	pageSize := normalizeListCommentsPageSize(in.GetPageSize())

	video, err := l.loadNormalVideo(videoID)
	if err != nil {
		return nil, err
	}

	cacheKey := ""
	version, ok := l.getCommentListVersion(videoID)
	if ok {
		cacheKey = rediskey.CommentListCacheKey(videoID, version, commentListCursorKey(cursorCreatedAt, cursorCommentID), pageSize)
		if resp, hit := l.loadCommentListCache(cacheKey, version, viewerID, video.AuthorID); hit {
			return resp, nil
		}
	}

	//缓存未命中，拿锁
	var lockKey, lockToken string
	locked := false
	if cacheKey != "" {
		lockKey, lockToken, locked = l.tryLockCommentListCache(cacheKey)
		if !locked {
			if resp, hit := l.waitAndReloadCommentListCache(cacheKey, version, viewerID, video.AuthorID); hit {
				return resp, nil
			}
		}
	}
	if locked {
		//释放锁
		defer l.releaseCommentListCacheLock(lockKey, lockToken)
	}

	// 本地 SingleFlight 单机合并 DB 查询
	//第一个请求执行 DB 查询
	//同 key 并发请求阻塞等待，复用第一个请求的结果
	dbLoadKey := commentListDBLoadKey(videoID, cursorCreatedAt, cursorCommentID, pageSize)
	comments, hasMore, err := commentListLoadGroup.Do(l.ctx, dbLoadKey, func() ([]model.Comment, bool, error) {
		return l.loadCommentsFromDB(videoID, cursorCreatedAt, cursorCommentID, pageSize)
	})
	if err != nil {
		l.Errorf("list comments from db failed, video_id: %d, error: %v", videoID, err)
		return nil, status.Error(codes.Internal, "查询评论列表失败")
	}

	resp := buildListCommentsResp(comments, viewerID, video.AuthorID, hasMore)
	if cacheKey != "" {
		l.saveCommentListCache(cacheKey, version, video.ID, video.CommentsCount, cursorCreatedAt, cursorCommentID, resp)
	}

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

// 限制每一次查询的pagesize
func normalizeListCommentsPageSize(pageSize int64) int64 {
	if pageSize <= 0 {
		return defaultListCommentsPageSize
	}
	if pageSize > maxListCommentsPageSize {
		return maxListCommentsPageSize
	}
	return pageSize
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

func commentListCursorKey(cursorCreatedAt int64, cursorCommentID uint64) string {
	return fmt.Sprintf("created_at:%d:comment_id:%d", cursorCreatedAt, cursorCommentID)
}

func commentListDBLoadKey(videoID uint64, cursorCreatedAt int64, cursorCommentID uint64, pageSize int64) string {
	return fmt.Sprintf("video:%d:cursor:%s:size:%d", videoID, commentListCursorKey(cursorCreatedAt, cursorCommentID), pageSize)
}

func (g *localCommentListLoadGroup) Do(ctx context.Context, key string, fn func() ([]model.Comment, bool, error)) ([]model.Comment, bool, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*commentListLoadCall)
	}
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

// 读取评论的缓存
func (l *ListCommentsLogic) loadCommentListCache(cacheKey string, expectedVersion int64, viewerID uint64, videoAuthorID uint64) (*interaction.ListCommentsResp, bool) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()

	data, err := l.svcCtx.RedisCli.Get(redisCtx, cacheKey).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			l.Errorf("get comment list cache failed, key: %s, error: %v", cacheKey, err)
		}
		return nil, false
	}

	var cached commentListCache
	if err := json.Unmarshal(data, &cached); err != nil {
		l.Errorf("unmarshal comment list cache failed, key: %s, error: %v", cacheKey, err)
		return nil, false
	}
	//判断版本号是否相符
	if cached.Version != expectedVersion {
		return nil, false
	}

	items := make([]*interaction.CommentInfo, 0, len(cached.Comments))
	for _, item := range cached.Comments {
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

	return &interaction.ListCommentsResp{
		Comments:            items,
		NextCursorCreatedAt: cached.NextCursorCreatedAt,
		NextCursorCommentId: cached.NextCursorCommentID,
		HasMore:             cached.HasMore,
	}, true
}

// 写redis缓存
func (l *ListCommentsLogic) saveCommentListCache(cacheKey string, version int64, videoID uint64, videoCommentsCount int64, cursorCreatedAt int64, cursorCommentID uint64, resp *interaction.ListCommentsResp) {
	cached := commentListCache{
		Version:             version,
		Comments:            make([]commentCacheItem, 0, len(resp.GetComments())),
		NextCursorCreatedAt: resp.GetNextCursorCreatedAt(),
		NextCursorCommentID: resp.GetNextCursorCommentId(),
		HasMore:             resp.GetHasMore(),
	}
	for _, item := range resp.GetComments() {
		cached.Comments = append(cached.Comments, commentCacheItem{
			CommentID: item.GetCommentId(),
			VideoID:   item.GetVideoId(),
			UserID:    item.GetUserId(),
			Username:  item.GetUsername(),
			Content:   item.GetContent(),
			CreatedAt: item.GetCreatedAt(),
			UpdatedAt: item.GetUpdatedAt(),
		})
	}

	data, err := json.Marshal(cached)
	if err != nil {
		l.Errorf("marshal comment list cache failed, key: %s, error: %v", cacheKey, err)
		return
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	if err := l.svcCtx.RedisCli.Set(redisCtx, cacheKey, data, commentListCacheTTL(videoID, videoCommentsCount, cursorCreatedAt, cursorCommentID)).Err(); err != nil {
		l.Errorf("set comment list cache failed, key: %s, error: %v", cacheKey, err)
	}
}

// 抢锁
func (l *ListCommentsLogic) tryLockCommentListCache(cacheKey string) (string, string, bool) {
	lockToken, err := randomHex(8)
	if err != nil {
		l.Errorf("generate comment list cache lock token failed, key: %s, error: %v", cacheKey, err)
		return "", "", false
	}

	lockKey := rediskey.CommentListCacheBuildLockKey(cacheKey)
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	//分布式业务锁，控制并发请求，防止都去查DB
	locked, err := l.svcCtx.RedisCli.SetNX(redisCtx, lockKey, lockToken, commentListCacheLockTTL).Result()
	if err != nil {
		l.Errorf("lock comment list cache build failed, key: %s, error: %v", cacheKey, err)
		return "", "", false
	}
	return lockKey, lockToken, locked
}

// 没拿到锁的先休眠，醒了先去查redis，三次没等到自己去查DB
func (l *ListCommentsLogic) waitAndReloadCommentListCache(cacheKey string, version int64, viewerID uint64, videoAuthorID uint64) (*interaction.ListCommentsResp, bool) {
	for i := 0; i < commentListCacheRetryAttempts; i++ {
		timer := time.NewTimer(commentListCacheRetryDelay)
		select {
		case <-l.ctx.Done():
			timer.Stop()
			return nil, false
		case <-timer.C:
			if resp, hit := l.loadCommentListCache(cacheKey, version, viewerID, videoAuthorID); hit {
				return resp, true
			}
		}
	}
	return nil, false
}

func (l *ListCommentsLogic) releaseCommentListCacheLock(lockKey string, lockToken string) {
	if lockKey == "" || lockToken == "" {
		return
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	if err := releaseRedisLock(redisCtx, l.svcCtx.RedisCli, lockKey, lockToken); err != nil {
		l.Errorf("release comment list cache lock failed, key: %s, error: %v", lockKey, err)
	}
}

// 按热度分级
func commentListCacheTTL(videoID uint64, videoCommentsCount int64, cursorCreatedAt int64, cursorCommentID uint64) time.Duration {
	base := commentListFirstPageCacheTTL
	if cursorCreatedAt > 0 || cursorCommentID > 0 {
		base = commentListHistoryPageCacheTTL //如果是翻页的历史请求，统一
	} else if videoCommentsCount >= hotCommentListThreshold { //热度高，短缓存
		base = commentListHotFirstPageCacheTTL
	} else if videoCommentsCount <= coldCommentListThreshold { //热度低，长缓存
		base = commentListColdFirstPageCacheTTL
	}

	//随即偏移，防止大量缓存同时过期
	jitter := time.Duration((int64(videoID) + cursorCreatedAt + int64(cursorCommentID)) % 10)
	if jitter < 0 {
		jitter = -jitter
	}
	return base + jitter*time.Second
}
