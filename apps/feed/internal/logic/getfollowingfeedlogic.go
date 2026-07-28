package logic

import (
	"context"

	"feedsystem-zero/apps/feed/feed"
	"feedsystem-zero/apps/feed/internal/svc"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetFollowingFeedLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetFollowingFeedLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFollowingFeedLogic {
	return &GetFollowingFeedLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 关注流：viewer 关注的所有作者的最新视频，按发布时间倒序
func (l *GetFollowingFeedLogic) GetFollowingFeed(in *feed.GetFollowingFeedReq) (*feed.GetFollowingFeedResp, error) {
	viewerID := in.GetViewerId()
	if viewerID == 0 {
		return nil, status.Error(codes.Unauthenticated, "用户未登录")
	}

	cursorPublishedAt := in.GetCursorPublishedAt()
	cursorVideoID := in.GetCursorVideoId()
	if err := validateFeedCursor(cursorPublishedAt, cursorVideoID); err != nil {
		return nil, err
	}
	pageSize := normalizeFeedPageSize(l.svcCtx, in.GetPageSize())

	// 用户首次访问或 Timeline 过期时，通过本地 SingleFlight 和 Redis 分布式锁
	// 从 MySQL 的 follows + videos 构建快照，并用版本号避免覆盖并发写入。
	if err := ensureFollowingTimeline(l.ctx, l.svcCtx, viewerID); err != nil {
		l.Errorf(
			"ensure following timeline failed, viewer_id:%d cursor_published_at:%d cursor_video_id:%d error:%v",
			viewerID,
			cursorPublishedAt,
			cursorVideoID,
			err,
		)
		return nil, err
	}

	page, err := loadTimelinePage(
		l.ctx,
		l.svcCtx,
		rediskey.FeedTimelineKey(viewerID),
		cursorPublishedAt,
		cursorVideoID,
		pageSize,
	)
	if err != nil {
		l.Errorf(
			"load following feed failed, viewer_id:%d cursor_published_at:%d cursor_video_id:%d page_size:%d error:%v",
			viewerID,
			cursorPublishedAt,
			cursorVideoID,
			pageSize,
			err,
		)
		if status.Code(err) == codes.InvalidArgument {
			return nil, err
		}
		return nil, status.Error(codes.Unavailable, "关注流暂时不可用，请稍后重试")
	}

	// TTL续期
	refreshFollowingTimelineTTL(l.ctx, l.svcCtx, viewerID)

	resp := &feed.GetFollowingFeedResp{
		Items:   page.Items,
		HasMore: page.HasMore,
	}
	if len(page.Items) > 0 {
		lastItem := page.Items[len(page.Items)-1]
		resp.NextCursorPublishedAt = lastItem.GetPublishedAt()
		resp.NextCursorVideoId = lastItem.GetVideoId()
	}

	return resp, nil
}
