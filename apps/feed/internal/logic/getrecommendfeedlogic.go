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

type GetRecommendFeedLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetRecommendFeedLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetRecommendFeedLogic {
	return &GetRecommendFeedLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 推荐流：当前返回全局最新视频，未登录也可访问
func (l *GetRecommendFeedLogic) GetRecommendFeed(in *feed.GetRecommendFeedReq) (*feed.GetRecommendFeedResp, error) {
	// 推荐流允许游客访问，viewer_id=0 是合法值。Feed RPC 只返回候选视频
	viewerID := in.GetViewerId()
	cursorPublishedAt := in.GetCursorPublishedAt()
	cursorVideoID := in.GetCursorVideoId()

	if err := validateFeedCursor(cursorPublishedAt, cursorVideoID); err != nil {
		return nil, err
	}
	pageSize := normalizeFeedPageSize(l.svcCtx, in.GetPageSize())

	if err := ensureGlobalTimeline(l.ctx, l.svcCtx); err != nil {
		l.Errorf(
			"ensure global timeline failed, viewer_id:%d cursor_published_at:%d cursor_video_id:%d error:%v",
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
		rediskey.FeedGlobalTimelineKey(),
		cursorPublishedAt,
		cursorVideoID,
		pageSize,
	)
	if err != nil {
		l.Errorf(
			"load recommend feed failed, viewer_id:%d cursor_published_at:%d cursor_video_id:%d page_size:%d error:%v",
			viewerID,
			cursorPublishedAt,
			cursorVideoID,
			pageSize,
			err,
		)
		if status.Code(err) == codes.InvalidArgument {
			return nil, err
		}
		return nil, status.Error(codes.Unavailable, "推荐流暂时不可用，请稍后重试")
	}

	resp := &feed.GetRecommendFeedResp{
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
