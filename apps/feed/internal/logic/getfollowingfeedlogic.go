package logic

import (
	"context"

	"feedsystem-zero/apps/feed/feed"
	"feedsystem-zero/apps/feed/internal/svc"

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

// GetFollowingFeed 返回 viewer 的关注流。
// 推拉结合：小 V 视频通过 inbox 推送；大 V（accounts.is_big_v = 1）视频由读侧按需拉取该作者的 outbox 并与 inbox 做归并合并，避免对大 V 的每次发布做扇出风暴。
// is_big_v 只升不降，因此掉粉的大 V 仍会被读侧合并 outbox，历史视频不会消失。
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

	// inbox 冷启动仍由 rpc 自身兜底：如果 timeline 未 ready，就从 MySQL
	// 重建只包含小 V 的快照；大 V 始终由后续 outbox 拉取路径补齐。
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

	page, err := loadFollowingFeedMerged(
		l.ctx,
		l.svcCtx,
		viewerID,
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

	// inbox TTL 续期，让活跃用户持续保有更长的 timeline 生命周期。
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
