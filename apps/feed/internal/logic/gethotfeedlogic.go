package logic

import (
	"context"

	"feedsystem-zero/apps/feed/feed"
	"feedsystem-zero/apps/feed/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetHotFeedLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetHotFeedLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetHotFeedLogic {
	return &GetHotFeedLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 热榜：按固定分钟快照读取最近时间窗口内的热门视频，未登录也可访问
func (l *GetHotFeedLogic) GetHotFeed(in *feed.GetHotFeedReq) (*feed.GetHotFeedResp, error) {
	viewerID := in.GetViewerId()
	requestedSnapshotAt := in.GetSnapshotAt()
	offset := in.GetOffset()

	query, err := normalizeHotFeedQuery(l.svcCtx, requestedSnapshotAt, offset, in.GetPageSize())
	if err != nil {
		return nil, err
	}

	//缓存未命中时通过 SingleFlight + Redis 锁合并分钟窗口生成固定快照。
	if err := ensureHotRankSnapshot(l.ctx, l.svcCtx, query.SnapshotAt, query.Offset); err != nil {
		l.Errorf(
			"ensure hot rank snapshot failed, viewer_id:%d snapshot_at:%d offset:%d page_size:%d error:%v",
			viewerID, query.SnapshotAt, query.Offset, query.PageSize, err,
		)
		if code := status.Code(err); code == codes.InvalidArgument || code == codes.FailedPrecondition {
			return nil, err
		}
		return nil, status.Error(codes.Unavailable, "热榜暂时不可用，请稍后重试")
	}

	page, err := loadHotRankPage(l.ctx, l.svcCtx, query.SnapshotAt, query.Offset, query.PageSize)
	if err != nil {
		l.Errorf(
			"load hot rank page failed, viewer_id:%d snapshot_at:%d offset:%d page_size:%d error:%v",
			viewerID, query.SnapshotAt, query.Offset, query.PageSize, err,
		)
		return nil, status.Error(codes.Unavailable, "热榜暂时不可用，请稍后重试")
	}

	resp := &feed.GetHotFeedResp{
		Items:      page.Items,
		SnapshotAt: query.SnapshotAt,
		HasMore:    page.HasMore,
	}
	if page.HasMore {
		resp.NextOffset = query.Offset + int64(len(page.Items))
	}
	return resp, nil
}
