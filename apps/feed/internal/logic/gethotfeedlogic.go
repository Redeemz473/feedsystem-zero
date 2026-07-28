package logic

import (
	"context"

	"feedsystem-zero/apps/feed/feed"
	"feedsystem-zero/apps/feed/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
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

// 热榜：按固定分钟快照读取最近时间窗口内的热门视频
func (l *GetHotFeedLogic) GetHotFeed(in *feed.GetHotFeedReq) (*feed.GetHotFeedResp, error) {
	// HotRank Job 已负责把互动事件写成 UTC 分钟窗口，这里只需串联 helper：
	//  1. query, err := normalizeHotFeedQuery(l.svcCtx, in.GetSnapshotAt(),
	//     in.GetOffset(), in.GetPageSize())
	//     该函数会完成分钟规范化、快照时效、offset 上限和翻页快照校验；
	//  2. ensureHotRankSnapshot(l.ctx, l.svcCtx, query.SnapshotAt, query.Offset)
	//     缓存未命中时会通过 SingleFlight + Redis 锁合并分钟窗口并生成固定快照；
	//  3. page, err := loadHotRankPage(l.ctx, l.svcCtx, query.SnapshotAt,
	//     query.Offset, query.PageSize)
	//     helper 已完成 ZREVRANGE、脏 member 清理、rank 计算和 has_more 判断；
	//  4. 组装 GetHotFeedResp：
	//     Items=page.Items，SnapshotAt=query.SnapshotAt，
	//     NextOffset=query.Offset+int64(len(page.Items))，HasMore=page.HasMore；
	//  5. Feed RPC 只返回候选视频ID、热度分和排名。视频详情、作者资料以及
	//     当前访问者的点赞状态由 gateway 批量聚合，不在这里跨服务查询。

	return &feed.GetHotFeedResp{}, nil
}
