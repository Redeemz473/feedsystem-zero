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
	// 等 job/hotrank 完成后，按以下流程实现：
	//  1. 校验 snapshot_at、offset 和 page_size，offset 不能为负数，
	//     page_size 使用统一函数规范为默认20、最大50；
	//  2. snapshot_at=0 时固定为当前分钟，否则规范化到对应分钟边界。
	//     后续分页必须继续使用首次响应返回的 snapshot_at；
	//  3. 使用 snapshot_at 对应的 Redis 聚合榜单 Key。缓存不存在时，通过分布式锁
	//     合并最近一段时间的分钟热榜，并给聚合结果设置较短 TTL；
	//  4. 通过 ZREVRANGE WITHSCORES 按 offset 读取 pageSize+1 条，
	//     多取的一条只用于判断 has_more；
	//  5. 将结果转换为 HotFeedVideoItem，rank=offset+当前下标+1，
	//     next_offset=offset+实际返回数量；
	//  6. Feed RPC 只返回候选视频ID、热度分和排名。视频详情、作者资料以及
	//     当前访问者的点赞状态由 gateway 批量聚合，不在这里跨服务查询。

	return &feed.GetHotFeedResp{}, nil
}
