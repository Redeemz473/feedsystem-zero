package logic

import (
	"context"

	"feedsystem-zero/apps/feed/feed"
	"feedsystem-zero/apps/feed/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
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
	// 你只需要在这里完成以下编排，底层 Redis/MySQL/并发构建能力已经在 feedhelper.go：
	//  1. 校验 viewer_id>0。该值必须由 gateway 从 JWT 解析后填入，不能信任前端用户ID；
	//  2. 调用 validateFeedCursor 校验两个游标必须同时为空或同时有效；
	//  3. 调用 normalizeFeedPageSize 得到默认20、最大50的 pageSize；
	//  4. 调用 ensureFollowingTimeline。首次访问或 Timeline 过期时，它会通过分布式锁
	//     从 MySQL 的 follows+videos 构建完整快照，并用版本号处理与 Kafka fanout 的并发；
	//  5. 调用 loadTimelinePage，key 使用 rediskey.FeedTimelineKey(viewerID)；
	//  6. 读取成功后调用 refreshFollowingTimelineTTL，为活跃用户延长 Timeline、Ready和Version；
	//  7. 把 page.Items、page.HasMore 和最后一项的复合游标写入响应；空列表正常返回，
	//     是否降级到推荐流应由 gateway 决定，Feed RPC 不要偷偷混入推荐视频；
	//  8. 底层错误要记录 viewer_id 与游标，对外返回稳定的 gRPC 状态码。

	return &feed.GetFollowingFeedResp{}, nil
}
