package logic

import (
	"context"

	"feedsystem-zero/apps/feed/feed"
	"feedsystem-zero/apps/feed/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
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
	// 你只需要在这里完成以下编排，底层 Redis/MySQL/并发构建能力已经在 feedhelper.go：
	//  1. 推荐流允许未登录，因此 viewer_id=0 合法，不需要校验登录态；
	//  2. 调用 validateFeedCursor 校验两个游标必须同时为空或同时有效；
	//  3. 调用 normalizeFeedPageSize 得到默认20、最大50的 pageSize；
	//  4. 调用 ensureGlobalTimeline。Redis 冷启动时它会通过分布式锁从 MySQL
	//     构建全局最新视频快照，并通过版本校验防止覆盖并发发布/删除事件；
	//  5. 调用 loadTimelinePage，key 使用 rediskey.FeedGlobalTimelineKey()；
	//  6. 把 page.Items 和 page.HasMore 写入响应。Items 非空时，使用最后一项的
	//     PublishedAt/VideoId 作为 next_cursor_published_at/next_cursor_video_id；
	//  7. 底层错误要记录包含游标的日志，对外返回 codes.Unavailable 或 codes.Internal，
	//     不要把 Redis/MySQL 原始错误直接暴露给客户端。

	return &feed.GetRecommendFeedResp{}, nil
}
