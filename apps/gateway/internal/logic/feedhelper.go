package logic

import (
	"context"

	"feedsystem-zero/apps/feed/feedclient"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
)

// hydrateFeedVideos 保持 Feed 候选顺序，批量补齐视频实体与实时互动统计。
// 已被删除或下架但尚未来得及从 Timeline 清理的视频会被自然跳过。
func hydrateFeedVideos(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	viewerID uint64,
	items []*feedclient.FeedVideoItem,
) ([]types.VideoInfo, error) {
	videoIDs := make([]uint64, 0, len(items))
	for _, item := range items {
		if item.GetVideoId() != 0 {
			videoIDs = append(videoIDs, item.GetVideoId())
		}
	}
	videoMap, err := loadHTTPVideosByIDs(ctx, svcCtx.AccountRpc, svcCtx.VideoRpc, svcCtx.InteractionRpc, viewerID, videoIDs)
	if err != nil {
		return nil, err
	}

	videos := make([]types.VideoInfo, 0, len(items))
	for _, item := range items {
		video, ok := videoMap[item.GetVideoId()]
		if !ok {
			continue
		}
		videos = append(videos, video)
	}
	return videos, nil
}
