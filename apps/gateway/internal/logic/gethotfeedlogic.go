// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/feed/feedclient"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetHotFeedLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetHotFeedLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetHotFeedLogic {
	return &GetHotFeedLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetHotFeedLogic) GetHotFeed(req *types.GetHotFeedReq) (resp *types.GetHotFeedResp, err error) {
	// 未登录也可访问，viewerID=0 表示游客。
	viewerID := optionalUserIDFromCtx(l.ctx)

	rpcResp, err := l.svcCtx.FeedRpc.GetHotFeed(l.ctx, &feedclient.GetHotFeedReq{
		ViewerId:   viewerID,
		SnapshotAt: req.Snapshotat,
		Offset:     req.Offset,
		PageSize:   req.Pagesize,
	})
	if err != nil {
		return nil, err
	}

	rpcItems := rpcResp.GetItems()
	videoIDs := make([]uint64, 0, len(rpcItems))
	for _, it := range rpcItems {
		if it.GetVideoId() != 0 {
			videoIDs = append(videoIDs, it.GetVideoId())
		}
	}
	videoMap, err := loadHTTPVideosByIDs(l.ctx, l.svcCtx.VideoRpc, l.svcCtx.InteractionRpc, viewerID, videoIDs)
	if err != nil {
		return nil, err
	}

	items := make([]types.HotFeedVideo, 0, len(rpcItems))
	for _, it := range rpcItems {
		video, ok := videoMap[it.GetVideoId()]
		if !ok {
			continue
		}
		items = append(items, types.HotFeedVideo{
			Video:    video,
			Hotscore: it.GetHotScore(),
			Rank:     it.GetRank(),
		})
	}

	return &types.GetHotFeedResp{
		Items:      items,
		Snapshotat: rpcResp.GetSnapshotAt(),
		Nextoffset: rpcResp.GetNextOffset(),
		Hasmore:    rpcResp.GetHasMore(),
	}, nil
}
