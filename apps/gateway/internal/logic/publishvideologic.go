// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/video/videoclient"

	"github.com/zeromicro/go-zero/core/logx"
)

type PublishVideoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewPublishVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PublishVideoLogic {
	return &PublishVideoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *PublishVideoLogic) PublishVideo(req *types.PublishVideoReq) (resp *types.PublishVideoResp, err error) {
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	profile, err := l.svcCtx.AccountRpc.GetProfile(l.ctx, &accountclient.GetProfileReq{
		UserId: userID,
	})
	if err != nil {
		return nil, err
	}

	// NOTE: file_assets.ref_count 的 +1（reserve）已经下沉到 video-rpc 的 PublishVideo 内部，
	//       与 videos 插入共用同一个本地事务，任何失败都会自动回滚，
	//       gateway 侧不再需要 reserve/release/defer 的补偿逻辑，
	//       从根本上避免了跨库跨 RPC 的引用计数漂移问题。
	rpcResp, err := l.svcCtx.VideoRpc.PublishVideo(l.ctx, &videoclient.PublishVideoReq{
		AuthorId:       userID,
		AuthorUsername: profile.GetUsername(),
		Title:          req.Title,
		Description:    req.Description,
		PlayUrl:        req.Playurl,
		CoverUrl:       req.Coverurl,
		Tags:           req.Tags,
	})
	if err != nil {
		return nil, err
	}

	videos := []types.VideoInfo{
		toHTTPVideoInfo(rpcResp.GetVideo()),
	}
	if enrichedVideos, enrichErr := enrichHTTPVideoInteractions(l.ctx, l.svcCtx.InteractionRpc, userID, videos); enrichErr != nil {
		l.Errorf("enrich published video interaction stats failed, video_id: %d, user_id: %d, error: %v", rpcResp.GetVideo().GetVideoId(), userID, enrichErr)
	} else {
		videos = enrichedVideos
	}

	return &types.PublishVideoResp{
		Msg:   rpcResp.GetMsg(),
		Video: videos[0],
	}, nil
}
