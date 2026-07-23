// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"strings"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/video/videoclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	// 发布视频幂等：优先使用客户端上送的 request_id，缺失时 gateway 兜底生成一个。
	// 与 PublishComment 一致：客户端主动上送能防住"客户端超时后自己重试"，
	// gateway 兜底只能防住"gateway 层内部重试或代理重试"，但至少保证到达 video-rpc 时 request_id 非空。
	requestID := strings.TrimSpace(req.Requestid)
	if requestID == "" {
		requestID, err = gatewayGeneratedPublishRequestID(userID)
		if err != nil {
			return nil, status.Error(codes.Internal, "生成视频发布幂等ID失败")
		}
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
		RequestId:      requestID,
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
