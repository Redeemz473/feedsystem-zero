// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"errors"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/video/videoclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
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

	//如果是一个已经有资源的视频，则直接添加
	if err := reserveFileAssetRefByURL(l.ctx, l.svcCtx.GormDB, req.Playurl); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.InvalidArgument, "视频资源不存在或已失效，请重新上传")
		}
		l.Errorf("reserve video file ref failed, play_url: %s, error: %v", req.Playurl, err)
		return nil, status.Error(codes.Internal, "锁定视频资源失败")
	}

	//延迟回滚，把引用计数减一
	videoReserved := true
	coverReserved := false
	defer func() {
		if err == nil {
			return
		}
		if coverReserved {
			if releaseErr := releaseFileAssetRefByURL(l.ctx, l.svcCtx.GormDB, req.Coverurl); releaseErr != nil {
				l.Errorf("release cover file ref failed, cover_url: %s, error: %v", req.Coverurl, releaseErr)
			}
		}
		if videoReserved {
			if releaseErr := releaseFileAssetRefByURL(l.ctx, l.svcCtx.GormDB, req.Playurl); releaseErr != nil {
				l.Errorf("release video file ref failed, play_url: %s, error: %v", req.Playurl, releaseErr)
			}
		}
	}()

	if err := reserveFileAssetRefByURL(l.ctx, l.svcCtx.GormDB, req.Coverurl); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.InvalidArgument, "封面资源不存在或已失效，请重新上传")
		}
		l.Errorf("reserve cover file ref failed, cover_url: %s, error: %v", req.Coverurl, err)
		return nil, status.Error(codes.Internal, "锁定封面资源失败")
	}
	coverReserved = true

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
