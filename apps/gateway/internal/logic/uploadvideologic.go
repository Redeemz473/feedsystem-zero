// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"net/http"

	"feedsystem-zero/apps/gateway/internal/model"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UploadVideoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUploadVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UploadVideoLogic {
	return &UploadVideoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UploadVideoLogic) UploadVideo(r *http.Request) (resp *types.UploadVideoResp, err error) {
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	asset, err := saveVideoUpload(r, l.svcCtx.Config.Upload)
	if err != nil {
		return nil, err
	}
	if err := upsertFileAsset(l.ctx, l.svcCtx.GormDB, model.FileAssetTypeVideo, asset.FileHash, asset.URL, asset.StoragePath, asset.Size); err != nil {
		l.Errorf("upsert video file asset failed, play_url: %s, error: %v", asset.URL, err)
		return nil, status.Error(codes.Internal, "保存视频资源失败")
	}
	ttl := uploadedFileTTL(l.svcCtx.Config.Upload)
	pipe := l.svcCtx.RedisCli.TxPipeline()
	pipe.Set(l.ctx, rediskey.ChunkUploadHashKey(userID, asset.FileHash), asset.URL, ttl)
	pipe.Set(l.ctx, rediskey.ChunkUploadGlobalHashKey(asset.FileHash), asset.URL, ttl)
	if _, err := pipe.Exec(l.ctx); err != nil {
		l.Errorf("save instant upload cache failed, file_hash: %s, error: %v", asset.FileHash, err)
	}

	return &types.UploadVideoResp{
		Msg:     "上传成功",
		Playurl: asset.URL,
	}, nil
}
