// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"net/http"

	"feedsystem-zero/apps/gateway/internal/model"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UploadCoverLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUploadCoverLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UploadCoverLogic {
	return &UploadCoverLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UploadCoverLogic) UploadCover(r *http.Request) (resp *types.UploadCoverResp, err error) {
	if _, err := userIDFromCtx(l.ctx); err != nil {
		return nil, err
	}

	asset, err := saveCoverUpload(r, l.svcCtx.Config.Upload)
	if err != nil {
		return nil, err
	}
	canonicalAsset, err := upsertFileAsset(l.ctx, l.svcCtx.GormDB, model.FileAssetTypeCover, asset.FileHash, asset.URL, asset.StoragePath, asset.Size)
	if err != nil {
		l.Errorf("upsert cover file asset failed, cover_url: %s, error: %v", asset.URL, err)
		return nil, status.Error(codes.Internal, "保存封面资源失败")
	}

	return &types.UploadCoverResp{
		Msg:      "上传成功",
		Coverurl: canonicalAsset.URL,
	}, nil
}
