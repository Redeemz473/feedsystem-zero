// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/video/videoclient"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeleteVideoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewDeleteVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteVideoLogic {
	return &DeleteVideoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *DeleteVideoLogic) DeleteVideo(req *types.DeleteVideoReq) (resp *types.DeleteVideoResp, err error) {
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	// NOTE: 视频软删、file_assets.ref_count -1、outbox 事件写入
	//       全部在 video-rpc.DeleteVideo 的本地事务里完成，
	//       gateway 只负责鉴权和转发，不再需要 GetVideo + 事务外清理的两阶段调用。
	//       物理文件的磁盘清理由独立的 cleanup job 扫描 status=PendingDelete 后执行。
	rpcResp, err := l.svcCtx.VideoRpc.DeleteVideo(l.ctx, &videoclient.DeleteVideoReq{
		VideoId:    req.Videoid,
		OperatorId: userID,
	})
	if err != nil {
		return nil, err
	}

	return &types.DeleteVideoResp{
		Msg: rpcResp.GetMsg(),
	}, nil
}
