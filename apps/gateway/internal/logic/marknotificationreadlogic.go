// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/notification/notificationclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MarkNotificationReadLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewMarkNotificationReadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MarkNotificationReadLogic {
	return &MarkNotificationReadLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *MarkNotificationReadLogic) MarkNotificationRead(req *types.MarkNotificationReadReq) (resp *types.MarkNotificationReadResp, err error) {
	receiverID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}
	if req.Notificationid == 0 {
		return nil, status.Error(codes.InvalidArgument, "非法通知 ID")
	}

	rpcResp, err := l.svcCtx.NotificationRpc.MarkNotificationRead(l.ctx, &notificationclient.MarkNotificationReadReq{
		ReceiverId:     receiverID,
		NotificationId: req.Notificationid,
	})
	if err != nil {
		return nil, err
	}

	return &types.MarkNotificationReadResp{
		Changed: rpcResp.GetChanged(),
	}, nil
}
