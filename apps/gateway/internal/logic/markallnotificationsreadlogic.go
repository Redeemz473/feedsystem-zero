// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/notification/notificationclient"

	"github.com/zeromicro/go-zero/core/logx"
)

type MarkAllNotificationsReadLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewMarkAllNotificationsReadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MarkAllNotificationsReadLogic {
	return &MarkAllNotificationsReadLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *MarkAllNotificationsReadLogic) MarkAllNotificationsRead(_ *types.MarkAllNotificationsReadReq) (resp *types.MarkAllNotificationsReadResp, err error) {
	receiverID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	rpcResp, err := l.svcCtx.NotificationRpc.MarkAllNotificationsRead(l.ctx, &notificationclient.MarkAllNotificationsReadReq{
		ReceiverId: receiverID,
	})
	if err != nil {
		return nil, err
	}

	return &types.MarkAllNotificationsReadResp{
		Changedcount: rpcResp.GetChangedCount(),
	}, nil
}
