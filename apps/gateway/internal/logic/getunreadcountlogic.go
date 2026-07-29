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

type GetUnreadCountLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetUnreadCountLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUnreadCountLogic {
	return &GetUnreadCountLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetUnreadCountLogic) GetUnreadCount(_ *types.GetUnreadCountReq) (resp *types.GetUnreadCountResp, err error) {
	receiverID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	rpcResp, err := l.svcCtx.NotificationRpc.GetUnreadCount(l.ctx, &notificationclient.GetUnreadCountReq{
		ReceiverId: receiverID,
	})
	if err != nil {
		return nil, err
	}

	return &types.GetUnreadCountResp{
		Unreadcount: rpcResp.GetUnreadCount(),
	}, nil
}
