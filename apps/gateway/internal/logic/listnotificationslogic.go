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

type ListNotificationsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListNotificationsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListNotificationsLogic {
	return &ListNotificationsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListNotificationsLogic) ListNotifications(req *types.ListNotificationsReq) (resp *types.ListNotificationsResp, err error) {
	receiverID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	rpcResp, err := l.svcCtx.NotificationRpc.ListNotifications(l.ctx, &notificationclient.ListNotificationsReq{
		ReceiverId:           receiverID,
		CursorOccurredAt:     req.Cursoroccurredat,
		CursorNotificationId: req.Cursornotificationid,
		PageSize:             req.Pagesize,
	})
	if err != nil {
		return nil, err
	}

	notifications, err := hydrateNotifications(l.ctx, l.svcCtx, rpcResp.GetNotifications())
	if err != nil {
		return nil, err
	}

	return &types.ListNotificationsResp{
		Notifications:            notifications,
		Nextcursoroccurredat:     rpcResp.GetNextCursorOccurredAt(),
		Nextcursornotificationid: rpcResp.GetNextCursorNotificationId(),
		Hasmore:                  rpcResp.GetHasMore(),
	}, nil
}
