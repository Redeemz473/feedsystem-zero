// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type LogoutLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLogoutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LogoutLogic {
	return &LogoutLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LogoutLogic) Logout(req *types.LogoutReq) (resp *types.LogoutResp, err error) {
	userid, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}
	rpcresp, err := l.svcCtx.AccountRpc.Logout(l.ctx, &accountclient.LogoutReq{
		UserId: userid,
	})
	if err != nil {
		return nil, err
	}

	return &types.LogoutResp{
		Msg: rpcresp.Msg,
	}, nil
}
