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

type RegisterLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRegisterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RegisterLogic {
	return &RegisterLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RegisterLogic) Register(req *types.RegisterReq) (resp *types.RegisterResp, err error) {
	rpcresp, err := l.svcCtx.AccountRpc.Register(l.ctx, &accountclient.RegisterReq{
		Username:     req.Username,
		Password:     req.Password,
		Email:        req.Email,
		Verification: req.Verification,
	})
	if err != nil {
		return nil, err
	}

	return &types.RegisterResp{
		Msg: rpcresp.Msg,
	}, nil
}
