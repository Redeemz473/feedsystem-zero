// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"strings"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type SendVerificationLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSendVerificationLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SendVerificationLogic {
	return &SendVerificationLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SendVerificationLogic) SendVerification(req *types.VerificationReq) (resp *types.VerificationResp, err error) {
	//调用account的sendverification服务
	email := strings.TrimSpace(req.Email)
	rpcresp, err := l.svcCtx.AccountRpc.SendVerification(l.ctx, &accountclient.VerificationReq{
		Email: email,
	})
	if err != nil {
		return nil, err
	}

	return &types.VerificationResp{
		Verification: rpcresp.Verification,
	}, nil
}
