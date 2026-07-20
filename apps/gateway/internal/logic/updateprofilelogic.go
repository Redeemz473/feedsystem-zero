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

type UpdateProfileLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateProfileLogic {
	return &UpdateProfileLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateProfileLogic) UpdateProfile(req *types.UpdateProfileReq) (resp *types.UpdateProfileResp, err error) {
	userid, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}
	rpcresp, err := l.svcCtx.AccountRpc.UpdateProfile(l.ctx, &accountclient.UpdateProfileReq{
		UserId:    userid,
		Username:  req.Username,
		AvatarUrl: req.Avatarurl,
		Bio:       req.Bio,
	})
	if err != nil {
		return nil, err
	}
	return &types.UpdateProfileResp{
		Msg: rpcresp.Msg,
	}, nil
}
