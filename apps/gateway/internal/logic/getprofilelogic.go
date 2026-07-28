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

type GetProfileLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetProfileLogic {
	return &GetProfileLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetProfileLogic) GetProfile(req *types.GetProfileReq) (resp *types.GetProfileResp, err error) {
	userid, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}
	rpcresp, err := l.svcCtx.AccountRpc.GetProfile(l.ctx, &accountclient.GetProfileReq{
		UserId: userid,
	})
	if err != nil {
		return nil, err
	}

	return &types.GetProfileResp{
		Userid:          rpcresp.UserId,
		Username:        rpcresp.Username,
		Email:           rpcresp.Email,
		Avatarurl:       rpcresp.AvatarUrl,
		Bio:             rpcresp.Bio,
		Followerscount:  rpcresp.FollowerCount,
		Followingscount: rpcresp.FollowingCount,
	}, nil
}
