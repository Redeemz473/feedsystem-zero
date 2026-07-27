// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/social/socialclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ListFollowingsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListFollowingsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListFollowingsLogic {
	return &ListFollowingsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListFollowingsLogic) ListFollowings(req *types.ListFollowingsReq) (resp *types.ListFollowingsResp, err error) {
	if req == nil || req.Userid == 0 {
		return nil, status.Error(codes.InvalidArgument, "用户ID不能为空")
	}

	rpcResp, err := l.svcCtx.SocialRpc.ListFollowings(l.ctx, &socialclient.ListFollowingsReq{
		UserId:          req.Userid,
		ViewerId:        optionalUserIDFromCtx(l.ctx),
		CursorUpdatedAt: req.Cursorupdatedat,
		CursorFollowId:  req.Cursorfollowid,
		PageSize:        req.Pagesize,
	})
	if err != nil {
		return nil, err
	}

	relations := rpcResp.GetFollowings()
	userIDs := make([]uint64, 0, len(relations))
	for _, relation := range relations {
		if relation != nil {
			userIDs = append(userIDs, relation.GetFollowingId())
		}
	}
	profileMap, err := loadSocialUserInfoMap(l.ctx, l.svcCtx.AccountRpc, userIDs)
	if err != nil {
		return nil, err
	}

	followings := make([]types.FollowRelationInfo, 0, len(relations))
	for _, relation := range relations {
		if relation == nil {
			continue
		}
		profile, ok := profileMap[relation.GetFollowingId()]
		if !ok {
			continue
		}
		followings = append(followings, types.FollowRelationInfo{
			Relationid:        relation.GetRelationId(),
			User:              profile,
			Followedat:        relation.GetFollowedAt(),
			Viewerisfollowing: relation.GetViewerIsFollowing(),
		})
	}

	return &types.ListFollowingsResp{
		Followings:          followings,
		Nextcursorupdatedat: rpcResp.GetNextCursorUpdatedAt(),
		Nextcursorfollowid:  rpcResp.GetNextCursorFollowId(),
		Hasmore:             rpcResp.GetHasMore(),
	}, nil
}
