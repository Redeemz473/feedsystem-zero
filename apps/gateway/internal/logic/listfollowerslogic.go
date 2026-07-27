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

type ListFollowersLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListFollowersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListFollowersLogic {
	return &ListFollowersLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListFollowersLogic) ListFollowers(req *types.ListFollowersReq) (resp *types.ListFollowersResp, err error) {
	if req == nil || req.Userid == 0 {
		return nil, status.Error(codes.InvalidArgument, "用户ID不能为空")
	}

	rpcResp, err := l.svcCtx.SocialRpc.ListFollowers(l.ctx, &socialclient.ListFollowersReq{
		UserId:          req.Userid,
		ViewerId:        optionalUserIDFromCtx(l.ctx),
		CursorUpdatedAt: req.Cursorupdatedat,
		CursorFollowId:  req.Cursorfollowid,
		PageSize:        req.Pagesize,
	})
	if err != nil {
		return nil, err
	}

	relations := rpcResp.GetFollowers()
	userIDs := make([]uint64, 0, len(relations))
	for _, relation := range relations {
		if relation != nil {
			userIDs = append(userIDs, relation.GetFollowerId())
		}
	}
	profileMap, err := loadSocialUserInfoMap(l.ctx, l.svcCtx.AccountRpc, userIDs)
	if err != nil {
		return nil, err
	}

	followers := make([]types.FollowRelationInfo, 0, len(relations))
	for _, relation := range relations {
		if relation == nil {
			continue
		}
		profile, ok := profileMap[relation.GetFollowerId()]
		if !ok {
			continue
		}
		followers = append(followers, types.FollowRelationInfo{
			Relationid:        relation.GetRelationId(),
			User:              profile,
			Followedat:        relation.GetFollowedAt(),
			Viewerisfollowing: relation.GetViewerIsFollowing(),
		})
	}

	return &types.ListFollowersResp{
		Followers:           followers,
		Nextcursorupdatedat: rpcResp.GetNextCursorUpdatedAt(),
		Nextcursorfollowid:  rpcResp.GetNextCursorFollowId(),
		Hasmore:             rpcResp.GetHasMore(),
	}, nil
}
