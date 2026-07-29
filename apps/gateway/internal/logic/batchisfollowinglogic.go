// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/social/socialclient"

	"github.com/zeromicro/go-zero/core/logx"
)

// 单次请求批量查询上限，超过会截断（前端应自行分页）。
const batchIsFollowingMaxTargets = 200

type BatchIsFollowingLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewBatchIsFollowingLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BatchIsFollowingLogic {
	return &BatchIsFollowingLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *BatchIsFollowingLogic) BatchIsFollowing(req *types.BatchIsFollowingReq) (resp *types.BatchIsFollowingResp, err error) {
	viewerID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	targetIDs := req.Targetuserids
	if len(targetIDs) == 0 {
		return &types.BatchIsFollowingResp{States: []types.FollowingStateInfo{}}, nil
	}
	if len(targetIDs) > batchIsFollowingMaxTargets {
		targetIDs = targetIDs[:batchIsFollowingMaxTargets]
	}

	rpcResp, err := l.svcCtx.SocialRpc.BatchIsFollowing(l.ctx, &socialclient.BatchIsFollowingReq{
		ViewerId:      viewerID,
		TargetUserIds: targetIDs,
	})
	if err != nil {
		return nil, err
	}

	states := make([]types.FollowingStateInfo, 0, len(rpcResp.GetStates()))
	for _, s := range rpcResp.GetStates() {
		if s == nil {
			continue
		}
		states = append(states, types.FollowingStateInfo{
			Targetuserid: s.GetTargetUserId(),
			Following:    s.GetFollowing(),
		})
	}
	return &types.BatchIsFollowingResp{States: states}, nil
}
