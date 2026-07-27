package logic

import (
	"context"

	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"

	"github.com/zeromicro/go-zero/core/logx"
)

type BatchIsFollowingLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBatchIsFollowingLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BatchIsFollowingLogic {
	return &BatchIsFollowingLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *BatchIsFollowingLogic) BatchIsFollowing(in *social.BatchIsFollowingReq) (*social.BatchIsFollowingResp, error) {
	// target_user_ids 最多允许 100 个；过滤 0、去重，并保留请求顺序。
	originalTargetUserIDs := in.GetTargetUserIds()
	targetUserIDs, err := normalizeUserIDs(originalTargetUserIDs, 100)
	if err != nil {
		return nil, err
	}
	states := make([]*social.FollowingState, 0, len(targetUserIDs))

	// viewer_id=0 时不访问 Redis/MySQL，直接按请求顺序返回全部 false。
	viewerID := in.GetViewerId()
	if viewerID == 0 {
		for _, userID := range targetUserIDs {
			states = append(states, &social.FollowingState{
				TargetUserId: userID,
				Following:    false,
			})
		}
		return &social.BatchIsFollowingResp{
			States: states,
		}, nil
	}

	result, err := batchLoadFollowingStates(l.ctx, l.svcCtx, viewerID, targetUserIDs)
	if err != nil {
		return nil, err
	}

	// 必须按去重后的请求顺序返回，不能遍历 map 或依赖 SQL IN 的返回顺序。
	for _, userID := range targetUserIDs {
		states = append(states, &social.FollowingState{
			TargetUserId: userID,
			Following:    result[userID],
		})
	}
	return &social.BatchIsFollowingResp{
		States: states,
	}, nil
}
