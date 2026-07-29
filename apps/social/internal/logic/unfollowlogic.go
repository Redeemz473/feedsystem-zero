package logic

import (
	"context"
	"errors"
	"time"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/social/internal/model"
	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"
	"feedsystem-zero/common/eventx"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type UnfollowLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUnfollowLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnfollowLogic {
	return &UnfollowLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// Unfollow 执行取消关注。follower_id 只能来自 gateway 解析后的 JWT；
// 写请求始终检查 MySQL 当前状态，不能用可能过期的 Redis 状态提前返回。
func (l *UnfollowLogic) Unfollow(in *social.UnfollowReq) (*social.UnfollowResp, error) {
	followerID := in.GetFollowerId()
	if followerID == 0 {
		return nil, status.Error(codes.Unauthenticated, "用户未登录")
	}

	followingID := in.GetFollowingId()
	if followingID == 0 {
		return nil, status.Error(codes.InvalidArgument, "目标用户不能为空")
	}
	if followerID == followingID {
		return nil, status.Error(codes.InvalidArgument, "用户不能取消关注自己")
	}

	if _, err := l.svcCtx.AccountRpc.GetProfile(l.ctx, &accountclient.GetProfileReq{UserId: followingID}); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "目标用户不存在")
		}
		l.Errorf("get following profile failed, following_id: %d, error: %v", followingID, err)
		return nil, status.Error(codes.Internal, "校验目标用户失败")
	}

	now := time.Now()
	stateChanged := false
	if err := l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockFollowAccounts(l.ctx, tx, followerID, followingID); err != nil {
			return err
		}

		var follow model.Follow
		//加锁查询关注关系
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("follower_id = ? AND following_id = ?", followerID, followingID).
			Take(&follow).Error

		//如果查到了这条关系
		if err == nil {
			// 已经不是有效关注，直接幂等成功，不重复写取关事件。
			if follow.Status != model.FollowStatusActive || follow.DeletedAt != nil {
				return nil
			}

			// 仍是有效关注，则软删除并继续写 follow.deleted outbox。
			if err := tx.Model(&model.Follow{}).
				Where("id = ?", follow.ID).
				Updates(map[string]any{
					"status":     model.FollowStatusDeleted,
					"deleted_at": now,
					"updated_at": now,
				}).Error; err != nil {
				return err
			}
			stateChanged = true
		} else if errors.Is(err, gorm.ErrRecordNotFound) {
			// 没有关注关系，目标状态已经是未关注，幂等成功。
			return nil
		} else {
			return err
		}

		if !stateChanged {
			return nil
		}

		// 维护 accounts 表冗余计数：被关注者粉丝数 -1，关注者关注数 -1。
		// 用 GREATEST(..., 0) 防止并发异常导致计数变成负数。
		if err := tx.Model(&model.Account{}).
			Where("id = ?", followingID).
			UpdateColumn("follower_count", gorm.Expr("GREATEST(follower_count - 1, 0)")).Error; err != nil {
			return err
		}
		if err := tx.Model(&model.Account{}).
			Where("id = ?", followerID).
			UpdateColumn("following_count", gorm.Expr("GREATEST(following_count - 1, 0)")).Error; err != nil {
			return err
		}

		eventID, err := newSocialEventID("unfollow")
		if err != nil {
			return err
		}
		outboxEvent, err := buildFollowOutboxEvent(eventID, followerID, followingID, eventx.FollowActionUnfollow, now)
		if err != nil {
			return err
		}
		if err := tx.Create(outboxEvent).Error; err != nil {
			return err
		}

		notificationEventID, err := newSocialEventID("notifyUnfollow")
		if err != nil {
			return err
		}
		notificationOutbox, err := buildFollowNotificationOutbox(
			notificationEventID,
			eventID,
			followerID,
			followingID,
			eventx.FollowActionUnfollow,
			now,
		)
		if err != nil {
			return err
		}
		return tx.Create(notificationOutbox).Error
	}); err != nil {
		l.Errorf("unfollow transaction failed, follower_id: %d, following_id: %d, error: %v", followerID, followingID, err)
		return nil, status.Error(codes.Internal, "取消关注失败")
	}

	if err := applyFollowCacheAfterCommit(l.ctx, l.svcCtx, followerID, followingID, false, stateChanged); err != nil {
		l.Errorf("apply unfollow cache after commit failed, follower_id: %d, following_id: %d, error: %v", followerID, followingID, err)
	}

	return &social.UnfollowResp{
		Msg:        "取消关注成功",
		Unfollowed: true,
	}, nil
}
