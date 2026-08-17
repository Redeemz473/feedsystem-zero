package logic

import (
	"context"
	"errors"
	"time"

	"feedsystem-zero/apps/social/internal/model"
	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"
	"feedsystem-zero/common/eventx"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
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

	// 提前构造事件以缩短事务内账户锁持有时间，并在死锁重试时复用 event_id。
	now := time.Now()
	eventID, err := newSocialEventID("unfollow")
	if err != nil {
		return nil, status.Error(codes.Internal, "生成取消关注事件ID失败")
	}
	outboxEvent, err := buildFollowOutboxEvent(eventID, followerID, followingID, eventx.FollowActionUnfollow, now)
	if err != nil {
		return nil, err
	}
	notificationEventID, err := newSocialEventID("notifyUnfollow")
	if err != nil {
		return nil, status.Error(codes.Internal, "生成取消关注通知事件ID失败")
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
		return nil, err
	}

	stateChanged := false
	if err := runSocialWriteTransaction(l.ctx, l.svcCtx.GormDB, func(tx *gorm.DB) error {
		stateChanged = false
		if _, err := lockFollowAccounts(l.ctx, tx, followerID, followingID); err != nil {
			return err
		}

		var follow model.Follow
		// 同一关系的写入已经被双方账户行串行化，普通查询即可；对不存在关系执行 FOR UPDATE 会持有 gap lock，并阻塞并发 Follow 的 INSERT。
		err := tx.
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

		// 一条 UPDATE 同时维护双方计数；大 V 标记只升不降，取关不修改。
		if err := updateFollowAccountCounters(tx, followerID, followingID, -1, false); err != nil {
			return err
		}

		return createSocialOutboxEvents(tx, outboxEvent, notificationOutbox)
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "目标用户不存在")
		}
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
