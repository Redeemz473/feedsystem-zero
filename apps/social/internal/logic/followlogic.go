package logic

import (
	"context"
	"errors"
	"time"

	"feedsystem-zero/apps/social/internal/model"
	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/feedx"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type FollowLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewFollowLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FollowLogic {
	return &FollowLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// Follow 执行关注写操作。写请求始终以 MySQL 为事实来源；
// Redis 只在事务提交后更新，不能因为缓存显示“已关注”就跳过数据库事务。
func (l *FollowLogic) Follow(in *social.FollowReq) (*social.FollowResp, error) {
	followerID := in.GetFollowerId()
	if followerID == 0 {
		return nil, status.Error(codes.Unauthenticated, "用户未登录")
	}

	followingID := in.GetFollowingId()
	if followingID == 0 {
		return nil, status.Error(codes.InvalidArgument, "目标用户不能为空")
	}
	if followerID == followingID {
		return nil, status.Error(codes.InvalidArgument, "用户不能关注自己")
	}

	var now time.Time
	stateChanged := false
	if err := runSocialWriteTransaction(l.ctx, l.svcCtx.GormDB, func(tx *gorm.DB) error {
		// 重试必须丢弃上一轮已回滚事务的局部状态。
		now = time.Now()
		stateChanged = false
		if err := lockFollowAccounts(l.ctx, tx, followerID, followingID); err != nil {
			return err
		}

		var follow model.Follow
		// 双方账户行已经充当该关注关系的事务互斥锁。这里使用普通查询，避免
		// 对不存在的联合唯一键加 gap lock，随后 INSERT 时形成插入意向死锁。
		err := tx.
			Where("follower_id = ? AND following_id = ?", followerID, followingID).
			Take(&follow).Error

		//如果查到了这条关系
		if err == nil {
			//已关注
			if follow.Status == model.FollowStatusActive && follow.DeletedAt == nil {
				return nil
			}

			//未关注，则更新状态为已关注
			if err := tx.Model(&model.Follow{}).
				Where("id = ?", follow.ID).
				Updates(map[string]any{
					"status":     model.FollowStatusActive,
					"deleted_at": nil,
					"updated_at": now,
				}).Error; err != nil {
				return err
			}
			stateChanged = true
		} else if errors.Is(err, gorm.ErrRecordNotFound) { //没查到
			//唯一键冲突
			result := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "follower_id"},
					{Name: "following_id"},
				},
				DoNothing: true,
			}).Create(&model.Follow{
				FollowerID:  followerID,
				FollowingID: followingID,
				Status:      model.FollowStatusActive,
				CreatedAt:   now,
				UpdatedAt:   now,
			})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected > 0 {
				stateChanged = true
			}
			//并发冲突后重新回读
			if result.RowsAffected == 0 {
				if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
					Where("follower_id = ? AND following_id = ?", followerID, followingID).
					Take(&follow).Error; err != nil {
					return err
				}
				if follow.Status == model.FollowStatusActive && follow.DeletedAt == nil {
					return nil
				}
				if err := tx.Model(&model.Follow{}).
					Where("id = ?", follow.ID).
					Updates(map[string]any{
						"status":     model.FollowStatusActive,
						"deleted_at": nil,
						"updated_at": now,
					}).Error; err != nil {
					return err
				}
				stateChanged = true
			}
		} else {
			return err
		}

		if !stateChanged {
			return nil
		}

		// 维护 accounts 表冗余计数：被关注者粉丝数 +1，关注者关注数 +1。
		// 与关注关系写在同一个事务里，保证计数与关系强一致。
		if err := tx.Model(&model.Account{}).
			Where("id = ?", followingID).
			UpdateColumn("follower_count", gorm.Expr("follower_count + 1")).Error; err != nil {
			return err
		}
		if err := tx.Model(&model.Account{}).
			Where("id = ?", followerID).
			UpdateColumn("following_count", gorm.Expr("following_count + 1")).Error; err != nil {
			return err
		}

		// 大 V 只升不降标记维护：读回被关注者的最新粉丝数与 is_big_v，
		// 若首次达到阈值则在同事务内把 is_big_v 置 1。
		// 使用 WHERE is_big_v = 0 的条件 UPDATE 保持并发幂等，
		// 多个并发关注请求最多只有一个会真正把 0 改为 1。
		var followingAccount model.Account
		if err := tx.Model(&model.Account{}).
			Select("id", "follower_count", "is_big_v").
			Where("id = ?", followingID).
			Take(&followingAccount).Error; err != nil {
			return err
		}
		if feedx.ShouldPromoteBigCreator(followingAccount.FollowerCount, followingAccount.IsBigV) {
			if err := tx.Model(&model.Account{}).
				Where("id = ? AND is_big_v = ?", followingID, false).
				UpdateColumn("is_big_v", true).Error; err != nil {
				return err
			}
		}

		eventID, err := newSocialEventID("follow")
		if err != nil {
			return err
		}
		outboxEvent, err := buildFollowOutboxEvent(eventID, followerID, followingID, eventx.FollowActionFollow, now)
		if err != nil {
			return err
		}
		if err := tx.Create(outboxEvent).Error; err != nil {
			return err
		}

		notificationEventID, err := newSocialEventID("notifyFollow")
		if err != nil {
			return err
		}
		notificationOutbox, err := buildFollowNotificationOutbox(
			notificationEventID,
			eventID,
			followerID,
			followingID,
			eventx.FollowActionFollow,
			now,
		)
		if err != nil {
			return err
		}
		return tx.Create(notificationOutbox).Error
	}); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "目标用户不存在")
		}
		l.Errorf("follow transaction failed, follower_id: %d, following_id: %d, error: %v", followerID, followingID, err)
		return nil, status.Error(codes.Internal, "关注失败")
	}

	if err := applyFollowCacheAfterCommit(l.ctx, l.svcCtx, followerID, followingID, true, stateChanged); err != nil {
		l.Errorf("apply follow cache after commit failed, follower_id: %d, following_id: %d, error: %v", followerID, followingID, err)
	}

	return &social.FollowResp{Msg: "关注成功", Followed: true}, nil
}
