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
		var follow model.Follow
		//加悲观行锁查询关注关系，防止并发出现问题导致粉丝数被刷双倍
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
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
		l.Errorf("follow transaction failed, follower_id: %d, following_id: %d, error: %v", followerID, followingID, err)
		return nil, status.Error(codes.Internal, "关注失败")
	}

	if err := applyFollowCacheAfterCommit(l.ctx, l.svcCtx, followerID, followingID, true, stateChanged); err != nil {
		l.Errorf("apply follow cache after commit failed, follower_id: %d, following_id: %d, error: %v", followerID, followingID, err)
	}

	return &social.FollowResp{Msg: "关注成功", Followed: true}, nil
}
