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
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
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

// Follow 执行关注写操作。Redis 只作为“目标状态已达成”的幂等快速路径，
// MySQL 事务仍然是关注关系的最终事实来源。
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

	followingStateKey := rediskey.SocialFollowingStateKey(followerID, followingID)
	followingState, err := l.svcCtx.RedisCli.Get(l.ctx, followingStateKey).Result()
	switch {
	case err == nil:
		if followingState == "1" {
			return &social.FollowResp{Msg: "关注成功", Followed: true}, nil
		}
		if followingState != "0" {
			l.Errorf("unexpected follow state cache value, key: %s, value: %s", followingStateKey, followingState)
		}
	case errors.Is(err, redis.Nil):
		// 缓存未命中，查MySQL.
	default:
		l.Errorf("get follow state from redis failed, key: %s, error: %v", followingStateKey, err)
	}

	now := time.Now()
	if err := l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		var follow model.Follow
		//加锁查询关注关系
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
		} else if errors.Is(err, gorm.ErrRecordNotFound) { //没查到
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
			}
		} else {
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
		return tx.Create(outboxEvent).Error
	}); err != nil {
		l.Errorf("follow transaction failed, follower_id: %d, following_id: %d, error: %v", followerID, followingID, err)
		return nil, status.Error(codes.Internal, "关注失败")
	}

	if err := applyFollowCacheAfterCommit(l.ctx, l.svcCtx, followerID, followingID, true); err != nil {
		l.Errorf("apply follow cache after commit failed, follower_id: %d, following_id: %d, error: %v", followerID, followingID, err)
	}

	return &social.FollowResp{Msg: "关注成功", Followed: true}, nil
}
