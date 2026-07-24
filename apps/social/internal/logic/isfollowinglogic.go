package logic

import (
	"context"
	"errors"

	"feedsystem-zero/apps/social/internal/model"
	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type IsFollowingLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewIsFollowingLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IsFollowingLogic {
	return &IsFollowingLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *IsFollowingLogic) IsFollowing(in *social.IsFollowingReq) (*social.IsFollowingResp, error) {
	followerID := in.GetFollowerId()
	followingID := in.GetFollowingId()
	if followingID == 0 {
		return nil, status.Error(codes.InvalidArgument, "目标用户不能为空")
	}

	if followerID == 0 || followerID == followingID {
		return &social.IsFollowingResp{Following: false}, nil
	}

	followingStateKey := rediskey.SocialFollowingStateKey(followerID, followingID)
	followingState, err := l.svcCtx.RedisCli.Get(l.ctx, followingStateKey).Result()
	switch {
	case err == nil:
		switch followingState {
		case "1":
			return &social.IsFollowingResp{Following: true}, nil
		case "0":
			return &social.IsFollowingResp{Following: false}, nil
		default:
			l.Errorf("unexpected follow state cache value, key: %s, value: %s", followingStateKey, followingState)
		}
	case errors.Is(err, redis.Nil):
		// Cache miss, fall back to MySQL.
	default:
		l.Errorf("get follow state from redis failed, key: %s, error: %v", followingStateKey, err)
	}

	following := false
	var follow model.Follow
	err = l.svcCtx.GormDB.WithContext(l.ctx).
		Select("id").
		Where("follower_id = ? AND following_id = ? AND status = ? AND deleted_at IS NULL", followerID, followingID, model.FollowStatusActive).
		Take(&follow).Error
	switch {
	case err == nil:
		following = true
	case errors.Is(err, gorm.ErrRecordNotFound):
		following = false
	default:
		l.Errorf("get follow state from mysql failed, follower_id: %d, following_id: %d, error: %v", followerID, followingID, err)
		return nil, status.Error(codes.Internal, "查询关注状态失败")
	}

	cacheValue := "0"
	if following {
		cacheValue = "1"
	}
	//为了防止并发，同时读写，让读只能在redis里key不存在的时候才设置值，存在则不做任何修改
	if err := l.svcCtx.RedisCli.SetNX(l.ctx, followingStateKey, cacheValue, rediskey.SocialFollowingStateTTL).Err(); err != nil {
		l.Errorf("backfill follow state cache failed, key: %s, value: %s, error: %v", followingStateKey, cacheValue, err)
	}

	return &social.IsFollowingResp{Following: following}, nil
}
