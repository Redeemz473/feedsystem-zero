package logic

import (
	"context"
	"strconv"

	"feedsystem-zero/apps/social/internal/model"
	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	followStatsFieldFollowers  = "followers_count"
	followStatsFieldFollowings = "followings_count"
)

type GetFollowStatsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetFollowStatsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFollowStatsLogic {
	return &GetFollowStatsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetFollowStatsLogic) GetFollowStats(in *social.GetFollowStatsReq) (*social.GetFollowStatsResp, error) {
	userID := in.GetUserId()
	if userID == 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id 不能为空")
	}

	followStatsKey := rediskey.SocialFollowStatsKey(userID)

	if followersCount, followingsCount, ok := l.loadStatsFromCache(followStatsKey); ok {
		return &social.GetFollowStatsResp{
			FollowersCount:  followersCount,
			FollowingsCount: followingsCount,
		}, nil
	}

	var followersCount int64
	if err := l.svcCtx.GormDB.WithContext(l.ctx).Model(&model.Follow{}).
		Where("following_id = ? AND status = ? AND deleted_at IS NULL", userID, model.FollowStatusActive).
		Count(&followersCount).Error; err != nil {
		l.Errorf("count followers from mysql failed, user_id: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "查询粉丝数失败")
	}

	var followingsCount int64
	if err := l.svcCtx.GormDB.WithContext(l.ctx).Model(&model.Follow{}).
		Where("follower_id = ? AND status = ? AND deleted_at IS NULL", userID, model.FollowStatusActive).
		Count(&followingsCount).Error; err != nil {
		l.Errorf("count followings from mysql failed, user_id: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "查询关注数失败")
	}

	pipe := l.svcCtx.RedisCli.Pipeline()
	pipe.HSet(l.ctx, followStatsKey,
		followStatsFieldFollowers, followersCount,
		followStatsFieldFollowings, followingsCount,
	)
	pipe.Expire(l.ctx, followStatsKey, rediskey.SocialFollowStatsTTL)
	if _, err := pipe.Exec(l.ctx); err != nil {
		l.Errorf("backfill follow stats cache failed, key: %s, error: %v", followStatsKey, err)
	}

	return &social.GetFollowStatsResp{
		FollowersCount:  followersCount,
		FollowingsCount: followingsCount,
	}, nil
}

func (l *GetFollowStatsLogic) loadStatsFromCache(key string) (int64, int64, bool) {
	stats, err := l.svcCtx.RedisCli.HGetAll(l.ctx, key).Result()
	if err != nil {
		l.Errorf("get follow stats from redis failed, key: %s, error: %v", key, err)
		return 0, 0, false
	}
	if len(stats) == 0 {
		return 0, 0, false
	}

	followersStr, okF := stats[followStatsFieldFollowers]
	followingsStr, okG := stats[followStatsFieldFollowings]
	if !okF || !okG {
		return 0, 0, false
	}

	followersCount, errF := strconv.ParseInt(followersStr, 10, 64)
	followingsCount, errG := strconv.ParseInt(followingsStr, 10, 64)
	if errF != nil || errG != nil || followersCount < 0 || followingsCount < 0 {
		l.Errorf("unexpected follow stats cache value, key: %s, stats: %v", key, stats)
		return 0, 0, false
	}

	return followersCount, followingsCount, true
}
