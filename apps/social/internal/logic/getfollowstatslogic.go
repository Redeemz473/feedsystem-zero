package logic

import (
	"context"
	"errors"
	"strconv"

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

const (
	followStatsFieldFollowers  = "followers_count"
	followStatsFieldFollowings = "followings_count"
	followStatsFieldVersion    = "version"
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
	followStatsVersionKey := rediskey.SocialFollowStatsVersionKey(userID)

	if followersCount, followingsCount, ok := l.loadStatsFromCache(followStatsKey, followStatsVersionKey); ok {
		return &social.GetFollowStatsResp{
			FollowersCount:  followersCount,
			FollowingsCount: followingsCount,
		}, nil
	}

	// 版本读取失败不阻断查询，只是不回填缓存。
	versionBefore, versionErr := l.loadFollowStatsVersion(followStatsVersionKey)
	if versionErr != nil {
		l.Errorf("load follow stats version before query failed, key: %s, error: %v", followStatsVersionKey, versionErr)
	}

	var followersCount int64
	var followingsCount int64
	if err := l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.Follow{}).
			Where("following_id = ? AND status = ? AND deleted_at IS NULL", userID, model.FollowStatusActive).
			Count(&followersCount).Error; err != nil {
			return err
		}

		return tx.Model(&model.Follow{}).
			Where("follower_id = ? AND status = ? AND deleted_at IS NULL", userID, model.FollowStatusActive).
			Count(&followingsCount).Error
	}); err != nil {
		l.Errorf("count follow stats from mysql failed, user_id: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "查询关注统计失败")
	}

	// 只有查询前后版本一致，才允许把本次快照写入缓存。
	// 查询后再发生的关注变化，会通过“删除缓存 + 递增版本”使该缓存失效。
	versionAfter, err := l.loadFollowStatsVersion(followStatsVersionKey)
	switch {
	case err != nil:
		l.Errorf("load follow stats version after query failed, key: %s, error: %v", followStatsVersionKey, err)
	case versionErr == nil && versionBefore == versionAfter:
		pipe := l.svcCtx.RedisCli.TxPipeline()
		pipe.HSet(l.ctx, followStatsKey,
			followStatsFieldFollowers, followersCount,
			followStatsFieldFollowings, followingsCount,
			followStatsFieldVersion, versionAfter,
		)
		pipe.Expire(l.ctx, followStatsKey, rediskey.SocialFollowStatsTTL)
		if _, err := pipe.Exec(l.ctx); err != nil {
			l.Errorf("backfill follow stats cache failed, key: %s, error: %v", followStatsKey, err)
		}
	default:
		l.Infof(
			"skip follow stats cache because version changed during query, user_id: %d, before: %d, after: %d",
			userID,
			versionBefore,
			versionAfter,
		)
	}

	return &social.GetFollowStatsResp{
		FollowersCount:  followersCount,
		FollowingsCount: followingsCount,
	}, nil
}

func (l *GetFollowStatsLogic) loadStatsFromCache(statsKey string, versionKey string) (int64, int64, bool) {
	pipe := l.svcCtx.RedisCli.TxPipeline()
	statsCmd := pipe.HGetAll(l.ctx, statsKey)
	versionCmd := pipe.Get(l.ctx, versionKey)
	if _, err := pipe.Exec(l.ctx); err != nil && !errors.Is(err, redis.Nil) {
		l.Errorf("get follow stats from redis failed, key: %s, error: %v", statsKey, err)
		return 0, 0, false
	}

	stats, err := statsCmd.Result()
	if err != nil {
		l.Errorf("get follow stats hash failed, key: %s, error: %v", statsKey, err)
		return 0, 0, false
	}
	if len(stats) == 0 {
		return 0, 0, false
	}

	followersStr, okF := stats[followStatsFieldFollowers]
	followingsStr, okG := stats[followStatsFieldFollowings]
	cachedVersionStr, okV := stats[followStatsFieldVersion]
	if !okF || !okG || !okV {
		return 0, 0, false
	}

	followersCount, errF := strconv.ParseInt(followersStr, 10, 64)
	followingsCount, errG := strconv.ParseInt(followingsStr, 10, 64)
	cachedVersion, errV := strconv.ParseInt(cachedVersionStr, 10, 64)
	if errF != nil || errG != nil || errV != nil || followersCount < 0 || followingsCount < 0 || cachedVersion < 0 {
		l.Errorf("unexpected follow stats cache value, key: %s, stats: %v", statsKey, stats)
		return 0, 0, false
	}

	currentVersion := int64(0)
	versionStr, err := versionCmd.Result()
	switch {
	case err == nil:
		currentVersion, err = strconv.ParseInt(versionStr, 10, 64)
		if err != nil || currentVersion < 0 {
			l.Errorf("unexpected follow stats version, key: %s, value: %s", versionKey, versionStr)
			return 0, 0, false
		}
	case errors.Is(err, redis.Nil):
		currentVersion = 0
	default:
		l.Errorf("get follow stats version failed, key: %s, error: %v", versionKey, err)
		return 0, 0, false
	}

	if cachedVersion != currentVersion {
		return 0, 0, false
	}

	return followersCount, followingsCount, true
}

func (l *GetFollowStatsLogic) loadFollowStatsVersion(key string) (int64, error) {
	version, err := l.svcCtx.RedisCli.Get(l.ctx, key).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if version < 0 {
		return 0, status.Error(codes.Internal, "关注统计版本异常")
	}
	return version, nil
}
