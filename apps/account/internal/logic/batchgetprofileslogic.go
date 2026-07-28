package logic

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"feedsystem-zero/apps/account/account"
	"feedsystem-zero/apps/account/internal/model"
	"feedsystem-zero/apps/account/internal/svc"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxBatchProfileIDs          = 100
	publicProfileCacheTTLJitter = 5 * time.Minute
)

type publicProfileCacheValue struct {
	Missing        bool   `json:"missing,omitempty"`
	UserID         uint64 `json:"user_id"`
	Username       string `json:"username,omitempty"`
	AvatarURL      string `json:"avatar_url,omitempty"`
	Bio            string `json:"bio,omitempty"`
	FollowerCount  int64  `json:"follower_count,omitempty"`
	FollowingCount int64  `json:"following_count,omitempty"`
}

// 同一实例内合并相同 ID 集合的并发回源，降低热点缓存刚过期时的 MySQL 瞬时压力。
var publicProfileDBLoadGroup = syncx.NewSingleFlight()

type BatchGetProfilesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBatchGetProfilesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BatchGetProfilesLogic {
	return &BatchGetProfilesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 批量查询公开用户资料
func (l *BatchGetProfilesLogic) BatchGetProfiles(in *account.BatchGetProfilesReq) (*account.BatchGetProfilesResp, error) {
	// 限制批量规模并去重保序，避免超长 IN 查询和重复缓存访问。
	userIDs, err := normalizeBatchProfileIDs(in.GetUserIds())
	if err != nil {
		return nil, err
	}
	if len(userIDs) == 0 {
		return &account.BatchGetProfilesResp{Profiles: []*account.PublicProfile{}}, nil
	}

	// 批量读取公开资料缓存。Redis 异常时自动降级为一次 MySQL 批量查询。
	profileMap := make(map[uint64]*account.PublicProfile, len(userIDs))
	missUserIDs, cacheVersions, cacheAvailable := l.loadPublicProfilesFromCache(userIDs, profileMap)

	// 缓存 miss 只执行一次主键 IN 查询，并且只选择公开字段。
	if len(missUserIDs) > 0 {
		dbProfiles, err := l.loadPublicProfilesFromDB(missUserIDs)
		if err != nil {
			l.Errorf("batch query public profiles failed, user_ids: %v, error: %v", missUserIDs, err)
			return nil, status.Error(codes.Internal, "批量查询用户资料失败")
		}
		for userID, profile := range dbProfiles {
			profileMap[userID] = profile
		}

		// 回填前再次校验版本，避免并发更新资料时把旧快照写入新缓存。
		if cacheAvailable {
			l.cachePublicProfileMisses(missUserIDs, cacheVersions, profileMap)
		}
	}

	// 按去重后的请求顺序返回；不存在的用户由负缓存记录，但不出现在响应中。
	profiles := make([]*account.PublicProfile, 0, len(userIDs))
	for _, userID := range userIDs {
		if profile, ok := profileMap[userID]; ok {
			profiles = append(profiles, profile)
		}
	}

	return &account.BatchGetProfilesResp{Profiles: profiles}, nil
}

func normalizeBatchProfileIDs(rawUserIDs []uint64) ([]uint64, error) {
	if len(rawUserIDs) > maxBatchProfileIDs {
		return nil, status.Errorf(codes.InvalidArgument, "一次最多查询%d个用户", maxBatchProfileIDs)
	}

	seen := make(map[uint64]struct{}, len(rawUserIDs))
	userIDs := make([]uint64, 0, len(rawUserIDs))
	for _, userID := range rawUserIDs {
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		userIDs = append(userIDs, userID)
	}
	return userIDs, nil
}

func (l *BatchGetProfilesLogic) loadPublicProfilesFromDB(userIDs []uint64) (map[uint64]*account.PublicProfile, error) {
	value, err := publicProfileDBLoadGroup.Do(publicProfileDBLoadKey(userIDs), func() (any, error) {
		var users []model.Account
		if err := l.svcCtx.GormDB.WithContext(l.ctx).
			Select("id", "username", "avatar_url", "bio", "follower_count", "following_count").
			Where("id IN ?", userIDs).
			Find(&users).Error; err != nil {
			return nil, err
		}

		profiles := make(map[uint64]*account.PublicProfile, len(users))
		for i := range users {
			user := &users[i]
			profiles[user.ID] = &account.PublicProfile{
				UserId:         user.ID,
				Username:       user.Username,
				AvatarUrl:      user.AvatarURL,
				Bio:            user.Bio,
				FollowerCount:  user.FollowerCount,
				FollowingCount: user.FollowingCount,
			}
		}
		return profiles, nil
	})
	if err != nil {
		return nil, err
	}
	profiles, ok := value.(map[uint64]*account.PublicProfile)
	if !ok {
		return nil, errors.New("批量用户资料查询结果类型异常")
	}
	return profiles, nil
}

// 让相同 ID 集合的调用能被识别为同一次请求并合并。
func publicProfileDBLoadKey(userIDs []uint64) string {
	sortedUserIDs := append([]uint64(nil), userIDs...)
	sort.Slice(sortedUserIDs, func(i, j int) bool {
		return sortedUserIDs[i] < sortedUserIDs[j]
	})

	var builder strings.Builder
	for _, userID := range sortedUserIDs {
		builder.WriteString(strconv.FormatUint(userID, 10))
		builder.WriteByte(',')
	}
	return builder.String()
}

// loadPublicProfilesFromCache 分两段 pipeline 读取版本与缓存值。
// 第二段会再次读取版本，防止两次 Redis 往返期间用户资料刚好发生更新。
func (l *BatchGetProfilesLogic) loadPublicProfilesFromCache(
	userIDs []uint64,
	profileMap map[uint64]*account.PublicProfile,
) ([]uint64, map[uint64]int64, bool) {
	//一次拿到所有版本号
	versionPipe := l.svcCtx.RedisCli.Pipeline()
	versionCmds := make(map[uint64]*redis.StringCmd, len(userIDs))
	for _, userID := range userIDs {
		versionCmds[userID] = versionPipe.Get(l.ctx, rediskey.AccountPublicProfileVersionKey(userID))
	}
	if _, err := versionPipe.Exec(l.ctx); err != nil && !errors.Is(err, redis.Nil) {
		l.Errorf("batch get public profile versions failed, error: %v", err)
		return append([]uint64(nil), userIDs...), nil, false
	}

	//分离 能查版本缓存 和 直接 miss
	versions := make(map[uint64]int64, len(userIDs))
	cacheableUserIDs := make([]uint64, 0, len(userIDs))
	missUserIDs := make([]uint64, 0, len(userIDs))
	for _, userID := range userIDs {
		version, err := publicProfileVersionResult(versionCmds[userID])
		if err != nil {
			l.Errorf("get public profile version failed, user_id: %d, error: %v", userID, err)
			missUserIDs = append(missUserIDs, userID)
			continue
		}
		versions[userID] = version
		cacheableUserIDs = append(cacheableUserIDs, userID)
	}
	if len(cacheableUserIDs) == 0 {
		return missUserIDs, versions, true
	}

	//查数据缓存，并且再次查版本缓存用来验证，防止中间有更新
	cachePipe := l.svcCtx.RedisCli.Pipeline()
	valueCmds := make(map[uint64]*redis.StringCmd, len(cacheableUserIDs))
	verifyVersionCmds := make(map[uint64]*redis.StringCmd, len(cacheableUserIDs))
	for _, userID := range cacheableUserIDs {
		valueCmds[userID] = cachePipe.Get(l.ctx, rediskey.AccountPublicProfileKey(userID, versions[userID]))
		verifyVersionCmds[userID] = cachePipe.Get(l.ctx, rediskey.AccountPublicProfileVersionKey(userID))
	}
	if _, err := cachePipe.Exec(l.ctx); err != nil && !errors.Is(err, redis.Nil) {
		l.Errorf("batch get public profile cache failed, error: %v", err)
		return append([]uint64(nil), userIDs...), nil, false
	}

	//检验第二次读的版本号
	for _, userID := range cacheableUserIDs {
		currentVersion, err := publicProfileVersionResult(verifyVersionCmds[userID])
		if err != nil {
			l.Errorf("verify public profile version failed, user_id: %d, error: %v", userID, err)
			missUserIDs = append(missUserIDs, userID)
			delete(versions, userID)
			continue
		}
		//如果是版本发生了改变
		if currentVersion != versions[userID] {
			versions[userID] = currentVersion
			missUserIDs = append(missUserIDs, userID)
			continue
		}

		data, err := valueCmds[userID].Bytes()
		switch {
		//版本对得上但是数据缓存miss，属于普通冷miss，去查mysql
		case errors.Is(err, redis.Nil):
			missUserIDs = append(missUserIDs, userID)
			continue
		//其他错误也走miss
		case err != nil:
			l.Errorf("get public profile cache failed, user_id: %d, error: %v", userID, err)
			missUserIDs = append(missUserIDs, userID)
			continue
		}

		var cached publicProfileCacheValue
		if err := json.Unmarshal(data, &cached); err != nil {
			l.Errorf("unmarshal public profile cache failed, user_id: %d, error: %v", userID, err)
			missUserIDs = append(missUserIDs, userID)
			_ = l.svcCtx.RedisCli.Del(l.ctx, rediskey.AccountPublicProfileKey(userID, currentVersion)).Err()
			continue
		}
		if cached.UserID != userID {
			l.Errorf("public profile cache user mismatch, expected_user_id: %d, cached_user_id: %d", userID, cached.UserID)
			missUserIDs = append(missUserIDs, userID)
			_ = l.svcCtx.RedisCli.Del(l.ctx, rediskey.AccountPublicProfileKey(userID, currentVersion)).Err()
			continue
		}
		//防穿透，表示这个用户ID在mysql里也不存在，不加入missUserIDs
		if cached.Missing {
			continue
		}

		profileMap[userID] = &account.PublicProfile{
			UserId:         cached.UserID,
			Username:       cached.Username,
			AvatarUrl:      cached.AvatarURL,
			Bio:            cached.Bio,
			FollowerCount:  cached.FollowerCount,
			FollowingCount: cached.FollowingCount,
		}
	}

	return missUserIDs, versions, true
}

func (l *BatchGetProfilesLogic) cachePublicProfileMisses(
	userIDs []uint64,
	expectedVersions map[uint64]int64,
	profileMap map[uint64]*account.PublicProfile,
) {
	versionPipe := l.svcCtx.RedisCli.Pipeline()
	versionCmds := make(map[uint64]*redis.StringCmd, len(userIDs))
	for _, userID := range userIDs {
		if _, ok := expectedVersions[userID]; !ok {
			continue
		}
		versionCmds[userID] = versionPipe.Get(l.ctx, rediskey.AccountPublicProfileVersionKey(userID))
	}
	if len(versionCmds) == 0 {
		return
	}
	if _, err := versionPipe.Exec(l.ctx); err != nil && !errors.Is(err, redis.Nil) {
		l.Errorf("verify public profile versions before cache backfill failed, error: %v", err)
		return
	}

	cachePipe := l.svcCtx.RedisCli.Pipeline()
	writes := 0
	for _, userID := range userIDs {
		expectedVersion, ok := expectedVersions[userID]
		if !ok {
			continue
		}
		currentVersion, err := publicProfileVersionResult(versionCmds[userID])
		if err != nil || currentVersion != expectedVersion {
			continue
		}

		cached := publicProfileCacheValue{UserID: userID, Missing: true}
		ttl := rediskey.AccountPublicProfileMissingTTL
		if profile, found := profileMap[userID]; found {
			cached = publicProfileCacheValue{
				UserID:         profile.GetUserId(),
				Username:       profile.GetUsername(),
				AvatarURL:      profile.GetAvatarUrl(),
				Bio:            profile.GetBio(),
				FollowerCount:  profile.GetFollowerCount(),
				FollowingCount: profile.GetFollowingCount(),
			}
			ttl = publicProfileCacheTTL(userID)
		}

		data, err := json.Marshal(cached)
		if err != nil {
			l.Errorf("marshal public profile cache failed, user_id: %d, error: %v", userID, err)
			continue
		}
		cachePipe.Set(l.ctx, rediskey.AccountPublicProfileKey(userID, expectedVersion), data, ttl)
		writes++
	}
	if writes == 0 {
		return
	}
	if _, err := cachePipe.Exec(l.ctx); err != nil {
		l.Errorf("backfill public profile cache failed, error: %v", err)
	}
}

func publicProfileVersionResult(cmd *redis.StringCmd) (int64, error) {
	version, err := cmd.Int64()
	//第一次Pipe查版本号如果不存在，可能是用户从未被更新过
	// 数据存在，但是版本号不存在
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if version < 0 {
		return 0, errors.New("公开资料缓存版本不能为负数")
	}
	return version, nil
}

func publicProfileCacheTTL(userID uint64) time.Duration {
	jitterRange := uint64(publicProfileCacheTTLJitter/time.Second) + 1
	jitter := time.Duration(userID%jitterRange) * time.Second
	return rediskey.AccountPublicProfileCacheTTL + jitter
}
