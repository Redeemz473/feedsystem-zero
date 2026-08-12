package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/syncx"
)

const (
	defaultAnonymousHotFeedCacheTTL = 2 * time.Second
	defaultHotFeedCacheRedisTimeout = 300 * time.Millisecond
	defaultHotFeedCacheLockTTL      = 2 * time.Second
	defaultHotFeedCacheBuildWait    = 250 * time.Millisecond
	hotFeedCachePollInterval        = 25 * time.Millisecond
	defaultHotFeedPageSize          = int64(20)
	maxHotFeedPageSize              = int64(50)
)

var anonymousHotFeedPageBuildGroup = syncx.NewSingleFlight()

const releaseHotFeedCacheLockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`

func (l *GetHotFeedLogic) getCachedAnonymousHotFeed(req *types.GetHotFeedReq) (*types.GetHotFeedResp, error) {
	cacheKey, ok := anonymousHotFeedPageCacheKey(req, time.Now())
	if !ok {
		return l.buildHotFeed(req, 0)
	}
	if cached, hit := l.loadAnonymousHotFeedPage(cacheKey); hit {
		return cached, nil
	}

	value, err := anonymousHotFeedPageBuildGroup.Do(cacheKey, func() (any, error) {
		if cached, hit := l.loadAnonymousHotFeedPage(cacheKey); hit {
			return cached, nil
		}

		lockKey := rediskey.GatewayAnonymousHotFeedPageBuildLockKey(cacheKey)
		lockToken, locked, lockErr := l.acquireAnonymousHotFeedBuildLock(lockKey)
		if lockErr == nil && !locked {
			if cached, hit := l.waitAnonymousHotFeedPage(cacheKey); hit {
				return cached, nil
			}
			// 其他实例构建超时只影响缓存收益，当前请求直接回源，避免扩大用户延迟。
			return l.buildHotFeed(req, 0)
		}

		if locked {
			defer l.releaseAnonymousHotFeedBuildLock(lockKey, lockToken)
		}
		resp, buildErr := l.buildHotFeed(req, 0)
		if buildErr != nil {
			return nil, buildErr
		}
		l.saveAnonymousHotFeedPage(cacheKey, resp)
		return resp, nil
	})
	if err != nil {
		return nil, err
	}
	resp, ok := value.(*types.GetHotFeedResp)
	if !ok || resp == nil {
		return nil, fmt.Errorf("游客热榜缓存返回类型异常: %T", value)
	}
	return resp, nil
}

func anonymousHotFeedPageCacheKey(req *types.GetHotFeedReq, now time.Time) (string, bool) {
	if req == nil || req.Snapshotat < 0 || req.Offset < 0 || req.Pagesize < 0 {
		return "", false
	}
	snapshotKey := req.Snapshotat
	if snapshotKey == 0 {
		snapshotKey = now.UTC().Truncate(time.Minute).Unix()
	} else {
		snapshotKey = time.Unix(snapshotKey, 0).UTC().Truncate(time.Minute).Unix()
	}
	pageSize := req.Pagesize
	if pageSize == 0 {
		pageSize = defaultHotFeedPageSize
	} else if pageSize > maxHotFeedPageSize {
		pageSize = maxHotFeedPageSize
	}
	return rediskey.GatewayAnonymousHotFeedPageCacheKey(snapshotKey, req.Offset, pageSize), true
}

func (l *GetHotFeedLogic) loadAnonymousHotFeedPage(cacheKey string) (*types.GetHotFeedResp, bool) {
	redisCtx, cancel := context.WithTimeout(l.ctx, hotFeedCacheRedisTimeout(l.svcCtx))
	defer cancel()

	payload, err := l.svcCtx.RedisCli.Get(redisCtx, cacheKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false
	}
	if err != nil {
		l.Errorf("load anonymous hot feed cache failed, key:%s error:%v", cacheKey, err)
		return nil, false
	}

	var resp types.GetHotFeedResp
	if err := json.Unmarshal(payload, &resp); err != nil || resp.Snapshotat <= 0 {
		l.Errorf("decode anonymous hot feed cache failed, key:%s error:%v", cacheKey, err)
		_ = l.svcCtx.RedisCli.Unlink(redisCtx, cacheKey).Err()
		return nil, false
	}
	return &resp, true
}

func (l *GetHotFeedLogic) saveAnonymousHotFeedPage(cacheKey string, resp *types.GetHotFeedResp) {
	payload, err := json.Marshal(resp)
	if err != nil {
		l.Errorf("encode anonymous hot feed cache failed, key:%s error:%v", cacheKey, err)
		return
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, hotFeedCacheRedisTimeout(l.svcCtx))
	defer cancel()
	if err := l.svcCtx.RedisCli.Set(redisCtx, cacheKey, payload, anonymousHotFeedCacheTTL(l.svcCtx)).Err(); err != nil {
		l.Errorf("save anonymous hot feed cache failed, key:%s error:%v", cacheKey, err)
	}
}

func (l *GetHotFeedLogic) acquireAnonymousHotFeedBuildLock(lockKey string) (string, bool, error) {
	lockToken, err := randomHotFeedCacheToken()
	if err != nil {
		return "", false, err
	}
	redisCtx, cancel := context.WithTimeout(l.ctx, hotFeedCacheRedisTimeout(l.svcCtx))
	defer cancel()
	locked, err := l.svcCtx.RedisCli.SetNX(
		redisCtx,
		lockKey,
		lockToken,
		hotFeedCacheLockTTL(l.svcCtx),
	).Result()
	return lockToken, locked, err
}

func (l *GetHotFeedLogic) waitAnonymousHotFeedPage(cacheKey string) (*types.GetHotFeedResp, bool) {
	wait := hotFeedCacheBuildWait(l.svcCtx)
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	ticker := time.NewTicker(hotFeedCachePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return nil, false
		case <-deadline.C:
			return nil, false
		case <-ticker.C:
			if cached, hit := l.loadAnonymousHotFeedPage(cacheKey); hit {
				return cached, true
			}
		}
	}
}

func (l *GetHotFeedLogic) releaseAnonymousHotFeedBuildLock(lockKey string, lockToken string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(l.ctx), hotFeedCacheRedisTimeout(l.svcCtx))
	defer cancel()
	if err := l.svcCtx.RedisCli.Eval(ctx, releaseHotFeedCacheLockScript, []string{lockKey}, lockToken).Err(); err != nil && !errors.Is(err, redis.Nil) {
		l.Errorf("release anonymous hot feed cache lock failed, key:%s error:%v", lockKey, err)
	}
}

func randomHotFeedCacheToken() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func anonymousHotFeedCacheTTL(svcCtx *svc.ServiceContext) time.Duration {
	value := svcCtx.Config.HotFeedCache.AnonymousTTLMilliseconds
	if value <= 0 {
		return defaultAnonymousHotFeedCacheTTL
	}
	return time.Duration(value) * time.Millisecond
}

func hotFeedCacheRedisTimeout(svcCtx *svc.ServiceContext) time.Duration {
	value := svcCtx.Config.HotFeedCache.RedisOpTimeoutMs
	if value <= 0 {
		return defaultHotFeedCacheRedisTimeout
	}
	return time.Duration(value) * time.Millisecond
}

func hotFeedCacheLockTTL(svcCtx *svc.ServiceContext) time.Duration {
	value := svcCtx.Config.HotFeedCache.BuildLockTTLMilliseconds
	if value <= 0 {
		return defaultHotFeedCacheLockTTL
	}
	return time.Duration(value) * time.Millisecond
}

func hotFeedCacheBuildWait(svcCtx *svc.ServiceContext) time.Duration {
	value := svcCtx.Config.HotFeedCache.BuildWaitMs
	if value <= 0 {
		return defaultHotFeedCacheBuildWait
	}
	return time.Duration(value) * time.Millisecond
}
