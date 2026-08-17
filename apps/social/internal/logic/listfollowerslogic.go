package logic

import (
	"context"
	"time"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/social/internal/model"
	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ListFollowersLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListFollowersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListFollowersLogic {
	return &ListFollowersLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// ListFollowers 查询“谁关注了 user_id”。
// 首页共享一份按 updated_at DESC、id DESC 排序的固定 50 条基础关系缓存；
// 小页从该窗口切片，历史页直接使用复合游标查询 MySQL。
// 本地 SingleFlight 合并相同数据库查询，Redis token 锁负责跨实例缓存构建；
// 缓存写入还会原子核对版本，避免关注关系变化后写入旧数据库快照。
// viewer_is_following 不进入公共缓存，而是按当前 viewer_id 动态批量计算。
//
// 缓存失效规则：
// Follow/Unfollow 的 MySQL 事务提交且关系真实发生变化后，applyFollowCacheAfterCommit 会执行：
// INCR SocialFollowersListVersionKey(followingID)
// INCR SocialFollowingsListVersionKey(followerID)
// 旧版本首页缓存不扫描删除，等待自身 TTL 自动淘汰。
func (l *ListFollowersLogic) ListFollowers(in *social.ListFollowersReq) (*social.ListFollowersResp, error) {
	userID := in.GetUserId()
	if userID == 0 {
		return nil, status.Error(codes.InvalidArgument, "用户ID不能为空")
	}

	pageSize, err := normalizeSocialPage(in.GetPageSize())
	if err != nil {
		return nil, err
	}

	cursorUpdatedAt := in.GetCursorUpdatedAt()
	cursorFollowID := in.GetCursorFollowId()
	cursorTime, hasCursor, err := validateFollowCursor(cursorUpdatedAt, cursorFollowID)
	if err != nil {
		return nil, err
	}

	if _, err := l.svcCtx.AccountRpc.GetProfile(l.ctx, &accountclient.GetProfileReq{
		UserId: userID,
	}); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "目标用户不存在")
		}
		l.Errorf("get followers target profile failed, user_id: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "校验目标用户失败")
	}

	viewerID := in.GetViewerId()
	cacheKey := ""
	version := int64(0)
	//当前请求是首页请求
	useFixedWindow := false
	//当前请求是否可以写入redis缓存
	cacheWriteAllowed := false
	var lockKey, lockToken string

	if !hasCursor {
		if currentVersion, ok := l.getFollowersListVersion(userID); ok {
			version = currentVersion
			cacheKey = rediskey.SocialFollowersFirstPageCacheKey(userID, version)
			if cached, hit := l.loadFollowersFirstPageCache(cacheKey, version, userID); hit {
				return l.buildListFollowersResp(cached.Relations, viewerID, pageSize, cached.HasMoreAfterWindow)
			}

			var locked bool
			lockKey, lockToken, locked, err = l.tryLockFollowersFirstPageCache(cacheKey)
			switch {
			case err != nil:
				// Redis 异常时关闭缓存构建路径，按请求大小直接查询 MySQL。
				cacheKey = ""
			case locked:
				useFixedWindow = true
				cacheWriteAllowed = true
			default:
				if cached, hit := l.waitAndReloadFollowersFirstPageCache(cacheKey, version, userID); hit {
					return l.buildListFollowersResp(cached.Relations, viewerID, pageSize, cached.HasMoreAfterWindow)
				}
				// 有界等待后仍未命中，允许回源保证可用性，但无锁请求不能写缓存。
				useFixedWindow = true
			}
		}
	}
	if cacheWriteAllowed {
		defer l.releaseFollowersFirstPageCacheLock(lockKey, lockToken)
	}

	dbPageSize := pageSize
	dbLoadKey := followersDBLoadKey(userID, cursorUpdatedAt, cursorFollowID, dbPageSize)
	if useFixedWindow {
		dbPageSize = socialFirstPageWindowSize
		// 版本化缓存 key 参与 SingleFlight，避免新版本请求复用旧版本数据库快照。
		dbLoadKey = "cache:" + cacheKey
	}

	follows, hasMoreAfterWindow, err := followListLoadGroup.Do(l.ctx, dbLoadKey, func() ([]model.Follow, bool, error) {
		return l.loadFollowersFromDB(userID, cursorTime, cursorFollowID, hasCursor, dbPageSize)
	})
	if err != nil {
		if l.ctx.Err() != nil {
			return nil, status.FromContextError(l.ctx.Err()).Err()
		}
		l.Errorf("list followers from db failed, user_id: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "查询粉丝列表失败")
	}

	if cacheWriteAllowed {
		l.saveFollowersFirstPageCache(cacheKey, version, userID, follows, hasMoreAfterWindow)
	}

	relations := followRowsToCacheItems(follows)
	return l.buildListFollowersResp(relations, viewerID, pageSize, hasMoreAfterWindow)
}

// loadFollowersFromDB 始终按 updated_at DESC, id DESC 返回稳定有序结果。
func (l *ListFollowersLogic) loadFollowersFromDB(
	userID uint64,
	cursorTime time.Time,
	cursorFollowID uint64,
	hasCursor bool,
	pageSize int,
) ([]model.Follow, bool, error) {
	query := l.svcCtx.GormDB.WithContext(l.ctx).
		Where(
			"following_id = ? AND status = ? AND deleted_at IS NULL",
			userID,
			model.FollowStatusActive,
		)
	if hasCursor {
		query = query.Where(
			"(updated_at < ? OR (updated_at = ? AND id < ?))",
			cursorTime,
			cursorTime,
			cursorFollowID,
		)
	}

	follows := make([]model.Follow, 0, pageSize+1)
	if err := query.
		Order("updated_at DESC").
		Order("id DESC").
		Limit(pageSize + 1).
		Find(&follows).Error; err != nil {
		return nil, false, err
	}

	hasMore := len(follows) > pageSize
	if hasMore {
		follows = follows[:pageSize]
	}
	return follows, hasMore, nil
}

// buildListFollowersResp 保持 relations 的既有顺序，只用状态 map 做按 ID 补值。
func (l *ListFollowersLogic) buildListFollowersResp(
	relations []followListCacheItem,
	viewerID uint64,
	pageSize int,
	hasMoreAfterWindow bool,
) (*social.ListFollowersResp, error) {
	pageRelations, hasMore := selectFollowListPage(relations, pageSize, hasMoreAfterWindow)

	followerIDs := make([]uint64, 0, len(pageRelations))
	for _, relation := range pageRelations {
		followerIDs = append(followerIDs, relation.FollowerID)
	}

	followingStates, err := batchLoadFollowingStates(l.ctx, l.svcCtx, viewerID, followerIDs)
	if err != nil {
		l.Errorf("batch load following states failed, viewer_id: %d, error: %v", viewerID, err)
		return nil, err
	}

	items := make([]*social.FollowRelation, 0, len(pageRelations))
	for _, relation := range pageRelations {
		items = append(items, &social.FollowRelation{
			RelationId:        relation.RelationID,
			FollowerId:        relation.FollowerID,
			FollowingId:       relation.FollowingID,
			FollowedAt:        relation.FollowedAt,
			ViewerIsFollowing: followingStates[relation.FollowerID],
		})
	}

	var nextCursorUpdatedAt int64
	var nextCursorFollowID uint64
	if hasMore && len(pageRelations) > 0 {
		last := pageRelations[len(pageRelations)-1]
		nextCursorUpdatedAt = last.FollowedAt
		nextCursorFollowID = last.RelationID
	}

	return &social.ListFollowersResp{
		Followers:           items,
		NextCursorUpdatedAt: nextCursorUpdatedAt,
		NextCursorFollowId:  nextCursorFollowID,
		HasMore:             hasMore,
	}, nil
}
