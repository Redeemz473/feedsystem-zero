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

type ListFollowingsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListFollowingsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListFollowingsLogic {
	return &ListFollowingsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// ListFollowings 查询“user_id 关注了谁”，缓存方案与 ListFollowers 对称。

// 关注/取关事务提交后，需要递增：
// SocialFollowersListVersionKey(followingID) 和
// SocialFollowingsListVersionKey(followerID)。
func (l *ListFollowingsLogic) ListFollowings(in *social.ListFollowingsReq) (*social.ListFollowingsResp, error) {
	//  1. 校验 user_id 非 0；统一 page_size，并校验 cursor_updated_at/cursor_follow_id 双游标组合。
	//     AccountRpc 校验放到真正回源查 MySQL 前调用，命中首页缓存的路径可以省掉一次 RPC。
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

	viewerID := in.GetViewerId()
	cacheKey := ""
	version := int64(0)
	useFixedWindow := false
	cacheWriteAllowed := false
	var lockKey, lockToken string
	//  2. 仅首页读取 SocialFollowingsListVersionKey 和
	//     SocialFollowingsFirstPageCacheKey。所有首页 page_size 共用一份
	//     固定 50 条窗口，缓存只保存基础关系和 has_more_after_window，
	//     不保存请求级 has_more、下一页游标和 viewer_is_following。
	//     命中后按 page_size 切片，并动态计算响应游标和 has_more。
	if !hasCursor {
		if currentVersion, ok := l.getFollowingsListVersion(userID); ok {
			version = currentVersion
			cacheKey = rediskey.SocialFollowingsFirstPageCacheKey(userID, version)
			if cached, hit := l.loadFollowingsFirstPageCache(cacheKey, version, userID); hit {
				return l.buildListFollowingsResp(cached.Relations, viewerID, pageSize, cached.HasMoreAfterWindow)
			}

			var locked bool
			lockKey, lockToken, locked, err = l.tryLockFollowingsFirstPageCache(cacheKey)
			switch {
			case err != nil:
				// Redis 异常时关闭缓存构建路径，按请求大小直接查询 MySQL。
				cacheKey = ""
			case locked:
				useFixedWindow = true
				cacheWriteAllowed = true
			default:
				if cached, hit := l.waitAndReloadFollowingsFirstPageCache(cacheKey, version, userID); hit {
					return l.buildListFollowingsResp(cached.Relations, viewerID, pageSize, cached.HasMoreAfterWindow)
				}
				// 有界等待后仍未命中，允许回源保证可用性，但无锁请求不能写缓存。
				useFixedWindow = true
			}
		}
	}
	//  3.非首页读取或首页读取未命中
	// 如果抢到锁需要释放掉
	if cacheWriteAllowed {
		defer l.releaseFollowingsFirstPageCacheLock(lockKey, lockToken)
	}

	dbPageSize := pageSize
	// 使用 followings 专属命名空间，避免与 ListFollowers 的 SingleFlight key 撞在一起。
	dbLoadKey := followingsDBLoadKey(userID, cursorUpdatedAt, cursorFollowID, dbPageSize)
	// 如果是首页查询的话，查mysql也会相应的改变
	if useFixedWindow {
		dbPageSize = socialFirstPageWindowSize
		dbLoadKey = "cache:" + cacheKey
	}

	// 走到这里说明首页缓存未命中或本身就是历史页，需要真正回源。
	// 只在此时校验目标用户是否存在，命中缓存的路径可以省掉一次 AccountRpc。
	if _, err := l.svcCtx.AccountRpc.GetProfile(l.ctx, &accountclient.GetProfileReq{
		UserId: userID,
	}); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "目标用户不存在")
		}
		l.Errorf("get followings target profile failed, user_id: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "校验目标用户失败")
	}

	follows, hasMoreAfterWindow, err := followListLoadGroup.Do(l.ctx, dbLoadKey, func() ([]model.Follow, bool, error) {
		return l.loadFollowingsFromDB(userID, cursorTime, cursorFollowID, hasCursor, dbPageSize)
	})
	if err != nil {
		if l.ctx.Err() != nil {
			return nil, status.FromContextError(l.ctx.Err()).Err()
		}
		l.Errorf("list followings from db failed, user_id: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "查询用户关注列表失败")
	}

	if cacheWriteAllowed {
		l.saveFollowingsFirstPageCache(cacheKey, version, userID, follows, hasMoreAfterWindow)
	}

	relations := followRowsToCacheItems(follows)
	return l.buildListFollowingsResp(relations, viewerID, pageSize, hasMoreAfterWindow)
}

// loadFollowingsFromDB 始终按 updated_at DESC, id DESC 返回稳定有序结果。
func (l *ListFollowingsLogic) loadFollowingsFromDB(
	userID uint64,
	cursorTime time.Time,
	cursorFollowID uint64,
	hasCursor bool,
	pageSize int,
) ([]model.Follow, bool, error) {
	query := l.svcCtx.GormDB.WithContext(l.ctx).
		Where(
			"follower_id = ? AND status = ? AND deleted_at IS NULL",
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
func (l *ListFollowingsLogic) buildListFollowingsResp(
	relations []followListCacheItem,
	viewerID uint64,
	pageSize int,
	hasMoreAfterWindow bool,
) (*social.ListFollowingsResp, error) {
	pageRelations, hasMore := selectFollowListPage(relations, pageSize, hasMoreAfterWindow)

	followingIDs := make([]uint64, 0, len(pageRelations))
	for _, relation := range pageRelations {
		followingIDs = append(followingIDs, relation.FollowingID)
	}

	followingStates, err := batchLoadFollowingStates(l.ctx, l.svcCtx, viewerID, followingIDs)
	if err != nil {
		l.Errorf("batch load following states failed, viewer_id: %d, error: %v", viewerID, err)
		return nil, err
	}

	items := make([]*social.FollowRelation, 0, len(pageRelations))
	for _, relation := range pageRelations {
		items = append(items, &social.FollowRelation{
			RelationId:  relation.RelationID,
			FollowerId:  relation.FollowerID,
			FollowingId: relation.FollowingID,
			FollowedAt:  relation.FollowedAt,
			// followings 列表按“被关注者”维度返回，是否被 viewer 关注要按 FollowingID 取。
			ViewerIsFollowing: followingStates[relation.FollowingID],
		})
	}

	var nextCursorUpdatedAt int64
	var nextCursorFollowID uint64
	if hasMore && len(pageRelations) > 0 {
		last := pageRelations[len(pageRelations)-1]
		nextCursorUpdatedAt = last.FollowedAt
		nextCursorFollowID = last.RelationID
	}

	return &social.ListFollowingsResp{
		Followings:          items,
		NextCursorUpdatedAt: nextCursorUpdatedAt,
		NextCursorFollowId:  nextCursorFollowID,
		HasMore:             hasMore,
	}, nil
}
