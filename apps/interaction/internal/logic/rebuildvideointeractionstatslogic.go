package logic

import (
	"context"
	"time"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type RebuildVideoInteractionStatsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRebuildVideoInteractionStatsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RebuildVideoInteractionStatsLogic {
	return &RebuildVideoInteractionStatsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 重建用于数据修复接口
// 当 MySQL 的计数和 Redis 的增量加起来不对时，用 likes/comments 表从源头重算一遍。
func (l *RebuildVideoInteractionStatsLogic) RebuildVideoInteractionStats(in *interaction.RebuildVideoInteractionStatsReq) (*interaction.RebuildVideoInteractionStatsResp, error) {
	videoIDs, err := normalizeRebuildVideoIDs(in.GetVideoIds())
	if err != nil {
		return nil, err
	}
	if len(videoIDs) == 0 {
		return &interaction.RebuildVideoInteractionStatsResp{Stats: []*interaction.VideoInteractionStats{}}, nil
	}

	rebuildLockKey, rebuildLockToken, locked, err := l.acquireRebuildStatsLock()
	if err != nil {
		l.Errorf("acquire rebuild stats lock failed, error:%v", err)
		return nil, status.Error(codes.Internal, "获取统计重建锁失败")
	} else if !locked {
		return nil, status.Error(codes.Aborted, "互动统计重建任务正在处理中")
	}
	defer l.releaseRebuildStatsLock(rebuildLockKey, rebuildLockToken)
	if err := l.waitForStatsMutationsDrained(); err != nil {
		l.Errorf("wait for interaction stats mutations drained failed, error:%v", err)
		return nil, status.Error(codes.Aborted, "等待互动写入结束超时，请稍后重试")
	}

	existingVideoIDs, err := l.loadExistingNormalVideoIDs(videoIDs)
	if err != nil {
		l.Errorf("load rebuild videos failed, video_ids:%v error:%v", videoIDs, err)
		return nil, status.Error(codes.Internal, "查询视频失败")
	}
	if len(existingVideoIDs) == 0 {
		return nil, status.Error(codes.NotFound, "视频不存在或已删除")
	}

	likeCounts, err := l.countActiveLikes(existingVideoIDs)
	if err != nil {
		l.Errorf("count active likes failed, video_ids:%v error:%v", existingVideoIDs, err)
		return nil, status.Error(codes.Internal, "重建点赞数失败")
	}
	commentCounts, err := l.countActiveComments(existingVideoIDs)
	if err != nil {
		l.Errorf("count active comments failed, video_ids:%v error:%v", existingVideoIDs, err)
		return nil, status.Error(codes.Internal, "重建评论数失败")
	}

	statsMap := make(map[uint64]*interaction.VideoInteractionStats, len(existingVideoIDs))
	for _, videoID := range existingVideoIDs {
		likesCount := likeCounts[videoID]
		commentsCount := commentCounts[videoID]
		statsMap[videoID] = &interaction.VideoInteractionStats{
			VideoId:       videoID,
			LikesCount:    likesCount,
			CommentsCount: commentsCount,
			Popularity:    likesCount*likePopularityWeight + commentsCount*commentPopularityWeight,
		}
	}

	// Redis 增量不能在重建时粗暴 HDEL。这里读取当前增量快照，并把写入 videos 的基准值调整为
	// true_count - redis_delta。这样 BatchGetVideoStats 继续按“基准值 + Redis 增量”返回真实值，
	// 后续 flush job 再把这部分 delta 正常刷入 MySQL。
	deltaSnapshot, err := l.loadRebuildDeltaSnapshot(existingVideoIDs)
	if err != nil {
		l.Errorf("load rebuild delta snapshot failed, video_ids:%v error:%v", existingVideoIDs, err)
		return nil, status.Error(codes.Internal, "读取实时增量失败")
	}
	baseStatsMap := buildRebuiltBaseStats(statsMap, deltaSnapshot)
	if err := l.saveRebuiltStats(baseStatsMap); err != nil {
		l.Errorf("save rebuilt stats failed, video_ids:%v error:%v", existingVideoIDs, err)
		return nil, status.Error(codes.Internal, "保存重建统计失败")
	}
	l.refreshRedisAfterRebuild(baseStatsMap)

	resp := &interaction.RebuildVideoInteractionStatsResp{
		Stats: make([]*interaction.VideoInteractionStats, 0, len(statsMap)),
	}
	for _, videoID := range videoIDs {
		if stats, ok := statsMap[videoID]; ok {
			resp.Stats = append(resp.Stats, stats)
		}
	}
	return resp, nil
}

func (l *RebuildVideoInteractionStatsLogic) acquireRebuildStatsLock() (string, string, bool, error) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	return tryAcquireInteractionRebuildLock(redisCtx, l.svcCtx.RedisCli)
}

func (l *RebuildVideoInteractionStatsLogic) releaseRebuildStatsLock(lockKey string, lockToken string) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	if err := releaseRedisLock(redisCtx, l.svcCtx.RedisCli, lockKey, lockToken); err != nil {
		l.Errorf("release rebuild stats lock failed, key:%s error:%v", lockKey, err)
	}
}

func (l *RebuildVideoInteractionStatsLogic) waitForStatsMutationsDrained() error {
	return waitForInteractionStatsMutationsDrained(l.ctx, l.svcCtx.RedisCli)
}

func normalizeRebuildVideoIDs(rawVideoIDs []uint64) ([]uint64, error) {
	if len(rawVideoIDs) > maxRebuildInteractionStatItems {
		return nil, status.Errorf(codes.InvalidArgument, "一次最多重建%d个视频", maxRebuildInteractionStatItems)
	}

	seen := make(map[uint64]struct{}, len(rawVideoIDs))
	videoIDs := make([]uint64, 0, len(rawVideoIDs))
	for _, videoID := range rawVideoIDs {
		if videoID == 0 {
			return nil, status.Error(codes.InvalidArgument, "video_id不能为空")
		}
		if _, ok := seen[videoID]; ok {
			continue
		}
		seen[videoID] = struct{}{}
		videoIDs = append(videoIDs, videoID)
	}
	return videoIDs, nil
}

func (l *RebuildVideoInteractionStatsLogic) loadExistingNormalVideoIDs(videoIDs []uint64) ([]uint64, error) {
	var videos []model.Video
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Select("id").
		Where("id IN ? AND status = ? AND deleted_at IS NULL", videoIDs, model.VideoStatusNormal).
		Find(&videos).Error; err != nil {
		return nil, err
	}

	exists := make(map[uint64]struct{}, len(videos))
	for _, video := range videos {
		exists[video.ID] = struct{}{}
	}

	// 按请求顺序返回，方便调用方对照结果。
	result := make([]uint64, 0, len(videos))
	for _, videoID := range videoIDs {
		if _, ok := exists[videoID]; ok {
			result = append(result, videoID)
		}
	}
	return result, nil
}

func (l *RebuildVideoInteractionStatsLogic) countActiveLikes(videoIDs []uint64) (map[uint64]int64, error) {
	counts := make(map[uint64]int64, len(videoIDs))
	var rows []videoStatCount
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Model(&model.Like{}).
		Select("video_id, COUNT(*) AS count").
		Where("video_id IN ? AND status = ? AND deleted_at IS NULL", videoIDs, model.LikeStatusActive).
		Group("video_id").
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		counts[row.VideoID] = row.Count
	}
	return counts, nil
}

func (l *RebuildVideoInteractionStatsLogic) countActiveComments(videoIDs []uint64) (map[uint64]int64, error) {
	counts := make(map[uint64]int64, len(videoIDs))
	var rows []videoStatCount
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Model(&model.Comment{}).
		Select("video_id, COUNT(*) AS count").
		Where("video_id IN ? AND status = ? AND deleted_at IS NULL", videoIDs, model.CommentStatusNormal).
		Group("video_id").
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		counts[row.VideoID] = row.Count
	}
	return counts, nil
}

func (l *RebuildVideoInteractionStatsLogic) saveRebuiltStats(statsMap map[uint64]*interaction.VideoInteractionStats) error {
	return l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		for videoID, stats := range statsMap {
			if err := tx.Model(&model.Video{}).
				Where("id = ? AND status = ? AND deleted_at IS NULL", videoID, model.VideoStatusNormal).
				Updates(map[string]any{
					"likes_count":    stats.GetLikesCount(),
					"comments_count": stats.GetCommentsCount(),
					"popularity":     stats.GetPopularity(),
				}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (l *RebuildVideoInteractionStatsLogic) loadRebuildDeltaSnapshot(videoIDs []uint64) (map[uint64]videoStatDelta, error) {
	result := make(map[uint64]videoStatDelta, len(videoIDs))
	for _, videoID := range videoIDs {
		result[videoID] = videoStatDelta{}
	}
	if len(videoIDs) == 0 {
		return result, nil
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()

	fields := make([]string, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		fields = append(fields, redisHashField(videoID))
	}

	pipe := l.svcCtx.RedisCli.Pipeline()
	likeDeltasCmd := pipe.HMGet(redisCtx, rediskey.VideoLikeDeltaKey(), fields...)
	commentDeltasCmd := pipe.HMGet(redisCtx, rediskey.VideoCommentDeltaKey(), fields...)
	popularityDeltasCmd := pipe.HMGet(redisCtx, rediskey.VideoPopularityDeltaKey(), fields...)
	if _, err := pipe.Exec(redisCtx); err != nil {
		return nil, err
	}

	likeDeltas := likeDeltasCmd.Val()
	commentDeltas := commentDeltasCmd.Val()
	popularityDeltas := popularityDeltasCmd.Val()
	for index, videoID := range videoIDs {
		result[videoID] = videoStatDelta{
			LikeDelta:       parseRedisInt64At(likeDeltas, index),
			CommentDelta:    parseRedisInt64At(commentDeltas, index),
			PopularityDelta: parseRedisInt64At(popularityDeltas, index),
		}
	}
	return result, nil
}

func buildRebuiltBaseStats(statsMap map[uint64]*interaction.VideoInteractionStats, deltaSnapshot map[uint64]videoStatDelta) map[uint64]*interaction.VideoInteractionStats {
	baseStatsMap := make(map[uint64]*interaction.VideoInteractionStats, len(statsMap))
	for videoID, stats := range statsMap {
		delta := deltaSnapshot[videoID]
		baseStatsMap[videoID] = &interaction.VideoInteractionStats{
			VideoId:       videoID,
			LikesCount:    nonNegative(stats.GetLikesCount() - delta.LikeDelta),
			CommentsCount: nonNegative(stats.GetCommentsCount() - delta.CommentDelta),
			Popularity:    nonNegative(stats.GetPopularity() - delta.PopularityDelta),
		}
	}
	return baseStatsMap
}

func (l *RebuildVideoInteractionStatsLogic) refreshRedisAfterRebuild(statsMap map[uint64]*interaction.VideoInteractionStats) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()

	now := time.Now().UnixMilli()
	pipe := l.svcCtx.RedisCli.Pipeline()
	for videoID, stats := range statsMap {
		// 这里缓存的是调整后的基准值，Redis 增量继续保留，由 BatchGetVideoStats 叠加。
		pipe.HSet(redisCtx, rediskey.VideoStatsCacheKey(videoID), map[string]any{
			"likes_count":    stats.GetLikesCount(),
			"comments_count": stats.GetCommentsCount(),
			"popularity":     stats.GetPopularity(),
			"updated_at":     now,
		})
		pipe.Expire(redisCtx, rediskey.VideoStatsCacheKey(videoID), videoStatsCacheTTL)
	}
	if _, err := pipe.Exec(redisCtx); err != nil {
		l.Errorf("refresh redis after stats rebuild failed, error:%v", err)
	}
}
