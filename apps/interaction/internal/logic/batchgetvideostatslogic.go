package logic

import (
	"context"
	"strconv"
	"time"

	"feedsystem-zero/apps/interaction/interaction"
	imodel "feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxBatchStatsVideoIDs = 50
	videoStatsCacheTTL    = 2 * time.Minute
)

type videoBaseStats struct {
	LikesCount    int64
	CommentsCount int64
	Popularity    int64
}

type BatchGetVideoStatsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBatchGetVideoStatsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BatchGetVideoStatsLogic {
	return &BatchGetVideoStatsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *BatchGetVideoStatsLogic) BatchGetVideoStats(in *interaction.BatchGetVideoStatsReq) (*interaction.BatchGetVideoStatsResp, error) {
	// 1. 校验 video_ids，并去重保序；限制单次批量数量，避免大请求压垮 Redis/MySQL。
	viewerID := in.GetViewerId()
	videoIDs, err := normalizeBatchVideoIDs(in.GetVideoIds())
	if err != nil {
		return nil, err
	}
	if len(videoIDs) == 0 {
		return &interaction.BatchGetVideoStatsResp{Stats: []*interaction.VideoInteractionStats{}}, nil
	}

	// 2. 批量查询当前用户是否点赞。未登录 viewer_id=0 时，helper 会全部返回 false。
	likedMap, err := batchLoadLikeStates(
		l.ctx,
		l.svcCtx.RedisCli,
		l.svcCtx.GormDB,
		videoIDs,
		viewerID,
	)
	if err != nil {
		return nil, status.Error(codes.Internal, "查询点赞状态失败")
	}

	// 3. 批量读取 Redis 统计缓存；缓存缺失的视频后续批量查 MySQL。
	statsMap := make(map[uint64]videoBaseStats, len(videoIDs))
	for _, videoID := range videoIDs {
		statsMap[videoID] = videoBaseStats{}
	}

	missVideoIDs := l.loadStatsCache(videoIDs, statsMap)
	if len(missVideoIDs) > 0 {
		foundVideoIDs, err := l.loadStatsFromDB(missVideoIDs, statsMap)
		if err != nil {
			l.Errorf("batch load stats from db failed, video_ids:%v err:%v", missVideoIDs, err)
			return nil, status.Error(codes.Internal, "查询视频统计失败")
		}
		l.cacheBaseStats(foundVideoIDs, statsMap)
	}

	// 4. 叠加 Redis 尚未被 job 刷入 MySQL 的实时增量。
	if err := l.applyRealtimeDeltas(videoIDs, statsMap); err != nil {
		l.Errorf("apply realtime stat deltas failed, video_ids:%v err:%v", videoIDs, err)
	}

	// 5. 按请求顺序组装返回，方便 gateway 直接按 video_id 映射回视频列表。
	respStats := make([]*interaction.VideoInteractionStats, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		stats := statsMap[videoID]
		respStats = append(respStats, &interaction.VideoInteractionStats{
			VideoId:       videoID,
			LikesCount:    nonNegative(stats.LikesCount),
			CommentsCount: nonNegative(stats.CommentsCount),
			Popularity:    nonNegative(stats.Popularity),
			IsLiked:       likedMap[videoID],
		})
	}

	return &interaction.BatchGetVideoStatsResp{Stats: respStats}, nil
}

func normalizeBatchVideoIDs(rawVideoIDs []uint64) ([]uint64, error) {
	if len(rawVideoIDs) > maxBatchStatsVideoIDs {
		return nil, status.Errorf(codes.InvalidArgument, "一次最多查询%d个视频", maxBatchStatsVideoIDs)
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

func (l *BatchGetVideoStatsLogic) loadStatsCache(videoIDs []uint64, statsMap map[uint64]videoBaseStats) []uint64 {
	pipe := l.svcCtx.RedisCli.Pipeline()
	cmdMap := make(map[uint64]*redis.MapStringStringCmd, len(videoIDs))
	for _, videoID := range videoIDs {
		cmdMap[videoID] = pipe.HGetAll(l.ctx, rediskey.VideoStatsCacheKey(videoID))
	}

	if _, err := pipe.Exec(l.ctx); err != nil {
		l.Errorf("batch get stats cache failed, err:%v", err)
		return append([]uint64(nil), videoIDs...)
	}

	missVideoIDs := make([]uint64, 0)
	for _, videoID := range videoIDs {
		cmd := cmdMap[videoID]
		hashMap, err := cmd.Result()
		if err != nil {
			l.Errorf("get stats cache failed, video_id:%d err:%v", videoID, err)
			missVideoIDs = append(missVideoIDs, videoID)
			continue
		}
		if len(hashMap) == 0 {
			missVideoIDs = append(missVideoIDs, videoID)
			continue
		}

		statsMap[videoID] = videoBaseStats{
			LikesCount:    parseInt64(hashMap["likes_count"]),
			CommentsCount: parseInt64(hashMap["comments_count"]),
			Popularity:    parseInt64(hashMap["popularity"]),
		}
	}

	return missVideoIDs
}

// 查video表得到点赞数、评论数、热度值
func (l *BatchGetVideoStatsLogic) loadStatsFromDB(videoIDs []uint64, statsMap map[uint64]videoBaseStats) ([]uint64, error) {
	var videos []imodel.Video
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Select("id", "likes_count", "comments_count", "popularity").
		Where("id IN ? AND status = ? AND deleted_at IS NULL", videoIDs, imodel.VideoStatusNormal).
		Find(&videos).Error; err != nil {
		return nil, err
	}

	foundVideoIDs := make([]uint64, 0, len(videos))
	for _, video := range videos {
		foundVideoIDs = append(foundVideoIDs, video.ID)
		statsMap[video.ID] = videoBaseStats{
			LikesCount:    video.LikesCount,
			CommentsCount: video.CommentsCount,
			Popularity:    video.Popularity,
		}
	}

	return foundVideoIDs, nil
}

func (l *BatchGetVideoStatsLogic) cacheBaseStats(videoIDs []uint64, statsMap map[uint64]videoBaseStats) {
	if len(videoIDs) == 0 {
		return
	}

	now := time.Now().UnixMilli()
	pipe := l.svcCtx.RedisCli.Pipeline()
	for _, videoID := range videoIDs {
		stats := statsMap[videoID]
		pipe.HSet(l.ctx, rediskey.VideoStatsCacheKey(videoID), map[string]any{
			"likes_count":    stats.LikesCount,
			"comments_count": stats.CommentsCount,
			"popularity":     stats.Popularity,
			"updated_at":     now,
		})
		pipe.Expire(l.ctx, rediskey.VideoStatsCacheKey(videoID), videoStatsCacheTTL)
	}
	if _, err := pipe.Exec(l.ctx); err != nil {
		l.Errorf("cache base stats failed, video_ids:%v err:%v", videoIDs, err)
	}
}

func (l *BatchGetVideoStatsLogic) applyRealtimeDeltas(videoIDs []uint64, statsMap map[uint64]videoBaseStats) error {
	fields := make([]string, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		fields = append(fields, redisHashField(videoID))
	}

	pipe := l.svcCtx.RedisCli.Pipeline()
	likeDeltasCmd := pipe.HMGet(l.ctx, rediskey.VideoLikeDeltaKey(), fields...)
	commentDeltasCmd := pipe.HMGet(l.ctx, rediskey.VideoCommentDeltaKey(), fields...)
	popularityDeltasCmd := pipe.HMGet(l.ctx, rediskey.VideoPopularityDeltaKey(), fields...)

	if _, err := pipe.Exec(l.ctx); err != nil {
		return err
	}

	likeDeltas := likeDeltasCmd.Val()
	commentDeltas := commentDeltasCmd.Val()
	popularityDeltas := popularityDeltasCmd.Val()

	for i, videoID := range videoIDs {
		stats := statsMap[videoID]
		stats.LikesCount = nonNegative(stats.LikesCount + parseRedisInt64At(likeDeltas, i))
		stats.CommentsCount = nonNegative(stats.CommentsCount + parseRedisInt64At(commentDeltas, i))
		stats.Popularity = nonNegative(stats.Popularity + parseRedisInt64At(popularityDeltas, i))
		statsMap[videoID] = stats
	}

	return nil
}

func parseInt64(value string) int64 {
	n, _ := strconv.ParseInt(value, 10, 64)
	return n
}

func parseRedisInt64At(values []any, index int) int64 {
	if index < 0 || index >= len(values) || values[index] == nil {
		return 0
	}

	switch value := values[index].(type) {
	case int64:
		return value
	case int:
		return int64(value)
	case uint64:
		return int64(value)
	case string:
		return parseInt64(value)
	case []byte:
		return parseInt64(string(value))
	default:
		return 0
	}
}
