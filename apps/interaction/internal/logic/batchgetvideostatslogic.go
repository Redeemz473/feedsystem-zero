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
)

type videoBaseStats struct {
	LikesCount    int64
	CommentsCount int64
	Popularity    int64
	StatsVersion  uint64
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

// BatchGetVideoStats 优先读取 Redis 版本化统计投影；miss 时批量读取 MySQL 持久快照，
// 再用一条 Pipeline 原子冷启动全部 key。Redis 故障时直接降级返回 MySQL 快照。
func (l *BatchGetVideoStatsLogic) BatchGetVideoStats(in *interaction.BatchGetVideoStatsReq) (*interaction.BatchGetVideoStatsResp, error) {
	// 校验 video_ids，并去重保序；限制单次批量数量，避免大请求压垮 Redis/MySQL。
	viewerID := in.GetViewerId()
	videoIDs, err := normalizeBatchVideoIDs(in.GetVideoIds())
	if err != nil {
		return nil, err
	}
	if len(videoIDs) == 0 {
		return &interaction.BatchGetVideoStatsResp{Stats: []*interaction.VideoInteractionStats{}}, nil
	}

	// 批量查询当前用户是否点赞。未登录 viewer_id=0 时，helper 会全部返回 false。
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

	// 从 Redis 权威 Hash 批量读；未命中的 video 后续用 DB 冷备值冷启动。
	statsMap := make(map[uint64]videoBaseStats, len(videoIDs))
	for _, videoID := range videoIDs {
		statsMap[videoID] = videoBaseStats{}
	}

	missVideoIDs := l.loadAuthStats(videoIDs, statsMap)

	// Redis miss 的视频用 MySQL 冷备值做冷启动：读取 videos 冷备字段，
	// 通过 readVideoStatsAuthScript 的 EXISTS==0 分支原子建立 auth key（避免 double init）。
	if len(missVideoIDs) > 0 {
		if err := l.coldStartAuthStats(missVideoIDs, statsMap); err != nil {
			l.Errorf("cold start auth stats failed, video_ids:%v err:%v", missVideoIDs, err)
			return nil, status.Error(codes.Internal, "查询视频统计失败")
		}
	}

	// 按请求顺序组装返回，方便 gateway 直接按 video_id 映射回视频列表。
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

// loadAuthStats 用 Pipeline 批量 HGetAll 并刷新 TTL。字段不完整的旧 key 视为 miss，
// 后续会用带 stats_version 的 MySQL 快照重建。
func (l *BatchGetVideoStatsLogic) loadAuthStats(videoIDs []uint64, statsMap map[uint64]videoBaseStats) []uint64 {
	pipe := l.svcCtx.RedisCli.Pipeline()
	cmdMap := make(map[uint64]*redis.MapStringStringCmd, len(videoIDs))
	for _, videoID := range videoIDs {
		key := rediskey.VideoStatsAuthKey(videoID)
		cmdMap[videoID] = pipe.HGetAll(l.ctx, key)
		pipe.Expire(l.ctx, key, rediskey.VideoStatsAuthTTL)
	}

	if _, err := pipe.Exec(l.ctx); err != nil {
		l.Errorf("batch hgetall auth stats failed, err:%v", err)
		return append([]uint64(nil), videoIDs...)
	}

	missVideoIDs := make([]uint64, 0)
	for _, videoID := range videoIDs {
		cmd := cmdMap[videoID]
		hashMap, err := cmd.Result()
		if err != nil {
			l.Errorf("hgetall auth stats failed, video_id:%d err:%v", videoID, err)
			missVideoIDs = append(missVideoIDs, videoID)
			continue
		}
		authStats, ok := parseVideoStatsAuthHash(hashMap)
		if !ok {
			missVideoIDs = append(missVideoIDs, videoID)
			continue
		}

		statsMap[videoID] = videoBaseStats{
			LikesCount:    authStats.LikesCount,
			CommentsCount: authStats.CommentsCount,
			Popularity:    authStats.Popularity,
			StatsVersion:  authStats.StatsVersion,
		}
	}

	return missVideoIDs
}

// coldStartAuthStats 一次性从 MySQL 读取 miss 视频的持久统计，
// 再通过单条 Pipeline 批量执行版本化冷启动 Lua，避免每个视频一次网络往返。
// 已被删除的视频保持 statsMap 里的零值，读侧对外仍返回 0。
func (l *BatchGetVideoStatsLogic) coldStartAuthStats(videoIDs []uint64, statsMap map[uint64]videoBaseStats) error {
	var videos []imodel.Video
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Select("id", "likes_count", "comments_count", "popularity", "stats_version").
		Where("id IN ? AND status = ? AND deleted_at IS NULL", videoIDs, imodel.VideoStatusNormal).
		Find(&videos).Error; err != nil {
		return err
	}

	pipe := l.svcCtx.RedisCli.Pipeline()
	cmdMap := make(map[uint64]*redis.Cmd, len(videos))
	for _, video := range videos {
		base := videoBaseStatsFromDB(video)
		// 先写入 DB 降级值；Pipeline 整体失败时无需再串行重试 Redis。
		statsMap[video.ID] = videoBaseStats{
			LikesCount:    base.LikesCount,
			CommentsCount: base.CommentsCount,
			Popularity:    base.Popularity,
			StatsVersion:  base.StatsVersion,
		}
		cmdMap[video.ID] = pipe.Eval(
			l.ctx,
			readVideoStatsAuthScript,
			[]string{rediskey.VideoStatsAuthKey(video.ID)},
			strconv.FormatInt(base.LikesCount, 10),
			strconv.FormatInt(base.CommentsCount, 10),
			strconv.FormatInt(base.Popularity, 10),
			strconv.FormatUint(base.StatsVersion, 10),
			strconv.FormatInt(int64(rediskey.VideoStatsAuthTTL/time.Second), 10),
		)
	}

	if _, err := pipe.Exec(l.ctx); err != nil {
		l.Errorf("batch cold start auth stats failed, videos:%d err:%v", len(videos), err)
		return nil
	}

	for _, video := range videos {
		values, err := cmdMap[video.ID].Slice()
		if err != nil {
			l.Errorf("parse cold start auth stats failed, video_id:%d err:%v", video.ID, err)
			continue
		}
		authStats := parseVideoStatsAuthResult(values)
		statsMap[video.ID] = videoBaseStats{
			LikesCount:    authStats.LikesCount,
			CommentsCount: authStats.CommentsCount,
			Popularity:    authStats.Popularity,
			StatsVersion:  authStats.StatsVersion,
		}
	}
	return nil
}
