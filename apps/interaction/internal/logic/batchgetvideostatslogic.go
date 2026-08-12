package logic

import (
	"context"
	"strconv"

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

// BatchGetVideoStats 读侧完全以 Redis 权威 Hash（VideoStatsAuthKey）为准。
// 命中即返回，miss 时用 MySQL videos 冷备值调 Lua 冷启动 auth key，之后所有读者都直接读 Redis。
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

	// 3. 从 Redis 权威 Hash 批量读；未命中的 video 后续用 DB 冷备值冷启动。
	statsMap := make(map[uint64]videoBaseStats, len(videoIDs))
	for _, videoID := range videoIDs {
		statsMap[videoID] = videoBaseStats{}
	}

	missVideoIDs := l.loadAuthStats(videoIDs, statsMap)

	// 4. Redis miss 的视频用 MySQL 冷备值做冷启动：读取 videos 冷备字段，
	//    通过 readVideoStatsAuthScript 的 EXISTS==0 分支原子建立 auth key（避免 double init）。
	if len(missVideoIDs) > 0 {
		if err := l.coldStartAuthStats(missVideoIDs, statsMap); err != nil {
			l.Errorf("cold start auth stats failed, video_ids:%v err:%v", missVideoIDs, err)
			return nil, status.Error(codes.Internal, "查询视频统计失败")
		}
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

// loadAuthStats 用 Pipeline 批量 HGetAll 权威 Hash。命中的视频写入 statsMap，
// 未命中的返回给调用方后续走 DB 冷启动。
func (l *BatchGetVideoStatsLogic) loadAuthStats(videoIDs []uint64, statsMap map[uint64]videoBaseStats) []uint64 {
	pipe := l.svcCtx.RedisCli.Pipeline()
	cmdMap := make(map[uint64]*redis.MapStringStringCmd, len(videoIDs))
	for _, videoID := range videoIDs {
		cmdMap[videoID] = pipe.HGetAll(l.ctx, rediskey.VideoStatsAuthKey(videoID))
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

// coldStartAuthStats 一次性从 MySQL 读取 miss 视频的冷备统计，
// 然后逐个视频通过 readVideoStatsAuthScript 原子建立 auth key（并发安全）。
// 已被删除的视频保持 statsMap 里的零值，读侧对外仍返回 0。
func (l *BatchGetVideoStatsLogic) coldStartAuthStats(videoIDs []uint64, statsMap map[uint64]videoBaseStats) error {
	var videos []imodel.Video
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Select("id", "likes_count", "comments_count", "popularity").
		Where("id IN ? AND status = ? AND deleted_at IS NULL", videoIDs, imodel.VideoStatusNormal).
		Find(&videos).Error; err != nil {
		return err
	}

	for _, video := range videos {
		base := videoBaseStatsFromDB(video)
		authStats, err := readVideoStatsAuthWithBase(l.ctx, l.svcCtx.RedisCli, video.ID, base)
		if err != nil {
			// Redis 不可用时降级：直接把 DB 冷备值写入 statsMap，读侧返回冷备快照。
			l.Errorf("cold start read auth stats failed, video_id:%d err:%v", video.ID, err)
			statsMap[video.ID] = videoBaseStats{
				LikesCount:    base.LikesCount,
				CommentsCount: base.CommentsCount,
				Popularity:    base.Popularity,
			}
			continue
		}
		statsMap[video.ID] = videoBaseStats{
			LikesCount:    authStats.LikesCount,
			CommentsCount: authStats.CommentsCount,
			Popularity:    authStats.Popularity,
		}
	}
	return nil
}

func parseInt64(value string) int64 {
	n, _ := strconv.ParseInt(value, 10, 64)
	return n
}
