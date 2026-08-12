package logic

import (
	"context"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/apps/interaction/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxRebuildInteractionStatItems = 100
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

// RebuildVideoInteractionStats 在方案 B 架构下改为只读对账（reconciliation）。
//
// 方案 B 中 Redis 权威 Hash（VideoStatsAuthKey）是用户可见值的唯一权威来源，
// MySQL videos 冷备值只用于 Redis miss 时的冷启动兜底。曾经的"用 COUNT(*) 覆盖 videos + 强制清 Redis 增量"
// 操作在方案 B 下是有害的——一旦覆盖 Redis 权威 Hash，短时窗口内客户端会看到计数跳变，
// 且如果覆盖恰好发生在增量落 Kafka 之前，还会导致 double-count。
//
// 因此这里保留 RPC 接口以维持 proto 兼容，但语义降级为"三源对账"：
//  1. COUNT likes/comments 明细表，作为绝对权威。
//  2. HGETALL VideoStatsAuthKey，读取当前 Redis 权威值。
//  3. 读取 videos 冷备值，检查异步 flush job 是否落后。
//
// 返回的 stats 字段是 COUNT 明细得到的准确值，供运维观测；本方法不修改任何存储。
// 如果检测到严重漂移（Redis vs COUNT 明细偏差 > 阈值），会打印 error 日志供告警系统采集，
// 但不会主动"修正"——修正统一由后续增量事件通过 Kafka 消费收敛，或人工介入通过运维脚本处理。
func (l *RebuildVideoInteractionStatsLogic) RebuildVideoInteractionStats(in *interaction.RebuildVideoInteractionStatsReq) (*interaction.RebuildVideoInteractionStatsResp, error) {
	videoIDs, err := normalizeRebuildVideoIDs(in.GetVideoIds())
	if err != nil {
		return nil, err
	}
	if len(videoIDs) == 0 {
		return &interaction.RebuildVideoInteractionStatsResp{Stats: []*interaction.VideoInteractionStats{}}, nil
	}

	existingVideoIDs, err := l.loadExistingNormalVideoIDs(videoIDs)
	if err != nil {
		l.Errorf("load reconcile videos failed, video_ids:%v error:%v", videoIDs, err)
		return nil, status.Error(codes.Internal, "查询视频失败")
	}
	if len(existingVideoIDs) == 0 {
		return nil, status.Error(codes.NotFound, "视频不存在或已删除")
	}

	// 1. COUNT 明细表得到绝对权威值。
	likeCounts, err := l.countActiveLikes(existingVideoIDs)
	if err != nil {
		l.Errorf("count active likes failed, video_ids:%v error:%v", existingVideoIDs, err)
		return nil, status.Error(codes.Internal, "对账点赞数失败")
	}
	commentCounts, err := l.countActiveComments(existingVideoIDs)
	if err != nil {
		l.Errorf("count active comments failed, video_ids:%v error:%v", existingVideoIDs, err)
		return nil, status.Error(codes.Internal, "对账评论数失败")
	}

	// 2. 只读对账：把 COUNT 权威值组装返回，并对严重漂移写日志。这里不修改任何存储。
	statsMap := make(map[uint64]*interaction.VideoInteractionStats, len(existingVideoIDs))
	for _, videoID := range existingVideoIDs {
		likesCount := likeCounts[videoID]
		commentsCount := commentCounts[videoID]
		trueStats := &interaction.VideoInteractionStats{
			VideoId:       videoID,
			LikesCount:    likesCount,
			CommentsCount: commentsCount,
			Popularity:    likesCount*likePopularityWeight + commentsCount*commentPopularityWeight,
		}
		statsMap[videoID] = trueStats
		l.reportReconcileDiff(videoID, trueStats)
	}

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

// reportReconcileDiff 对比 COUNT 明细、Redis 权威、DB 冷备三者，将差异写日志。
// 只观测不修正——修正统一由增量事件通过 Kafka 消费收敛，或运维脚本处理。
func (l *RebuildVideoInteractionStatsLogic) reportReconcileDiff(videoID uint64, trueStats *interaction.VideoInteractionStats) {
	// 读取 Redis 权威 Hash 当前值（不做冷启动，只观测）。
	authStats, authErr := readVideoStatsAuthWithBase(l.ctx, l.svcCtx.RedisCli, videoID, videoStatsAuthResult{})
	if authErr != nil {
		l.Errorf("reconcile: read redis auth stats failed, video_id:%d error:%v", videoID, authErr)
	} else if authStats.LikesCount != trueStats.GetLikesCount() ||
		authStats.CommentsCount != trueStats.GetCommentsCount() {
		l.Errorf(
			"reconcile diff redis vs count, video_id:%d redis_likes:%d count_likes:%d redis_comments:%d count_comments:%d",
			videoID,
			authStats.LikesCount, trueStats.GetLikesCount(),
			authStats.CommentsCount, trueStats.GetCommentsCount(),
		)
	}

	// 读取 MySQL 冷备值，检查异步 flush 是否落后。
	var video model.Video
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Select("id", "likes_count", "comments_count", "popularity").
		Where("id = ?", videoID).
		First(&video).Error; err != nil {
		l.Errorf("reconcile: load video cold stats failed, video_id:%d error:%v", videoID, err)
		return
	}
	if video.LikesCount != trueStats.GetLikesCount() ||
		video.CommentsCount != trueStats.GetCommentsCount() {
		l.Errorf(
			"reconcile diff mysql_cold vs count, video_id:%d cold_likes:%d count_likes:%d cold_comments:%d count_comments:%d",
			videoID,
			video.LikesCount, trueStats.GetLikesCount(),
			video.CommentsCount, trueStats.GetCommentsCount(),
		)
	}
}

func normalizeRebuildVideoIDs(rawVideoIDs []uint64) ([]uint64, error) {
	if len(rawVideoIDs) > maxRebuildInteractionStatItems {
		return nil, status.Errorf(codes.InvalidArgument, "一次最多对账%d个视频", maxRebuildInteractionStatItems)
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
