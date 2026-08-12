package logic

import (
	"context"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/rediskey"

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

// RebuildVideoInteractionStats 对比明细表、MySQL 持久快照和 Redis 投影。
//
// 这里保留 RPC 接口以维持 proto 兼容，语义是"三源对账 + 安全投影修复"：
//  1. COUNT likes/comments 明细表，作为绝对权威。
//  2. 读取 videos 持久快照，检查异步 flush job 是否落后。
//  3. 纯 HGETALL 读取 Redis，不允许因观测 miss 创建零值 key。
//  4. Redis key 缺失或版本落后时，用 MySQL 快照做 CAS 投影修复。
//
// 本方法不会直接用 COUNT 覆盖 videos：未消费事件可能已写入明细表，直接覆盖后再消费 delta 会重复计数。
// DB 与明细漂移只告警，待 Kafka 追平后再由运维确认；Redis 失写则可通过版本快照安全修复。
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

// reportReconcileDiff 对比 COUNT 明细、Redis 投影、DB 持久快照，并修复缺失/落后的 Redis 投影。
func (l *RebuildVideoInteractionStatsLogic) reportReconcileDiff(videoID uint64, trueStats *interaction.VideoInteractionStats) {
	// 先读取 MySQL 持久快照，作为 Redis 可安全恢复的基准。
	var video model.Video
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Select("id", "likes_count", "comments_count", "popularity", "stats_version").
		Where("id = ?", videoID).
		First(&video).Error; err != nil {
		l.Errorf("reconcile: load video stats snapshot failed, video_id:%d error:%v", videoID, err)
		return
	}
	if video.LikesCount != trueStats.GetLikesCount() ||
		video.CommentsCount != trueStats.GetCommentsCount() {
		l.Errorf(
			"reconcile diff mysql vs count, video_id:%d mysql_likes:%d count_likes:%d mysql_comments:%d count_comments:%d",
			videoID,
			video.LikesCount, trueStats.GetLikesCount(),
			video.CommentsCount, trueStats.GetCommentsCount(),
		)
	}

	// 纯读 Redis。旧格式、缺失或版本落后时用 MySQL 快照修复；不会写入零值基准。
	hashValues, authErr := l.svcCtx.RedisCli.HGetAll(l.ctx, rediskey.VideoStatsAuthKey(videoID)).Result()
	authStats, authOK := parseVideoStatsAuthHash(hashValues)
	if authErr != nil {
		l.Errorf("reconcile: read redis stats projection failed, video_id:%d error:%v", videoID, authErr)
		return
	}
	if _, legacyOK := parseLegacyVideoStatsAuthHash(hashValues); legacyOK {
		// 滚动升级期间不覆盖旧 Hash 中可能尚未消费的在线增量；正常读写 Lua 会只补版本字段。
		l.Infof("reconcile: legacy redis stats hash awaits lazy version upgrade, video_id:%d", videoID)
		return
	}
	if !authOK || authStats.StatsVersion < video.StatsVersion {
		if err := projectVideoStatsBatch(l.ctx, l.svcCtx.RedisCli, []videoStatsProjection{{
			VideoID: videoID,
			Stats:   videoBaseStatsFromDB(video),
		}}); err != nil {
			l.Errorf("reconcile: repair redis stats projection failed, video_id:%d error:%v", videoID, err)
		}
		return
	}
	if authStats.LikesCount != trueStats.GetLikesCount() ||
		authStats.CommentsCount != trueStats.GetCommentsCount() {
		l.Errorf(
			"reconcile diff redis vs count, video_id:%d redis_likes:%d count_likes:%d redis_comments:%d count_comments:%d redis_version:%d mysql_version:%d",
			videoID,
			authStats.LikesCount, trueStats.GetLikesCount(),
			authStats.CommentsCount, trueStats.GetCommentsCount(),
			authStats.StatsVersion, video.StatsVersion,
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
