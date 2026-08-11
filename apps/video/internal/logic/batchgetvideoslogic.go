package logic

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"feedsystem-zero/apps/video/internal/model"
	"feedsystem-zero/apps/video/internal/svc"
	videopb "feedsystem-zero/apps/video/video"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxBatchVideoEntityIDs     = 100
	videoEntityCacheTTLJitter  = 5 * time.Minute
	setVideoEntityCacheIfMatch = `
local current = redis.call("GET", KEYS[1])
if not current then
    current = "0"
end
if current ~= ARGV[1] then
    return 0
end
redis.call("SET", KEYS[2], ARGV[2], "PX", ARGV[3])
return 1
`
)

// videoEntityCacheValue 只缓存 Video 服务拥有的实体快照。
// 互动统计可能随后变化，Gateway 必须再用 InteractionRpc.BatchGetVideoStats 覆盖。
type videoEntityCacheValue struct {
	Version        int64    `json:"version"`
	Missing        bool     `json:"missing,omitempty"`
	VideoID        uint64   `json:"video_id"`
	AuthorID       uint64   `json:"author_id,omitempty"`
	AuthorUsername string   `json:"author_username,omitempty"`
	Title          string   `json:"title,omitempty"`
	Description    string   `json:"description,omitempty"`
	PlayURL        string   `json:"play_url,omitempty"`
	CoverURL       string   `json:"cover_url,omitempty"`
	LikesCount     int64    `json:"likes_count,omitempty"`
	CommentsCount  int64    `json:"comments_count,omitempty"`
	Popularity     int64    `json:"popularity,omitempty"`
	Status         int32    `json:"status,omitempty"`
	CreatedAt      int64    `json:"created_at,omitempty"`
	UpdatedAt      int64    `json:"updated_at,omitempty"`
	Tags           []string `json:"tags,omitempty"`
}

var videoEntityDBLoadGroup = syncx.NewSingleFlight()

type BatchGetVideosLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBatchGetVideosLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BatchGetVideosLogic {
	return &BatchGetVideosLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// BatchGetVideos 批量返回正常视频实体，不存在、已删除或已下架的视频直接跳过。
func (l *BatchGetVideosLogic) BatchGetVideos(in *videopb.BatchGetVideosReq) (*videopb.BatchGetVideosResp, error) {
	// 限制批次、过滤 0、去重并保留输入顺序。
	videoIDs, err := normalizeBatchVideoEntityIDs(in.GetVideoIds())
	if err != nil {
		return nil, err
	}
	if len(videoIDs) == 0 {
		return &videopb.BatchGetVideosResp{Videos: []*videopb.VideoInfo{}}, nil
	}

	// Redis pipeline 批量读取实体和版本；Redis 异常时降级为一次 MySQL 批量查询。
	videoMap := make(map[uint64]*videopb.VideoInfo, len(videoIDs))
	missVideoIDs, cacheVersions, cacheAvailable := l.loadVideoEntitiesFromCache(videoIDs, videoMap)

	// 相同 miss 集合在单实例内通过 SingleFlight 合并，只执行一次视频查询和一次标签查询。
	if len(missVideoIDs) > 0 {
		dbVideos, err := l.loadVideoEntitiesFromDB(missVideoIDs)
		if err != nil {
			l.Errorf("batch query video entities failed, video_ids: %v, error: %v", missVideoIDs, err)
			return nil, status.Error(codes.Internal, "批量查询视频失败")
		}
		for videoID, videoInfo := range dbVideos {
			videoMap[videoID] = videoInfo
		}

		// Lua 在写缓存时原子比较版本，防止并发删除后旧数据库快照重新写回缓存。
		if cacheAvailable {
			l.cacheVideoEntityMisses(missVideoIDs, cacheVersions, videoMap)
		}
	}

	// 按请求顺序组装；无效视频跳过，不让调用方再处理空占位。
	videos := make([]*videopb.VideoInfo, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		if videoInfo, ok := videoMap[videoID]; ok {
			videos = append(videos, videoInfo)
		}
	}
	return &videopb.BatchGetVideosResp{Videos: videos}, nil
}

func normalizeBatchVideoEntityIDs(rawVideoIDs []uint64) ([]uint64, error) {
	if len(rawVideoIDs) > maxBatchVideoEntityIDs {
		return nil, status.Errorf(codes.InvalidArgument, "一次最多查询%d个视频", maxBatchVideoEntityIDs)
	}

	seen := make(map[uint64]struct{}, len(rawVideoIDs))
	videoIDs := make([]uint64, 0, len(rawVideoIDs))
	for _, videoID := range rawVideoIDs {
		if videoID == 0 {
			continue
		}
		if _, ok := seen[videoID]; ok {
			continue
		}
		seen[videoID] = struct{}{}
		videoIDs = append(videoIDs, videoID)
	}
	return videoIDs, nil
}

// Redis 批量读缓存
func (l *BatchGetVideosLogic) loadVideoEntitiesFromCache(
	videoIDs []uint64,
	videoMap map[uint64]*videopb.VideoInfo,
) ([]uint64, map[uint64]int64, bool) {
	pipe := l.svcCtx.RedisCli.Pipeline()
	valueCmds := make(map[uint64]*redis.StringCmd, len(videoIDs))
	versionCmds := make(map[uint64]*redis.StringCmd, len(videoIDs))
	for _, videoID := range videoIDs {
		// 先取实体再取版本。若删除操作恰好插入两条命令之间，版本不匹配会强制回源。
		valueCmds[videoID] = pipe.Get(l.ctx, rediskey.VideoEntityKey(videoID))
		versionCmds[videoID] = pipe.Get(l.ctx, rediskey.VideoEntityVersionKey(videoID))
	}
	if _, err := pipe.Exec(l.ctx); err != nil && !errors.Is(err, redis.Nil) {
		l.Errorf("batch get video entity cache failed, error: %v", err)
		return append([]uint64(nil), videoIDs...), nil, false
	}

	versions := make(map[uint64]int64, len(videoIDs))
	missVideoIDs := make([]uint64, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		version, err := videoEntityVersionResult(versionCmds[videoID])
		if err != nil {
			l.Errorf("get video entity version failed, video_id: %d, error: %v", videoID, err)
			missVideoIDs = append(missVideoIDs, videoID)
			continue
		}
		versions[videoID] = version

		data, err := valueCmds[videoID].Bytes()
		switch {
		case errors.Is(err, redis.Nil):
			missVideoIDs = append(missVideoIDs, videoID)
			continue
		case err != nil:
			l.Errorf("get video entity cache failed, video_id: %d, error: %v", videoID, err)
			missVideoIDs = append(missVideoIDs, videoID)
			continue
		}

		var cached videoEntityCacheValue
		if err := json.Unmarshal(data, &cached); err != nil {
			l.Errorf("unmarshal video entity cache failed, video_id: %d, error: %v", videoID, err)
			missVideoIDs = append(missVideoIDs, videoID)
			_ = l.svcCtx.RedisCli.Del(l.ctx, rediskey.VideoEntityKey(videoID)).Err()
			continue
		}
		if cached.Version != version || cached.VideoID != videoID {
			missVideoIDs = append(missVideoIDs, videoID)
			continue
		}
		// Missing则证明在DB里也不存在，防缓存穿透
		if cached.Missing {
			continue
		}
		if cached.Status != model.VideoStatusNormal {
			missVideoIDs = append(missVideoIDs, videoID)
			_ = l.svcCtx.RedisCli.Del(l.ctx, rediskey.VideoEntityKey(videoID)).Err()
			continue
		}
		videoMap[videoID] = cached.toVideoInfo()
	}

	return missVideoIDs, versions, true
}

func (l *BatchGetVideosLogic) loadVideoEntitiesFromDB(videoIDs []uint64) (map[uint64]*videopb.VideoInfo, error) {
	value, err := videoEntityDBLoadGroup.Do(videoEntityDBLoadKey(videoIDs), func() (any, error) {
		var videos []model.Video
		if err := l.svcCtx.GormDB.WithContext(l.ctx).
			Select(
				"id", "author_id", "author_username", "title", "description", "play_url", "cover_url",
				"likes_count", "comments_count", "popularity", "status", "created_at", "updated_at",
			).
			Where("id IN ? AND status = ? AND deleted_at IS NULL", videoIDs, model.VideoStatusNormal).
			Find(&videos).Error; err != nil {
			return nil, err
		}

		foundVideoIDs := make([]uint64, 0, len(videos))
		for _, video := range videos {
			foundVideoIDs = append(foundVideoIDs, video.ID)
		}
		tagsMap, err := loadTagsByVideoIDs(l.ctx, l.svcCtx.GormDB, foundVideoIDs)
		if err != nil {
			return nil, err
		}

		result := make(map[uint64]*videopb.VideoInfo, len(videos))
		for i := range videos {
			video := &videos[i]
			result[video.ID] = toVideoInfo(video, tagsMap[video.ID], false)
		}
		return result, nil
	})
	if err != nil {
		return nil, err
	}
	result, ok := value.(map[uint64]*videopb.VideoInfo)
	if !ok {
		return nil, errors.New("批量视频查询结果类型异常")
	}
	return result, nil
}

func (l *BatchGetVideosLogic) cacheVideoEntityMisses(
	videoIDs []uint64,
	expectedVersions map[uint64]int64,
	videoMap map[uint64]*videopb.VideoInfo,
) {
	pipe := l.svcCtx.RedisCli.Pipeline()
	writes := 0
	for _, videoID := range videoIDs {
		version, ok := expectedVersions[videoID]
		if !ok {
			continue
		}

		cached := videoEntityCacheValue{
			Version: version,
			Missing: true,
			VideoID: videoID,
		}
		ttl := rediskey.VideoEntityMissingTTL
		if videoInfo, found := videoMap[videoID]; found {
			cached = newVideoEntityCacheValue(version, videoInfo)
			ttl = videoEntityCacheTTL(videoID)
		}
		data, err := json.Marshal(cached)
		if err != nil {
			l.Errorf("marshal video entity cache failed, video_id: %d, error: %v", videoID, err)
			continue
		}

		pipe.Eval(
			l.ctx,
			setVideoEntityCacheIfMatch,
			[]string{rediskey.VideoEntityVersionKey(videoID), rediskey.VideoEntityKey(videoID)},
			strconv.FormatInt(version, 10),
			string(data),
			strconv.FormatInt(ttl.Milliseconds(), 10),
		)
		writes++
	}
	if writes == 0 {
		return
	}
	if _, err := pipe.Exec(l.ctx); err != nil {
		l.Errorf("backfill video entity cache failed, error: %v", err)
	}
}

func videoEntityVersionResult(cmd *redis.StringCmd) (int64, error) {
	version, err := cmd.Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if version < 0 {
		return 0, errors.New("视频实体缓存版本不能为负数")
	}
	return version, nil
}

func videoEntityDBLoadKey(videoIDs []uint64) string {
	sortedVideoIDs := append([]uint64(nil), videoIDs...)
	sort.Slice(sortedVideoIDs, func(i, j int) bool {
		return sortedVideoIDs[i] < sortedVideoIDs[j]
	})

	var builder strings.Builder
	for _, videoID := range sortedVideoIDs {
		builder.WriteString(strconv.FormatUint(videoID, 10))
		builder.WriteByte(',')
	}
	return builder.String()
}

func videoEntityCacheTTL(videoID uint64) time.Duration {
	jitterRange := uint64(videoEntityCacheTTLJitter/time.Second) + 1
	return rediskey.VideoEntityCacheTTL + time.Duration(videoID%jitterRange)*time.Second
}

func newVideoEntityCacheValue(version int64, videoInfo *videopb.VideoInfo) videoEntityCacheValue {
	return videoEntityCacheValue{
		Version:        version,
		VideoID:        videoInfo.GetVideoId(),
		AuthorID:       videoInfo.GetAuthorId(),
		AuthorUsername: videoInfo.GetAuthorUsername(),
		Title:          videoInfo.GetTitle(),
		Description:    videoInfo.GetDescription(),
		PlayURL:        videoInfo.GetPlayUrl(),
		CoverURL:       videoInfo.GetCoverUrl(),
		LikesCount:     videoInfo.GetLikesCount(),
		CommentsCount:  videoInfo.GetCommentsCount(),
		Popularity:     videoInfo.GetPopularity(),
		Status:         videoInfo.GetStatus(),
		CreatedAt:      videoInfo.GetCreatedAt(),
		UpdatedAt:      videoInfo.GetUpdatedAt(),
		Tags:           append([]string(nil), videoInfo.GetTags()...),
	}
}

func (c videoEntityCacheValue) toVideoInfo() *videopb.VideoInfo {
	return &videopb.VideoInfo{
		VideoId:        c.VideoID,
		AuthorId:       c.AuthorID,
		AuthorUsername: c.AuthorUsername,
		Title:          c.Title,
		Description:    c.Description,
		PlayUrl:        c.PlayURL,
		CoverUrl:       c.CoverURL,
		LikesCount:     c.LikesCount,
		CommentsCount:  c.CommentsCount,
		Popularity:     c.Popularity,
		Status:         c.Status,
		CreatedAt:      c.CreatedAt,
		UpdatedAt:      c.UpdatedAt,
		Tags:           append([]string(nil), c.Tags...),
	}
}
