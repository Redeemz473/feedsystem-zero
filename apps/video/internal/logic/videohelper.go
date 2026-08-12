package logic

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"feedsystem-zero/apps/video/internal/model"
	videopb "feedsystem-zero/apps/video/video"
	"feedsystem-zero/common/rediskey"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

var tagRegexp = regexp.MustCompile(`#([\p{L}\p{N}_]+)`)

// tag个数限制
const maxVideoTags = 20

// tag长度限制
const maxTagNameLen = 50

// errDuplicateVideoRequest 表示并发发布同一 (author_id, request_id) 时，
// 后到的事务撞上了 uk_video_request 唯一键。外层需要回读已入库的视频再返回，实现幂等。
var errDuplicateVideoRequest = errors.New("duplicate video request_id")

// mysqlDuplicateEntryErrNo 是 MySQL 报 Duplicate entry 错误的固定 errno（1062）。
const mysqlDuplicateEntryErrNo uint16 = 1062

// isDuplicateKeyError 判断一个 GORM 返回的 error 是否由 MySQL 唯一键冲突产生。
// 优先使用 gorm 提供的 ErrDuplicatedKey；对老版本或 driver 直接透出 *mysql.MySQLError 的情况兜底判断 errno=1062。
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlDuplicateEntryErrNo {
		return true
	}
	return false
}

// loadVideoByAuthorRequestID 用独立连接按 (author_id, request_id) 回读视频，
// 用于并发抢占后事务已回滚的场景，把已经写入的那条视频返回给客户端。
func loadVideoByAuthorRequestID(ctx context.Context, db *gorm.DB, authorID uint64, requestID string) (*model.Video, error) {
	var v model.Video
	result := db.WithContext(ctx).
		Where("author_id = ? AND request_id = ?", authorID, requestID).
		Limit(1).
		Find(&v)
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected == 0 {
		// 幂等预检未命中是首次发布的正常分支。使用 Find + RowsAffected，避免 GORM 把每次正常 miss 记录为 record not found 错误日志。
		return nil, gorm.ErrRecordNotFound
	}
	return &v, nil
}

func extractTags(text string) []string {
	matches := tagRegexp.FindAllStringSubmatch(text, -1)
	rawTags := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) >= 2 {
			rawTags = append(rawTags, match[1])
		}
	}

	return normalizeTags(rawTags)
}

func normalizeTags(rawTags []string) []string {
	seen := make(map[string]struct{}, len(rawTags))
	tags := make([]string, 0, len(rawTags))

	for _, rawTag := range rawTags {
		tag := strings.TrimSpace(rawTag)
		tag = strings.TrimPrefix(tag, "#")
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" {
			continue
		}
		if len([]rune(tag)) > maxTagNameLen {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}

		seen[tag] = struct{}{}
		tags = append(tags, tag)
	}

	return tags
}

func toVideoInfo(v *model.Video, tags []string, isLiked bool) *videopb.VideoInfo {
	if v == nil {
		return nil
	}

	return &videopb.VideoInfo{
		VideoId:        v.ID,
		AuthorId:       v.AuthorID,
		AuthorUsername: v.AuthorUsername,
		Title:          v.Title,
		Description:    v.Description,
		PlayUrl:        v.PlayURL,
		CoverUrl:       v.CoverURL,
		LikesCount:     v.LikesCount,
		CommentsCount:  v.CommentsCount,
		Popularity:     v.Popularity,
		Status:         v.Status,
		CreatedAt:      v.CreatedAt.UnixMilli(),
		UpdatedAt:      v.UpdatedAt.UnixMilli(),
		IsLiked:        isLiked,
		Tags:           tags,
	}
}

type videoTagRow struct {
	VideoID uint64 `gorm:"column:video_id"`
	Name    string `gorm:"column:name"`
}

func loadTagsByVideoIDs(ctx context.Context, db *gorm.DB, videoIDs []uint64) (map[uint64][]string, error) {
	tagsMap := make(map[uint64][]string, len(videoIDs))
	if len(videoIDs) == 0 {
		return tagsMap, nil
	}

	var rows []videoTagRow
	err := db.WithContext(ctx).
		Table("video_tags").
		Select("video_tags.video_id, tags.name").
		Joins("JOIN tags ON tags.id = video_tags.tag_id").
		Where("video_tags.video_id IN ?", videoIDs).
		Order("video_tags.video_id ASC, tags.id ASC").
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		tagsMap[row.VideoID] = append(tagsMap[row.VideoID], row.Name)
	}

	return tagsMap, nil
}

// invalidateVideoEntityCache 在 MySQL 状态提交后递增版本并清理实体、详情和统计快照。
// TxPipeline 保证版本递增与删除在 Redis 内原子执行；并发回源只能写入匹配版本的实体缓存。
//
// VideoStatsAuthKey 是互动计数的 Redis 权威 Hash（方案 B）。视频被删除或下架后，
// 清理该 key 可以让后续访问在权限校验阶段就返回"视频不存在"，避免访问遗留计数。
// 由于删除已经在 MySQL 层保证不再有新的互动写入，这里的 DEL 不会与在线路径的 HINCRBY 冲突。
func invalidateVideoEntityCache(ctx context.Context, redisCli *redis.Client, videoID uint64) error {
	pipe := redisCli.TxPipeline()
	pipe.Incr(ctx, rediskey.VideoEntityVersionKey(videoID))
	pipe.Del(
		ctx,
		rediskey.VideoEntityKey(videoID),
		rediskey.VideoDetailKey(videoID),
		rediskey.VideoStatsAuthKey(videoID),
	)
	_, err := pipe.Exec(ctx)
	return err
}
