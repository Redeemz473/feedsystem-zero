package logic

import (
	"context"
	"regexp"
	"strings"

	"feedsystem-zero/apps/video/internal/model"
	videopb "feedsystem-zero/apps/video/video"

	"gorm.io/gorm"
)

var tagRegexp = regexp.MustCompile(`#([\p{L}\p{N}_]+)`)

// tag个数限制
const maxVideoTags = 20

// tag长度限制
const maxTagNameLen = 50

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
