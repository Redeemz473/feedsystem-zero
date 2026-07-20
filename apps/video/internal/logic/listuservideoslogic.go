package logic

import (
	"context"
	"time"

	"feedsystem-zero/apps/video/internal/model"
	"feedsystem-zero/apps/video/internal/svc"
	videopb "feedsystem-zero/apps/video/video"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultListUserVideosPageSize int64 = 20
	maxListUserVideosPageSize     int64 = 50
)

type ListUserVideosLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListUserVideosLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListUserVideosLogic {
	return &ListUserVideosLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取作者发布的视频列表
func (l *ListUserVideosLogic) ListUserVideos(in *videopb.ListUserVideosReq) (*videopb.ListUserVideosResp, error) {
	authorID := in.GetAuthorId()
	viewerID := in.GetViewerId()
	cursorCreatedAt := in.GetCursorCreatedAt()
	cursorVideoID := in.GetCursorVideoId()
	pageSize := in.GetPageSize()

	if authorID == 0 {
		return nil, status.Error(codes.InvalidArgument, "作者ID不能为空")
	}
	if pageSize <= 0 {
		pageSize = defaultListUserVideosPageSize
	}
	if pageSize > maxListUserVideosPageSize {
		pageSize = maxListUserVideosPageSize
	}

	query := l.svcCtx.GormDB.WithContext(l.ctx).
		Where("author_id = ? AND status = ? AND deleted_at IS NULL", authorID, model.VideoStatusNormal)
	if cursorCreatedAt > 0 && cursorVideoID > 0 {
		cursorTime := time.UnixMilli(cursorCreatedAt)
		query = query.Where(
			"(created_at < ? OR (created_at = ? AND id < ?))",
			cursorTime,
			cursorTime,
			cursorVideoID,
		)
	}

	videos := make([]model.Video, 0, pageSize+1)
	if err := query.
		Order("created_at DESC").
		Order("id DESC").
		Limit(int(pageSize + 1)).
		Find(&videos).Error; err != nil {
		l.Errorf("list user videos failed, author_id: %d, error: %v", authorID, err)
		return nil, status.Error(codes.Internal, "获取视频失败")
	}

	hasMore := int64(len(videos)) > pageSize
	if hasMore {
		videos = videos[:pageSize]
	}

	videoIDs := make([]uint64, 0, len(videos))
	for _, item := range videos {
		videoIDs = append(videoIDs, item.ID)
	}

	likedMap := make(map[uint64]bool, len(videoIDs))
	if viewerID != 0 && len(videoIDs) > 0 {
		var likes []model.Like
		if err := l.svcCtx.GormDB.WithContext(l.ctx).
			Select("video_id").
			Where("user_id = ? AND video_id IN ? AND status = ? AND deleted_at IS NULL", viewerID, videoIDs, model.LikeStatusActive).
			Find(&likes).Error; err != nil {
			l.Errorf("load liked videos failed, viewer_id: %d, error: %v", viewerID, err)
			return nil, status.Error(codes.Internal, "查询点赞状态失败")
		}
		for _, like := range likes {
			likedMap[like.VideoID] = true
		}
	}

	tagsMap, err := loadTagsByVideoIDs(l.ctx, l.svcCtx.GormDB, videoIDs)
	if err != nil {
		l.Errorf("load video tags failed, author_id: %d, error: %v", authorID, err)
		return nil, status.Error(codes.Internal, "获取视频标签失败")
	}

	videoInfos := make([]*videopb.VideoInfo, 0, len(videos))
	for i := range videos {
		item := &videos[i]
		videoInfos = append(videoInfos, toVideoInfo(item, tagsMap[item.ID], likedMap[item.ID]))
	}

	var nextCursorCreatedAt int64
	var nextCursorVideoID uint64
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextCursorCreatedAt = last.CreatedAt.UnixMilli()
		nextCursorVideoID = last.ID
	}

	return &videopb.ListUserVideosResp{
		Videos:              videoInfos,
		NextCursorCreatedAt: nextCursorCreatedAt,
		NextCursorVideoId:   nextCursorVideoID,
		HasMore:             hasMore,
	}, nil
}
