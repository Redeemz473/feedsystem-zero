package logic

import (
	"context"
	"strings"

	"feedsystem-zero/apps/video/internal/model"
	"feedsystem-zero/apps/video/internal/svc"
	videopb "feedsystem-zero/apps/video/video"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PublishVideoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewPublishVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PublishVideoLogic {
	return &PublishVideoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 发布视频
func (l *PublishVideoLogic) PublishVideo(in *videopb.PublishVideoReq) (*videopb.PublishVideoResp, error) {
	authorID := in.GetAuthorId()
	authorUsername := strings.TrimSpace(in.GetAuthorUsername())
	title := strings.TrimSpace(in.GetTitle())
	description := strings.TrimSpace(in.GetDescription())
	playURL := strings.TrimSpace(in.GetPlayUrl())
	coverURL := strings.TrimSpace(in.GetCoverUrl())

	if authorID == 0 {
		return nil, status.Error(codes.Unauthenticated, "未登录")
	}
	if authorUsername == "" {
		return nil, status.Error(codes.InvalidArgument, "作者用户名不能为空")
	}
	if title == "" {
		return nil, status.Error(codes.InvalidArgument, "标题不能为空")
	}
	if playURL == "" {
		return nil, status.Error(codes.InvalidArgument, "视频地址不能为空")
	}
	if coverURL == "" {
		return nil, status.Error(codes.InvalidArgument, "封面地址不能为空")
	}

	//拿到tags，是一个slice
	tags := normalizeTags(in.GetTags())
	if len(tags) == 0 {
		tags = extractTags(title + " " + description)
	}
	if len(tags) > maxVideoTags {
		return nil, status.Errorf(codes.InvalidArgument, "标签最多 %d 个", maxVideoTags)
	}

	publishedVideo := model.Video{
		AuthorID:       authorID,
		AuthorUsername: authorUsername,
		Title:          title,
		Description:    description,
		PlayURL:        playURL,
		CoverURL:       coverURL,
		Status:         model.VideoStatusNormal,
	}

	//开启事务，保证原子操作
	//事务内任何err事务自动回滚
	err := l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		//video写入mysql
		createdVideo := publishedVideo
		if err := tx.Create(&createdVideo).Error; err != nil {
			return err
		}
		publishedVideo = createdVideo

		if len(tags) > 0 {
			//一次性把所有标签组装成切片 tagRows modle.Tag
			tagRows := make([]model.Tag, 0, len(tags))
			for _, tagName := range tags {
				tagRows = append(tagRows, model.Tag{Name: tagName})
			}

			//OnConflict 策略：唯一键是 name
			//通过tagRows，N 个标签只执行一次 INSERT
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "name"}},
				DoNothing: true,
			}).Create(&tagRows).Error; err != nil {
				return err
			}

			//查询这个视频所有tag的tagid，为下面铺垫
			var savedTags []model.Tag
			if err := tx.Where("name IN ?", tags).Find(&savedTags).Error; err != nil {
				return err
			}

			///一次性把所有vidoeid---tagid组装成切片 vidoeTags
			videoTags := make([]model.VideoTag, 0, len(savedTags))
			for _, tag := range savedTags {
				if tag.ID == 0 {
					continue
				}
				videoTags = append(videoTags, model.VideoTag{
					VideoID: createdVideo.ID,
					TagID:   tag.ID,
				})
			}

			//批量一次性插入多条关联记录，仅 1 条 SQL；
			//冲突判断联合唯一索引 (video_id, tag_id)：
			//同一个视频重复绑定同一个标签时，直接忽略，不报错；
			if len(videoTags) > 0 {
				if err := tx.Clauses(clause.OnConflict{
					Columns: []clause.Column{
						{Name: "video_id"},
						{Name: "tag_id"},
					},
					DoNothing: true,
				}).Create(&videoTags).Error; err != nil {
					return err
				}
			}
		}

		return nil
	})
	if err != nil {
		l.Errorf("video publish failed, author_id: %d, play_url: %s, error: %v", authorID, playURL, err)
		return nil, status.Error(codes.Internal, "发布视频失败")
	}

	return &videopb.PublishVideoResp{
		Msg:   "发布成功",
		Video: toVideoInfo(&publishedVideo, tags, false),
	}, nil
}
