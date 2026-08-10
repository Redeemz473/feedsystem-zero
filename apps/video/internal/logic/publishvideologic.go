package logic

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"feedsystem-zero/apps/video/internal/model"
	"feedsystem-zero/apps/video/internal/svc"
	videopb "feedsystem-zero/apps/video/video"
	"feedsystem-zero/common/eventx"

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
	requestID := strings.TrimSpace(in.GetRequestId())

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
	if requestID == "" {
		// gateway 侧会为老客户端兜底生成，理论上到这里一定非空；
		// 为防止直接跨服务调用绕过 gateway，这里再兜一层，强制要求 request_id。
		return nil, status.Error(codes.InvalidArgument, "缺少幂等请求ID")
	}
	if len(requestID) > 128 {
		return nil, status.Error(codes.InvalidArgument, "幂等请求ID过长")
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
		RequestID:      requestID,
		Status:         model.VideoStatusNormal,
	}

	// 幂等预检不需要占用事务。首次请求继续执行；重试命中时直接返回原视频，
	// 并发首发仍由 (author_id, request_id) 唯一键兜底。
	existedVideo, err := loadVideoByAuthorRequestID(l.ctx, l.svcCtx.GormDB, authorID, requestID)
	if err == nil {
		return l.idempotentPublishResponse(existedVideo, playURL, coverURL, tags)
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		l.Errorf("video publish idempotent precheck failed, author_id: %d, request_id: %s, error: %v", authorID, requestID, err)
		return nil, status.Error(codes.Internal, "发布视频失败")
	}

	// 在事务外批量加载并检查唯一资产，避免数据库行锁覆盖磁盘 I/O 时间。
	// 事务内的条件 UPDATE 仍会复核 id/url/storage_path/status，防止预检后的并发状态变化。
	preparedAssets, err := preparePublishFileAssets(l.ctx, l.svcCtx.GormDB, playURL, coverURL)
	if err != nil {
		if clientErr, ok := invalidPublishAssetError(err, playURL); ok {
			return nil, clientErr
		}
		l.Errorf("prepare publish file assets failed, author_id: %d, play_url: %s, error: %v", authorID, playURL, err)
		return nil, status.Error(codes.Internal, "发布视频失败")
	}

	// 预生成 outbox 事件 ID，事务里根据 videos.id 组装 payload 后落 outbox_events。
	eventID, err := newEventID("video_published")
	if err != nil {
		return nil, status.Error(codes.Internal, "生成事件ID失败")
	}

	//开启事务，保证原子操作
	//事务内任何err事务自动回滚
	//NOTE: file_assets.ref_count +1、videos 插入、outbox_events(video.published) 全部放在
	//      同一个本地事务里，任一步失败都会自动回滚，从根本上杜绝
	//      "视频存在但 ref_count 被回滚" / "ref_count 已加但视频未创建" / "视频已发布但下游无感知"
	//      三类一致性漂移。
	// 幂等：事务外预检负责普通重试；并发写入撞唯一键 uk_video_request 时，当前事务整体回滚，再通过独立连接回读胜出请求。
	err = l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		// 按 URL 固定顺序执行条件原子 UPDATE。UPDATE 自身获取行锁，同 URL 的视频/封面引用已聚合到 RefDelta，无需重复查询或重复校验文件。
		for _, asset := range preparedAssets {
			if rerr := reservePreparedPublishFileAsset(l.ctx, tx, asset); rerr != nil {
				return rerr
			}
		}

		// video写入mysql
		createdVideo := publishedVideo
		if err := tx.Create(&createdVideo).Error; err != nil {
			// 并发场景：另一个协程/请求已经用相同 (author_id, request_id) 抢先入库， 这里会撞唯一键 uk_video_request。当前事务需要整体回滚（reserve 也一并撤销），外层捕获后再用独立连接回读原视频返回。
			if isDuplicateKeyError(err) {
				return errDuplicateVideoRequest
			}
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

		// 事务内写 outbox（video.published 事件，投递到 feed.video.events topic）
		// 与删除路径对称：下游 feed/推荐/通知等异步消费者只需订阅一个 topic 即可感知两类事件。
		now := time.Now()
		occurredAt := now.UnixMilli()
		payloadBytes, err := json.Marshal(eventx.FeedVideoEvent{
			EventID:    eventID,
			VideoID:    createdVideo.ID,
			AuthorID:   authorID,
			Action:     "publish",
			Tags:       tags,
			OccurredAt: occurredAt,
		})
		if err != nil {
			return err
		}
		envelopeBytes, err := json.Marshal(eventx.Envelope{
			EventID:       eventID,
			EventType:     eventx.EventTypeVideoPublished,
			AggregateType: eventx.AggregateVideo,
			AggregateID:   strconv.FormatUint(createdVideo.ID, 10),
			Producer:      "video-rpc",
			OccurredAt:    occurredAt,
			Payload:       payloadBytes,
		})
		if err != nil {
			return err
		}
		if err := tx.Create(&model.OutboxEvent{
			EventID:       eventID,
			Topic:         eventx.TopicFeedVideoEvents,
			EventType:     eventx.EventTypeVideoPublished,
			AggregateType: eventx.AggregateVideo,
			AggregateID:   strconv.FormatUint(createdVideo.ID, 10),
			Payload:       string(envelopeBytes),
			Status:        model.OutboxStatusPending,
			CreatedAt:     now,
			UpdatedAt:     now,
		}).Error; err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		if clientErr, ok := invalidPublishAssetError(err, playURL); ok {
			return nil, clientErr
		}
		// 并发撞唯一键：另一个请求已经用相同 (author_id, request_id) 抢先入库。
		// 事务已回滚，此处用独立连接把已存在的那条视频读出来幂等返回。
		if errors.Is(err, errDuplicateVideoRequest) {
			winner, loadErr := loadVideoByAuthorRequestID(l.ctx, l.svcCtx.GormDB, authorID, requestID)
			if loadErr != nil {
				l.Errorf("video publish idempotent read failed, author_id: %d, request_id: %s, error: %v", authorID, requestID, loadErr)
				return nil, status.Error(codes.Internal, "发布视频失败")
			}
			return l.idempotentPublishResponse(winner, playURL, coverURL, tags)
		} else {
			l.Errorf("video publish failed, author_id: %d, play_url: %s, error: %v", authorID, playURL, err)
			return nil, status.Error(codes.Internal, "发布视频失败")
		}
	}

	if err := invalidateVideoEntityCache(l.ctx, l.svcCtx.RedisCli, publishedVideo.ID); err != nil {
		l.Errorf("invalidate video entity cache after publish failed, video_id: %d, error: %v", publishedVideo.ID, err)
	}
	return &videopb.PublishVideoResp{
		Msg:   "发布成功",
		Video: toVideoInfo(&publishedVideo, tags, false),
	}, nil
}

func invalidPublishAssetError(err error, playURL string) (error, bool) {
	assetURL, ok := unavailablePublishFileAssetURL(err)
	if !ok {
		return nil, false
	}
	if assetURL == playURL {
		return status.Error(codes.InvalidArgument, "视频资源不存在或已失效，请重新上传"), true
	}
	return status.Error(codes.InvalidArgument, "封面资源不存在或已失效，请重新上传"), true
}

func (l *PublishVideoLogic) idempotentPublishResponse(
	video *model.Video,
	playURL string,
	coverURL string,
	fallbackTags []string,
) (*videopb.PublishVideoResp, error) {
	if video.PlayURL != playURL || video.CoverURL != coverURL {
		return nil, status.Error(codes.AlreadyExists, "幂等请求ID已被使用，请重新生成")
	}

	respTags := fallbackTags
	if loaded, err := loadTagsByVideoIDs(l.ctx, l.svcCtx.GormDB, []uint64{video.ID}); err == nil {
		respTags = loaded[video.ID]
	} else {
		l.Errorf("load tags for idempotent published video failed, video_id: %d, error: %v", video.ID, err)
	}
	if err := invalidateVideoEntityCache(l.ctx, l.svcCtx.RedisCli, video.ID); err != nil {
		l.Errorf("invalidate video entity cache after idempotent publish failed, video_id: %d, error: %v", video.ID, err)
	}
	return &videopb.PublishVideoResp{
		Msg:   "发布成功",
		Video: toVideoInfo(video, respTags, false),
	}, nil
}
