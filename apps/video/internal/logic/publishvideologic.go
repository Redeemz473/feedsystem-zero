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
	// 幂等：先按 (author_id, request_id) 查已有视频，命中则直接返回；
	//      未命中才走真正的 reserve + insert + outbox 流程。
	//      并发写入撞唯一键 uk_video_request 时，通过 duplicate error 兜底回读。
	var (
		assetErr        error
		alreadyExisted  bool
		existedVideo    model.Video
		existedTagsFlag bool
	)
	err = l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		// 0. 幂等预检：按 (author_id, request_id) 查询是否已经发布过。
		//    命中时直接返回原视频，跳过 reserve/create/outbox 等所有副作用。
		var existed model.Video
		if err := tx.
			Where("author_id = ? AND request_id = ?", authorID, requestID).
			Take(&existed).Error; err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		} else {
			// 命中：额外校验幂等键对应的其它入参一致，防止不同的 play_url/cover_url 复用同一个 request_id。
			if existed.PlayURL != playURL || existed.CoverURL != coverURL {
				assetErr = status.Error(codes.AlreadyExists, "幂等请求ID已被使用，请重新生成")
				return errors.New("request_id reused with different payload")
			}
			existedVideo = existed
			alreadyExisted = true
			existedTagsFlag = true
			return nil
		}

		// 1. 先在事务内 reserve 视频与封面的引用计数
		//    资源不存在时记录到 assetErr，事务返回错误自动回滚
		if rerr := reserveFileAssetRefByURL(l.ctx, tx, playURL); rerr != nil {
			if errors.Is(rerr, gorm.ErrRecordNotFound) {
				assetErr = status.Error(codes.InvalidArgument, "视频资源不存在或已失效，请重新上传")
				return rerr
			}
			return rerr
		}
		if rerr := reserveFileAssetRefByURL(l.ctx, tx, coverURL); rerr != nil {
			if errors.Is(rerr, gorm.ErrRecordNotFound) {
				assetErr = status.Error(codes.InvalidArgument, "封面资源不存在或已失效，请重新上传")
				return rerr
			}
			return rerr
		}

		// 2. video写入mysql
		createdVideo := publishedVideo
		if err := tx.Create(&createdVideo).Error; err != nil {
			// 并发场景：另一个协程/请求已经用相同 (author_id, request_id) 抢先入库，
			//           这里会撞唯一键 uk_video_request。当前事务需要整体回滚（reserve 也一并撤销），
			//           外层捕获后再用独立连接回读原视频返回。
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

		// 3. 事务内写 outbox（video.published 事件，投递到 feed.video.events topic）
		//    与删除路径对称：下游 feed/推荐/通知等异步消费者只需订阅一个 topic 即可感知两类事件。
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
		if assetErr != nil {
			return nil, assetErr
		}
		// 并发撞唯一键：另一个请求已经用相同 (author_id, request_id) 抢先入库。
		// 事务已回滚，此处用独立连接把已存在的那条视频读出来幂等返回。
		if errors.Is(err, errDuplicateVideoRequest) {
			winner, loadErr := loadVideoByAuthorRequestID(l.ctx, l.svcCtx.GormDB, authorID, requestID)
			if loadErr != nil {
				l.Errorf("video publish idempotent read failed, author_id: %d, request_id: %s, error: %v", authorID, requestID, loadErr)
				return nil, status.Error(codes.Internal, "发布视频失败")
			}
			existedVideo = *winner
			alreadyExisted = true
			existedTagsFlag = true
		} else {
			l.Errorf("video publish failed, author_id: %d, play_url: %s, error: %v", authorID, playURL, err)
			return nil, status.Error(codes.Internal, "发布视频失败")
		}
	}

	if alreadyExisted {
		// 幂等命中或并发抢占场景：用已存在视频的 tags 一起返回，行为对客户端完全一致。
		respTags := tags
		if existedTagsFlag {
			if loaded, tagsErr := loadTagsByVideoIDs(l.ctx, l.svcCtx.GormDB, []uint64{existedVideo.ID}); tagsErr == nil {
				respTags = loaded[existedVideo.ID]
			} else {
				l.Errorf("load tags for idempotent published video failed, video_id: %d, error: %v", existedVideo.ID, tagsErr)
			}
		}
		if err := invalidateVideoEntityCache(l.ctx, l.svcCtx.RedisCli, existedVideo.ID); err != nil {
			l.Errorf("invalidate video entity cache after idempotent publish failed, video_id: %d, error: %v", existedVideo.ID, err)
		}
		return &videopb.PublishVideoResp{
			Msg:   "发布成功",
			Video: toVideoInfo(&existedVideo, respTags, false),
		}, nil
	}

	if err := invalidateVideoEntityCache(l.ctx, l.svcCtx.RedisCli, publishedVideo.ID); err != nil {
		l.Errorf("invalidate video entity cache after publish failed, video_id: %d, error: %v", publishedVideo.ID, err)
	}
	return &videopb.PublishVideoResp{
		Msg:   "发布成功",
		Video: toVideoInfo(&publishedVideo, tags, false),
	}, nil
}
