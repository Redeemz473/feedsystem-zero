package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/notification/notification"
	"feedsystem-zero/apps/notification/notificationclient"
)

// 通知类型/状态到字符串枚举的映射，前端语义更直观。
const (
	notificationTypeLike    = "like"
	notificationTypeComment = "comment"
	notificationTypeFollow  = "follow"

	notificationStatusUnread = "unread"
	notificationStatusRead   = "read"
)

func notificationTypeToString(t notification.NotificationType) string {
	switch t {
	case notification.NotificationType_NOTIFICATION_TYPE_VIDEO_LIKE:
		return notificationTypeLike
	case notification.NotificationType_NOTIFICATION_TYPE_VIDEO_COMMENT:
		return notificationTypeComment
	case notification.NotificationType_NOTIFICATION_TYPE_FOLLOW:
		return notificationTypeFollow
	default:
		return ""
	}
}

func notificationStatusToString(s notification.NotificationStatus) string {
	switch s {
	case notification.NotificationStatus_NOTIFICATION_STATUS_UNREAD:
		return notificationStatusUnread
	case notification.NotificationStatus_NOTIFICATION_STATUS_READ:
		return notificationStatusRead
	default:
		return ""
	}
}

// hydrateNotifications 保持后端返回的顺序，批量补齐 actor 用户资料与视频缩略图。
// 已删除的 actor / 视频不会导致整条通知消失（actor 显示为空、video 置 nil），
// 避免通知列表因为副作用数据缺失而“漏一条”。
func hydrateNotifications(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	items []*notificationclient.NotificationInfo,
) ([]types.NotificationItem, error) {
	if len(items) == 0 {
		return []types.NotificationItem{}, nil
	}

	actorIDs := make([]uint64, 0, len(items))
	videoIDs := make([]uint64, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		if id := item.GetActorId(); id != 0 {
			actorIDs = append(actorIDs, id)
		}
		if id := item.GetVideoId(); id != 0 {
			videoIDs = append(videoIDs, id)
		}
	}

	actorMap, err := loadSocialUserInfoMap(ctx, svcCtx.AccountRpc, actorIDs)
	if err != nil {
		return nil, err
	}

	// 视频侧只需要标题+封面，直接复用 BatchGetVideos。viewerID 传 0 表示不查询点赞状态。
	videoMap, err := loadHTTPVideosByIDs(ctx, svcCtx.AccountRpc, svcCtx.VideoRpc, svcCtx.InteractionRpc, 0, videoIDs)
	if err != nil {
		// 视频信息获取失败时降级为空 map，通知列表本身不应因视频服务抖动而不可用。
		videoMap = map[uint64]types.VideoInfo{}
	}

	result := make([]types.NotificationItem, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		out := types.NotificationItem{
			Notificationid: item.GetNotificationId(),
			Type:           notificationTypeToString(item.GetNotificationType()),
			Status:         notificationStatusToString(item.GetStatus()),
			Occurredat:     item.GetOccurredAt(),
			Readat:         item.GetReadAt(),
			Commentid:      item.GetCommentId(),
		}

		if actor, ok := actorMap[item.GetActorId()]; ok {
			out.Actor = types.NotificationActor{
				Userid:    actor.Userid,
				Username:  actor.Username,
				Avatarurl: actor.Avatarurl,
			}
		} else if item.GetActorId() != 0 {
			out.Actor = types.NotificationActor{Userid: item.GetActorId()}
		}

		if videoID := item.GetVideoId(); videoID != 0 {
			if video, ok := videoMap[videoID]; ok {
				out.Video = &types.NotificationVideo{
					Videoid:  video.Videoid,
					Title:    video.Title,
					Coverurl: video.Coverurl,
				}
			} else {
				out.Video = &types.NotificationVideo{Videoid: videoID}
			}
		}

		result = append(result, out)
	}
	return result, nil
}
