package logic

import (
	"context"
	"errors"

	"feedsystem-zero/apps/notification/internal/model"
	"feedsystem-zero/apps/notification/internal/svc"
	"feedsystem-zero/apps/notification/notification"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// defaultListPageSize 未指定 page_size 时的默认页大小；与 API 层约定保持一致。
	defaultListPageSize int64 = 20
	// maxListPageSize 单次最多返回条数；防止调用方一次拉巨大页导致 MySQL 慢查询和响应体膨胀。
	maxListPageSize int64 = 50
)

type ListNotificationsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListNotificationsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListNotificationsLogic {
	return &ListNotificationsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// ListNotifications 按 (occurred_at DESC, id DESC) 分页返回当前用户的通知列表。
//
// 设计要点：
//  1. receiver_id 必须由 gateway 从 JWT 注入，SQL WHERE 里再兜底校验，
//     防止越权拉取他人通知。
//  2. 采用 (occurred_at, id) 复合游标：同一毫秒内多条通知也能稳定翻页，
//     不会像纯 offset 一样在中间插入新数据时出现重复/漏读。
//  3. status IN (未读, 已读)、deleted_at IS NULL —— 已撤回(canceled) 的通知不展示，
//     即使收撤回事件之前用户已经看到过。
//  4. 只多取 1 条判断 has_more；下一页游标取"本页最后一条"，避免把多取的第 N+1 条暴露出去。
//  5. 只返回关系类字段（actor_id/video_id/comment_id 等），用户资料和视频详情
//     由 gateway 批量聚合调 account-rpc / video-rpc，避免此处 N+1。
//  6. 第一版不做首页缓存：list 命中不同游标区间且实时性要求较高，
//     用带索引的 MySQL 查询已足够。若日后要缓存首页，必须同步在写侧 bump 相同版本。
func (l *ListNotificationsLogic) ListNotifications(in *notification.ListNotificationsReq) (*notification.ListNotificationsResp, error) {
	receiverID := in.GetReceiverId()
	if err := validateReceiverID(receiverID); err != nil {
		return nil, err
	}

	// pageSize 规范到 [1, maxListPageSize]，默认 defaultListPageSize。
	pageSize := in.GetPageSize()
	if pageSize <= 0 {
		pageSize = defaultListPageSize
	}
	if pageSize > maxListPageSize {
		pageSize = maxListPageSize
	}

	// 游标必须"同时为 0"或"同时非 0"：只填一半会得到语义不明的查询。
	// occurred_at=0 且 notification_id=0 视为首页。
	cursorOccurredAt := in.GetCursorOccurredAt()
	cursorNotificationID := in.GetCursorNotificationId()
	if (cursorOccurredAt == 0) != (cursorNotificationID == 0) {
		return nil, status.Error(codes.InvalidArgument, "游标参数必须同时为空或同时非空")
	}

	// 多取 1 条，用于判断 has_more；返回时截断到 pageSize。
	fetchLimit := int(pageSize + 1)

	// GORM 表达式必须与 (receiver_id, occurred_at, id) 联合索引方向匹配才能走索引下推。
	// status IN (未读, 已读) 用两个 int32 值参数化，避免依赖 MySQL 的枚举字符串。
	query := l.svcCtx.GormDB.WithContext(l.ctx).
		Model(&model.Notification{}).
		Where("receiver_id = ? AND status IN (?, ?) AND deleted_at IS NULL",
			receiverID,
			model.NotificationStatusUnread,
			model.NotificationStatusRead,
		)

	// 非首页：occurred_at < cursor 或（occurred_at = cursor AND id < cursor_id）。
	// 用 UTC 时间戳转 time.Local 与写入端保持时区一致；否则 MySQL 会因时区偏移导致比较错位。
	if cursorOccurredAt > 0 {
		cursorTime := millisecondsToDBTime(cursorOccurredAt)
		query = query.Where(
			"occurred_at < ? OR (occurred_at = ? AND id < ?)",
			cursorTime, cursorTime, cursorNotificationID,
		)
	}

	var rows []model.Notification
	if err := query.
		Order("occurred_at DESC, id DESC").
		Limit(fetchLimit).
		Find(&rows).Error; err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, "查询超时，请稍后重试")
		}
		l.Errorf(
			"list notifications failed, receiver_id:%d cursor_occurred_at:%d cursor_id:%d page_size:%d error:%v",
			receiverID, cursorOccurredAt, cursorNotificationID, pageSize, err,
		)
		return nil, status.Error(codes.Internal, "获取通知列表失败，请稍后重试")
	}

	// 判定 has_more：多取到第 N+1 条说明还有更多；返回给上游的仍然是前 pageSize 条。
	hasMore := int64(len(rows)) > pageSize
	if hasMore {
		rows = rows[:pageSize]
	}

	items := make([]*notification.NotificationInfo, 0, len(rows))
	for _, row := range rows {
		items = append(items, buildNotificationInfo(row))
	}

	// 下一页游标使用"本页最后一条"（rows[pageSize-1]）而非多取那条，
	// 让客户端下次直接从这条之后继续读。空列表则返回全零游标。
	var nextOccurredAt int64
	var nextNotificationID uint64
	if len(rows) > 0 {
		last := rows[len(rows)-1]
		nextOccurredAt = last.OccurredAt.UnixMilli()
		nextNotificationID = last.ID
	}

	return &notification.ListNotificationsResp{
		Notifications:            items,
		NextCursorOccurredAt:     nextOccurredAt,
		NextCursorNotificationId: nextNotificationID,
		HasMore:                  hasMore,
	}, nil
}

// buildNotificationInfo 把存储模型转换为 proto 响应对象。
// 只输出关系类数据；actor 昵称、头像、视频封面等展示信息由 gateway 批量聚合。
func buildNotificationInfo(row model.Notification) *notification.NotificationInfo {
	info := &notification.NotificationInfo{
		NotificationId:   row.ID,
		ActorId:          row.ActorID,
		NotificationType: notificationTypeStringToProto(row.NotificationType),
		Status:           mapModelStatusToProto(row.Status),
		OccurredAt:       row.OccurredAt.UnixMilli(),
	}
	if row.VideoID != nil {
		info.VideoId = *row.VideoID
	}
	if row.CommentID != nil {
		info.CommentId = *row.CommentID
	}
	if row.ReadAt != nil {
		info.ReadAt = row.ReadAt.UnixMilli()
	}
	return info
}
