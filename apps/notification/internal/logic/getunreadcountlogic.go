package logic

import (
	"context"

	"feedsystem-zero/apps/notification/internal/model"
	"feedsystem-zero/apps/notification/internal/svc"
	"feedsystem-zero/apps/notification/notification"
	"feedsystem-zero/common/notificationcache"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetUnreadCountLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUnreadCountLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUnreadCountLogic {
	return &GetUnreadCountLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetUnreadCount 返回当前用户的未读通知数量。
//
// 这是通知模块 QPS 最高的接口（首页红点、tab 徽标都会调），因此走缓存：
//  1. 通过 notificationcache.LoadUnreadCount 读 unread:{uid}:v:{version} 缓存；
//  2. 命中直接返回；未命中调用下面注入的 counter 走 MySQL COUNT 并回写；
//  3. Redis 完全不可用时会退化为直接 COUNT，功能不受影响。
//
// 与写侧（marknotificationreadlogic / markallnotificationsreadlogic / notification-job）
// 之间通过 version key 形成闭环：任何真正改变未读数的入口 INCR 一次 version 即可，
// 本接口下次读时旧缓存 key 自然 miss，重新 COUNT 得到最新值。
func (l *GetUnreadCountLogic) GetUnreadCount(in *notification.GetUnreadCountReq) (*notification.GetUnreadCountResp, error) {
	receiverID := in.GetReceiverId()
	if err := validateReceiverID(receiverID); err != nil {
		return nil, err
	}

	// counter 闭包封装真正的 MySQL 回源逻辑。
	// WHERE 命中 (receiver_id, status, occurred_at, id) 联合索引；deleted_at IS NULL 走行内过滤。
	counter := func(ctx context.Context, userID uint64) (int64, error) {
		var count int64
		if err := l.svcCtx.GormDB.WithContext(ctx).
			Model(&model.Notification{}).
			Where("receiver_id = ? AND status = ? AND deleted_at IS NULL",
				userID, model.NotificationStatusUnread).
			Count(&count).Error; err != nil {
			return 0, err
		}
		return count, nil
	}

	count, err := notificationcache.LoadUnreadCount(l.ctx, l.svcCtx.RedisCli, receiverID, counter)
	if err != nil {
		l.Errorf("get unread count failed, receiver_id:%d error:%v", receiverID, err)
		return nil, status.Error(codes.Internal, "获取未读通知数失败，请稍后重试")
	}

	return &notification.GetUnreadCountResp{
		UnreadCount: count,
	}, nil
}
