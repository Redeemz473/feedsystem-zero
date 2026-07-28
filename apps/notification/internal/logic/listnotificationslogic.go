package logic

import (
	"context"

	"feedsystem-zero/apps/notification/internal/svc"
	"feedsystem-zero/apps/notification/notification"

	"github.com/zeromicro/go-zero/core/logx"
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

func (l *ListNotificationsLogic) ListNotifications(in *notification.ListNotificationsReq) (*notification.ListNotificationsResp, error) {
	// TODO 按以下顺序实现：
	//  1. 校验 receiver_id 非 0。该值必须由 gateway 从 JWT 注入，不能信任前端传值。
	//  2. 将 page_size 规范为默认 20、最大 50；两个游标必须同时为 0 或同时非 0。
	//  3. 查询 receiver_id 对应且 deleted_at IS NULL、status IN (未读, 已读) 的通知。
	//     非首页使用：
	//       occurred_at < cursor_time
	//       OR (occurred_at = cursor_time AND id < cursor_notification_id)
	//     再按 occurred_at DESC, id DESC 排序并取 page_size+1 条。
	//  4. 多取的一条只用于判断 has_more；下一页游标取本页最后一条，而不是多取的那条。
	//  5. 将字符串 notification_type 映射为 proto enum，将本地时间统一转换为 Unix 毫秒。
	//     这里只返回 actor_id/video_id/comment_id 等基础关系数据，用户和视频详情由 gateway 批量聚合，
	//     不要在循环内逐条调用 AccountRpc 或 VideoRpc。
	//  6. 第一版先走带联合索引的 MySQL 游标查询。若后续缓存首页，必须同时设计 notification-job
	//     创建/撤回通知和已读接口的缓存失效，不能只在这里单方面 Set Redis。

	return &notification.ListNotificationsResp{}, nil
}
