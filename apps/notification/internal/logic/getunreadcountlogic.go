package logic

import (
	"context"

	"feedsystem-zero/apps/notification/internal/svc"
	"feedsystem-zero/apps/notification/notification"

	"github.com/zeromicro/go-zero/core/logx"
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

func (l *GetUnreadCountLogic) GetUnreadCount(in *notification.GetUnreadCountReq) (*notification.GetUnreadCountResp, error) {
	// TODO 按以下顺序实现：
	//  1. 校验 receiver_id 非 0，该值由 gateway 从 JWT 注入。
	//  2. 统计 receiver_id=? AND status=未读 AND deleted_at IS NULL 的记录数。
	//     SQL 会命中 notifications 的 (receiver_id,status,occurred_at,id) 联合索引。
	//  3. 返回 int64，并区分数据库错误与参数错误。
	//  4. 如果引入 Redis 未读数缓存，notification-job 创建/撤回通知、单条已读和全部已读
	//     都必须在 MySQL 成功后失效或更新同一个 key，并保留短 TTL 作为最终兜底。

	return &notification.GetUnreadCountResp{}, nil
}
