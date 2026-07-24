package logic

import (
	"context"

	"feedsystem-zero/apps/account/account"
	"feedsystem-zero/apps/account/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type BatchGetProfilesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBatchGetProfilesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BatchGetProfilesLogic {
	return &BatchGetProfilesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 批量查询公开用户资料
func (l *BatchGetProfilesLogic) BatchGetProfiles(in *account.BatchGetProfilesReq) (*account.BatchGetProfilesResp, error) {
	// TODO: 按下面步骤实现，注意这个接口只允许返回公开资料，不能返回 email/token/password_hash。
	//
	// 1. 参数归一化：
	//    - user_ids 为空时直接返回空 profiles；
	//    - 单次最多允许 100 个 ID，超过时返回 codes.InvalidArgument；
	//    - 过滤 0 并去重，但另外保留原始 ID 顺序，便于最终按请求顺序返回。
	// 2. 一次 SQL 批量查询：
	//    SELECT id, username, avatar_url, bio FROM accounts WHERE id IN ?
	//    不要在循环中逐个 First，避免 N+1 查询。
	// 3. 将查询结果构造成 map[userID]*account.PublicProfile。
	// 4. 按去重后的请求顺序组装 profiles；不存在的用户直接跳过。
	//    调用方可通过返回集合判断目标用户是否存在。
	// 5. DB 异常记录日志并返回 codes.Internal，不要把底层 SQL 错误直接暴露给客户端。

	return &account.BatchGetProfilesResp{}, nil
}
