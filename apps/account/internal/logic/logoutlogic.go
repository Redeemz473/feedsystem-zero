package logic

import (
	"context"

	"feedsystem-zero/apps/account/account"
	"feedsystem-zero/apps/account/internal/model"
	"feedsystem-zero/apps/account/internal/svc"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type LogoutLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLogoutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LogoutLogic {
	return &LogoutLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 登出
func (l *LogoutLogic) Logout(in *account.LogoutReq) (*account.LogoutResp, error) {
	userID := in.GetUserId()
	if userID == 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id为空")
	}

	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Model(&model.Account{}).
		Where("id = ?", userID).
		Update("refresh_token", gorm.Expr("NULL")).Error; err != nil {
		l.Errorf("clear refresh token failed, userID: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "退出失败")
	}

	if err := l.svcCtx.RedisCli.Del(l.ctx, rediskey.TokenKey(userID)).Err(); err != nil {
		l.Errorf("delete access token failed, userID: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "退出失败")
	}

	return &account.LogoutResp{
		Msg: "退出成功",
	}, nil
}
