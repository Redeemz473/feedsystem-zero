package logic

import (
	"context"
	"errors"

	"feedsystem-zero/apps/account/account"
	"feedsystem-zero/apps/account/internal/model"
	"feedsystem-zero/apps/account/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type GetProfileLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetProfileLogic {
	return &GetProfileLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 查用户
func (l *GetProfileLogic) GetProfile(in *account.GetProfileReq) (*account.GetProfileResp, error) {
	//取出userid，根据userid从mysql中拿到用户信息
	userid := in.GetUserId()
	if userid == 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id为空")
	}

	var user model.Account
	if err := l.svcCtx.GormDB.WithContext(l.ctx).Where("id=?", userid).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "用户不存在")
		}
		l.Errorf("query user profile failed, userID: %d, error: %v", userid, err)
		return nil, status.Error(codes.Internal, "查询用户信息失败")
	}

	//返回用户信息
	return &account.GetProfileResp{
		UserId:    user.ID,
		Username:  user.Username,
		Email:     user.Email,
		AvatarUrl: user.AvatarURL,
		Bio:       user.Bio,
	}, nil
}
