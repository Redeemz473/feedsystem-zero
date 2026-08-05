package logic

import (
	"context"
	"errors"
	"strings"
	"time"

	"feedsystem-zero/apps/account/account"
	"feedsystem-zero/apps/account/internal/model"
	"feedsystem-zero/apps/account/internal/svc"
	"feedsystem-zero/common/jwtx"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type RefreshTokenLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRefreshTokenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RefreshTokenLogic {
	return &RefreshTokenLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 刷新token
func (l *RefreshTokenLogic) RefreshToken(in *account.RefreshTokenReq) (*account.RefreshTokenResp, error) {
	//取出RefreshToken
	oldRefreshToken := strings.TrimSpace(in.GetRefreshToken())
	if oldRefreshToken == "" {
		return nil, status.Error(codes.InvalidArgument, "refresh_token为空")
	}

	//根据RefreshToken查用户,在mysql里面
	var user model.Account
	err := l.svcCtx.GormDB.WithContext(l.ctx).Where("refresh_token = ?", oldRefreshToken).First(&user).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.Unauthenticated, "refresh_token无效或已过期")
		}
		l.Errorf("query user by refresh token failed, error: %v", err)
		return nil, status.Error(codes.Internal, "refresh_token查询失败")
	}

	//生成新的accesstoken和refreshtoken
	accessToken, err := jwtx.GenerateAccessToken(user.ID, l.svcCtx.Config.Jwt.AccessSecret, l.svcCtx.Config.Jwt.AccessExpire)
	if err != nil {
		l.Errorf("generate access token failed, userID: %d, error: %v", user.ID, err)
		return nil, status.Error(codes.Internal, "token生成失败")
	}

	newRefreshToken, err := generateUniqueRefreshToken(l.ctx, l.svcCtx.GormDB)
	if err != nil {
		l.Errorf("generate refresh token failed, userID: %d, error: %v", user.ID, err)
		return nil, status.Error(codes.Internal, "refresh_token生成失败")
	}

	result := l.svcCtx.GormDB.WithContext(l.ctx).
		Model(&model.Account{}).
		Where("id = ? AND refresh_token = ?", user.ID, oldRefreshToken). //乐观锁，保证并发情况下只有一个能够改变refresh_token
		Update("refresh_token", newRefreshToken)
	if result.Error != nil {
		l.Errorf("save refresh token failed, userID: %d, error: %v", user.ID, result.Error)
		return nil, status.Error(codes.Internal, "登录refresh_token信息刷新存储失败")
	}
	if result.RowsAffected == 0 {
		return nil, status.Error(codes.Unauthenticated, "refresh_token无效或已过期")
	}

	tokenKey := rediskey.TokenKey(user.ID)
	tokenTTL := time.Duration(l.svcCtx.Config.Jwt.AccessExpire) * time.Second
	if err := l.svcCtx.RedisCli.Set(l.ctx, tokenKey, accessToken, tokenTTL).Err(); err != nil {
		_ = l.svcCtx.GormDB.WithContext(l.ctx).
			Model(&model.Account{}).
			Where("id = ? AND refresh_token = ?", user.ID, newRefreshToken).
			Update("refresh_token", oldRefreshToken).Error
		l.Errorf("save access token to redis failed, userID: %d, error: %v", user.ID, err)
		return nil, status.Error(codes.Internal, "token刷新存储失败")
	}

	//返回新的accesstoken和refreshtoken

	return &account.RefreshTokenResp{
		AccessToken:  accessToken,
		RefreshToken: newRefreshToken,
	}, nil
}
