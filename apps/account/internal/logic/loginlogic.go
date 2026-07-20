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
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type LoginLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 登录
func (l *LoginLogic) Login(in *account.LoginReq) (*account.LoginResp, error) {
	//取出用户名、密码
	username := strings.TrimSpace(in.GetUsername())
	if username == "" {
		return nil, status.Error(codes.InvalidArgument, "用户名为空")
	}
	password := in.GetPassword()
	if password == "" {
		return nil, status.Error(codes.InvalidArgument, "密码为空")
	}

	//判断用户名、密码是否正确
	var user model.Account
	err := l.svcCtx.GormDB.WithContext(l.ctx).Where("username=?", username).First(&user).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.InvalidArgument, "用户名或密码错误")
		}
		l.Errorf("query user failed, username: %s, error: %v", username, err)
		return nil, status.Error(codes.Internal, "查询用户失败")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, status.Error(codes.InvalidArgument, "用户名或密码错误")
	}

	userID := user.ID
	accessToken, err := jwtx.GenerateAccessToken(userID, l.svcCtx.Config.Jwt.AccessSecret, l.svcCtx.Config.Jwt.AccessExpire)
	if err != nil {
		l.Errorf("generate access token failed, userID: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "token生成失败")
	}

	refreshToken, err := generateUniqueRefreshToken(l.ctx, l.svcCtx.GormDB)
	if err != nil {
		l.Errorf("generate refresh token failed, userID: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "refresh_token生成失败")
	}

	tokenKey := rediskey.TokenKey(userID)
	tokenTTL := time.Duration(l.svcCtx.Config.Jwt.AccessExpire) * time.Second
	if err := l.svcCtx.RedisCli.Set(l.ctx, tokenKey, accessToken, tokenTTL).Err(); err != nil {
		l.Errorf("save access token to redis failed, userID: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "登录token信息存储失败")
	}

	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Model(&model.Account{}).
		Where("id = ?", user.ID).
		Update("refresh_token", refreshToken).Error; err != nil {
		_ = l.svcCtx.RedisCli.Del(l.ctx, tokenKey).Err()
		l.Errorf("save refresh token failed, userID: %d, error: %v", userID, err)
		return nil, status.Error(codes.Internal, "登录refresh_token信息存储失败")
	}

	return &account.LoginResp{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}
