package logic

import (
	"context"
	"errors"
	"net/mail"
	"strings"

	"feedsystem-zero/apps/account/account"
	"feedsystem-zero/apps/account/internal/model"
	"feedsystem-zero/apps/account/internal/svc"
	"feedsystem-zero/common/rediskey"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RegisterLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRegisterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RegisterLogic {
	return &RegisterLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 注册
func (l *RegisterLogic) Register(in *account.RegisterReq) (*account.RegisterResp, error) {
	//取出用户名、密码、邮箱号、验证码
	username := strings.TrimSpace(in.GetUsername())
	if username == "" {
		return nil, status.Error(codes.InvalidArgument, "用户名为空")
	}
	password := in.GetPassword()
	if password == "" {
		return nil, status.Error(codes.InvalidArgument, "密码为空")
	}
	if len(password) < 6 {
		return nil, status.Error(codes.InvalidArgument, "密码长度不能小于 6 位")
	}
	email := strings.ToLower(strings.TrimSpace(in.GetEmail()))
	if email == "" {
		return nil, status.Error(codes.InvalidArgument, "邮箱为空")
	}
	if _, err := mail.ParseAddress(email); err != nil {
		return nil, status.Error(codes.InvalidArgument, "无效邮箱")
	}
	verification := strings.TrimSpace(in.GetVerification())
	if verification == "" {
		return nil, status.Error(codes.InvalidArgument, "验证码为空")
	}

	//对比邮箱号和验证码是否正确，先对比验证码是否过期，再对比验证码是否正确
	key := rediskey.VerificationCodeKey(email)
	cmd := l.svcCtx.RedisCli.Get(l.ctx, key)
	savedverification, err := cmd.Result()
	if err != nil {
		if err == redis.Nil {
			return nil, status.Error(codes.InvalidArgument, "验证码不存在或已过期")
		}
		l.Errorf("query verification code failed, email: %s, error: %v", email, err)
		return nil, status.Error(codes.Internal, "验证码查询失败")
	}
	if savedverification != verification {
		return nil, status.Error(codes.InvalidArgument, "验证码错误")
	}

	//正确写入mysql
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		l.Errorf("generate password hash failed, username: %s, email: %s, error: %v", username, email, err)
		return nil, status.Error(codes.Internal, "密码加密失败")
	}

	user := model.Account{
		Username:     username,
		PasswordHash: string(passwordHash),
		Email:        email,
	}

	if err := l.svcCtx.GormDB.WithContext(l.ctx).Create(&user).Error; err != nil {
		if isDuplicateEntry(err) {
			if strings.Contains(err.Error(), "uk_username") {
				return nil, status.Error(codes.AlreadyExists, "用户名已存在")
			}
			if strings.Contains(err.Error(), "uk_email") {
				return nil, status.Error(codes.AlreadyExists, "邮箱已存在")
			}
			return nil, status.Error(codes.AlreadyExists, "用户已存在")
		}

		l.Errorf("create account failed, username: %s, email: %s, error: %v", username, email, err)
		return nil, status.Error(codes.Internal, "注册失败")
	}

	if err := l.svcCtx.RedisCli.Del(l.ctx, key).Err(); err != nil {
		l.Errorf("delete verification code failed after register, email: %s, error: %v", email, err)
	}

	return &account.RegisterResp{
		Msg: "注册成功",
	}, nil
}

func isDuplicateEntry(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}
