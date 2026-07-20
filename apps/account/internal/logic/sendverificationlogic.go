package logic

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"net/mail"
	"strings"

	"feedsystem-zero/apps/account/account"
	"feedsystem-zero/apps/account/internal/svc"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SendVerificationLogic struct {
	ctx    context.Context     //请求上下文
	svcCtx *svc.ServiceContext //全局资源
	logx.Logger
}

func NewSendVerificationLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SendVerificationLogic {
	return &SendVerificationLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func GenSafe6Code() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%06d", n.Int64()), nil
}

// 验证码
func (l *SendVerificationLogic) SendVerification(in *account.VerificationReq) (*account.VerificationResp, error) {
	//取出邮箱
	email := strings.ToLower(strings.TrimSpace(in.GetEmail()))
	if email == "" {
		return nil, status.Error(codes.InvalidArgument, "邮箱为空")
	}
	if _, err := mail.ParseAddress(email); err != nil {
		return nil, status.Error(codes.InvalidArgument, "无效邮箱")
	}

	//生成验证码
	code, err := GenSafe6Code()
	if err != nil {
		return nil, status.Error(codes.Internal, "生成验证码失败")
	}

	//将验证码保存到redis
	key := rediskey.VerificationCodeKey(email)
	if err := l.svcCtx.RedisCli.Set(l.ctx, key, code, rediskey.VerificationCodeTTL).Err(); err != nil {
		l.Errorf("set verification code to redis failed, email: %s, error: %v", email, err)
		return nil, status.Error(codes.Internal, "save verification code failed")
	}

	//发送验证码到邮箱
	if err := l.svcCtx.Config.Email.SendVerificationCode(l.ctx, email, code, rediskey.VerificationCodeTTL); err != nil {
		_ = l.svcCtx.RedisCli.Del(l.ctx, key).Err()
		l.Errorf("send verification email failed, email: %s, error: %v", email, err)
		return nil, status.Error(codes.Internal, "send verification email failed")
	}

	return &account.VerificationResp{
		Verification: "verification email sent",
	}, nil
}
