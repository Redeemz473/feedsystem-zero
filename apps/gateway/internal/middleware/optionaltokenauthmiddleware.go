// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package middleware

import (
	"context"
	"net/http"
	"strings"

	"feedsystem-zero/common/jwtx"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type OptionalTokenAuthMiddleware struct {
	redisCli     *redis.Client
	accessSecret string
}

func NewOptionalTokenAuthMiddleware(redisCli *redis.Client, accessSecret string) *OptionalTokenAuthMiddleware {
	return &OptionalTokenAuthMiddleware{
		redisCli:     redisCli,
		accessSecret: accessSecret,
	}
}

func (m *OptionalTokenAuthMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
		if authHeader == "" {
			next(w, r)
			return
		}

		accessToken, err := bearerToken(authHeader)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		claims, err := jwtx.ParseToken(accessToken, m.accessSecret)
		if err != nil || claims.UserID == 0 {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unauthenticated, "无效访问令牌"))
			return
		}

		savedToken, err := m.redisCli.Get(r.Context(), rediskey.TokenKey(claims.UserID)).Result()
		if err != nil {
			if err == redis.Nil {
				httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unauthenticated, "登录态已失效"))
				return
			}

			httpx.ErrorCtx(r.Context(), w, status.Error(codes.Internal, "登录态校验失败"))
			return
		}

		if savedToken != accessToken {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unauthenticated, "登录态已失效"))
			return
		}

		ctx := context.WithValue(r.Context(), userIDClaimKey, claims.UserID)
		next(w, r.WithContext(ctx))
	}
}
