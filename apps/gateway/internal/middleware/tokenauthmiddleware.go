// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const userIDClaimKey = "user_id"

type TokenAuthMiddleware struct {
	redisCli *redis.Client
}

func NewTokenAuthMiddleware(redisCli *redis.Client) *TokenAuthMiddleware {
	return &TokenAuthMiddleware{
		redisCli: redisCli,
	}
}

func (m *TokenAuthMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := userIDFromCtx(r.Context())
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		accessToken, err := bearerToken(r.Header.Get("Authorization"))
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		savedToken, err := m.redisCli.Get(r.Context(), rediskey.TokenKey(userID)).Result()
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

		next(w, r)
	}
}

func bearerToken(authHeader string) (string, error) {
	fields := strings.Fields(authHeader)
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
		return "", status.Error(codes.Unauthenticated, "缺少访问令牌")
	}

	return fields[1], nil
}

func userIDFromCtx(ctx context.Context) (uint64, error) {
	value := ctx.Value(userIDClaimKey)
	if value == nil {
		return 0, status.Error(codes.Unauthenticated, "未登录")
	}

	userID, err := claimValueToUint64(value)
	if err != nil || userID == 0 {
		return 0, status.Error(codes.Unauthenticated, "无效登录态")
	}

	return userID, nil
}

func claimValueToUint64(value any) (uint64, error) {
	switch v := value.(type) {
	case json.Number:
		return strconv.ParseUint(v.String(), 10, 64)
	case string:
		return strconv.ParseUint(v, 10, 64)
	case uint64:
		return v, nil
	case uint:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case int:
		if v < 0 {
			return 0, fmt.Errorf("negative user_id: %d", v)
		}
		return uint64(v), nil
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("negative user_id: %d", v)
		}
		return uint64(v), nil
	case int32:
		if v < 0 {
			return 0, fmt.Errorf("negative user_id: %d", v)
		}
		return uint64(v), nil
	case float64:
		if v < 0 || math.Trunc(v) != v {
			return 0, fmt.Errorf("invalid user_id: %v", v)
		}
		return uint64(v), nil
	default:
		return 0, fmt.Errorf("unsupported user_id type: %T", value)
	}
}
