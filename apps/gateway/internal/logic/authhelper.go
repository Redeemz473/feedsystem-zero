package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const userIDClaimKey = "user_id"

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
