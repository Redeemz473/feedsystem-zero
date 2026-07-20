package logic

import (
	"context"
	"errors"

	"feedsystem-zero/apps/account/internal/model"
	"feedsystem-zero/common/jwtx"

	"gorm.io/gorm"
)

const maxRefreshTokenGenerateAttempts = 5

func generateUniqueRefreshToken(ctx context.Context, db *gorm.DB) (string, error) {
	for i := 0; i < maxRefreshTokenGenerateAttempts; i++ {
		token, err := jwtx.GenerateRefreshToken()
		if err != nil {
			return "", err
		}

		var user model.Account
		err = db.WithContext(ctx).
			Select("id").
			Where("refresh_token = ?", token).
			First(&user).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return token, nil
		}
		if err != nil {
			return "", err
		}
	}

	return "", errors.New("generate unique refresh token failed")
}
