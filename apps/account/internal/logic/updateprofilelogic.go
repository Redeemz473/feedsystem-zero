package logic

import (
	"context"
	"errors"
	"strings"

	"feedsystem-zero/apps/account/account"
	"feedsystem-zero/apps/account/internal/model"
	"feedsystem-zero/apps/account/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type UpdateProfileLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateProfileLogic {
	return &UpdateProfileLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 更新用户信息
func (l *UpdateProfileLogic) UpdateProfile(in *account.UpdateProfileReq) (*account.UpdateProfileResp, error) {
	//取出需要更改的用户信息，并根据userid找到在mysql中的原信息
	userid := in.GetUserId()
	if userid == 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id为空")
	}

	var user model.Account
	if err := l.svcCtx.GormDB.WithContext(l.ctx).Where("id=?", userid).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "用户不存在")
		}
		l.Errorf("query user before update failed, userID: %d, error: %v", userid, err)
		return nil, status.Error(codes.Internal, "查询用户失败")
	}

	updateName := strings.TrimSpace(in.GetUsername())
	updateAvatarURL := strings.TrimSpace(in.GetAvatarUrl())

	updates := make(map[string]any)

	if updateName != "" && updateName != user.Username {
		if len(updateName) > 64 {
			return nil, status.Error(codes.InvalidArgument, "用户名长度不能超过64位")
		}
		updates["username"] = updateName
	}

	if updateAvatarURL != "" && updateAvatarURL != user.AvatarURL {
		if len(updateAvatarURL) > 512 {
			return nil, status.Error(codes.InvalidArgument, "头像地址长度不能超过512位")
		}
		updates["avatar_url"] = updateAvatarURL
	}

	if in.Bio != nil {
		updateBio := strings.TrimSpace(in.GetBio())
		if len(updateBio) > 512 {
			return nil, status.Error(codes.InvalidArgument, "个人简介长度不能超过512位")
		}
		if updateBio != user.Bio {
			updates["bio"] = updateBio
		}
	}

	if len(updates) == 0 {
		return &account.UpdateProfileResp{
			Msg: "无需更新",
		}, nil
	}

	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Model(&model.Account{}).
		Where("id = ?", userid).
		Updates(updates).Error; err != nil {
		if isDuplicateEntry(err) && strings.Contains(err.Error(), "uk_username") {
			return nil, status.Error(codes.AlreadyExists, "用户名已存在")
		}
		l.Errorf("update user profile failed, userID: %d, updates: %+v, error: %v", userid, updates, err)
		return nil, status.Error(codes.Internal, "更新用户信息失败")
	}

	return &account.UpdateProfileResp{
		Msg: "更新成功",
	}, nil
}
