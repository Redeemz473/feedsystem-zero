package logic

import (
	"context"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/gateway/internal/types"
)

const gatewayBatchProfileChunkSize = 100

// loadSocialUserInfoMap 批量补齐公开用户资料，避免关注列表逐项调用 GetProfile。
// 按 AccountRpc 的单批上限自动分片。
func loadSocialUserInfoMap(
	ctx context.Context,
	accountRpc accountclient.Account,
	rawUserIDs []uint64,
) (map[uint64]types.SocialUserInfo, error) {
	seen := make(map[uint64]struct{}, len(rawUserIDs))
	userIDs := make([]uint64, 0, len(rawUserIDs))
	for _, userID := range rawUserIDs {
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		userIDs = append(userIDs, userID)
	}

	profileMap := make(map[uint64]types.SocialUserInfo, len(userIDs))
	for start := 0; start < len(userIDs); start += gatewayBatchProfileChunkSize {
		end := start + gatewayBatchProfileChunkSize
		if end > len(userIDs) {
			end = len(userIDs)
		}

		rpcResp, err := accountRpc.BatchGetProfiles(ctx, &accountclient.BatchGetProfilesReq{
			UserIds: userIDs[start:end],
		})
		if err != nil {
			return nil, err
		}
		for _, profile := range rpcResp.GetProfiles() {
			if profile == nil || profile.GetUserId() == 0 {
				continue
			}
			profileMap[profile.GetUserId()] = types.SocialUserInfo{
				Userid:    profile.GetUserId(),
				Username:  profile.GetUsername(),
				Avatarurl: profile.GetAvatarUrl(),
				Bio:       profile.GetBio(),
			}
		}
	}

	return profileMap, nil
}
