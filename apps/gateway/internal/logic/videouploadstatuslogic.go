// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type VideoUploadStatusLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewVideoUploadStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *VideoUploadStatusLogic {
	return &VideoUploadStatusLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *VideoUploadStatusLogic) VideoUploadStatus(req *types.VideoUploadStatusReq) (resp *types.VideoUploadStatusResp, err error) {
	// 1. 从 JWT 获取 user_id，并校验 upload_id 属于当前用户。
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	uploadID := strings.TrimSpace(req.Uploadid)
	reqFileHash := strings.TrimSpace(req.Filehash)
	if uploadID == "" && reqFileHash == "" {
		return nil, status.Error(codes.InvalidArgument, "上传会话或文件 hash 不能为空")
	}

	var requestFileHash string
	if reqFileHash != "" {
		requestFileHash, err = normalizeUploadHash(reqFileHash)
		if err != nil {
			return nil, err
		}

		playURL, err := lookupInstantUploadedFile(l.ctx, l.svcCtx.RedisCli, l.svcCtx.GormDB, l.svcCtx.Config.Upload, userID, requestFileHash)
		if err != nil {
			return nil, err
		}
		if playURL != "" {
			return &types.VideoUploadStatusResp{
				Uploadid:       uploadID,
				Uploadedchunks: []int64{},
				Completed:      true,
				Playurl:        playURL,
			}, nil
		}
	}

	if uploadID == "" {
		uploadID, err = l.svcCtx.RedisCli.Get(l.ctx, rediskey.ChunkUploadSessionKey(userID, requestFileHash)).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil, status.Error(codes.NotFound, "上传会话不存在或已过期")
			}
			return nil, status.Error(codes.Internal, "查询上传会话失败")
		}
	}

	// 2. 读取 ChunkUploadMetaKey(upload_id) 获取 total_chunks/file_hash。
	meta, err := l.svcCtx.RedisCli.HGetAll(l.ctx, rediskey.ChunkUploadMetaKey(uploadID)).Result()
	if err != nil {
		return nil, status.Error(codes.Internal, "查询上传元数据失败")
	}
	if len(meta) == 0 {
		return nil, status.Error(codes.NotFound, "上传会话不存在或已过期")
	}
	if meta["user_id"] != strconv.FormatUint(userID, 10) {
		return nil, status.Error(codes.PermissionDenied, "无权限查询该上传会话")
	}
	fileHash, err := normalizeUploadHash(meta["file_hash"])
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "上传元数据异常")
	}
	if requestFileHash != "" && requestFileHash != fileHash {
		return nil, status.Error(codes.InvalidArgument, "文件 hash 与上传会话不匹配")
	}

	totalChunks, err := strconv.ParseInt(meta["total_chunks"], 10, 64)
	if err != nil || totalChunks <= 0 {
		return nil, status.Error(codes.InvalidArgument, "上传元数据异常")
	}

	// 3. 读取 ChunkUploadKey(upload_id) 获取已上传分片列表。
	uploadedChunks, err := loadUploadedChunks(l.ctx, l.svcCtx.RedisCli, uploadID)
	if err != nil {
		return nil, err
	}

	// 4. 如果 file_hash 对应秒传 key 已存在，返回 completed=true 和 play_url。
	playURL, err := lookupInstantUploadedFile(l.ctx, l.svcCtx.RedisCli, l.svcCtx.GormDB, l.svcCtx.Config.Upload, userID, fileHash)
	if err != nil {
		return nil, err
	}
	if playURL != "" {
		return &types.VideoUploadStatusResp{
			Uploadid:       uploadID,
			Uploadedchunks: uploadedChunks,
			Totalchunks:    totalChunks,
			Completed:      true,
			Playurl:        playURL,
		}, nil
	}

	ttl := chunkSessionTTL(l.svcCtx.Config.Upload)
	pipe := l.svcCtx.RedisCli.TxPipeline()
	pipe.Expire(l.ctx, rediskey.ChunkUploadMetaKey(uploadID), ttl)
	pipe.Expire(l.ctx, rediskey.ChunkUploadKey(uploadID), ttl)
	pipe.Expire(l.ctx, rediskey.ChunkUploadSessionKey(userID, fileHash), ttl)
	if _, err := pipe.Exec(l.ctx); err != nil {
		return nil, status.Error(codes.Internal, "刷新上传会话失败")
	}

	// 5. 返回 uploaded_chunks，前端只补传缺失分片。

	return &types.VideoUploadStatusResp{
		Uploadid:       uploadID,
		Uploadedchunks: uploadedChunks,
		Totalchunks:    totalChunks,
		Completed:      false,
		Playurl:        playURL,
	}, nil
}
