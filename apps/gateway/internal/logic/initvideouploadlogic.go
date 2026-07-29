// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"errors"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type InitVideoUploadLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewInitVideoUploadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *InitVideoUploadLogic {
	return &InitVideoUploadLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *InitVideoUploadLogic) InitVideoUpload(req *types.InitVideoUploadReq) (resp *types.InitVideoUploadResp, err error) {
	// 1. 从 JWT 获取 user_id。
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	// 2. 校验 filename/file_hash/file_size/chunk_size/total_chunks。
	filename := strings.TrimSpace(req.Filename)
	if filename == "" {
		return nil, status.Error(codes.InvalidArgument, "文件名不能为空")
	}
	finalExt, err := validateVideoFilename(filename)
	if err != nil {
		return nil, err
	}

	fileHash, err := normalizeUploadHash(req.Filehash)
	if err != nil {
		return nil, err
	}

	fileSize := req.Filesize
	if fileSize <= 0 {
		return nil, status.Error(codes.InvalidArgument, "文件大小不能为空")
	}
	if maxBytes := l.svcCtx.Config.Upload.MaxVideoBytes; maxBytes > 0 && fileSize > maxBytes {
		return nil, status.Error(codes.InvalidArgument, "视频文件过大")
	}

	//定义分片大小
	chunkSize := req.Chunksize
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize(l.svcCtx.Config.Upload)
	}
	if chunkSize <= 0 {
		return nil, status.Error(codes.InvalidArgument, "分片大小不合法")
	}
	if chunkSize > maxChunkBytes(l.svcCtx.Config.Upload) {
		return nil, status.Error(codes.InvalidArgument, "分片大小超过上限")
	}

	//计算分片数量
	expectedTotalChunks := int64(math.Ceil(float64(fileSize) / float64(chunkSize)))
	if expectedTotalChunks <= 0 {
		return nil, status.Error(codes.InvalidArgument, "分片总数不合法")
	}
	if req.Totalchunks > 0 && req.Totalchunks != expectedTotalChunks {
		return nil, status.Error(codes.InvalidArgument, "分片总数与文件大小不匹配")
	}

	thresholdBytes := chunkThresholdBytes(l.svcCtx.Config.Upload)

	// 3. file_size <= ChunkThresholdBytes 时返回 need_chunk=false，让前端走 /video/upload 小文件直传。
	// 4. EnableInstantUpload=true 时先查 rediskey.ChunkUploadHashKey 或 ChunkUploadGlobalHashKey，命中则直接返回 play_url。
	if l.svcCtx.Config.Upload.EnableInstantUpload {
		playURL, err := lookupInstantUploadedFile(l.ctx, l.svcCtx.RedisCli, l.svcCtx.GormDB, l.svcCtx.Config.Upload, userID, fileHash)
		if err != nil {
			return nil, err
		}
		if playURL != "" {
			return &types.InitVideoUploadResp{
				Msg:                 "文件已存在，秒传成功",
				Needupload:          false,
				Needchunk:           false,
				Playurl:             playURL,
				Uploadedchunks:      []int64{},
				Chunksize:           chunkSize,
				Chunkthresholdbytes: thresholdBytes,
			}, nil
		}
	}

	if !shouldUseChunkUpload(l.svcCtx.Config.Upload, fileSize) {
		return &types.InitVideoUploadResp{
			Msg:                 "文件较小，请使用普通上传",
			Needupload:          true,
			Needchunk:           false,
			Uploadedchunks:      []int64{},
			Chunksize:           chunkSize,
			Chunkthresholdbytes: thresholdBytes,
		}, nil
	}

	// 5. 未命中时生成 upload_id，写入 rediskey.ChunkUploadMetaKey(upload_id)，TTL 用 chunkSessionTTL。
	uploadID, uploadedChunks, sessionChunkSize, err := l.reuseUploadSession(userID, fileHash, fileSize, finalExt)
	if err != nil {
		return nil, err
	}
	isNewSession := false
	if uploadID == "" {
		uploadID, err = randomUploadID()
		if err != nil {
			return nil, status.Error(codes.Internal, "生成上传会话失败")
		}
		isNewSession = true
		uploadedChunks = []int64{}
	} else {
		chunkSize = sessionChunkSize
	}

	if err := os.MkdirAll(chunkTempDir(l.svcCtx.Config.Upload, uploadID), 0755); err != nil {
		return nil, status.Error(codes.Internal, "创建分片目录失败")
	}

	if isNewSession {
		ttl := chunkSessionTTL(l.svcCtx.Config.Upload)
		now := time.Now().Unix()
		metaKey := rediskey.ChunkUploadMetaKey(uploadID)
		sessionKey := rediskey.ChunkUploadSessionKey(userID, fileHash)
		chunkKey := rediskey.ChunkUploadKey(uploadID)

		pipe := l.svcCtx.RedisCli.TxPipeline()
		pipe.HSet(l.ctx, metaKey, map[string]any{
			"user_id":      userID,
			"file_name":    filename,
			"file_hash":    fileHash,
			"file_size":    fileSize,
			"chunk_size":   chunkSize,
			"total_chunks": expectedTotalChunks,
			"final_ext":    finalExt,
			"created_at":   now,
			"updated_at":   now,
		})
		pipe.Expire(l.ctx, metaKey, ttl)
		pipe.Set(l.ctx, sessionKey, uploadID, ttl)
		pipe.Expire(l.ctx, chunkKey, ttl)
		if _, err := pipe.Exec(l.ctx); err != nil {
			return nil, status.Error(codes.Internal, "初始化上传会话失败")
		}
	}

	return &types.InitVideoUploadResp{
		Msg:                 "初始化分片上传成功",
		Uploadid:            uploadID,
		Needupload:          true,
		Needchunk:           true,
		Uploadedchunks:      uploadedChunks,
		Chunksize:           chunkSize,
		Chunkthresholdbytes: thresholdBytes,
	}, nil
}

// 判断是否上传过，有则进行断点续传
func (l *InitVideoUploadLogic) reuseUploadSession(userID uint64, fileHash string, fileSize int64, finalExt string) (string, []int64, int64, error) {
	//查redis，看看有没有上传过
	uploadID, err := l.svcCtx.RedisCli.Get(l.ctx, rediskey.ChunkUploadSessionKey(userID, fileHash)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil, 0, nil
		}
		return "", nil, 0, status.Error(codes.Internal, "查询上传会话失败")
	}

	//获取分片的元数据，并进行强一致性校验
	meta, err := l.svcCtx.RedisCli.HGetAll(l.ctx, rediskey.ChunkUploadMetaKey(uploadID)).Result()
	if err != nil {
		return "", nil, 0, status.Error(codes.Internal, "查询上传元数据失败")
	}
	if len(meta) == 0 ||
		meta["file_hash"] != fileHash ||
		meta["user_id"] != strconv.FormatUint(userID, 10) ||
		meta["file_size"] != strconv.FormatInt(fileSize, 10) ||
		meta["final_ext"] != finalExt {
		return "", nil, 0, nil
	}

	chunkSize, err := strconv.ParseInt(meta["chunk_size"], 10, 64)
	if err != nil || chunkSize <= 0 {
		return "", nil, 0, nil
	}
	totalChunks, err := strconv.ParseInt(meta["total_chunks"], 10, 64)
	if err != nil || totalChunks <= 0 {
		return "", nil, 0, nil
	}
	expectedTotalChunks := int64(math.Ceil(float64(fileSize) / float64(chunkSize)))
	if totalChunks != expectedTotalChunks {
		return "", nil, 0, nil
	}

	//查当前任务已上传分片编号
	uploadedChunks, err := loadUploadedChunks(l.ctx, l.svcCtx.RedisCli, uploadID)
	if err != nil {
		return "", nil, 0, err
	}

	//刷新全部相关 Key 过期时间
	ttl := chunkSessionTTL(l.svcCtx.Config.Upload)
	pipe := l.svcCtx.RedisCli.TxPipeline()
	pipe.Expire(l.ctx, rediskey.ChunkUploadMetaKey(uploadID), ttl)
	pipe.Expire(l.ctx, rediskey.ChunkUploadKey(uploadID), ttl)
	pipe.Expire(l.ctx, rediskey.ChunkUploadSessionKey(userID, fileHash), ttl)
	if _, err := pipe.Exec(l.ctx); err != nil {
		return "", nil, 0, status.Error(codes.Internal, "刷新上传会话失败")
	}

	return uploadID, uploadedChunks, chunkSize, nil
}
