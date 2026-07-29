// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"feedsystem-zero/apps/gateway/internal/model"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CompleteVideoUploadLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCompleteVideoUploadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CompleteVideoUploadLogic {
	return &CompleteVideoUploadLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CompleteVideoUploadLogic) CompleteVideoUpload(req *types.CompleteVideoUploadReq) (resp *types.CompleteVideoUploadResp, err error) {
	// 1. 从 JWT 获取 user_id，并校验 upload_id 元数据归属。
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	uploadID := strings.TrimSpace(req.Uploadid)
	if uploadID == "" {
		return nil, status.Error(codes.InvalidArgument, "upload_id 不能为空")
	}

	fileHash, err := normalizeUploadHash(req.Filehash)
	if err != nil {
		return nil, err
	}
	if len(fileHash) != sha256.Size*2 {
		return nil, status.Error(codes.InvalidArgument, "文件 hash 必须是 SHA256")
	}

	filename := strings.TrimSpace(req.Filename)
	if filename == "" {
		return nil, status.Error(codes.InvalidArgument, "文件名不能为空")
	}
	finalExt, err := validateVideoFilename(filename)
	if err != nil {
		return nil, err
	}
	if req.Filesize <= 0 {
		return nil, status.Error(codes.InvalidArgument, "文件大小不能为空")
	}
	if req.Totalchunks <= 0 {
		return nil, status.Error(codes.InvalidArgument, "分片总数不能为空")
	}

	metaKey := rediskey.ChunkUploadMetaKey(uploadID)
	meta, err := l.svcCtx.RedisCli.HGetAll(l.ctx, metaKey).Result()
	if err != nil {
		return nil, status.Error(codes.Internal, "查询上传元数据失败")
	}
	if len(meta) == 0 {
		return nil, status.Error(codes.NotFound, "上传会话不存在或已过期")
	}
	if meta["user_id"] != strconv.FormatUint(userID, 10) {
		return nil, status.Error(codes.PermissionDenied, "无权限操作该上传会话")
	}
	if meta["file_hash"] != fileHash ||
		meta["file_size"] != strconv.FormatInt(req.Filesize, 10) ||
		meta["total_chunks"] != strconv.FormatInt(req.Totalchunks, 10) ||
		meta["final_ext"] != finalExt {
		return nil, status.Error(codes.InvalidArgument, "上传参数与初始化会话不一致")
	}

	// 2. 用 ChunkUploadLockKey(upload_id) 加短 TTL 分布式锁，避免重复合并。
	lockKey := rediskey.ChunkUploadLockKey(uploadID)
	lockToken, err := randomHex(16)
	if err != nil {
		return nil, status.Error(codes.Internal, "生成合并锁失败")
	}
	locked, err := l.svcCtx.RedisCli.SetNX(l.ctx, lockKey, lockToken, 5*time.Minute).Result()
	if err != nil {
		return nil, status.Error(codes.Internal, "获取合并锁失败")
	}
	if !locked {
		return nil, status.Error(codes.Aborted, "文件正在合并中，请稍后重试")
	}
	defer l.releaseUploadLock(lockKey, lockToken)

	// 3. 校验 uploaded_chunks 数量等于 total_chunks，缺失则返回明确错误。
	uploadedChunks, err := loadUploadedChunks(l.ctx, l.svcCtx.RedisCli, uploadID)
	if err != nil {
		return nil, err
	}
	if int64(len(uploadedChunks)) != req.Totalchunks {
		return nil, status.Error(codes.FailedPrecondition, "分片尚未全部上传")
	}
	chunkExists := make(map[int64]struct{}, len(uploadedChunks))
	for _, chunkIndex := range uploadedChunks {
		chunkExists[chunkIndex] = struct{}{}
	}
	for chunkIndex := int64(1); chunkIndex <= req.Totalchunks; chunkIndex++ {
		if _, ok := chunkExists[chunkIndex]; !ok {
			return nil, status.Errorf(codes.FailedPrecondition, "第 %d 个分片未上传", chunkIndex)
		}
	}

	// 4. 按 chunk_index 顺序读取 chunkFilePath 并合并到 finalVideoFilePath。
	finalPath := finalVideoFilePath(l.svcCtx.Config.Upload, fileHash, finalExt)
	playURL := finalVideoPublicURL(l.svcCtx.Config.Upload, fileHash, finalExt)
	if existingHash, err := hashFileSHA256(finalPath); err == nil {
		if existingHash != fileHash {
			return nil, status.Error(codes.Internal, "目标文件 hash 异常")
		}
		if err := validateUploadedFilePathSignature(finalPath, finalExt); err != nil {
			return nil, err
		}
		canonicalURL, err := l.finishCompletedUpload(userID, uploadID, fileHash, playURL, absolutePath(finalPath), req.Filesize)
		if err != nil {
			return nil, err
		}
		return &types.CompleteVideoUploadResp{
			Msg:      "合并成功",
			Playurl:  canonicalURL,
			Filehash: fileHash,
		}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, status.Error(codes.Internal, "检查目标文件失败")
	}

	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		return nil, status.Error(codes.Internal, "创建视频目录失败")
	}

	tmpToken, err := randomHex(8)
	if err != nil {
		return nil, status.Error(codes.Internal, "生成临时文件失败")
	}
	tmpPath := finalPath + "." + tmpToken + ".tmp"
	dst, err := os.Create(tmpPath)
	if err != nil {
		return nil, status.Error(codes.Internal, "创建合并文件失败")
	}
	needRemoveTmp := true
	defer func() {
		if needRemoveTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	hasher := sha256.New()
	writer := io.MultiWriter(dst, hasher)
	var written int64
	for chunkIndex := int64(1); chunkIndex <= req.Totalchunks; chunkIndex++ {
		chunkPath := chunkFilePath(l.svcCtx.Config.Upload, uploadID, chunkIndex)
		chunkFile, err := os.Open(chunkPath)
		if err != nil {
			_ = dst.Close()
			if errors.Is(err, os.ErrNotExist) {
				return nil, status.Errorf(codes.FailedPrecondition, "第 %d 个分片文件不存在", chunkIndex)
			}
			return nil, status.Error(codes.Internal, "读取分片失败")
		}

		n, copyErr := io.Copy(writer, chunkFile)
		closeErr := chunkFile.Close()
		if copyErr != nil {
			_ = dst.Close()
			return nil, status.Error(codes.Internal, "合并分片失败")
		}
		if closeErr != nil {
			_ = dst.Close()
			return nil, status.Error(codes.Internal, "读取分片失败")
		}
		written += n
	}
	if err := dst.Close(); err != nil {
		return nil, status.Error(codes.Internal, "保存合并文件失败")
	}

	// 5. 计算最终文件 SHA256，与 file_hash 对比，失败则删除合并文件。
	if written != req.Filesize {
		return nil, status.Error(codes.InvalidArgument, "合并文件大小与初始化不一致")
	}
	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if actualHash != fileHash {
		return nil, status.Error(codes.InvalidArgument, "合并文件 hash 校验失败")
	}
	if err := validateUploadedFilePathSignature(tmpPath, finalExt); err != nil {
		return nil, err
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return nil, status.Error(codes.Internal, "保存最终视频失败")
	}
	needRemoveTmp = false

	// 6. 写入 ChunkUploadHashKey/ChunkUploadGlobalHashKey 做秒传。
	// 7. 删除临时 chunk 目录和 upload 会话 key，返回 play_url。
	canonicalURL, err := l.finishCompletedUpload(userID, uploadID, fileHash, playURL, absolutePath(finalPath), req.Filesize)
	if err != nil {
		return nil, err
	}

	return &types.CompleteVideoUploadResp{
		Msg:      "合并成功",
		Playurl:  canonicalURL,
		Filehash: fileHash,
	}, nil
}

func (l *CompleteVideoUploadLogic) finishCompletedUpload(userID uint64, uploadID string, fileHash string, playURL string, storagePath string, fileSize int64) (string, error) {
	canonicalAsset, err := upsertFileAsset(l.ctx, l.svcCtx.GormDB, model.FileAssetTypeVideo, fileHash, playURL, storagePath, fileSize)
	if err != nil {
		l.Errorf("upsert video file asset failed, upload_id: %s, file_hash: %s, error: %v", uploadID, fileHash, err)
		return "", status.Error(codes.Internal, "保存视频资源失败")
	}

	ttl := uploadedFileTTL(l.svcCtx.Config.Upload)
	pipe := l.svcCtx.RedisCli.TxPipeline()
	pipe.Set(l.ctx, rediskey.ChunkUploadHashKey(userID, fileHash), canonicalAsset.URL, ttl)
	pipe.Set(l.ctx, rediskey.ChunkUploadGlobalHashKey(fileHash), canonicalAsset.URL, ttl)
	pipe.Del(l.ctx, rediskey.ChunkUploadMetaKey(uploadID))
	pipe.Del(l.ctx, rediskey.ChunkUploadKey(uploadID))
	pipe.Del(l.ctx, rediskey.ChunkUploadSessionKey(userID, fileHash))
	if _, err := pipe.Exec(l.ctx); err != nil {
		return "", status.Error(codes.Internal, "保存秒传状态失败")
	}

	if err := os.RemoveAll(chunkTempDir(l.svcCtx.Config.Upload, uploadID)); err != nil {
		l.Errorf("remove chunk temp dir failed, upload_id: %s, error: %v", uploadID, err)
	}
	return canonicalAsset.URL, nil
}

func (l *CompleteVideoUploadLogic) releaseUploadLock(lockKey string, lockToken string) {
	const unlockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`
	if err := l.svcCtx.RedisCli.Eval(l.ctx, unlockScript, []string{lockKey}, lockToken).Err(); err != nil {
		l.Errorf("release upload lock failed, lock_key: %s, error: %v", lockKey, err)
	}
}
