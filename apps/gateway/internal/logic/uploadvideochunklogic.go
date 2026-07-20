// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UploadVideoChunkLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUploadVideoChunkLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UploadVideoChunkLogic {
	return &UploadVideoChunkLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UploadVideoChunkLogic) UploadVideoChunk(req *types.UploadVideoChunkReq, r *http.Request) (resp *types.UploadVideoChunkResp, err error) {
	// 1. 从 JWT 获取 user_id，并用 upload_id 读取 ChunkUploadMetaKey 校验归属。
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}
	uploadID := strings.TrimSpace(req.Uploadid)
	if uploadID == "" {
		return nil, status.Error(codes.InvalidArgument, "上传会话不能为空")
	}

	meta, err := l.svcCtx.RedisCli.HGetAll(l.ctx, rediskey.ChunkUploadMetaKey(uploadID)).Result()
	if err != nil {
		return nil, status.Error(codes.Internal, "查询上传元数据失败")
	}
	if len(meta) == 0 {
		return nil, status.Error(codes.NotFound, "上传会话不存在或已过期")
	}
	//校验归属
	if meta["user_id"] != strconv.FormatUint(userID, 10) {
		return nil, status.Error(codes.PermissionDenied, "无权限上传该分片")
	}
	fileHash, err := normalizeUploadHash(meta["file_hash"])
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "上传元数据异常")
	}

	fileSize, err := strconv.ParseInt(meta["file_size"], 10, 64)
	if err != nil || fileSize <= 0 {
		return nil, status.Error(codes.InvalidArgument, "上传元数据异常")
	}
	totalChunks, err := strconv.ParseInt(meta["total_chunks"], 10, 64)
	if err != nil || totalChunks <= 0 {
		return nil, status.Error(codes.InvalidArgument, "上传元数据异常")
	}
	chunkSize, err := strconv.ParseInt(meta["chunk_size"], 10, 64)
	if err != nil || chunkSize <= 0 {
		return nil, status.Error(codes.InvalidArgument, "上传元数据异常")
	}
	if chunkSize > maxChunkBytes(l.svcCtx.Config.Upload) {
		return nil, status.Error(codes.InvalidArgument, "上传元数据异常")
	}

	// 2. 从 multipart/form-data 中读取字段 file/chunk。
	//解析HTTP 的 multipart/form-data
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		return nil, status.Error(codes.InvalidArgument, "上传文件解析失败")
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		file, header, err = r.FormFile("chunk")
	}
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "缺少分片文件")
	}
	defer file.Close()

	// 3. 校验 chunk_index 范围、chunk 大小 <= MaxChunkBytes。
	chunkIndex := req.Chunkindex
	if chunkIndex <= 0 || chunkIndex > totalChunks {
		return nil, status.Error(codes.InvalidArgument, "分片序号不合法")
	}
	//期望的分片的大小
	expectedChunkBytes := chunkSize
	if chunkIndex == totalChunks {
		expectedChunkBytes = fileSize - chunkSize*(totalChunks-1)
	}
	if expectedChunkBytes <= 0 || expectedChunkBytes > maxChunkBytes(l.svcCtx.Config.Upload) {
		return nil, status.Error(codes.InvalidArgument, "上传元数据异常")
	}
	if header.Size > 0 && header.Size > maxChunkBytes(l.svcCtx.Config.Upload) {
		return nil, status.Error(codes.InvalidArgument, "分片大小超过上限")
	}

	// 4. EnableChunkHashValidate=true 时计算当前分片 SHA256，与 chunk_hash 对比。
	chunkHash := strings.ToLower(strings.TrimSpace(req.Chunkhash))
	if l.svcCtx.Config.Upload.EnableChunkHashValidate {
		if len(chunkHash) != 64 || !isHexHash(chunkHash) {
			return nil, status.Error(codes.InvalidArgument, "分片 hash 格式错误")
		}
	}

	// 5. 将分片保存到 chunkFilePath(upload, upload_id, chunk_index)。
	if err := os.MkdirAll(chunkTempDir(l.svcCtx.Config.Upload, uploadID), 0755); err != nil {
		return nil, status.Error(codes.Internal, "创建分片目录失败")
	}

	path := chunkFilePath(l.svcCtx.Config.Upload, uploadID, chunkIndex)
	tmpToken, err := randomHex(8)
	if err != nil {
		return nil, status.Error(codes.Internal, "生成临时文件名失败")
	}
	tmpPath := path + "." + tmpToken + ".tmp"
	dst, err := os.Create(tmpPath)
	if err != nil {
		return nil, status.Error(codes.Internal, "保存分片文件失败")
	}

	//同时将分片写入临时文件和算hash
	hasher := sha256.New()
	limited := &io.LimitedReader{R: file, N: maxChunkBytes(l.svcCtx.Config.Upload) + 1}
	written, copyErr := io.Copy(io.MultiWriter(dst, hasher), limited)
	closeErr := dst.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return nil, status.Error(codes.Internal, "保存分片文件失败")
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return nil, status.Error(codes.Internal, "保存分片文件失败")
	}
	//保证这个分片完整的写入
	if written > maxChunkBytes(l.svcCtx.Config.Upload) {
		_ = os.Remove(tmpPath)
		return nil, status.Error(codes.InvalidArgument, "分片大小超过上限")
	}
	if written != expectedChunkBytes {
		_ = os.Remove(tmpPath)
		return nil, status.Error(codes.InvalidArgument, "分片大小不匹配")
	}
	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if l.svcCtx.Config.Upload.EnableChunkHashValidate && actualHash != chunkHash {
		_ = os.Remove(tmpPath)
		return nil, status.Error(codes.InvalidArgument, "分片 hash 校验失败")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return nil, status.Error(codes.Internal, "保存分片文件失败")
	}

	// 6. SADD rediskey.ChunkUploadKey(upload_id) 记录已上传分片，重复上传同一 chunk 应幂等覆盖/跳过。
	ttl := chunkSessionTTL(l.svcCtx.Config.Upload)
	//创建事务管道，保证一致性
	pipe := l.svcCtx.RedisCli.TxPipeline()
	pipe.SAdd(l.ctx, rediskey.ChunkUploadKey(uploadID), strconv.FormatInt(chunkIndex, 10))
	pipe.Expire(l.ctx, rediskey.ChunkUploadKey(uploadID), ttl)
	pipe.Expire(l.ctx, rediskey.ChunkUploadMetaKey(uploadID), ttl)
	pipe.Expire(l.ctx, rediskey.ChunkUploadSessionKey(userID, fileHash), ttl)
	if _, err := pipe.Exec(l.ctx); err != nil {
		return nil, status.Error(codes.Internal, "记录分片状态失败")
	}

	// 7. 返回 uploaded_chunks，前端据此断点续传。
	uploadedChunks, err := loadUploadedChunks(l.ctx, l.svcCtx.RedisCli, uploadID)
	if err != nil {
		return nil, err
	}

	return &types.UploadVideoChunkResp{
		Msg:            "上传分片成功",
		Uploadid:       uploadID,
		Chunkindex:     chunkIndex,
		Uploadedchunks: uploadedChunks,
	}, nil
}
