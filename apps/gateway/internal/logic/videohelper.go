package logic

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"feedsystem-zero/apps/gateway/internal/config"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/interaction/interactionclient"
	"feedsystem-zero/apps/video/videoclient"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultMaxVideoBytes          = 100 << 20
	defaultMaxCoverBytes          = 10 << 20
	defaultChunkThresholdBytes    = 20 << 20
	defaultChunkBytes             = 8 << 20
	defaultMaxChunkBytes          = 10 << 20
	defaultChunkSessionTTLSeconds = 24 * 60 * 60
	defaultUploadedFileTTLSeconds = 7 * 24 * 60 * 60
)

var (
	videoExts = map[string]struct{}{
		".mp4":  {},
		".mov":  {},
		".m4v":  {},
		".webm": {},
	}
	coverExts = map[string]struct{}{
		".jpg":  {},
		".jpeg": {},
		".png":  {},
		".webp": {},
	}
)

func optionalUserIDFromCtx(ctx context.Context) uint64 {
	userID, err := userIDFromCtx(ctx)
	if err != nil {
		return 0
	}
	return userID
}

func toHTTPVideoInfo(v *videoclient.VideoInfo) types.VideoInfo {
	if v == nil {
		return types.VideoInfo{}
	}

	return types.VideoInfo{
		Videoid:        v.GetVideoId(),
		Authorid:       v.GetAuthorId(),
		Authorusername: v.GetAuthorUsername(),
		Title:          v.GetTitle(),
		Description:    v.GetDescription(),
		Playurl:        v.GetPlayUrl(),
		Coverurl:       v.GetCoverUrl(),
		Likescount:     v.GetLikesCount(),
		Commentscount:  v.GetCommentsCount(),
		Popularity:     v.GetPopularity(),
		Status:         v.GetStatus(),
		Createdat:      v.GetCreatedAt(),
		Updatedat:      v.GetUpdatedAt(),
		Isliked:        v.GetIsLiked(),
		Tags:           v.GetTags(),
	}
}

const gatewayBatchStatsChunkSize = 50

func enrichHTTPVideoInteractions(ctx context.Context, interactionRpc interactionclient.Interaction, viewerID uint64, videos []types.VideoInfo) ([]types.VideoInfo, error) {
	if len(videos) == 0 {
		return videos, nil
	}

	videoIDs := uniqueHTTPVideoIDs(videos)
	statsMap := make(map[uint64]*interactionclient.VideoInteractionStats, len(videoIDs))
	for start := 0; start < len(videoIDs); start += gatewayBatchStatsChunkSize {
		end := start + gatewayBatchStatsChunkSize
		if end > len(videoIDs) {
			end = len(videoIDs)
		}

		rpcResp, err := interactionRpc.BatchGetVideoStats(ctx, &interactionclient.BatchGetVideoStatsReq{
			ViewerId: viewerID,
			VideoIds: videoIDs[start:end],
		})
		if err != nil {
			return nil, err
		}
		for _, stat := range rpcResp.GetStats() {
			statsMap[stat.GetVideoId()] = stat
		}
	}

	for i := range videos {
		stat, ok := statsMap[videos[i].Videoid]
		if !ok {
			continue
		}
		videos[i].Likescount = stat.GetLikesCount()
		videos[i].Commentscount = stat.GetCommentsCount()
		videos[i].Popularity = stat.GetPopularity()
		videos[i].Isliked = stat.GetIsLiked()
	}

	return videos, nil
}

func uniqueHTTPVideoIDs(videos []types.VideoInfo) []uint64 {
	seen := make(map[uint64]struct{}, len(videos))
	videoIDs := make([]uint64, 0, len(videos))
	for _, video := range videos {
		if video.Videoid == 0 {
			continue
		}
		if _, ok := seen[video.Videoid]; ok {
			continue
		}
		seen[video.Videoid] = struct{}{}
		videoIDs = append(videoIDs, video.Videoid)
	}
	return videoIDs
}

func toHTTPCommentInfo(c *interactionclient.CommentInfo) types.CommentInfo {
	if c == nil {
		return types.CommentInfo{}
	}

	return types.CommentInfo{
		Commentid: c.GetCommentId(),
		Videoid:   c.GetVideoId(),
		Userid:    c.GetUserId(),
		Username:  c.GetUsername(),
		Content:   c.GetContent(),
		Createdat: c.GetCreatedAt(),
		Updatedat: c.GetUpdatedAt(),
		Candelete: c.GetCanDelete(),
	}
}

func gatewayGeneratedRequestID(userID uint64, videoID uint64) (string, error) {
	token, err := randomHex(16)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("gateway:%d:%d:%d:%s", userID, videoID, time.Now().UnixNano(), token), nil
}

func loadHTTPVideosByIDs(
	ctx context.Context,
	videoRpc videoclient.Video,
	interactionRpc interactionclient.Interaction,
	viewerID uint64,
	videoIDs []uint64,
) (map[uint64]types.VideoInfo, error) {
	uniqueIDs := uniqueUint64(videoIDs)
	videos := make([]types.VideoInfo, 0, len(uniqueIDs))
	for _, videoID := range uniqueIDs {
		rpcResp, err := videoRpc.GetVideo(ctx, &videoclient.GetVideoReq{
			VideoId:  videoID,
			ViewerId: viewerID,
		})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				continue
			}
			return nil, err
		}
		videos = append(videos, toHTTPVideoInfo(rpcResp.GetVideo()))
	}

	if enrichedVideos, err := enrichHTTPVideoInteractions(ctx, interactionRpc, viewerID, videos); err == nil {
		videos = enrichedVideos
	}

	videoMap := make(map[uint64]types.VideoInfo, len(videos))
	for _, video := range videos {
		videoMap[video.Videoid] = video
	}
	return videoMap, nil
}

func uniqueUint64(raw []uint64) []uint64 {
	seen := make(map[uint64]struct{}, len(raw))
	items := make([]uint64, 0, len(raw))
	for _, item := range raw {
		if item == 0 {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		items = append(items, item)
	}
	return items
}

type savedUploadAsset struct {
	URL         string
	StoragePath string
	FileHash    string
	Size        int64
}

func saveVideoUpload(r *http.Request, upload config.UploadConf) (*savedUploadAsset, error) {
	maxBytes := chunkThresholdBytes(upload)
	if upload.MaxVideoBytes > 0 && upload.MaxVideoBytes < maxBytes {
		maxBytes = upload.MaxVideoBytes
	}
	expectedHash := normalizeFormHash(r, "file_hash")
	return saveMultipartUpload(r, upload, "videos", maxBytes, videoExts, []string{"file", "video"}, expectedHash)
}

func saveCoverUpload(r *http.Request, upload config.UploadConf) (*savedUploadAsset, error) {
	maxBytes := upload.MaxCoverBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxCoverBytes
	}
	expectedHash := normalizeFormHash(r, "file_hash")
	return saveMultipartUpload(r, upload, "covers", maxBytes, coverExts, []string{"file", "cover"}, expectedHash)
}

// normalizeFormHash 从 multipart 表单中读取 file_hash 字段并归一化为标准哈希。
// 字段不存在或为空时返回空串（表示前端未提供 hash，跳过校验，保持向后兼容）。
func normalizeFormHash(r *http.Request, field string) string {
	if r == nil || r.MultipartForm == nil || r.MultipartForm.Value == nil {
		return ""
	}
	values := r.MultipartForm.Value[field]
	if len(values) == 0 {
		return ""
	}
	hash, err := normalizeUploadHash(values[0])
	if err != nil {
		return ""
	}
	return hash
}

// 整文件直传
func saveMultipartUpload(
	r *http.Request,
	upload config.UploadConf,
	subDir string,
	maxBytes int64,
	allowedExts map[string]struct{},
	fieldNames []string,
	expectedHash string,
) (*savedUploadAsset, error) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		return nil, status.Error(codes.InvalidArgument, "上传文件解析失败")
	}

	for _, fieldName := range fieldNames {
		file, header, err := r.FormFile(fieldName)
		if err != nil {
			continue
		}
		defer file.Close()

		ext := strings.ToLower(filepath.Ext(header.Filename))
		if _, ok := allowedExts[ext]; !ok {
			return nil, status.Error(codes.InvalidArgument, "不支持的文件类型")
		}
		if header.Size > maxBytes {
			return nil, status.Error(codes.InvalidArgument, "上传文件过大")
		}

		targetDir := filepath.Join(uploadBaseDir(upload), subDir)
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return nil, status.Error(codes.Internal, "创建上传目录失败")
		}

		tmpName, err := randomFilename(".tmp")
		if err != nil {
			return nil, status.Error(codes.Internal, "生成文件名失败")
		}
		tmpPath := filepath.Join(targetDir, tmpName)

		dst, err := os.Create(tmpPath)
		if err != nil {
			return nil, status.Error(codes.Internal, "保存上传文件失败")
		}

		hasher := sha256.New()
		limited := &io.LimitedReader{R: file, N: maxBytes + 1}
		written, copyErr := io.Copy(io.MultiWriter(dst, hasher), limited)
		closeErr := dst.Close()
		if copyErr != nil {
			_ = os.Remove(tmpPath)
			return nil, status.Error(codes.Internal, "保存上传文件失败")
		}
		if closeErr != nil {
			_ = os.Remove(tmpPath)
			return nil, status.Error(codes.Internal, "保存上传文件失败")
		}
		if written > maxBytes {
			_ = os.Remove(tmpPath)
			return nil, status.Error(codes.InvalidArgument, "上传文件过大")
		}

		fileHash := hex.EncodeToString(hasher.Sum(nil))
		// 整文件完整性对账：若前端提供了 file_hash，必须与后端计算值一致。
		// 防止传输比特翻转 / 中间代理篡改等非断连型静默损坏（TCP checksum 不足以防御）。
		if expectedHash != "" && fileHash != expectedHash {
			_ = os.Remove(tmpPath)
			return nil, status.Error(codes.InvalidArgument, "文件 hash 校验失败")
		}
		filename := fileHash + ext
		targetPath := filepath.Join(targetDir, filename)
		//判断目标路径是否存在
		if _, err := os.Stat(targetPath); err == nil {
			//存在直接删除临时文件
			_ = os.Remove(tmpPath)
		} else if errors.Is(err, os.ErrNotExist) {
			//不存在 把临时文件重命名为目标路径，落盘
			if err := os.Rename(tmpPath, targetPath); err != nil {
				_ = os.Remove(tmpPath)
				return nil, status.Error(codes.Internal, "保存上传文件失败")
			}
		} else {
			_ = os.Remove(tmpPath)
			return nil, status.Error(codes.Internal, "保存上传文件失败")
		}

		return &savedUploadAsset{
			URL:         publicURL(upload, subDir, filename),
			StoragePath: targetPath,
			FileHash:    fileHash,
			Size:        written,
		}, nil
	}

	return nil, status.Error(codes.InvalidArgument, "缺少上传文件")
}

func randomFilename(ext string) (string, error) {
	token, err := randomHex(16)
	if err != nil {
		return "", err
	}
	return token + ext, nil
}

func randomUploadID() (string, error) {
	return randomHex(20)
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func uploadBaseDir(upload config.UploadConf) string {
	if strings.TrimSpace(upload.Dir) == "" {
		return "uploads"
	}
	return upload.Dir
}

func uploadPublicPrefix(upload config.UploadConf) string {
	publicPrefix := strings.TrimRight(upload.PublicPrefix, "/")
	if publicPrefix == "" {
		return "/uploads"
	}
	return publicPrefix
}

func publicURL(upload config.UploadConf, subDir string, filename string) string {
	return uploadPublicPrefix(upload) + "/" + strings.Trim(subDir, "/") + "/" + filename
}

// 防止yaml漏配置 设置分片阈值
func chunkThresholdBytes(upload config.UploadConf) int64 {
	if upload.ChunkThresholdBytes > 0 {
		return upload.ChunkThresholdBytes
	}
	return defaultChunkThresholdBytes
}

func defaultChunkSize(upload config.UploadConf) int64 {
	if upload.DefaultChunkBytes > 0 {
		return upload.DefaultChunkBytes
	}
	return defaultChunkBytes
}

func maxChunkBytes(upload config.UploadConf) int64 {
	if upload.MaxChunkBytes > 0 {
		return upload.MaxChunkBytes
	}
	return defaultMaxChunkBytes
}

func chunkSessionTTL(upload config.UploadConf) time.Duration {
	seconds := upload.ChunkSessionTTLSeconds
	if seconds <= 0 {
		seconds = defaultChunkSessionTTLSeconds
	}
	return time.Duration(seconds) * time.Second
}

func uploadedFileTTL(upload config.UploadConf) time.Duration {
	seconds := upload.UploadedFileTTLSeconds
	if seconds <= 0 {
		seconds = defaultUploadedFileTTLSeconds
	}
	return time.Duration(seconds) * time.Second
}

func shouldUseChunkUpload(upload config.UploadConf, fileSize int64) bool {
	return fileSize > chunkThresholdBytes(upload)
}

// 创建某一组分片的临时存储根目录
func chunkTempDir(upload config.UploadConf, uploadID string) string {
	return filepath.Join(uploadBaseDir(upload), "chunks", uploadID)
}

// 生成单个分片文件的完整本地路径
func chunkFilePath(upload config.UploadConf, uploadID string, chunkIndex int64) string {
	return filepath.Join(chunkTempDir(upload, uploadID), fmt.Sprintf("%06d.part", chunkIndex))
}

func finalVideoFilePath(upload config.UploadConf, fileHash string, ext string) string {
	return filepath.Join(uploadBaseDir(upload), "videos", strings.ToLower(fileHash)+ext)
}

func finalVideoPublicURL(upload config.UploadConf, fileHash string, ext string) string {
	return publicURL(upload, "videos", strings.ToLower(fileHash)+ext)
}

// 判断文件格式后缀名是否正确
func validateVideoFilename(filename string) (string, error) {
	ext := strings.ToLower(filepath.Ext(strings.TrimSpace(filename)))
	if _, ok := videoExts[ext]; !ok {
		return "", status.Error(codes.InvalidArgument, "不支持的视频文件类型")
	}
	return ext, nil
}

// 返回统一小写无空格的标准哈希
func normalizeUploadHash(rawHash string) (string, error) {
	hash := strings.ToLower(strings.TrimSpace(rawHash))
	if !isHexHash(hash) {
		return "", status.Error(codes.InvalidArgument, "文件 hash 格式错误")
	}
	return hash, nil
}

func isHexHash(hash string) bool {
	if len(hash) != sha256.Size*2 {
		return false
	}

	for _, ch := range hash {
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') {
			continue
		}
		return false
	}
	return true
}

func hashFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	return hashReaderSHA256(file)
}

func hashReaderSHA256(reader io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, reader); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func loadUploadedChunks(ctx context.Context, redisCli *redis.Client, uploadID string) ([]int64, error) {
	values, err := redisCli.SMembers(ctx, rediskey.ChunkUploadKey(uploadID)).Result()
	if err != nil {
		return nil, status.Error(codes.Internal, "查询已上传分片失败")
	}

	uploadedChunks := make([]int64, 0, len(values))
	for _, value := range values {
		chunkIndex, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			continue
		}
		uploadedChunks = append(uploadedChunks, chunkIndex)
	}
	sort.Slice(uploadedChunks, func(i, j int) bool {
		return uploadedChunks[i] < uploadedChunks[j]
	})

	return uploadedChunks, nil
}

func lookupInstantUploadedFile(ctx context.Context, redisCli *redis.Client, upload config.UploadConf, userID uint64, fileHash string) (string, error) {
	playURL, err := redisCli.Get(ctx, rediskey.ChunkUploadHashKey(userID, fileHash)).Result()
	if err == nil {
		return playURL, nil
	}
	if !errors.Is(err, redis.Nil) {
		return "", status.Error(codes.Internal, "查询秒传缓存失败")
	}

	playURL, err = redisCli.Get(ctx, rediskey.ChunkUploadGlobalHashKey(fileHash)).Result()
	if err == nil {
		_ = redisCli.Set(
			ctx,
			rediskey.ChunkUploadHashKey(userID, fileHash),
			playURL,
			uploadedFileTTL(upload),
		).Err()
		return playURL, nil
	}
	if !errors.Is(err, redis.Nil) {
		return "", status.Error(codes.Internal, "查询秒传缓存失败")
	}

	return "", nil
}
