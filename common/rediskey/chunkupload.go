package rediskey

import "fmt"

// ChunkUploadKey 分片上传会话状态，建议用 SET 保存已上传分片索引。
// 格式: fsz:chunk:upload:{uploadID}
func ChunkUploadKey(uploadID string) string {
	return fmt.Sprintf("%s:chunk:upload:%s", prefix, uploadID)
}

// ChunkUploadMetaKey 分片上传元数据，建议用 HASH 保存 user_id/file_hash/file_size/chunk_size/total_chunks/final_ext。
// 格式: fsz:chunk:meta:{uploadID}
func ChunkUploadMetaKey(uploadID string) string {
	return fmt.Sprintf("%s:chunk:meta:%s", prefix, uploadID)
}

// ChunkUploadLockKey 分片合并锁，防止 complete 接口被重复并发调用。
// 格式: fsz:chunk:lock:{uploadID}
func ChunkUploadLockKey(uploadID string) string {
	return fmt.Sprintf("%s:chunk:lock:%s", prefix, uploadID)
}

// ChunkUploadSessionKey 用户未完成上传会话索引，value=uploadID。
// 用于前端刷新页面后，只凭 userID + fileHash 找回未完成上传，支持断点续传。
// 格式: fsz:chunk:session:{userID}:{fileHash}
func ChunkUploadSessionKey(userID uint64, fileHash string) string {
	return fmt.Sprintf("%s:chunk:session:%d:%s", prefix, userID, fileHash)
}

// ChunkUploadHashKey 文件秒传标记，value 建议保存 play_url。
// 格式: fsz:chunk:hash:{userID}:{fileHash}
func ChunkUploadHashKey(userID uint64, fileHash string) string {
	return fmt.Sprintf("%s:chunk:hash:%d:%s", prefix, userID, fileHash)
}

// ChunkUploadGlobalHashKey 全局文件秒传标记，value 建议保存 play_url。
// 如果希望不同用户上传同一文件也能秒传，可用这个 key；如果只允许用户维度秒传，用 ChunkUploadHashKey。
// 格式: fsz:chunk:global_hash:{fileHash}
func ChunkUploadGlobalHashKey(fileHash string) string {
	return fmt.Sprintf("%s:chunk:global_hash:%s", prefix, fileHash)
}
