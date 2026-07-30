package rediskey

import "fmt"

// ChunkUploadKey 分片上传会话状态。
// 数据结构: SET
// key: fsz:chunk:upload:{uploadID}  value: 已上传分片索引集合
// 用途: 分片上传接口每收到一片就 SADD 一次，用于判断合并前是否已收齐所有分片。
func ChunkUploadKey(uploadID string) string {
	return fmt.Sprintf("%s:chunk:upload:%s", prefix, uploadID)
}

// ChunkUploadMetaKey 分片上传元数据。
// 数据结构: HASH
// key: fsz:chunk:meta:{uploadID}  fields: user_id / file_hash / file_size / chunk_size / total_chunks / final_ext
// 用途: 会话初始化时写入，后续鉴权、合并、秒传都要依赖这些元信息。
func ChunkUploadMetaKey(uploadID string) string {
	return fmt.Sprintf("%s:chunk:meta:%s", prefix, uploadID)
}

// ChunkUploadLockKey 分片合并锁。
// 数据结构: STRING
// key: fsz:chunk:lock:{uploadID}  value: 持锁标识
// 用途: 防止 complete 接口被重复并发调用导致合并写坏文件。
func ChunkUploadLockKey(uploadID string) string {
	return fmt.Sprintf("%s:chunk:lock:%s", prefix, uploadID)
}

// ChunkUploadSessionKey 用户未完成上传会话索引。
// 数据结构: STRING
// key: fsz:chunk:session:{userID}:{fileHash}  value: uploadID
// 用途: 前端刷新页面后，只凭 userID + fileHash 找回未完成的 uploadID，支持断点续传。
func ChunkUploadSessionKey(userID uint64, fileHash string) string {
	return fmt.Sprintf("%s:chunk:session:%d:%s", prefix, userID, fileHash)
}

// ChunkUploadHashKey 用户维度文件秒传标记。
// 数据结构: STRING
// key: fsz:chunk:hash:{userID}:{fileHash}  value: play_url
// 用途: 同一用户重复上传同一文件时命中秒传；隔离用户之间的可见性。
func ChunkUploadHashKey(userID uint64, fileHash string) string {
	return fmt.Sprintf("%s:chunk:hash:%d:%s", prefix, userID, fileHash)
}

// ChunkUploadGlobalHashKey 全局文件秒传标记。
// 数据结构: STRING
// key: fsz:chunk:global_hash:{fileHash}  value: play_url
// 用途: 若允许跨用户秒传则用此 key；如只允许用户维度秒传，改用 ChunkUploadHashKey。
func ChunkUploadGlobalHashKey(fileHash string) string {
	return fmt.Sprintf("%s:chunk:global_hash:%s", prefix, fileHash)
}
