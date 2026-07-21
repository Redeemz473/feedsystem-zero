package logic

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// randomHex 生成 n 字节随机数并 hex 编码，用于事件 ID、锁 token 等场景。
func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// newEventID 按 "{prefix}_{纳秒时间戳}_{12字节随机}" 组装事件 ID，与其他服务风格保持一致。
func newEventID(prefix string) (string, error) {
	token, err := randomHex(12)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixNano(), token), nil
}
