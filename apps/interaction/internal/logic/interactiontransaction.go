package logic

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

const (
	interactionDBMaxRetries = 3
	interactionDBRetryBase  = 20 * time.Millisecond
	interactionDBRetryMax   = 200 * time.Millisecond
)

// runInteractionWriteTransaction 对在线互动写事务做有限重试。
// MySQL 发生死锁或锁等待超时时，GORM 会先回滚整个事务，再重新执行 fn；
// 调用方必须在事务外生成并复用业务 event_id，保证一次请求的业务身份稳定。
func runInteractionWriteTransaction(ctx context.Context, db *gorm.DB, fn func(tx *gorm.DB) error) error {
	for attempt := 0; ; attempt++ {
		err := db.WithContext(ctx).Transaction(fn)
		if err == nil {
			return nil
		}
		if !isRetryableInteractionDBError(err) {
			return err
		}
		if attempt >= interactionDBMaxRetries {
			return fmt.Errorf("interaction事务重试耗尽, retries:%d: %w", interactionDBMaxRetries, err)
		}

		retryNumber := attempt + 1
		delay := interactionDBRetryDelay(retryNumber)
		logx.WithContext(ctx).Infof(
			"interaction事务发生可重试锁冲突, retry:%d/%d delay:%s error:%v",
			retryNumber,
			interactionDBMaxRetries,
			delay,
			err,
		)
		if err := waitInteractionDBRetry(ctx, delay); err != nil {
			return err
		}
	}
}

func isRetryableInteractionDBError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 1213 || mysqlErr.Number == 1205
}

func interactionDBRetryDelay(retryNumber int) time.Duration {
	delay := interactionDBRetryBase
	for i := 1; i < retryNumber && delay < interactionDBRetryMax; i++ {
		if delay >= interactionDBRetryMax/2 {
			delay = interactionDBRetryMax
			break
		}
		delay *= 2
	}
	if delay > interactionDBRetryMax {
		delay = interactionDBRetryMax
	}
	if delay >= interactionDBRetryMax {
		return delay
	}

	// 最多增加 50% 抖动，避免同一批被回滚的请求再次同时争抢相同索引页。
	jitterWindow := delay / 2
	jitter := time.Duration(time.Now().UnixNano() % int64(jitterWindow+1))
	if delay+jitter > interactionDBRetryMax {
		return interactionDBRetryMax
	}
	return delay + jitter
}

func waitInteractionDBRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
