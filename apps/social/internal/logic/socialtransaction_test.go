package logic

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"feedsystem-zero/apps/social/internal/model"

	"github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestIsRetryableSocialDBError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "deadlock", err: &mysql.MySQLError{Number: 1213}, want: true},
		{name: "lock wait timeout", err: fmt.Errorf("wrapped: %w", &mysql.MySQLError{Number: 1205}), want: true},
		{name: "duplicate key", err: &mysql.MySQLError{Number: 1062}, want: false},
		{name: "generic", err: errors.New("boom"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryableSocialDBError(tt.err); got != tt.want {
				t.Fatalf("isRetryableSocialDBError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSocialDBRetryDelayIsBounded(t *testing.T) {
	tests := []struct {
		retry int
		min   time.Duration
		max   time.Duration
	}{
		{retry: 1, min: 20 * time.Millisecond, max: 30 * time.Millisecond},
		{retry: 2, min: 40 * time.Millisecond, max: 60 * time.Millisecond},
		{retry: 3, min: 80 * time.Millisecond, max: 120 * time.Millisecond},
		{retry: 8, min: 200 * time.Millisecond, max: 200 * time.Millisecond},
	}
	for _, tt := range tests {
		delay := socialDBRetryDelay(tt.retry)
		if delay < tt.min || delay > tt.max {
			t.Fatalf("socialDBRetryDelay(%d) = %s, want [%s, %s]", tt.retry, delay, tt.min, tt.max)
		}
	}
}

func TestUpdateFollowAccountCountersUsesSingleCaseUpdate(t *testing.T) {
	db := newSocialDryRunDB(t)
	result := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Exec(`UPDATE accounts
			SET follower_count = CASE
					WHEN id = ? THEN GREATEST(CAST(follower_count AS SIGNED) + ?, 0)
					ELSE follower_count
				END,
				following_count = CASE
					WHEN id = ? THEN GREATEST(CAST(following_count AS SIGNED) + ?, 0)
					ELSE following_count
				END,
				is_big_v = CASE
					WHEN id = ? AND ? = 1 THEN 1
					ELSE is_big_v
				END
			WHERE id IN (?, ?)`,
			2, 1, 1, 1, 2, 1, 1, 2,
		)
	})

	for _, fragment := range []string{
		"UPDATE accounts",
		"follower_count = CASE",
		"following_count = CASE",
		"is_big_v = CASE",
		"WHERE id IN (1, 2)",
	} {
		if !strings.Contains(result, fragment) {
			t.Fatalf("counter update SQL missing %q: %s", fragment, result)
		}
	}
}

func TestCreateSocialOutboxEventsBuildsBatchInsertWithoutReusingIDs(t *testing.T) {
	db := newSocialDryRunDB(t)
	first := &model.OutboxEvent{ID: 10, EventID: "follow-1", Topic: "social.follow.events"}
	second := &model.OutboxEvent{ID: 11, EventID: "notify-1", Topic: "notification.events"}

	rows := socialOutboxRows(first, nil, second)
	if len(rows) != 2 {
		t.Fatalf("outbox row count = %d, want 2", len(rows))
	}
	if rows[0].ID != 0 || rows[1].ID != 0 {
		t.Fatalf("outbox rows reused auto-increment ids: %d, %d", rows[0].ID, rows[1].ID)
	}

	query := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Create(&rows)
	})
	if !strings.Contains(query, "VALUES") || !strings.Contains(query, "),(") {
		t.Fatalf("outbox insert is not a multi-value insert: %s", query)
	}
	insertColumns := strings.SplitN(query, "VALUES", 2)[0]
	if strings.Contains(insertColumns, "`id`") {
		t.Fatalf("outbox insert unexpectedly includes auto-increment id column: %s", query)
	}

	// helper 必须复制输入，不能修改调用方持有的模板。
	if first.ID != 10 || second.ID != 11 {
		t.Fatalf("outbox templates were mutated: first=%d second=%d", first.ID, second.ID)
	}
}

func newSocialDryRunDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(gormmysql.New(gormmysql.Config{
		DSN:                       "root:password@tcp(127.0.0.1:3306)/test",
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DryRun:               true,
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run gorm: %v", err)
	}
	return db
}
