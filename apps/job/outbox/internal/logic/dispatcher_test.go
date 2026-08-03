package logic

import (
	"strings"
	"testing"
	"time"

	"feedsystem-zero/model"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestDueOutboxScopeGuardsAggregateOrder(t *testing.T) {
	t.Parallel()

	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN:                       "root:password@tcp(127.0.0.1:3306)/test",
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DryRun:               true,
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run gorm: %v", err)
	}

	now := time.Now()
	var events []model.OutboxEvent
	stmt := db.Model(&model.OutboxEvent{}).
		Scopes(dueOutboxScope(now, now.Add(-time.Minute))).
		Order("id ASC").
		Limit(100).
		Find(&events).Statement
	query := stmt.SQL.String()
	for _, fragment := range []string{
		"NOT EXISTS",
		"predecessor.aggregate_type = outbox_events.aggregate_type",
		"predecessor.aggregate_id = outbox_events.aggregate_id",
		"predecessor.id < outbox_events.id",
		"predecessor.status IN",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("claim query missing %q: %s", fragment, query)
		}
	}
}

func TestSplitOutboxBatchesPreservesOrderAndCoverage(t *testing.T) {
	t.Parallel()

	events := make([]model.OutboxEvent, 11)
	for i := range events {
		events[i].ID = uint64(i + 1)
	}
	batches := splitOutboxBatches(events, 4)
	if len(batches) != 4 {
		t.Fatalf("batch count = %d, want 4", len(batches))
	}

	var got []uint64
	for _, batch := range batches {
		for _, event := range batch {
			got = append(got, event.ID)
		}
	}
	if len(got) != len(events) {
		t.Fatalf("event count = %d, want %d", len(got), len(events))
	}
	for i, id := range got {
		if want := uint64(i + 1); id != want {
			t.Fatalf("event[%d] = %d, want %d", i, id, want)
		}
	}
}
