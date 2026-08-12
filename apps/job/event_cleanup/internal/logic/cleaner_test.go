package logic

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCleanupLimits(t *testing.T) {
	t.Parallel()

	if got := normalizeCleanupBatchSize(0); got != defaultCleanupBatchSize {
		t.Fatalf("default batch size=%d", got)
	}
	if got := normalizeCleanupBatchSize(maxCleanupBatchSize + 1); got != maxCleanupBatchSize {
		t.Fatalf("max batch size=%d", got)
	}
	if got := normalizeMaxCleanupBatches(0); got != defaultMaxBatchesPerRun {
		t.Fatalf("default max batches=%d", got)
	}
	if got := normalizeMaxCleanupBatches(maxCleanupBatchesPerRun + 1); got != maxCleanupBatchesPerRun {
		t.Fatalf("max batches=%d", got)
	}
}

func TestCleanupDurations(t *testing.T) {
	t.Parallel()

	if got := cleanupPollInterval(0); got != defaultCleanupInterval {
		t.Fatalf("default poll interval=%s", got)
	}
	if got := cleanupDeleteTimeout(250); got != 250*time.Millisecond {
		t.Fatalf("delete timeout=%s", got)
	}
	if got := cleanupBatchInterval(0); got != defaultBatchInterval {
		t.Fatalf("default batch interval=%s", got)
	}
	if got := cleanupMaxRunDuration(0); got != defaultMaxRunDuration {
		t.Fatalf("default run duration=%s", got)
	}
	if got := durationHoursOrDefault(24, time.Hour); got != 24*time.Hour {
		t.Fatalf("retention=%s", got)
	}
}

func TestSleepWithContextCanBeCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepWithContext(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("sleep error=%v, want context.Canceled", err)
	}
}
