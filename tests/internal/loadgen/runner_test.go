package loadgen

import (
	"context"
	"testing"
	"time"

	"feedsystem-zero/tests/internal/metrics"
)

func TestRunnerLetsInflightRequestFinishAfterPhaseDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	recorder := metrics.NewRecorder(1)
	runner := Runner{
		Concurrency: 1,
		Duration:    100 * time.Millisecond,
		Recorder:    recorder,
		Op: func(ctx context.Context, _ int) error {
			select {
			case <-time.After(150 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}

	runner.Run(ctx)
	summary := recorder.Compute("test", 1)
	if summary.Total != 1 {
		t.Fatalf("total = %d, want 1", summary.Total)
	}
	if summary.Success != 1 {
		t.Fatalf("success = %d, want 1; errors = %v", summary.Success, summary.Errors)
	}
}
