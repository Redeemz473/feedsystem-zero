package logic

import (
	"context"
	"errors"
	"testing"

	"feedsystem-zero/apps/job/interaction_sync/internal/config"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCallFlushRPCWithRetryRecoversFromLockContention(t *testing.T) {
	t.Parallel()

	attempts := 0
	got, err := callFlushRPCWithRetry(
		context.Background(),
		config.SyncConf{RpcTimeoutMs: 100, RetryBackoffMs: 1, MaxRetryBackoffMs: 2},
		"test",
		1,
		func(context.Context) (int, error) {
			attempts++
			if attempts < 3 {
				return 0, status.Error(codes.Aborted, "flush lock busy")
			}
			return 42, nil
		},
	)
	if err != nil {
		t.Fatalf("callFlushRPCWithRetry() error = %v", err)
	}
	if got != 42 || attempts != 3 {
		t.Fatalf("got=%d attempts=%d, want got=42 attempts=3", got, attempts)
	}
}

func TestCallFlushRPCWithRetryDoesNotRetryBusinessError(t *testing.T) {
	t.Parallel()

	attempts := 0
	_, err := callFlushRPCWithRetry(
		context.Background(),
		config.SyncConf{},
		"test",
		1,
		func(context.Context) (int, error) {
			attempts++
			return 0, status.Error(codes.InvalidArgument, "bad event")
		},
	)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error code = %s, want %s", status.Code(err), codes.InvalidArgument)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d, want 1", attempts)
	}
}

func TestCallFlushRPCWithRetryStopsWhenContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	_, err := callFlushRPCWithRetry(
		ctx,
		config.SyncConf{},
		"test",
		1,
		func(context.Context) (int, error) {
			attempts++
			return 0, status.Error(codes.Aborted, "busy")
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want context.Canceled", err)
	}
	if attempts != 0 {
		t.Fatalf("attempts=%d, want 0", attempts)
	}
}

func TestIsRetryableFlushRPCError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "lock contention", err: status.Error(codes.Aborted, "busy"), want: true},
		{name: "rpc unavailable", err: status.Error(codes.Unavailable, "down"), want: true},
		{name: "rpc timeout", err: status.Error(codes.DeadlineExceeded, "timeout"), want: true},
		{name: "overloaded", err: status.Error(codes.ResourceExhausted, "busy"), want: true},
		{name: "invalid event", err: status.Error(codes.InvalidArgument, "bad event"), want: false},
		{name: "internal data error", err: status.Error(codes.Internal, "db error"), want: false},
		{name: "plain error", err: errors.New("plain"), want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isRetryableFlushRPCError(tt.err); got != tt.want {
				t.Fatalf("isRetryableFlushRPCError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShouldLogFlushRPCRetry(t *testing.T) {
	t.Parallel()

	for _, attempt := range []int{1, 10, 20} {
		if !shouldLogFlushRPCRetry(attempt) {
			t.Fatalf("attempt %d should be logged", attempt)
		}
	}
	for _, attempt := range []int{2, 9, 11} {
		if shouldLogFlushRPCRetry(attempt) {
			t.Fatalf("attempt %d should not be logged", attempt)
		}
	}
}

func TestNormalizeSyncBatchSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   int
		want int
	}{
		{name: "default", in: 0, want: 500},
		{name: "configured", in: 200, want: 200},
		{name: "limited", in: 1000, want: 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeSyncBatchSize(tt.in); got != tt.want {
				t.Fatalf("normalizeSyncBatchSize(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}
