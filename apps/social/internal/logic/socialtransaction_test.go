package logic

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
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
