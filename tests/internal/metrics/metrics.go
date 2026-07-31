// Package metrics collects per-request latency and outcome data for a load
// test run, then produces a compact summary.
//
// It aims for zero allocations on the hot path: each Worker records into a
// preallocated slice; results are merged at the end.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"feedsystem-zero/tests/internal/httpclient"
)

// Recorder is safe for concurrent use across workers.
type Recorder struct {
	mu          sync.Mutex
	latencies   []time.Duration
	success     atomic.Int64
	errorsByKey sync.Map // key = string, value = *atomic.Int64
	started     time.Time
	stopped     time.Time
}

// NewRecorder pre-allocates a slice sized for the expected request count.
// `expected` is a hint; if it's off the slice will still grow.
func NewRecorder(expected int) *Recorder {
	return &Recorder{
		latencies: make([]time.Duration, 0, expected),
	}
}

// Start marks the beginning of the measurement window (excluding warmup).
func (r *Recorder) Start() {
	r.started = time.Now()
}

// Stop marks the end of the measurement window.
func (r *Recorder) Stop() {
	r.stopped = time.Now()
}

// Observe records a single request outcome.
//
// err == nil counts as success. On failure the classification is:
//   - *httpclient.APIError => "http-<status>"
//   - context deadline exceeded / net timeout => "timeout"
//   - anything else => "other"
func (r *Recorder) Observe(latency time.Duration, err error) {
	r.mu.Lock()
	r.latencies = append(r.latencies, latency)
	r.mu.Unlock()

	if err == nil {
		r.success.Add(1)
		return
	}
	r.incError(classify(err))
}

func (r *Recorder) incError(key string) {
	v, _ := r.errorsByKey.LoadOrStore(key, &atomic.Int64{})
	v.(*atomic.Int64).Add(1)
}

func classify(err error) string {
	var api *httpclient.APIError
	if errors.As(err, &api) {
		return fmt.Sprintf("http-%d", api.Status)
	}
	if isTimeout(err) {
		return "timeout"
	}
	return "other"
}

func isTimeout(err error) bool {
	type timeouter interface{ Timeout() bool }
	var t timeouter
	if errors.As(err, &t) {
		return t.Timeout()
	}
	return errors.Is(err, context.DeadlineExceeded)
}

// Summary is the final report produced by Format.
type Summary struct {
	Scenario           string
	Duration           time.Duration
	Concurrency        int
	Total              int64
	Success            int64
	Errors             map[string]int64
	QPS                float64
	P50, P95, P99, Max time.Duration
}

// Compute builds a Summary from the recorded data.
func (r *Recorder) Compute(scenario string, concurrency int) Summary {
	r.mu.Lock()
	defer r.mu.Unlock()

	dur := r.stopped.Sub(r.started)
	if dur <= 0 {
		dur = time.Since(r.started)
	}
	total := int64(len(r.latencies))
	success := r.success.Load()

	errs := map[string]int64{}
	r.errorsByKey.Range(func(k, v any) bool {
		errs[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})

	s := Summary{
		Scenario:    scenario,
		Duration:    dur,
		Concurrency: concurrency,
		Total:       total,
		Success:     success,
		Errors:      errs,
	}
	if dur > 0 {
		s.QPS = float64(total) / dur.Seconds()
	}
	if total > 0 {
		// Sort once; percentiles pick indices.
		sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })
		s.P50 = pctile(r.latencies, 0.50)
		s.P95 = pctile(r.latencies, 0.95)
		s.P99 = pctile(r.latencies, 0.99)
		s.Max = r.latencies[len(r.latencies)-1]
	}
	return s
}

func pctile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Format renders a Summary as an aligned, human-readable block.
func (s Summary) Format() string {
	successRate := 0.0
	if s.Total > 0 {
		successRate = float64(s.Success) * 100 / float64(s.Total)
	}
	buf := &strings.Builder{}
	fmt.Fprintf(buf, "=== Scenario: %s =====================================\n", s.Scenario)
	fmt.Fprintf(buf, "Duration       : %s\n", s.Duration.Round(10*time.Millisecond))
	fmt.Fprintf(buf, "Concurrency    : %d\n", s.Concurrency)
	fmt.Fprintf(buf, "Total          : %d requests\n", s.Total)
	fmt.Fprintf(buf, "Success        : %d (%.2f%%)\n", s.Success, successRate)
	fmt.Fprintf(buf, "QPS            : %.1f\n", s.QPS)
	fmt.Fprintf(buf, "Latency (ms)   : P50=%d  P95=%d  P99=%d  Max=%d\n",
		ms(s.P50), ms(s.P95), ms(s.P99), ms(s.Max))
	if len(s.Errors) > 0 {
		fmt.Fprintf(buf, "Errors         :\n")
		keys := make([]string, 0, len(s.Errors))
		for k := range s.Errors {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(buf, "  %-12s : %d\n", k, s.Errors[k])
		}
	}
	fmt.Fprintf(buf, "========================================================\n")
	return buf.String()
}

func ms(d time.Duration) int64 { return d.Milliseconds() }
