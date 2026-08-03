// Package loadgen drives N worker goroutines that repeatedly invoke a
// user-provided operation for a bounded duration. It intentionally has no
// notion of scenarios — a scenario is just "a function that returns an
// operation to execute". This keeps concurrency mechanics in one place.
package loadgen

import (
	"context"
	"log"
	"sync"
	"time"

	"feedsystem-zero/tests/internal/metrics"
)

// Op is one unit of work performed by a worker. It returns the error (if
// any) but not the latency — latency is measured by the runner itself so
// scenarios cannot cheat.
type Op func(ctx context.Context, workerID int) error

// Runner coordinates a load-test run.
type Runner struct {
	// Concurrency workers issue Op in tight loops.
	Concurrency int
	// Duration is the measured window (warmup already elapsed).
	Duration time.Duration
	// Warmup runs Op without collecting metrics; used to fill caches and
	// establish HTTP keep-alive connections.
	Warmup time.Duration
	// Recorder receives every measured request.
	Recorder *metrics.Recorder
	// Op is the operation each worker runs on every tick.
	Op Op
	// Verbose logs the first N errors per worker for debugging.
	Verbose bool
}

// Run blocks until warmup + duration elapse (or ctx cancelled).
func (r *Runner) Run(ctx context.Context) {
	if r.Warmup > 0 {
		r.runPhase(ctx, r.Warmup, false)
	}

	r.Recorder.Start()
	r.runPhase(ctx, r.Duration, true)
	r.Recorder.Stop()
}

func (r *Runner) runPhase(ctx context.Context, duration time.Duration, record bool) {
	// phaseCtx 只负责停止派发新请求。已经开始的请求继续使用父 ctx，
	// 由 HTTP 客户端自己的请求超时控制，避免在压测窗口结束时把每个 worker
	// 正在执行的最后一个请求误记为 timeout 或下游 cancellation 500。
	phaseCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(r.Concurrency)
	for i := 0; i < r.Concurrency; i++ {
		wid := i
		go func() {
			defer wg.Done()
			errsLogged := 0
			for {
				select {
				case <-phaseCtx.Done():
					return
				default:
				}
				start := time.Now()
				err := r.Op(ctx, wid)
				elapsed := time.Since(start)
				if record {
					r.Recorder.Observe(elapsed, err)
				}
				if err != nil && r.Verbose && errsLogged < 5 {
					log.Printf("[worker-%d] err: %v", wid, err)
					errsLogged++
				}
			}
		}()
	}
	wg.Wait()
}
