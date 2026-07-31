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
		warmCtx, cancel := context.WithTimeout(ctx, r.Warmup)
		r.runPhase(warmCtx, false)
		cancel()
	}

	r.Recorder.Start()
	runCtx, cancel := context.WithTimeout(ctx, r.Duration)
	defer cancel()
	r.runPhase(runCtx, true)
	r.Recorder.Stop()
}

func (r *Runner) runPhase(ctx context.Context, record bool) {
	var wg sync.WaitGroup
	wg.Add(r.Concurrency)
	for i := 0; i < r.Concurrency; i++ {
		wid := i
		go func() {
			defer wg.Done()
			errsLogged := 0
			for {
				select {
				case <-ctx.Done():
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
