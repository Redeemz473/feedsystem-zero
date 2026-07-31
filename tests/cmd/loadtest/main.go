// Command loadtest drives one of the scenarios in tests/internal/scenario
// against a running gateway, then prints an aligned metrics summary.
//
// Usage:
//
//	go run ./tests/cmd/loadtest -scenario like -c 100 -d 60s
//	go run ./tests/cmd/loadtest -scenario hot_feed -c 500 -d 30s
//
// Scenarios are registered in scenario.Registry; run with `-scenario ?` to
// print the list.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"feedsystem-zero/tests/internal/loadgen"
	"feedsystem-zero/tests/internal/metrics"
	"feedsystem-zero/tests/internal/scenario"
	"feedsystem-zero/tests/internal/testconfig"
)

func main() {
	var flags testconfig.LoadTestFlags
	fs := flag.NewFlagSet("loadtest", flag.ExitOnError)
	testconfig.RegisterLoadTestFlags(fs, &flags)
	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Fatalf("parse flags: %v", err)
	}

	if flags.Scenario == "?" || flags.Scenario == "list" {
		printScenarios()
		return
	}

	ctor, ok := scenario.Registry[flags.Scenario]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", flags.Scenario)
		printScenarios()
		os.Exit(2)
	}
	scn := ctor()

	// Ctrl+C stops the run early but still prints a summary.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("[loadtest] scenario=%s concurrency=%d duration=%s warmup=%s",
		flags.Scenario, flags.Concurrency, flags.Duration, flags.Warmup)
	if err := scn.Setup(ctx, flags); err != nil {
		log.Fatalf("setup: %v", err)
	}

	// Pre-size the recorder based on a very rough QPS guess so we don't
	// reallocate too often. If we're way off, the slice just grows.
	expected := flags.Concurrency * int(flags.Duration.Seconds()) * 200
	recorder := metrics.NewRecorder(expected)

	runner := &loadgen.Runner{
		Concurrency: flags.Concurrency,
		Duration:    flags.Duration,
		Warmup:      flags.Warmup,
		Recorder:    recorder,
		Op:          scn.Op(),
		Verbose:     flags.Verbose,
	}
	runner.Run(ctx)

	summary := recorder.Compute(scn.Name(), flags.Concurrency)
	fmt.Println()
	fmt.Print(summary.Format())
}

func printScenarios() {
	fmt.Fprintln(os.Stderr, "available scenarios:")
	names := make([]string, 0, len(scenario.Registry))
	for k := range scenario.Registry {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(os.Stderr, "  %s\n", n)
	}
}
