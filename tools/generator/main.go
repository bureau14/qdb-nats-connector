// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Package main: qdb-data-gen CLI - test data generator for QuasarDB NATS connector.
// Types: none
// Ex: qdb-data-gen template.yaml --count 10000
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator/continuous"
	_ "github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator/generators"
	"github.com/spf13/pflag"
)

// Version information - populated by ldflags at build time
var (
	version       = "3.15.0.dev0"
	commit        = "unknown"
	buildTime     = "unknown"
	buildMode     = "unknown"
	goamd64       = "v3"
	kernelVersion = "unknown"
)

// usageStr: CLI help text with all generator options
var usageStr = `
Usage: qdb-data-gen <template-file> [options]

Arguments:
    <template-file>                  Path to data generation template file

Options:
    --count <n>                      Number of records to generate (default: 1000)
    --mode <mode>                    Generation mode: batch or continuous (default: batch)
    --rate <rate>                    Messages per second (for continuous mode, default: 10000)
    --duration <duration>            Duration to run (0 = indefinite, e.g., 5m, 1h)
    -v, --version                    Show version information
    -h, --help                       Show this message

Examples:
    qdb-data-gen finance-template.yaml --count 10000
    qdb-data-gen sensor-template.yaml --mode continuous --rate 5000
    qdb-data-gen metrics.yaml --mode continuous --rate 1000 --duration 5m
`

// usage prints CLI help & exits
// Out: exit(0)
// Ex: usage() → help text printed
func usage() {
	fmt.Println(usageStr)
	os.Exit(0)
}

// showVersion prints version information & exits
// Out: exit(0)
// Ex: showVersion() → version info printed
func showVersion() {
	fmt.Printf("quasardb data generator version: %s\n", version)
	fmt.Printf("build: %s\n", commit)
	fmt.Printf("date: %s\n\n", buildTime)

	fmt.Printf("target: %s-%s-%s\n", runtime.GOARCH, runtime.GOOS, kernelVersion)
	fmt.Printf("compiler: %s\n", runtime.Version())

	// Only show arch level for amd64
	if runtime.GOARCH == "amd64" && goamd64 != "" {
		fmt.Printf("arch level: %s\n", goamd64)
	}

	fmt.Printf("\nbuild type: %s\n\n", buildMode)
	fmt.Println("Copyright (c) 2009-2025, quasardb SAS. All rights reserved.")
	os.Exit(0)
}

// Generate creates JSON records according to template specifications
// In: templateFile path, mode, count, rate, duration, writer for output
// Out: error if generation fails
// Ex: Generate("template.yaml", "batch", 1000, 0, 0, writer) → generates 1000 JSON records
func Generate(templateFile, mode string, count int, rate float64, duration time.Duration, writer io.Writer) error {
	// Create a new generation engine using the template file
	engine, err := generator.NewEngine(templateFile)
	if err != nil {
		return fmt.Errorf("failed to create generation engine from template %s: %w", templateFile, err)
	}

	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		slog.Info("received shutdown signal, stopping generation...")
		cancel()
	}()

	switch mode {
	case "continuous":
		// Extract template and generators from engine (we'll need to export these from Engine)
		continuousEngine := continuous.NewEngine(engine.GetTemplate(), engine.GetGenerators(), rate, duration)

		return continuousEngine.Run(ctx, writer)

	case "batch":
		// Original batch generation
		return engine.GenerateRecords(ctx, count, writer)

	default:
		return fmt.Errorf("unknown generation mode: %s", mode)
	}
}

// main starts the data generator process.
// In: command-line args (positional template file + flags)
// Out: generates test data according to template
// Ex: main() → generates data and outputs to stdout or file
func main() {
	exitCode := runMain()
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

// runMain executes generator with error handling
// In: none (reads os.Args)
// Out: exit code (0=success, 1=error)
// Ex: runMain() → 0 on successful generation
func runMain() int {
	fs := pflag.NewFlagSet("qdb-data-gen", pflag.ExitOnError)

	var showHelp bool
	var showVersionFlag bool
	var count int
	var mode string
	var rate float64
	var duration time.Duration

	// Define CLI flags
	fs.BoolVarP(&showHelp, "help", "h", false, "Show this message")
	fs.BoolVarP(&showVersionFlag, "version", "v", false, "Show version information")
	fs.IntVar(&count, "count", 1000, "Number of records to generate")
	fs.StringVar(&mode, "mode", "batch", "Generation mode")
	fs.Float64Var(&rate, "rate", 10000, "Messages per second (for continuous mode)")
	fs.DurationVar(&duration, "duration", 0, "Duration to run (0 = indefinite, e.g., 5m, 1h)")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)

		return 1
	}

	if showHelp {
		usage()

		return 0
	}

	if showVersionFlag {
		showVersion()

		return 0
	}

	// Get positional arguments (non-flag arguments)
	args := fs.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Error: template file path is required\n")
		usage()

		return 1
	}

	templateFile := args[0]

	// Create buffered writer for efficient stdout writing
	writer := bufio.NewWriter(os.Stdout)

	// Generate data using the extracted Generate function
	err = Generate(templateFile, mode, count, rate, duration, writer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating data: %v\n", err)

		return 1
	}

	// Flush buffered writer before returning
	err = writer.Flush()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error flushing stdout: %v\n", err)

		return 1
	}

	return 0
}
