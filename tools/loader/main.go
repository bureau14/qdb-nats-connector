// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Package main: qdb-data-loader CLI - loads data files into NATS JetStream.
// Types: none
// Ex: qdb-data-loader --file data.jsonl --topic sensor.data --stream DATA_STREAM
package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/logging"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal/batch"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal/detector"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal/parser"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal/publisher"
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

// usageStr: CLI help text with all loader options
var usageStr = `
Usage: qdb-data-loader [options]

NOTICE: This is a stress testing tool with NO BACKPRESSURE.
        Designed to find system limits through failure.
        Loader failing under load is a valid test result!

Options:
    --file <path>                    Input file or "-" for stdin
    --topic <topic>                  NATS topic (required)
    --stream <stream>                JetStream stream name (required)
    --batch-size <n>                 Messages per batch (default: 100)
    --batch-timeout <duration>       Max wait for batch (default: 100ms)
    --nats-url <url>                 NATS server URL (default: nats://localhost:4222)
    --workers <n>                    Number of parallel workers (default: 4)
    
    --adaptive                       Enable adaptive batching
    --min-batch-size <n>             Minimum batch size for adaptive mode (default: 10)
    --max-batch-size <n>             Maximum batch size for adaptive mode (default: 1000)
    --target-rate <n>                Target throughput in messages/sec (default: 10000)
    
    -h, --help                       Show this message
    -v, --version                    Show version information

Examples:
    qdb-data-loader --file data.jsonl --topic sensor.data --stream DATA_STREAM
    qdb-data-loader --file metrics.parquet --topic metrics --stream METRICS --workers 8
    cat data.json | qdb-data-loader --file - --topic events --stream EVENTS --adaptive

Note: Backpressure parameters (--max-queue-depth, --circuit-*) are deprecated and ignored.
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
	fmt.Printf("quasardb data loader version: %s\n", version)
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

// getFormatName returns a human-readable name for the format
func getFormatName(format int) string {
	switch format {
	case internal.FormatJSONLines:
		return "JSON Lines"
	case internal.FormatParquet:
		return "Parquet"
	case internal.FormatGzipJSON:
		return "Gzipped JSON"
	case internal.FormatBase64:
		return "Base64"
	default:
		return "Unknown"
	}
}

// main starts the data loader process.
// In: command-line args (flags for data loading config)
// Out: loads data into NATS JetStream
// Ex: main() → loads data until completion or SIGINT
func main() {
	exitCode := runMain()
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

// runMain executes loader with error handling
// In: none (reads os.Args)
// Out: exit code (0=success, 1=error)
// Ex: runMain() → 0 on successful load
func runMain() int {
	exe := "qdb-data-loader"

	// Setup structured logging
	logging.SetupDefault("exe", exe)

	// Define flags
	fs := pflag.NewFlagSet("qdb-data-loader", pflag.ExitOnError)

	var (
		showHelp            bool
		showVersionFlag     bool
		file                string
		topic               string
		stream              string
		batchSize           int
		batchTimeout        time.Duration
		natsURL             string
		workers             int
		adaptive            bool
		minBatchSize        int
		maxBatchSize        int
		targetRate          int
		maxQueueDepth       int
		circuitMaxFailures  int
		circuitResetTimeout time.Duration
	)

	// Define CLI flags
	fs.BoolVarP(&showHelp, "help", "h", false, "Show this message")
	fs.BoolVarP(&showVersionFlag, "version", "v", false, "Show version information")
	fs.StringVar(&file, "file", "", "Input file or \"-\" for stdin")
	fs.StringVar(&topic, "topic", "", "NATS topic (required)")
	fs.StringVar(&stream, "stream", "", "JetStream stream name (required)")
	fs.IntVar(&batchSize, "batch-size", 100, "Messages per batch (fixed mode)")
	fs.DurationVar(&batchTimeout, "batch-timeout", 100*time.Millisecond, "Max wait for batch")
	fs.StringVar(&natsURL, "nats-url", "nats://localhost:4222", "NATS server URL")
	fs.IntVar(&workers, "workers", 4, "Number of parallel workers for publishing")

	// Adaptive batching flags
	fs.BoolVar(&adaptive, "adaptive", false, "Enable adaptive batching")
	fs.IntVar(&minBatchSize, "min-batch-size", 10, "Minimum batch size (adaptive mode)")
	fs.IntVar(&maxBatchSize, "max-batch-size", 1000, "Maximum batch size (adaptive mode)")
	fs.IntVar(&targetRate, "target-rate", 10000, "Target throughput in messages/sec (adaptive mode)")

	// Deprecated backpressure flags - kept for compatibility but ignored
	fs.IntVar(&maxQueueDepth, "max-queue-depth", 100, "DEPRECATED - ignored")
	fs.IntVar(&circuitMaxFailures, "circuit-max-failures", 5, "DEPRECATED - ignored")
	fs.DurationVar(&circuitResetTimeout, "circuit-reset-timeout", 10*time.Second, "DEPRECATED - ignored")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		slog.Error("Error parsing flags", "error", err)

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

	// Validate required flags
	if topic == "" {
		slog.Error("topic flag is required")
		usage()

		return 1
	}

	if stream == "" {
		slog.Error("stream flag is required")
		usage()

		return 1
	}

	// Execute the loading logic
	err = runLoader(file, topic, stream, batchSize, batchTimeout, natsURL, workers,
		adaptive, minBatchSize, maxBatchSize, targetRate, maxQueueDepth,
		circuitMaxFailures, circuitResetTimeout)
	if err != nil {
		slog.Error("Loading failed", "error", err)

		return 1
	}

	return 0
}

// runLoader contains the main loading logic extracted from cobra command
func runLoader(file, topic, stream string, batchSize int, batchTimeout time.Duration,
	natsURL string, workers int, adaptive bool, minBatchSize, maxBatchSize, targetRate,
	maxQueueDepth, circuitMaxFailures int, circuitResetTimeout time.Duration,
) error {
	// Create context with cancellation for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Get flag values and create NATS connection with signal context
	connection := internal.NewConnection(natsURL)
	err := connection.Connect(ctx)
	if err != nil {
		return connectorErrors.NewConnectionFailedError("NATS", natsURL, err)
	}
	defer connection.Close()

	// Input detection logic
	var input *os.File
	if file == "" || file == "-" {
		input = os.Stdin
	} else {
		input, err = os.Open(file) // #nosec G304 - file path from CLI flag
		if err != nil {
			return connectorErrors.NewInvalidConfigError("loader", "failed to open file: "+err.Error())
		}
		defer func() {
			closeErr := input.Close()
			if closeErr != nil {
				slog.Error("Failed to close input file", "error", closeErr)
			}
		}()
	}

	// Create buffered reader for format detection
	reader := bufio.NewReader(input)

	// Detect format
	formatDetector := detector.NewDetector()
	format, err := formatDetector.Detect(reader)
	if err != nil {
		return err
	}

	// Log detected format
	slog.Info("Format detected", "format", getFormatName(format))

	// Get appropriate parser
	dataParser, err := parser.GetParser(format)
	if err != nil {
		return err
	}

	// Parse data using channels
	messageChannel, errorChannel := dataParser.Parse(reader)

	var messageCount int
	shutdownRequested := false

	// Create done channel and WaitGroup for goroutines
	done := make(chan error, 1)
	var wg sync.WaitGroup
	var pub *publisher.Publisher

	// Create input channel for batcher (now handles Message types)
	batchInput := make(chan internal.Message, 100)

	// Create batcher (adaptive or regular based on flag)
	var batcherInterface interface {
		Start(context.Context) error
		GetMetrics() batch.Metrics
	}
	if adaptive {
		adaptiveBatcher, batchOutput := batch.NewAdaptiveBatcher(minBatchSize, maxBatchSize, targetRate, batchTimeout, batchInput)
		batcherInterface = adaptiveBatcher

		// Log adaptive configuration
		slog.Info("Using adaptive batching",
			"min_size", minBatchSize,
			"max_size", maxBatchSize,
			"target_rate", targetRate,
			"timeout", batchTimeout)

		// Create publisher with adaptive batcher output
		pub = publisher.NewPublisherWithBackpressure(connection, workers, batchOutput, topic, maxQueueDepth, circuitMaxFailures, circuitResetTimeout)
	} else {
		regularBatcher, batchOutput := batch.NewBatcher(batchSize, batchTimeout, batchInput)
		batcherInterface = regularBatcher

		// Log regular configuration
		slog.Info("Using fixed batching", "size", batchSize, "timeout", batchTimeout)

		// Create publisher with regular batcher output
		pub = publisher.NewPublisherWithBackpressure(connection, workers, batchOutput, topic, maxQueueDepth, circuitMaxFailures, circuitResetTimeout)
	}

	// Start batcher
	err = batcherInterface.Start(ctx)
	if err != nil {
		return connectorErrors.NewInvalidConfigError("loader", "failed to start batcher: "+err.Error())
	}

	// Start goroutine to handle messages from parser and send to batcher
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(batchInput)
		for message := range messageChannel {
			// Check if shutdown was requested
			if shutdownRequested {
				break
			}

			// Send complete message to batcher (including tombstone)
			select {
			case batchInput <- message:
				// Message sent to batcher
			case <-ctx.Done():
				return
			}
		}
		slog.Info("Parser completed, all messages sent to batcher")
	}()

	// Start publisher
	err = pub.Start(ctx)
	if err != nil {
		return connectorErrors.NewInvalidConfigError("loader", "failed to start publisher: "+err.Error())
	}

	// Start goroutine for completion tracking and metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		pubTicker := time.NewTicker(1 * time.Second)
		defer pubTicker.Stop()

		var metricsTicker *time.Ticker
		if adaptive {
			metricsTicker = time.NewTicker(10 * time.Second)
			defer metricsTicker.Stop()
		}

		var lastCount int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-pub.GetCompletionChannel():
				// All workers have completed after processing tombstone
				messages, batches := pub.GetMetrics()
				slog.Info("All messages processed via tombstone signal", "total_messages", messages, "total_batches", batches)

				return
			case <-pubTicker.C:
				if shutdownRequested {
					return
				}

				messages, batches := pub.GetMetrics()
				if messages != lastCount {
					messageCount = int(messages)
					if messageCount%1000 == 0 {
						slog.Info("Published messages", "count", messageCount, "topic", topic, "batches", batches, "workers", workers)
					}
					lastCount = messages
				}
			case <-func() <-chan time.Time {
				if metricsTicker != nil {
					return metricsTicker.C
				}

				return nil
			}():
				if shutdownRequested {
					return
				}

				// Log adaptive batch metrics
				batchMetrics := batcherInterface.GetMetrics()
				slog.Info("Adaptive batch metrics",
					"current_batch_size", batchMetrics.CurrentBatchSize,
					"throughput", batchMetrics.Throughput,
					"avg_batch_size", batchMetrics.AverageBatchSize,
					"messages_processed", batchMetrics.MessagesProcessed,
					"batches_created", batchMetrics.BatchesCreated)
			}
		}
	}()

	// Start goroutine to handle errors
	wg.Add(1)
	go func() {
		defer wg.Done()
		for parseErr := range errorChannel {
			// Log parse errors as warnings, don't fail
			slog.Warn("Parse error encountered", "error", parseErr)
		}
	}()

	// Start goroutine to wait for completion
	go func() {
		wg.Wait()
		done <- nil
	}()

	// Handle completion or shutdown
	select {
	case err := <-done:
		// Normal completion
		if err != nil {
			return err
		}
	case <-ctx.Done():
		// Shutdown signal received
		slog.Info("Shutdown signal received, stopping...")
		shutdownRequested = true

		// Wait for processing to complete with timeout
		shutdownTimeout := time.NewTimer(5 * time.Second)
		defer shutdownTimeout.Stop()

		select {
		case err := <-done:
			if err != nil {
				slog.Error("Error during shutdown", "error", err)
			}
		case <-shutdownTimeout.C:
			slog.Warn("Shutdown timeout exceeded, forcing exit")
		}
	}

	// Get final metrics from publisher
	messages, batches := pub.GetMetrics()
	circuitTrips, backpressureEvents := pub.GetBackpressureMetrics()
	messageCount = int(messages)

	// Log summary with backpressure statistics
	slog.Info("Data loading completed",
		"total_messages", messageCount,
		"total_batches", batches,
		"workers", workers,
		"format", getFormatName(format),
		"topic", topic,
		"stream", stream,
		"circuit_trips", circuitTrips,
		"backpressure_events", backpressureEvents)

	return nil
}
