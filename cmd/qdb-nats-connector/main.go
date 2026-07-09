// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Package main: NATS→QuasarDB connector CLI.
// Types: none
// Ex: qdb-nats-connector --nats nats://localhost:4222
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/internal/logging"
	"github.com/bureau14/qdb-nats-connector/internal/metrics"
	"github.com/bureau14/qdb-nats-connector/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
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

// usageStr: CLI help text with all connector options
var usageStr = `
Usage: qdb-nats-connector [options]

NATS JetStream Options:
    -n, --nats <host>:<port>         NATS cluster endpoint (e.g. 10.192.172.166:4222)
    --stream <name>                  JetStream stream name (required)
    --consumer <name>                Consumer name (default: qdb-connector)
    --nats-creds-file <file>         NATS credentials (.creds) file for JWT auth
    --nats-ca-file <file>            CA certificate file for NATS TLS
    -w, --workers <n>                Number of concurrent workers (default: 4)
    --batch-size <n>                 Messages per fetch (default: 100)
    --batch-timeout <duration>       Max wait for batch (default: 1s)
    --fetch-timeout <duration>       Total fetch timeout (default: 5s)
    --max-retries <n>                Poison message threshold (default: 3)

QuasarDB Connection Options:
    --qdb qdb://<host>:<port>        QuasarDB cluster endpoint (e.g. qdb://127.0.0.1:2836)
    --qdb-pubkey-file <file>         QuasarDB cluster public key file
    --qdb-user-sec-file <file>       QuasarDB user security file
    --qdb-encryption <type>          QuasarDB sink encryption (none|aes)
    --qdb-compression <type>         QuasarDB sink compression (none|balanced)

Performance Options:
    --qdb-push-mode <mode>           QuasarDB sink push mode (transactional|async|fast)
    --qdb-deduplication-mode <mode>  QuasarDB deduplication mode (disabled|drop|upsert, default: drop)
    --qdb-client-max-parallelism <n> QuasarDB sink max parallelism
    --qdb-client-inbuf-size <size>   QuasarDB sink max input buffer size

General Options:
    -P, --pid <file>                 File to store PID
    -v, --version                    Show version information
    -h, --help                       Show this message

Environment Variables:
    All options can be set via environment variables with QDB_NATS_ prefix.
    Examples: QDB_NATS_NATS, QDB_NATS_STREAM, QDB_NATS_CONSUMER, QDB_NATS_WORKERS
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
	fmt.Printf("quasardb nats connector version: %s\n", version)
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

// main starts the NATS→QuasarDB connector process.
// In: command-line args (flags for NATS/QDB config)
// Out: runs connector until shutdown signal
// Ex: main() → connector processes messages until SIGINT
func main() {
	exitCode := runMain()
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

// runMain executes connector with error handling
// In: none (reads os.Args)
// Out: exit code (0=success, 1=error)
// Ex: runMain() → 0 on clean shutdown
func runMain() int {
	exe := "qdb-nats-connector"

	// Setup structured logging
	logging.SetupDefault("exe", exe)

	// Check for --version flag early
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-v" {
			showVersion()
		}
	}

	// 1. Load configuration: CLI overrides env vars
	opts, err := connector.LoadConfig(os.Args[1:], usage)
	if err != nil {
		slog.Error("Unable to parse options", "error", err)

		return 1
	}

	slog.Info("Parsed configuration options", "options", opts)

	// 2. Build the metrics registry and inject the hot-path histograms
	// into Options before the connector (and its workers) exist.
	reg, err := buildMetrics(opts)
	if err != nil {
		slog.Error("Unable to build metrics registry", "error", err)

		return 1
	}

	// 3. Build the console stats logger and inject its per-table event
	// callback -- also before the connector, which wires workers at
	// construction.
	stats := buildStats(opts)

	// Create connector (parser is created internally by workers)
	c, err := connector.NewConnector(opts)
	if err != nil {
		slog.Error("Unable to launch NATS connector", "error", err, "connector", c)

		return 1
	}
	defer c.Close()

	// 4. Serve probes (/livez, /readyz) + /metrics around the connector
	tel, err := startTelemetry(opts.HTTPAddr, c, reg)
	if err != nil {
		slog.Error("Unable to start telemetry server", "error", err)

		return 1
	}
	startStats(stats, c)

	slog.Info("Starting connector")

	// Create root context for the application
	ctx := context.Background()

	// 5. Run connector: blocks until shutdown|error
	err = c.RunWithContext(ctx)
	stopStats(stats)
	stopTelemetry(tel)
	if err != nil && err != context.Canceled {
		slog.Error("Connector failed", "error", err)

		return 1
	}

	return 0
}

// buildMetrics creates the registry, hot-path histograms, and build-info
// gauge; nil registry when telemetry is fully disabled (opts.Metrics
// stays nil and every observation is a no-op). Console stats read the
// same lag histogram, so an enabled stats interval keeps the metrics
// alive even with the HTTP server off; SERVING stays gated on HTTPAddr.
// In: opts *connector.Options - HTTPAddr/StatsInterval gates + bucket
// config; Metrics field is set as a side effect
// Out: *prometheus.Registry, error - registry for /metrics, nil when off
// Ex: reg, _ := buildMetrics(opts) → opts.Metrics wired
func buildMetrics(opts *connector.Options) (*prometheus.Registry, error) {
	if opts.HTTPAddr == "" && opts.StatsInterval <= 0 {
		return nil, nil
	}

	reg := prometheus.NewRegistry()

	m, err := metrics.New(reg, opts.MetricsConfig())
	if err != nil {
		return nil, err
	}
	opts.Metrics = m

	err = metrics.RegisterBuildInfo(reg, version, commit)
	if err != nil {
		return nil, err
	}

	return reg, nil
}

// startTelemetry binds and serves the probe + metrics server; nil when
// disabled. The collector reads the connector at scrape time.
// In: addr string - listen address ("" = disabled), c - probe + stats
// source, reg *prometheus.Registry - /metrics registry (nil = no route)
// Out: *telemetry.Server, error - running server, nil/nil when disabled
// Ex: startTelemetry(":9100", c, reg) → srv, nil
func startTelemetry(addr string, c *connector.Connector, reg *prometheus.Registry) (*telemetry.Server, error) {
	if addr == "" {
		return nil, nil
	}

	var gatherer prometheus.Gatherer
	if reg != nil {
		err := reg.Register(telemetry.NewCollector(c, slog.Default()))
		if err != nil {
			return nil, err
		}
		gatherer = reg
	}

	srv, err := telemetry.NewServer(addr, c, gatherer, slog.Default())
	if err != nil {
		return nil, err
	}
	srv.Start()
	slog.Info("Telemetry server listening", "addr", srv.Addr())

	return srv, nil
}

// buildStats constructs the console stats logger and injects the
// per-table event callback into opts BEFORE the connector (and its
// workers) exist; nil when --stats-interval is 0.
// In: opts *connector.Options - StatsInterval gate + Metrics source;
// OnTableWrites is set as a side effect
// Out: *telemetry.StatsLogger - unstarted logger, nil when disabled
// Ex: stats := buildStats(opts) → opts.OnTableWrites wired
func buildStats(opts *connector.Options) *telemetry.StatsLogger {
	if opts.StatsInterval <= 0 {
		return nil
	}

	sl := telemetry.NewStatsLogger(opts.Metrics, opts.StatsInterval, slog.Default())
	opts.OnTableWrites = sl.Record
	slog.Info("Console stats enabled", "interval", opts.StatsInterval)

	return sl
}

// startStats launches the stats tick goroutine once the connector
// exists (nil-safe).
// In: sl *telemetry.StatsLogger - logger or nil, c - stats source
// Out: none
// Ex: startStats(stats, c) → one stats line per interval
func startStats(sl *telemetry.StatsLogger, c *connector.Connector) {
	if sl == nil {
		return
	}

	sl.Start(c)
}

// stopStats stops the stats tick goroutine (nil-safe).
// In: sl *telemetry.StatsLogger - running logger or nil
// Out: none
// Ex: stopStats(stats) → tick goroutine exited
func stopStats(sl *telemetry.StatsLogger) {
	if sl == nil {
		return
	}

	sl.Stop()
}

// stopTelemetry gracefully stops the probe server (nil-safe).
// In: srv *telemetry.Server - running server or nil
// Out: none - shutdown failures are logged, never fatal
// Ex: stopTelemetry(srv) → probe listener closed
func stopTelemetry(srv *telemetry.Server) {
	if srv == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := srv.Shutdown(ctx)
	if err != nil {
		slog.Warn("Telemetry shutdown failed", "error", err)
	}
}
