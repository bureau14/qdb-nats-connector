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

	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/internal/logging"
)

// usageStr: CLI help text with all connector options
var usageStr = `
Usage: qdb-nats-connector [options]

Configuration Options:
    --config <file>                  Configuration file path (yaml, json, toml)

NATS JetStream Options:
    -n, --nats <host>:<port>         NATS cluster endpoint (e.g. 10.192.172.166:4222)
    --stream <name>                  JetStream stream name (required)
    -t, --topic <topic>              Topic filter (can be repeated)
    --consumer-prefix <prefix>       Consumer name prefix (default: qdb-connector)
    --batch-size <n>                 Messages per fetch (default: 100)
    --batch-timeout <duration>       Max wait for batch (default: 1s)
    --fetch-timeout <duration>       Total fetch timeout (default: 5s)
    --ack-wait <duration>            Message ACK timeout (default: 30s)
    --max-deliver <n>                Max delivery attempts (default: 3)
    --max-retries <n>                Poison message threshold (default: 3)

QuasarDB Connection Options:
    --qdb qdb://<host>:<port>        QuasarDB cluster endpoint (e.g. qdb://127.0.0.1:2836)
    --qdb-pubkey-file <file>         QuasarDB cluster public key file
    --qdb-user-sec-file <file>       QuasarDB user security file
    --qdb-encryption <type>          QuasarDB sink encryption (none|aes)
    --qdb-compression <type>         QuasarDB sink compression (none|fast|balanced)

Performance Options:
    --qdb-push-mode <mode>           QuasarDB sink push mode (transactional|async|fast)
    --qdb-deduplication-mode <mode>  QuasarDB deduplication mode (disabled|drop|upsert, default: drop)
    --qdb-client-max-parallelism <n> QuasarDB sink max parallelism
    --qdb-client-inbuf-size <size>   QuasarDB sink max input buffer size

General Options:
    -P, --pid <file>                 File to store PID
    -h, --help                       Show this message

Environment Variables:
    All options can be set via environment variables with QDB_NATS_ prefix.
    Examples: QDB_NATS_NATS_ENDPOINT, QDB_NATS_STREAM, QDB_NATS_QDB_CLUSTER_URI

Configuration Files:
    Default locations: ./qdb-nats-connector.yaml, ~/.config/qdb-nats-connector/qdb-nats-connector.yaml
    Example config file structure:
      nats:
        endpoint: "nats://localhost:4222"
        topic: "sensors.>"
      qdb:
        cluster_uri: "qdb://127.0.0.1:2836"
        compression: "best"
        encryption: "none"
        push_mode: "async"
        deduplication_mode: "drop"
`

// usage prints CLI help & exits
// Out: exit(0)
// Ex: usage() → help text printed
func usage() {
	fmt.Println(usageStr)
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

	// 1. Load configuration: CLI overrides env vars, config files override defaults
	opts, err := connector.LoadConfig(os.Args[1:], usage)
	if err != nil {
		slog.Error("Unable to parse options", "error", err)

		return 1
	}

	slog.Info("Parsed configuration options", "options", opts)

	// 2. Create JSON parser: default format handler
	// Create connector (parser is created internally by workers)
	c, err := connector.NewConnector(opts)
	if err != nil {
		slog.Error("Unable to launch NATS connector", "error", err, "connector", c)

		return 1
	}
	defer c.Close()

	slog.Info("Starting connector")

	// Create root context for the application
	ctx := context.Background()

	// 3. Run connector: blocks until shutdown|error
	err = c.RunWithContext(ctx)
	if err != nil && err != context.Canceled {
		slog.Error("Connector failed", "error", err)

		return 1
	}

	return 0
}
