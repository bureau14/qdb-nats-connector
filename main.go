// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package main: NATS→QuasarDB connector entry point
// Types: none
// Ex: ./qdb-nats-connector -n host:4222 -t topic → connector runs
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/internal/logging"
	"github.com/bureau14/qdb-nats-connector/internal/parser"
)

// usageStr: CLI help text with all connector options
var usageStr = `
Usage: qdb-nats-connector [options]

Configuration Options:
    --config <file>                  Configuration file path (yaml, json, toml)

NATS Options:
    -n, --nats <host>:<port>         NATS cluster endpoint (e.g. 10.192.172.166:4222)
    -t, --topic <topic>              Topic to subscribe to

QuasarDB Connection Options:
    --qdb-cluster qdb://<host>:<port> QuasarDB cluster endpoint (e.g. qdb://127.0.0.1:2836)
    --qdb-cluster-public-key-file <file> QuasarDB cluster public key file
    --qdb-user-security-file <file>  QuasarDB user security file
    --qdb-encryption <type>          QuasarDB sink encryption (none|aes)
    --qdb-compression <type>         QuasarDB sink compression (none|fast|speed|best)

Performance Options:
    --qdb-client-max-parallelism <n> QuasarDB sink max parallelism
    --qdb-client-inbuf-size <size>   QuasarDB sink max input buffer size

General Options:
    -P, --pid <file>                 File to store PID
    -h, --help                       Show this message

Environment Variables:
    All options can be set via environment variables with QDB_NATS_ prefix.
    Examples: QDB_NATS_NATS_ENDPOINT, QDB_NATS_NATS_TOPIC, QDB_NATS_QDB_CLUSTER_URI

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
`

// usage prints CLI help & exits
// Out: exit(0)
// Ex: usage() → help text printed
func usage() {
	fmt.Println(usageStr)
	os.Exit(0)
}

// main runs NATS→QDB connector
// Approach: parse CLI→create parser→run connector
// 1. Parse options - CLI>config precedence
// 2. Create JSON parser - default parser
// 3. Run connector - blocks until shutdown
// Ex: main() → connector runs until SIGINT
func main() {
	exe := "qdb-nats-connector"

	// Setup structured logging
	logging.SetupDefault("exe", exe)

	// 1. Load configuration: CLI overrides env vars, config files override defaults
	opts, err := connector.LoadConfig(os.Args[1:], usage)

	if err != nil {
		slog.Error("Unable to parse options", "error", err)
		os.Exit(1)
	}

	slog.Info("Parsed configuration options", "options", opts)

	// 2. Create JSON parser: default format handler
	jsonParser, err := parser.NewJsonParser()
	if err != nil {
		slog.Error("Unable to create JSON parser", "error", err)
		os.Exit(1)
	}

	// Create connector with JSON parser
	c, err := connector.NewConnector(opts, jsonParser)
	if err != nil {
		slog.Error("Unable to launch NATS connector", "error", err, "connector", c)
		os.Exit(1)
	}
	defer c.Close()

	slog.Info("Starting connector")

	// Create root context for the application
	ctx := context.Background()

	// 3. Run connector: blocks until shutdown|error
	if err := c.RunWithContext(ctx); err != nil && err != context.Canceled {
		slog.Error("Connector failed", "error", err)
		os.Exit(1)
	}
}
