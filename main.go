// Package main provides the entry point for the qdb-nats-connector application.
// This connector bridges NATS messaging with QuasarDB time series storage,
// enabling real-time data ingestion from NATS topics into QuasarDB tables.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/internal/logging"
	"github.com/bureau14/qdb-nats-connector/internal/parser"
)

var usageStr = `
Usage: qdb-nats-connector [options]

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
`

// usage prints the CLI usage instructions and exits with status code 0.
// Decision rationale:
// - Exit code 0 indicates help was requested, not an error
// - Direct printf to stdout for standard help display convention
func usage() {
	fmt.Println(usageStr)
	os.Exit(0)
}

// main initializes and runs the NATS to QuasarDB connector.
// Key assumptions:
// - CLI arguments take precedence over any config files
// - Panics on critical errors as this is a long-running service
// - Connector manages its own lifecycle including reconnection logic
// Decision rationale:
// - Uses panic for unrecoverable startup errors (config parsing, connector creation)
// - Sets debug logging by default for troubleshooting production issues
// - Defers connector.Close() to ensure clean shutdown on any exit path
func main() {
	exe := "qdb-nats-connector"

	// Setup structured logging
	logging.SetupDefault("exe", exe)

	// Create a FlagSet and sets the usage
	fs := flag.NewFlagSet(exe, flag.ExitOnError)
	fs.Usage = usage

	// Configure the options from the flags/config file
	opts, err := connector.ConfigureOptions(fs, os.Args[1:], fs.Usage)

	if err != nil {
		slog.Error("Unable to parse options", "error", err)
		os.Exit(1)
	}

	slog.Info("Parsed configuration options", "options", opts)

	// Create JSON parser
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

	// Run the connector with context - this blocks until error or shutdown
	if err := c.RunWithContext(ctx); err != nil && err != context.Canceled {
		slog.Error("Connector failed", "error", err)
		os.Exit(1)
	}
}
