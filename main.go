// Package main provides the entry point for the qdb-nats-connector application.
// This connector bridges NATS messaging with QuasarDB time series storage,
// enabling real-time data ingestion from NATS topics into QuasarDB tables.
package main

import (
	"flag"
	"fmt"

	"os"

	log "github.com/sirupsen/logrus"

	"github.com/bureau14/qdb-nats-connector/connector"
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
	fmt.Printf("%s\n", usageStr)
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

	// Create a FlagSet and sets the usage
	fs := flag.NewFlagSet(exe, flag.ExitOnError)
	fs.Usage = usage

	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)

	// Configure the options from the flags/config file
	opts, err := connector.ConfigureOptions(fs, os.Args[1:], fs.Usage)

	if err != nil {
		log.WithFields(log.Fields{"err": err}).Panic("Unable to parse options")
	}

	log.WithFields(log.Fields{"options": opts}).Info("Parsed configuration options")

	c, err := connector.NewConnector(opts)

	if err != nil {
		log.WithFields(log.Fields{"err": err, "connector": c}).Panic("Unable to launch NATS connector")
	}
	defer c.Close()

	log.WithFields(log.Fields{"err": err}).Debug("Connected to NATS, invoking nc.Subscribe()")

	// Simple Async Subscriber
	// nc.Subscribe("foo", func(m *nats.Msg) {
	// 	fmt.Printf("Received a message: %s\n", string(m.Data))
	// })

	// log.Debug("Invoked subscribe")

	// log.Debug("Draining")
	// nc.Drain()

	// log.Debug("Closing")
	// nc.Close()
}
