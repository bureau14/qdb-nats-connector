package main

import (
	"flag"
	"fmt"

	log "github.com/sirupsen/logrus"
	"os"

	"github.com/bureau14/qdb-nats-connector/connector"
)

var usageStr = `
Usage: qdb-nats-connector [options]

Server Options:
    -n, --nats <host>:<port>         NATS cluster endpoint (e.g. 10.192.172.166:4222)
    -t, --topic <topic>              Topic to subscribe to
    -P, --pid <file>                 File to store PID
`

func usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

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

	err = connector.ValidateOptions(opts)
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Panic("Configuration validation error")
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
