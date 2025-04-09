package connector

import (
	"flag"

	"github.com/nats-io/nats.go"
)

type Options struct {
	NatsEndpoint string `json:"nats"`
	NatsTopic    string `json:"topic"`
	PidFile      string `json:"pid"`
}

func ConfigureOptions(fs *flag.FlagSet, args []string, printHelp func()) (*Options, error) {
	opts := &Options{}
	var (
		showHelp bool
	)

	fs.BoolVar(&showHelp, "h", false, "Show this message.")
	fs.BoolVar(&showHelp, "help", false, "Show this message.")

	fs.StringVar(&opts.NatsEndpoint, "n", nats.DefaultURL, "NATS cluster endpoint (e.g. 10.192.172.166:4222)")
	fs.StringVar(&opts.NatsEndpoint, "nats", nats.DefaultURL, "NATS cluster endpoint (e.g. 10.192.172.166:4222)")

	fs.StringVar(&opts.NatsTopic, "t", "", "Topic to subscribe to.")
	fs.StringVar(&opts.NatsTopic, "topic", "", "Topic to subscribe to.")

	fs.StringVar(&opts.NatsTopic, "P", "", "File to store PID.")
	fs.StringVar(&opts.NatsTopic, "pid", "", "File to store PID.")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if showHelp {
		printHelp()
		return nil, nil
	}

	return opts, nil
}

func ValidateOptions(opts *Options) error {
	if opts.NatsTopic == "" {
		return ErrNoTopicProvided
	}

	return nil
}
