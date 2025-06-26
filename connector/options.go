// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: NATS→QuasarDB pipeline orchestrator
// Types: Connector, Options
// Ex: connector.NewConnector(opts, parser).Run() → data flows
package connector

import (
	"encoding/json"
	"flag"
	"fmt"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/nats-io/nats.go"
)

// Options: connector config aggregating NATS & QDB settings
type Options struct {
	PidFile string `json:"pid"`

	// sourceOptions contains NATS connection settings including endpoint and topic.
	sourceOptions source.Options

	// sinkOptions contains QuasarDB connection and performance settings.
	sinkOptions sink.Options
}

var (
	_ sink.OptionsProvider   = (*Options)(nil)
	_ source.OptionsProvider = (*Options)(nil)
)

// ConfigureOptions parses CLI args→Options.
// Args:
//
//	fs: *flag.FlagSet - CLI flag parser
//	args: []string - command line arguments
//	printHelp: func() - help display callback
//
// Returns:
//
//	*Options: parsed config (nil if help shown)
//	error: parsing/validation fails
//
// Example:
//
//	ConfigureOptions(fs, os.Args[1:], usage) // → opts, nil
func ConfigureOptions(fs *flag.FlagSet, args []string, printHelp func()) (*Options, error) {
	opts := &Options{}
	var (
		showHelp bool
	)

	fs.BoolVar(&showHelp, "h", false, "Show this message.")
	fs.BoolVar(&showHelp, "help", false, "Show this message.")

	fs.StringVar(&opts.sourceOptions.Endpoint, "n", nats.DefaultURL, "NATS cluster endpoint (e.g. 10.192.172.166:4222)")
	fs.StringVar(&opts.sourceOptions.Endpoint, "nats", nats.DefaultURL, "NATS cluster endpoint (e.g. 10.192.172.166:4222)")

	fs.StringVar(&opts.sourceOptions.Topic, "t", "", "Topic to subscribe to.")
	fs.StringVar(&opts.sourceOptions.Topic, "topic", "", "Topic to subscribe to.")

	fs.StringVar(&opts.PidFile, "P", "", "File to store PID.")
	fs.StringVar(&opts.PidFile, "pid", "", "File to store PID.")

	// QuasarDB connection flags
	fs.StringVar(&opts.sinkOptions.ClusterUri, "qdb", "", "QuasarDB cluster endpoint (e.g. qdb://127.0.0.1:2836)")
	fs.StringVar(&opts.sinkOptions.ClusterPublicKeyFile, "qdb-pubkey-file", "", "QuasarDB cluster public key file")
	fs.StringVar(&opts.sinkOptions.UserSecurityFile, "qdb-user-sec-file", "", "QuasarDB user security file")

	// Compression (custom flag.Value)
	fs.Var(&compressionFlag{dst: &opts.sinkOptions.Compression}, "qdb-compression", "QuasarDB sink compression (none|best|speed)")
	// Encryption (custom flag.Value)
	fs.Var(&encryptionFlag{dst: &opts.sinkOptions.Encryption}, "qdb-encryption", "QuasarDB sink encryption (none|aes)")

	// Performance-tuning flags
	var pm uint
	fs.UintVar(&pm, "qdb-client-max-parallelism", 0, "QuasarDB sink max parallelism")
	opts.sinkOptions.ClientMaxParallelism = &pm

	var ib uint
	fs.UintVar(&ib, "qdb-client-inbuf-size", 0, "QuasarDB sink max input buffer size")
	opts.sinkOptions.ClientMaxInBufSize = &ib

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if showHelp {
		printHelp()
		return nil, nil
	}

	return opts, nil
}

// ValidateOptions checks required fields.
// In: opts *Options
// Out: *ConnectorError - nil if valid
// Ex: ValidateOptions(opts) → nil
func ValidateOptions(opts *Options) *errors.ConnectorError {
	if opts.sourceOptions.Topic == "" {
		return errors.NewNoTopicProvidedError("connector")
	}

	return nil
}

// ClusterUri returns QDB cluster URI.
// Out: string - cluster endpoint
// Ex: ClusterUri() → "qdb://host:2836"
func (o *Options) ClusterUri() string { return o.sinkOptions.ClusterUri }

// ClusterPublicKeyFile returns QDB pubkey path.
// Out: string - file path
// Ex: ClusterPublicKeyFile() → "/path/to/key"
func (o *Options) ClusterPublicKeyFile() string { return o.sinkOptions.ClusterPublicKeyFile }

// UserSecurityFile returns QDB user sec path.
// Out: string - file path
// Ex: UserSecurityFile() → "/path/to/sec"
func (o *Options) UserSecurityFile() string { return o.sinkOptions.UserSecurityFile }

// Encryption returns QDB encryption mode.
// Out: *qdb.Encryption - none|aes
// Ex: Encryption() → &EncryptAES
func (o *Options) Encryption() *qdb.Encryption { return o.sinkOptions.Encryption }

// Compression returns QDB compression mode.
// Out: *qdb.Compression - none|fast|best
// Ex: Compression() → &CompFast
func (o *Options) Compression() *qdb.Compression { return o.sinkOptions.Compression }

// ClientMaxParallelism returns QDB max parallelism.
// Out: *uint - worker threads
// Ex: ClientMaxParallelism() → &8
func (o *Options) ClientMaxParallelism() *uint { return o.sinkOptions.ClientMaxParallelism }

// ClientMaxInBufSize returns QDB input buffer size.
// Out: *uint - bytes
// Ex: ClientMaxInBufSize() → &1024
func (o *Options) ClientMaxInBufSize() *uint { return o.sinkOptions.ClientMaxInBufSize }

// Endpoint returns NATS endpoint.
// Out: string - connection URI
// Ex: Endpoint() → "nats://host:4222"
func (o *Options) Endpoint() string { return o.sourceOptions.Endpoint }

// Topic returns NATS subscription topic.
// Out: string - subject pattern
// Ex: Topic() → "sensors.>"
func (o *Options) Topic() string { return o.sourceOptions.Topic }

// compressionFlag: CLI flag wrapper for qdb.Compression pointer
type compressionFlag struct{ dst **qdb.Compression }

// String returns compression as JSON.
// Out: string - JSON representation
// Ex: String() → "\"fast\""
func (f *compressionFlag) String() string {
	if f == nil || f.dst == nil || *f.dst == nil {
		return ""
	}

	b, _ := json.Marshal(*f.dst)
	return string(b)
}

// Set parses compression from string.
// In: val string - none|fast|speed|best
// Out: error - parsing failure
// Ex: Set("fast") → nil
func (f *compressionFlag) Set(val string) error {
	comp, err := parseCompression(val)
	if err != nil {
		return err
	}
	*f.dst = &comp
	return nil
}

// encryptionFlag: CLI flag wrapper for qdb.Encryption pointer
type encryptionFlag struct{ dst **qdb.Encryption }

// String returns encryption as string.
// Out: string - none|aes
// Ex: String() → "aes"
func (f *encryptionFlag) String() string {
	if f == nil || f.dst == nil || *f.dst == nil {
		return ""
	}
	switch **f.dst {
	case qdb.EncryptNone:
		return "none"
	case qdb.EncryptAES:
		return "aes"
	default:
		return ""
	}
}

// Set parses encryption from string.
// In: val string - none|aes
// Out: error - parsing failure
// Ex: Set("aes") → nil
func (f *encryptionFlag) Set(val string) error {
	enc, err := parseEncryption(val)
	if err != nil {
		return err
	}
	*f.dst = &enc
	return nil
}

// parseCompression converts string→qdb.Compression.
// In: val string - none|fast|speed|best
// Out: qdb.Compression, error
// Ex: parseCompression("fast") → CompFast, nil
func parseCompression(val string) (qdb.Compression, error) {
	switch val {
	case "none":
		return qdb.CompNone, nil
	case "fast", "speed":
		return qdb.CompFast, nil
	case "best":
		return qdb.CompBest, nil
	default:
		return qdb.CompNone, errors.NewInvalidConfigError("connector", fmt.Sprintf("invalid compression value: %s (valid values: none, fast, speed, best)", val))
	}
}

// parseEncryption converts string→qdb.Encryption.
// In: val string - none|aes
// Out: qdb.Encryption, error
// Ex: parseEncryption("aes") → EncryptAES, nil
func parseEncryption(val string) (qdb.Encryption, error) {
	switch val {
	case "none":
		return qdb.EncryptNone, nil
	case "aes":
		return qdb.EncryptAES, nil
	default:
		return qdb.EncryptNone, errors.NewInvalidConfigError("connector", fmt.Sprintf("invalid encryption value: %s (valid values: none, aes)", val))
	}
}
