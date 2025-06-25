// Package connector bridges NATS messaging with QuasarDB time series storage.
// This package provides the core orchestration logic for subscribing to NATS topics,
// parsing messages through a configurable pipeline, and storing the results in QuasarDB.
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

// Options aggregates configuration for the entire connector pipeline.
// It implements both source.OptionsProvider and sink.OptionsProvider
// to allow seamless configuration propagation to child components.
// Decision rationale:
// - Embeds source and sink options to avoid duplication
// - Uses provider pattern for flexible configuration passing
// - JSON tags enable configuration via files
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

// ConfigureOptions parses command-line arguments into a validated Options struct.
// Key assumptions:
// - Short and long flag variants are equivalent (e.g., -n and --nats)
// - Zero values for performance tuning flags mean "use defaults"
// - Help request returns nil options without error
// Decision rationale:
// - Supports both short and long flags for user convenience
// - Custom flag.Value implementations for type-safe parsing
// - Performance flags use pointers to distinguish unset from zero
// Usage example:
//
//	opts, err := ConfigureOptions(flag.CommandLine, os.Args[1:], usage)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if opts == nil {
//	    return // help was requested
//	}
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

// ValidateOptions ensures all required connector options are properly set.
// Key assumptions:
// - Topic is the only mandatory field (endpoints have defaults)
// - Further validation happens in component constructors
// Decision rationale:
// - Minimal validation here, components validate their own requirements
// - Early failure on missing topic prevents cryptic downstream errors
func ValidateOptions(opts *Options) *errors.ConnectorError {
	if opts.sourceOptions.Topic == "" {
		return errors.NewNoTopicProvidedError("connector")
	}

	return nil
}

// sink.OptionsProvider implementation
// These methods delegate to embedded sinkOptions to fulfill the provider interface.
// Decision rationale:
// - Simple delegation avoids data duplication
// - Provider pattern allows sink to remain decoupled from connector
func (o *Options) ClusterUri() string            { return o.sinkOptions.ClusterUri }
func (o *Options) ClusterPublicKeyFile() string  { return o.sinkOptions.ClusterPublicKeyFile }
func (o *Options) UserSecurityFile() string      { return o.sinkOptions.UserSecurityFile }
func (o *Options) Encryption() *qdb.Encryption   { return o.sinkOptions.Encryption }
func (o *Options) Compression() *qdb.Compression { return o.sinkOptions.Compression }
func (o *Options) ClientMaxParallelism() *uint   { return o.sinkOptions.ClientMaxParallelism }
func (o *Options) ClientMaxInBufSize() *uint     { return o.sinkOptions.ClientMaxInBufSize }

// source.OptionsProvider implementation
// These methods delegate to embedded sourceOptions to fulfill the provider interface.
// Decision rationale:
// - Simple delegation avoids data duplication
// - Provider pattern allows source to remain decoupled from connector
func (o *Options) Endpoint() string { return o.sourceOptions.Endpoint }
func (o *Options) Topic() string    { return o.sourceOptions.Topic }

// compressionFlag implements flag.Value for *qdb.Compression.
// Decision rationale:
// - Double pointer allows distinguishing unset from explicit "none"
// - Custom parsing provides user-friendly string values
type compressionFlag struct{ dst **qdb.Compression }

func (f *compressionFlag) String() string {
	// Keep the output symmetrical with encryptionFlag.
	if f == nil || f.dst == nil || *f.dst == nil {
		return ""
	}

	b, _ := json.Marshal(*f.dst)
	return string(b)
}
func (f *compressionFlag) Set(val string) error {
	comp, err := parseCompression(val)
	if err != nil {
		return err
	}
	*f.dst = &comp
	return nil
}

// encryptionFlag implements flag.Value for *qdb.Encryption.
// Decision rationale:
// - Double pointer allows distinguishing unset from default
// - String representation matches command-line input format
type encryptionFlag struct{ dst **qdb.Encryption }

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
func (f *encryptionFlag) Set(val string) error {
	enc, err := parseEncryption(val)
	if err != nil {
		return err
	}
	*f.dst = &enc
	return nil
}

// parseCompression parses string compression values into qdb.Compression constants.
// Key assumptions:
// - "fast" and "speed" are synonyms for user convenience
// - Invalid values return CompNone with error
// Decision rationale:
// - Explicit string matching avoids strconv parsing issues
// - Clear error messages guide users to valid options
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

// parseEncryption parses string encryption values into qdb.Encryption constants.
// Key assumptions:
// - Only "none" and "aes" are valid options
// - Invalid values return EncryptNone with error
// Decision rationale:
// - Type-safe parsing prevents invalid encryption settings
// - Defaults to no encryption on error for safety
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
