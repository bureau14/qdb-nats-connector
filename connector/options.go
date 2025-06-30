// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: NATS→QuasarDB pipeline orchestrator
// Types: Connector, Options
// Ex: connector.NewConnector(opts, parser).Run() → data flows
package connector

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/nats-io/nats.go"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
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

// LoadConfig loads configuration using viper with proper precedence.
// Precedence: defaults < config file < env vars < CLI flags
// Args:
//
//	args: []string - command line arguments
//	printHelp: func() - help display callback
//
// Returns:
//
//	*Options: loaded config (nil if help shown)
//	error: loading/validation fails
//
// Example:
//
//	LoadConfig(os.Args[1:], usage) // → opts, nil
func LoadConfig(args []string, printHelp func()) (*Options, error) {
	v := viper.New()
	fs := pflag.NewFlagSet("qdb-nats-connector", pflag.ExitOnError)

	// Configure viper
	v.SetConfigName("qdb-nats-connector")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("$HOME/.config/qdb-nats-connector")
	v.AddConfigPath("/etc/qdb-nats-connector")

	// Set environment variable prefix and replacer
	v.SetEnvPrefix("QDB_NATS")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// Set defaults
	v.SetDefault("nats.endpoint", nats.DefaultURL)
	v.SetDefault("qdb.compression", "best")
	v.SetDefault("qdb.encryption", "none")
	v.SetDefault("qdb.push_mode", "async")

	opts := &Options{}
	var showHelp bool
	var configFile string

	// Define CLI flags with pflag
	fs.BoolVarP(&showHelp, "help", "h", false, "Show this message.")
	fs.StringVar(&configFile, "config", "", "Configuration file path")

	fs.StringVarP(&opts.sourceOptions.Endpoint, "nats", "n", nats.DefaultURL, "NATS cluster endpoint (e.g. 10.192.172.166:4222)")
	fs.StringVarP(&opts.sourceOptions.Topic, "topic", "t", "", "Topic to subscribe to.")
	fs.StringVarP(&opts.PidFile, "pid", "P", "", "File to store PID.")

	// QuasarDB connection flags
	fs.StringVar(&opts.sinkOptions.ClusterUri, "qdb", "", "QuasarDB cluster endpoint (e.g. qdb://127.0.0.1:2836)")
	fs.StringVar(&opts.sinkOptions.ClusterPublicKeyFile, "qdb-pubkey-file", "", "QuasarDB cluster public key file")
	fs.StringVar(&opts.sinkOptions.UserSecurityFile, "qdb-user-sec-file", "", "QuasarDB user security file")

	// Compression, encryption, and push mode flags (using string for simplicity with pflag)
	var compressionStr, encryptionStr, pushModeStr string
	fs.StringVar(&compressionStr, "qdb-compression", "", "QuasarDB sink compression (none|best|speed)")
	fs.StringVar(&encryptionStr, "qdb-encryption", "", "QuasarDB sink encryption (none|aes)")
	fs.StringVar(&pushModeStr, "qdb-push-mode", "", "QuasarDB sink push mode (transactional|async|fast)")

	// Performance-tuning flags
	var pm, ib uint
	fs.UintVar(&pm, "qdb-client-max-parallelism", 0, "QuasarDB sink max parallelism")
	fs.UintVar(&ib, "qdb-client-inbuf-size", 0, "QuasarDB sink max input buffer size")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if showHelp {
		printHelp()
		return nil, nil
	}

	// Load config file if specified
	if configFile != "" {
		v.SetConfigFile(configFile)
	}

	// Read config file (ignore if not found)
	if err := v.ReadInConfig(); err != nil {
		// Only return error if config file was explicitly specified
		if configFile != "" {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
		// For automatic config file discovery, ignore all errors (file not found, parse errors, etc.)
	}

	// Override with viper values where CLI flags weren't explicitly set
	// This maintains CLI flag precedence while allowing config file and env vars
	if !fs.Changed("nats") {
		opts.sourceOptions.Endpoint = v.GetString("nats.endpoint")
	}
	if !fs.Changed("topic") {
		opts.sourceOptions.Topic = v.GetString("nats.topic")
	}
	if !fs.Changed("pid") {
		opts.PidFile = v.GetString("pid")
	}
	if !fs.Changed("qdb") {
		opts.sinkOptions.ClusterUri = v.GetString("qdb.cluster_uri")
	}
	if !fs.Changed("qdb-pubkey-file") {
		opts.sinkOptions.ClusterPublicKeyFile = v.GetString("qdb.cluster_public_key_file")
	}
	if !fs.Changed("qdb-user-sec-file") {
		opts.sinkOptions.UserSecurityFile = v.GetString("qdb.user_security_file")
	}

	// Handle compression
	if fs.Changed("qdb-compression") {
		comp, err := parseCompression(compressionStr)
		if err != nil {
			return nil, err
		}
		opts.sinkOptions.Compression = comp
	} else if compStr := v.GetString("qdb.compression"); compStr != "" {
		comp, err := parseCompression(compStr)
		if err != nil {
			return nil, err
		}
		opts.sinkOptions.Compression = comp
	}

	// Handle encryption
	if fs.Changed("qdb-encryption") {
		enc, err := parseEncryption(encryptionStr)
		if err != nil {
			return nil, err
		}
		opts.sinkOptions.Encryption = &enc
	} else if encStr := v.GetString("qdb.encryption"); encStr != "" {
		enc, err := parseEncryption(encStr)
		if err != nil {
			return nil, err
		}
		opts.sinkOptions.Encryption = &enc
	}

	// Handle push mode
	if fs.Changed("qdb-push-mode") {
		pushMode, err := parsePushMode(pushModeStr)
		if err != nil {
			return nil, err
		}
		opts.sinkOptions.PushMode = pushMode
	} else if pushModeStr := v.GetString("qdb.push_mode"); pushModeStr != "" {
		pushMode, err := parsePushMode(pushModeStr)
		if err != nil {
			return nil, err
		}
		opts.sinkOptions.PushMode = pushMode
	}

	// Handle performance settings
	if fs.Changed("qdb-client-max-parallelism") {
		opts.sinkOptions.ClientMaxParallelism = &pm
	} else if maxPar := v.GetUint("qdb.client_max_parallelism"); maxPar > 0 {
		opts.sinkOptions.ClientMaxParallelism = &maxPar
	}

	if fs.Changed("qdb-client-inbuf-size") {
		opts.sinkOptions.ClientMaxInBufSize = &ib
	} else if maxBuf := v.GetUint("qdb.client_max_inbuf_size"); maxBuf > 0 {
		opts.sinkOptions.ClientMaxInBufSize = &maxBuf
	}

	return opts, nil
}

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
	// Push mode (custom flag.Value)
	fs.Var(&pushModeFlag{dst: &opts.sinkOptions.PushMode}, "qdb-push-mode", "QuasarDB sink push mode (transactional|async|fast)")

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
// Out: qdb.Compression - none|fast|best
// Ex: Compression() → CompFast
func (o *Options) Compression() qdb.Compression { return o.sinkOptions.Compression }

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

// compressionFlag: CLI flag wrapper for qdb.Compression value
type compressionFlag struct{ dst *qdb.Compression }

// String returns compression as JSON.
// Out: string - JSON representation
// Ex: String() → "\"fast\""
func (f *compressionFlag) String() string {
	if f == nil || f.dst == nil {
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
	*f.dst = comp
	return nil
}

// pushModeFlag: CLI flag wrapper for qdb.WriterPushMode value
type pushModeFlag struct{ dst *qdb.WriterPushMode }

// String returns push mode as string.
// Out: string - transactional|async|fast
// Ex: String() → "async"
func (f *pushModeFlag) String() string {
	if f == nil || f.dst == nil {
		return ""
	}
	switch *f.dst {
	case qdb.WriterPushModeTransactional:
		return "transactional"
	case qdb.WriterPushModeAsync:
		return "async"
	case qdb.WriterPushModeFast:
		return "fast"
	default:
		return ""
	}
}

// Set parses push mode from string.
// In: val string - transactional|async|fast
// Out: error - parsing failure
// Ex: Set("async") → nil
func (f *pushModeFlag) Set(val string) error {
	pushMode, err := parsePushMode(val)
	if err != nil {
		return err
	}
	*f.dst = pushMode
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

// parsePushMode converts string→qdb.WriterPushMode.
// In: val string - transactional|async|fast
// Out: qdb.WriterPushMode, error
// Ex: parsePushMode("async") → WriterPushModeAsync, nil
func parsePushMode(val string) (qdb.WriterPushMode, error) {
	switch val {
	case "transactional":
		return qdb.WriterPushModeTransactional, nil
	case "async":
		return qdb.WriterPushModeAsync, nil
	case "fast":
		return qdb.WriterPushModeFast, nil
	default:
		return qdb.WriterPushModeAsync, errors.NewInvalidConfigError("connector", fmt.Sprintf("invalid push mode value: %s (valid values: transactional, async, fast)", val))
	}
}
