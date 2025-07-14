// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: NATS→QuasarDB pipeline orchestration
// Types: Connector, Worker, Options
// Ex: connector.New(opts).Run() → streams data
package connector

import (
	"fmt"
	"os"
	"strings"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/nats-io/nats.go"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Options: connector config, implements sink/source.OptionsProvider
type Options struct {
	PidFile string `mapstructure:"pid"`

	// NATS options
	NatsEndpoint       string        `mapstructure:"nats"`
	NatsStreamName     string        `mapstructure:"stream"`
	NatsTopicFilters   []string      `mapstructure:"topic"`
	NatsConsumerPrefix string        `mapstructure:"consumer-prefix"`
	NatsBatchSize      int           `mapstructure:"batch-size"`
	NatsBatchTimeout   time.Duration `mapstructure:"batch-timeout"`
	NatsFetchTimeout   time.Duration `mapstructure:"fetch-timeout"`
	NatsAckWait        time.Duration `mapstructure:"ack-wait"`
	NatsMaxDeliver     int           `mapstructure:"max-deliver"`

	// QDB options
	QdbClusterUri           string `mapstructure:"qdb"`
	QdbClusterPublicKeyFile string `mapstructure:"qdb-pubkey-file"`
	QdbUserSecurityFile     string `mapstructure:"qdb-user-sec-file"`
	QdbCompression          string `mapstructure:"qdb-compression"`
	QdbEncryption           string `mapstructure:"qdb-encryption"`
	QdbPushMode             string `mapstructure:"qdb-push-mode"`
	QdbDeduplicationMode    string `mapstructure:"qdb-deduplication-mode"`
	QdbClientMaxParallelism uint   `mapstructure:"qdb-client-max-parallelism"`
	QdbClientMaxInBufSize   uint   `mapstructure:"qdb-client-inbuf-size"`

	MaxRetries                     int           `mapstructure:"max-retries"`
	CircuitBreakerFailureThreshold int           `mapstructure:"circuit-breaker-failure-threshold"`
	CircuitBreakerSuccessThreshold int           `mapstructure:"circuit-breaker-success-threshold"`
	CircuitBreakerTimeout          time.Duration `mapstructure:"circuit-breaker-timeout"`
	CircuitBreakerShared           bool          `mapstructure:"circuit-breaker-shared"`
	CircuitBreakerJitterMax        time.Duration `mapstructure:"circuit-breaker-jitter-max"`
	CircuitBreakerHalfOpenBase     int           `mapstructure:"circuit-breaker-half-open-base"`
	// CircuitBreakerHalfOpenMax is the maximum requests allowed in half-open state.
	// Special values:
	//   0: No limit - allows unlimited requests in half-open state
	//   >0: Maximum number of concurrent requests before closing circuit
	CircuitBreakerHalfOpenMax int `mapstructure:"circuit-breaker-half-open-max"`

	// Hooks for observability/testing
	Hooks *hooks.HookRegistry `json:"-"`

	// Internal parsed options for OptionsProvider interface
	parsedSinkOptions   sink.Options
	parsedSourceOptions struct {
		URL            string
		StreamName     string
		TopicFilters   []string
		ConsumerPrefix string
		BatchSize      int
		BatchTimeout   time.Duration
		FetchTimeout   time.Duration
		AckWait        time.Duration
		MaxDeliver     int
	}
}

var (
	_ sink.OptionsProvider   = (*Options)(nil)
	_ source.OptionsProvider = (*Options)(nil)
)

// LoadConfig loads config from CLI arguments and environment variables.
// Args:
//
//	args: command-line arguments
//	printHelp: help display function
//
// Returns:
//
//	*Options: parsed config (nil if help requested)
//	error: parsing/validation failure
//
// Example:
//
//	LoadConfig(os.Args[1:], showHelp) // → *Options
func LoadConfig(args []string, printHelp func()) (*Options, error) {
	v := viper.New()
	fs := pflag.NewFlagSet("qdb-nats-connector", pflag.ExitOnError)

	// Configure viper for environment variables only

	// Set environment variable prefix and replacer
	v.SetEnvPrefix("QDB_NATS")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	// Set defaults
	v.SetDefault("nats", nats.DefaultURL)
	v.SetDefault("consumer-prefix", "qdb-connector")
	v.SetDefault("batch-size", 100)
	v.SetDefault("batch-timeout", time.Second)
	v.SetDefault("fetch-timeout", 5*time.Second)
	v.SetDefault("ack-wait", 30*time.Second)
	v.SetDefault("max-deliver", 3)
	v.SetDefault("max-retries", 3)
	v.SetDefault("qdb-compression", "none")
	v.SetDefault("qdb-encryption", "none")
	v.SetDefault("qdb-push-mode", "async")
	v.SetDefault("qdb-deduplication-mode", "drop")
	v.SetDefault("circuit-breaker-failure-threshold", 5)
	v.SetDefault("circuit-breaker-success-threshold", 2)
	v.SetDefault("circuit-breaker-timeout", 30*time.Second)
	v.SetDefault("circuit-breaker-shared", true)
	v.SetDefault("circuit-breaker-jitter-max", 100*time.Millisecond)
	v.SetDefault("circuit-breaker-half-open-base", 1)
	v.SetDefault("circuit-breaker-half-open-max", 32)

	var showHelp bool

	// Define CLI flags with pflag
	fs.BoolVarP(&showHelp, "help", "h", false, "Show this message.")

	// NATS flags
	fs.StringP("nats", "n", nats.DefaultURL, "NATS cluster endpoint (e.g. 10.192.172.166:4222)")
	fs.String("stream", "", "JetStream stream name")
	fs.String("consumer-prefix", "qdb-connector", "Consumer name prefix")
	fs.Int("batch-size", 100, "Messages per fetch")
	fs.Duration("batch-timeout", time.Second, "Max wait for batch")
	fs.Duration("fetch-timeout", 5*time.Second, "Total fetch timeout")
	fs.Duration("ack-wait", 30*time.Second, "Message ACK timeout")
	fs.Int("max-deliver", 3, "Max delivery attempts")
	fs.StringSliceP("topic", "t", []string{}, "Topic filter (can be repeated)")

	// General flags
	fs.Int("max-retries", 3, "Poison message threshold")
	fs.StringP("pid", "P", "", "File to store PID.")

	// QuasarDB connection flags
	fs.String("qdb", "", "QuasarDB cluster endpoint (e.g. qdb://127.0.0.1:2836)")
	fs.String("qdb-pubkey-file", "", "QuasarDB cluster public key file")
	fs.String("qdb-user-sec-file", "", "QuasarDB user security file")
	fs.String("qdb-compression", "", "QuasarDB sink compression (none|balanced)")
	fs.String("qdb-encryption", "", "QuasarDB sink encryption (none|aes)")
	fs.String("qdb-push-mode", "", "QuasarDB sink push mode (transactional|async|fast)")
	fs.String("qdb-deduplication-mode", "", "QuasarDB deduplication mode (disabled|drop|upsert)")
	fs.Uint("qdb-client-max-parallelism", 0, "QuasarDB sink max parallelism")
	fs.Uint("qdb-client-inbuf-size", 0, "QuasarDB sink max input buffer size")

	// Circuit breaker flags
	fs.Int("circuit-breaker-failure-threshold", 5, "Circuit breaker failure threshold")
	fs.Int("circuit-breaker-success-threshold", 2, "Circuit breaker success threshold")
	fs.Duration("circuit-breaker-timeout", 30*time.Second, "Circuit breaker timeout")
	fs.Bool("circuit-breaker-shared", true, "Enable shared circuit breakers across workers")
	fs.Duration("circuit-breaker-jitter-max", 100*time.Millisecond, "Maximum jitter for circuit breaker operations")
	fs.Int("circuit-breaker-half-open-base", 1, "Initial requests allowed in half-open state")
	fs.Int("circuit-breaker-half-open-max", 32, "Maximum requests before closing circuit")

	// Check for deprecated --config flag and warn user
	for _, arg := range args {
		if arg == "--config" {
			fmt.Fprintf(os.Stderr, "WARNING: --config flag is no longer supported. Please use command-line arguments or environment variables instead.\n")

			break
		}
	}

	err := fs.Parse(args)
	if err != nil {
		return nil, err
	}

	if showHelp {
		printHelp()

		return nil, nil
	}

	// Bind flags to viper - this handles precedence automatically
	err = v.BindPFlags(fs)
	if err != nil {
		return nil, fmt.Errorf("error binding flags: %w", err)
	}

	// Direct unmarshal to Options
	opts := &Options{}
	err = v.Unmarshal(opts)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	// Parse and set the typed fields
	err = parseAndSetFields(opts)
	if err != nil {
		return nil, err
	}

	// Validate options
	validationErr := validateOptions(opts)
	if validationErr != nil {
		return nil, validationErr
	}

	return opts, nil
}

// parseAndSetFields parses string fields into typed fields and sets internal options
func parseAndSetFields(opts *Options) error {
	// Set internal parsed source options
	opts.parsedSourceOptions.URL = opts.NatsEndpoint
	opts.parsedSourceOptions.StreamName = opts.NatsStreamName
	opts.parsedSourceOptions.TopicFilters = opts.NatsTopicFilters
	opts.parsedSourceOptions.ConsumerPrefix = opts.NatsConsumerPrefix
	opts.parsedSourceOptions.BatchSize = opts.NatsBatchSize
	opts.parsedSourceOptions.BatchTimeout = opts.NatsBatchTimeout
	opts.parsedSourceOptions.FetchTimeout = opts.NatsFetchTimeout
	opts.parsedSourceOptions.AckWait = opts.NatsAckWait
	opts.parsedSourceOptions.MaxDeliver = opts.NatsMaxDeliver

	// Set sink options
	opts.parsedSinkOptions.ClusterUri = opts.QdbClusterUri
	opts.parsedSinkOptions.ClusterPublicKeyFile = opts.QdbClusterPublicKeyFile
	opts.parsedSinkOptions.UserSecurityFile = opts.QdbUserSecurityFile

	// Parse compression
	if opts.QdbCompression != "" {
		comp, err := parseCompression(opts.QdbCompression)
		if err != nil {
			return err
		}
		opts.parsedSinkOptions.Compression = comp
	}

	// Parse encryption
	if opts.QdbEncryption != "" {
		enc, err := parseEncryption(opts.QdbEncryption)
		if err != nil {
			return err
		}
		opts.parsedSinkOptions.Encryption = &enc
	}

	// Parse push mode
	if opts.QdbPushMode != "" {
		pushMode, err := parsePushMode(opts.QdbPushMode)
		if err != nil {
			return err
		}
		opts.parsedSinkOptions.PushMode = pushMode
	}

	// Parse deduplication mode
	if opts.QdbDeduplicationMode != "" {
		dedupMode, err := parseDeduplicationMode(opts.QdbDeduplicationMode)
		if err != nil {
			return err
		}
		opts.parsedSinkOptions.DeduplicationMode = dedupMode
	}

	// Set performance settings
	if opts.QdbClientMaxParallelism > 0 {
		opts.parsedSinkOptions.ClientMaxParallelism = &opts.QdbClientMaxParallelism
	}
	if opts.QdbClientMaxInBufSize > 0 {
		opts.parsedSinkOptions.ClientMaxInBufSize = &opts.QdbClientMaxInBufSize
	}

	return nil
}

// validateOptions validates required config fields.
// Args:
//
//	opts: configuration to validate
//
// Returns:
//
//	*errors.ConnectorError: validation error or nil
//
// Example:
//
//	validateOptions(opts) // → nil if valid
func validateOptions(opts *Options) *errors.ConnectorError {
	if len(opts.NatsTopicFilters) == 0 {
		return errors.NewNoTopicProvidedError("connector")
	}

	if opts.NatsStreamName == "" {
		return errors.NewInvalidConfigError("connector", "stream name is required")
	}

	if opts.NatsEndpoint == "" {
		return errors.NewInvalidConfigError("connector", "NATS URL is required")
	}

	// Validate circuit breaker configuration
	if opts.CircuitBreakerFailureThreshold <= 0 {
		return errors.NewInvalidConfigError("connector", "circuit breaker failure threshold must be positive")
	}

	if opts.CircuitBreakerSuccessThreshold <= 0 {
		return errors.NewInvalidConfigError("connector", "circuit breaker success threshold must be positive")
	}

	if opts.CircuitBreakerTimeout <= 0 {
		return errors.NewInvalidConfigError("connector", "circuit breaker timeout must be positive")
	}

	// Validate half-open progression
	if opts.CircuitBreakerHalfOpenBase <= 0 {
		return errors.NewInvalidConfigError("connector", "circuit breaker half-open base must be positive")
	}

	if opts.CircuitBreakerHalfOpenMax != 0 && opts.CircuitBreakerHalfOpenMax < opts.CircuitBreakerHalfOpenBase {
		return errors.NewInvalidConfigError("connector", "circuit breaker half-open max must be >= base (or 0 for no limit)")
	}

	// Special case: 0 means "no limit" for max
	if opts.CircuitBreakerHalfOpenMax != 0 && opts.CircuitBreakerHalfOpenMax > 1000000 {
		return errors.NewInvalidConfigError("connector", "circuit breaker half-open max too large (max 1000000, or 0 for no limit)")
	}

	// Validate jitter - it should not exceed timeout
	if opts.CircuitBreakerJitterMax < 0 {
		return errors.NewInvalidConfigError("connector", "circuit breaker jitter max cannot be negative")
	}

	if opts.CircuitBreakerJitterMax > opts.CircuitBreakerTimeout/2 {
		return errors.NewInvalidConfigError("connector", "circuit breaker jitter max cannot exceed half the timeout")
	}

	return nil
}

// ClusterUri returns QuasarDB URI
// In: none
// Out: string - cluster URI
// Ex: ClusterUri() → "qdb://localhost:2836"
func (o *Options) ClusterUri() string { return o.parsedSinkOptions.ClusterUri }

// ClusterPublicKeyFile returns public key path
// In: none
// Out: string - path or empty
// Ex: ClusterPublicKeyFile() → "/etc/qdb/cluster.pub"
func (o *Options) ClusterPublicKeyFile() string { return o.parsedSinkOptions.ClusterPublicKeyFile }

// UserSecurityFile returns user auth file path
// In: none
// Out: string - path or empty
// Ex: UserSecurityFile() → "/etc/qdb/user.key"
func (o *Options) UserSecurityFile() string { return o.parsedSinkOptions.UserSecurityFile }

// Encryption returns QDB encryption mode
// In: none
// Out: *qdb.Encryption - nil=none
// Ex: Encryption() → &EncryptAES
func (o *Options) Encryption() *qdb.Encryption { return o.parsedSinkOptions.Encryption }

// Compression returns QDB compression mode
// In: none
// Out: qdb.Compression - none|balanced
// Ex: Compression() → CompBalanced
func (o *Options) Compression() qdb.Compression { return o.parsedSinkOptions.Compression }

// DeduplicationMode returns QDB deduplication mode
// In: none
// Out: qdb.WriterDeduplicationMode - disabled|drop|upsert
// Ex: DeduplicationMode() → WriterDeduplicationModeDrop
func (o *Options) DeduplicationMode() qdb.WriterDeduplicationMode {
	return o.parsedSinkOptions.DeduplicationMode
}

// ClientMaxParallelism returns max QDB threads
// In: none
// Out: *uint - nil=default
// Ex: ClientMaxParallelism() → &8
func (o *Options) ClientMaxParallelism() *uint { return o.parsedSinkOptions.ClientMaxParallelism }

// ClientMaxInBufSize returns max QDB buffer
// In: none
// Out: *uint - nil=default
// Ex: ClientMaxInBufSize() → &1048576
func (o *Options) ClientMaxInBufSize() *uint { return o.parsedSinkOptions.ClientMaxInBufSize }

// Timeout returns QDB operation timeout
// In: none
// Out: *time.Duration - nil=default
// Ex: Timeout() → &30s
func (o *Options) Timeout() *time.Duration { return o.parsedSinkOptions.Timeout }

// WorkerCreationDelay returns worker stagger delay
// In: none
// Out: time.Duration - delay
// Ex: WorkerCreationDelay() → 100ms
func (o *Options) WorkerCreationDelay() time.Duration { return o.parsedSinkOptions.WorkerCreationDelay }

// URL returns NATS server endpoint
// In: none
// Out: string - NATS URL
// Ex: URL() → "nats://localhost:4222"
func (o *Options) URL() string { return o.parsedSourceOptions.URL }

// StreamName returns JetStream stream name
// In: none
// Out: string - stream name
// Ex: StreamName() → "EVENTS"
func (o *Options) StreamName() string { return o.parsedSourceOptions.StreamName }

// TopicFilter returns first topic filter
// In: none
// Out: string - topic or empty
// Ex: TopicFilter() → "sensors.>"
func (o *Options) TopicFilter() string {
	if len(o.parsedSourceOptions.TopicFilters) > 0 {
		return o.parsedSourceOptions.TopicFilters[0]
	}

	return ""
}

// ConsumerName returns JetStream consumer prefix
// In: none
// Out: string - prefix
// Ex: ConsumerName() → "qdb-connector"
func (o *Options) ConsumerName() string { return o.parsedSourceOptions.ConsumerPrefix }

// BatchSize returns messages per fetch.
// Out: int - batch size
// Ex: BatchSize() → 100
func (o *Options) BatchSize() int { return o.parsedSourceOptions.BatchSize }

// BatchTimeout returns max wait for batch.
// Out: time.Duration - timeout
// Ex: BatchTimeout() → 1s
func (o *Options) BatchTimeout() time.Duration { return o.parsedSourceOptions.BatchTimeout }

// FetchTimeout returns total fetch timeout.
// Out: time.Duration - timeout
// Ex: FetchTimeout() → 5s
func (o *Options) FetchTimeout() time.Duration { return o.parsedSourceOptions.FetchTimeout }

// AckWait returns message ACK timeout.
// Out: time.Duration - timeout
// Ex: AckWait() → 30s
func (o *Options) AckWait() time.Duration { return o.parsedSourceOptions.AckWait }

// MaxDeliver returns max redelivery count.
// Out: int - max attempts
// Ex: MaxDeliver() → 3
func (o *Options) MaxDeliver() int { return o.parsedSourceOptions.MaxDeliver }

// parseCompression converts string→qdb.Compression.
// In: val string - none|balanced
// Out: qdb.Compression, error
// Ex: parseCompression("none") → CompNone, nil
func parseCompression(val string) (qdb.Compression, error) {
	// Check for deprecated "fast" compression and warn user
	if val == "fast" {
		fmt.Fprintf(os.Stderr, "WARNING: 'fast' compression is no longer supported. Please use 'none' or 'balanced' instead.\n")
	}

	switch val {
	case "none":
		return qdb.CompNone, nil
	case "balanced":
		return qdb.CompBalanced, nil
	default:
		return qdb.CompNone, errors.NewInvalidConfigError("connector", fmt.Sprintf("invalid compression value: %s (valid values: none, balanced)", val))
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

// parseDeduplicationMode converts string→qdb.WriterDeduplicationMode.
// In: val string - disabled|drop|upsert
// Out: qdb.WriterDeduplicationMode, error
// Ex: parseDeduplicationMode("drop") → WriterDeduplicationModeDrop, nil
func parseDeduplicationMode(val string) (qdb.WriterDeduplicationMode, error) {
	switch val {
	case "disabled":
		return qdb.WriterDeduplicationModeDisabled, nil
	case "drop":
		return qdb.WriterDeduplicationModeDrop, nil
	case "upsert":
		return qdb.WriterDeduplicationModeUpsert, nil
	default:
		return qdb.WriterDeduplicationModeDrop, errors.NewInvalidConfigError("connector", fmt.Sprintf("invalid deduplication mode value: %s (valid values: disabled, drop, upsert)", val))
	}
}
