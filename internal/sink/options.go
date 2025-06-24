package sink

import (
	qdb "github.com/bureau14/qdb-api-go/v3"
)

// Option is a function that configures an Options object.
// Decision rationale:
// - Functional options pattern provides flexible configuration
// - Immutable options prevent concurrent modification issues
// - Builder pattern enables fluent API
type Option func(Options) Options

// Options configures the QuasarDB sink behavior and connection parameters.
// Key assumptions:
// - Either file paths or direct values for security credentials
// - Nil pointers mean "use QuasarDB defaults"
// - JSON tags enable config file support
type Options struct {
	ClusterUri           string `json:"cluster_uri"`
	ClusterPublicKeyFile string `json:"cluster_public_key_file"`
	ClusterPublicKey     string `json:"cluster_public_key"`
	UserSecurityFile     string `json:"user_security_file"`
	UserName             string `json:"user_name"`
	UserSecret           string `json:"user_secret"`
	Encryption           *qdb.Encryption

	// Defaults to `qdb.CompBest`
	Compression *qdb.Compression

	ClientMaxParallelism *uint
	ClientMaxInBufSize   *uint

	// Worker pool configuration
	NumWriters    int `json:"num_writers"`
	QueueSize     int `json:"queue_size"`
	RetryAttempts int `json:"retry_attempts"`
}

// NewOptions creates a new Options object and serves as the entrypoint for the
// builder pattern.
//
// Usage example:
//
//	opts := sink.NewOptions(
//	           sink.WithClusterUri("qdb://127.0.0.1:2836"),
//	           sink.WithNumWriters(4),
//	         )
func NewOptions(opts ...Option) Options {
	defComp := qdb.CompBest
	options := Options{
		Compression:   &defComp,
		NumWriters:    4,
		QueueSize:     100,
		RetryAttempts: 3,
	}
	for _, opt := range opts {
		options = opt(options)
	}
	return options
}

// WithClusterUri configures the QuasarDB cluster uri for the sink.
// Key assumptions:
// - URI format follows qdb:// scheme (e.g., qdb://host:port)
// - Connection validation happens during sink initialization
func WithClusterUri(uri string) Option {
	return func(o Options) Options {
		o.ClusterUri = uri
		return o
	}
}

// WithClusterPublicKeyFile sets the cluster public key file path.
// Key assumptions:
// - File path is absolute or relative to working directory
// - File contains valid QuasarDB cluster public key
func WithClusterPublicKeyFile(file string) Option {
	return func(o Options) Options {
		o.ClusterPublicKeyFile = file
		return o
	}
}

// WithClusterPublicKey sets the cluster public key content directly.
// Decision rationale:
// - Alternative to WithClusterPublicKeyFile for embedded key content
// - Useful for containerized deployments with secret injection
func WithClusterPublicKey(key string) Option {
	return func(o Options) Options {
		o.ClusterPublicKey = key
		return o
	}
}

// WithUserSecurityFile sets the user security file path.
func WithUserSecurityFile(file string) Option {
	return func(o Options) Options {
		o.UserSecurityFile = file
		return o
	}
}

// WithUserName sets the user name.
func WithUserName(name string) Option {
	return func(o Options) Options {
		o.UserName = name
		return o
	}
}

// WithUserSecret sets the user secret.
func WithUserSecret(secret string) Option {
	return func(o Options) Options {
		o.UserSecret = secret
		return o
	}
}

// WithEncryption sets the encryption config.
func WithEncryption(enc *qdb.Encryption) Option {
	return func(o Options) Options {
		o.Encryption = enc
		return o
	}
}

// WithCompression sets the compression mode.
func WithCompression(c *qdb.Compression) Option {
	return func(o Options) Options {
		o.Compression = c
		return o
	}
}

// WithClientMaxParallelism sets the maximum client parallelism.
func WithClientMaxParallelism(par uint) Option {
	return func(o Options) Options {
		o.ClientMaxParallelism = &par
		return o
	}
}

// WithClientMaxInBufSize sets the maximum input buffer size.
func WithClientMaxInBufSize(size uint) Option {
	return func(o Options) Options {
		o.ClientMaxInBufSize = &size
		return o
	}
}

// WithNumWriters sets the number of writer workers.
// Decision rationale:
// - Controls parallelism for QuasarDB write operations
// - Each worker maintains dedicated connection handle
// Performance trade-offs:
// - More workers increase memory usage but improve throughput
// - Optimal value depends on QuasarDB cluster capacity
func WithNumWriters(num int) Option {
	return func(o Options) Options {
		o.NumWriters = num
		return o
	}
}

// WithQueueSize sets the queue size for buffering messages.
// Decision rationale:
// - Provides backpressure control for incoming NATS messages
// - Larger queue handles traffic spikes but uses more memory
// Performance trade-offs:
// - Smaller queue fails fast on overload but may drop messages
// - Larger queue smooths traffic but increases memory usage
func WithQueueSize(size int) Option {
	return func(o Options) Options {
		o.QueueSize = size
		return o
	}
}

// WithRetryAttempts sets the maximum number of retry attempts.
// Decision rationale:
// - Handles transient QuasarDB errors (async pipeline full, network issues)
// - Exponential backoff prevents overwhelming downstream systems
// Performance trade-offs:
// - More retries increase resilience but delay error reporting
// - Fewer retries fail fast but may drop data on transient issues
func WithRetryAttempts(attempts int) Option {
	return func(o Options) Options {
		o.RetryAttempts = attempts
		return o
	}
}

// OptionsProvider defines an interface for providing sink options.
// This allows decoupling the connector's options from the sink's options,
// making the configuration scalable.
type OptionsProvider interface {
	ClusterUri() string
	ClusterPublicKeyFile() string
	UserSecurityFile() string
	Encryption() *qdb.Encryption
	Compression() *qdb.Compression
	ClientMaxParallelism() *uint
	ClientMaxInBufSize() *uint
}

// FromOptionsProvider creates a new Options object from a provider.
//
// Decision rationale:
// - Uses the builder pattern to construct the Options object.
// - Checks for nil pointers on optional values to avoid panics.
func FromOptionsProvider(p OptionsProvider) Options {
	// Build base options with required fields
	opts := []Option{
		WithClusterUri(p.ClusterUri()),
		WithClusterPublicKeyFile(p.ClusterPublicKeyFile()),
		WithUserSecurityFile(p.UserSecurityFile()),
		WithEncryption(p.Encryption()),
		WithCompression(p.Compression()),
	}

	// Use Go 1.21+ slice operations for conditional appends
	if p.ClientMaxParallelism() != nil {
		opts = append(opts, WithClientMaxParallelism(*p.ClientMaxParallelism()))
	}
	if p.ClientMaxInBufSize() != nil {
		opts = append(opts, WithClientMaxInBufSize(*p.ClientMaxInBufSize()))
	}
	return NewOptions(opts...)
}
