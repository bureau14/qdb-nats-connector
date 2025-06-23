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
}

// NewOptions creates a new Options object and serves as the entrypoint for the
// builder pattern.
//
// Usage example:
//
//	opts := sink.NewOptions(
//	           sink.WithClusterUri("qdb://127.0.0.1:2836"),
//	         )
func NewOptions(opts ...Option) Options {
	defComp := qdb.CompBest           // take address of a var, not a const
	options := Options{
		Compression: &defComp,
	}
	for _, opt := range opts {
		options = opt(options)
	}
	return options
}

// WithClusterUri configures the QuasarDB cluster uri for the sink.
func WithClusterUri(uri string) Option {
	return func(o Options) Options {
		o.ClusterUri = uri
		return o
	}
}

// WithClusterPublicKeyFile sets the cluster public key file path.
func WithClusterPublicKeyFile(file string) Option {
	return func(o Options) Options {
		o.ClusterPublicKeyFile = file
		return o
	}
}

// WithClusterPublicKey sets the cluster public key content.
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
	opts := []Option{
		WithClusterUri(p.ClusterUri()),
		WithClusterPublicKeyFile(p.ClusterPublicKeyFile()),
		WithUserSecurityFile(p.UserSecurityFile()),
		WithEncryption(p.Encryption()),
		WithCompression(p.Compression()),
	}
	if p.ClientMaxParallelism() != nil {
		opts = append(opts, WithClientMaxParallelism(*p.ClientMaxParallelism()))
	}
	if p.ClientMaxInBufSize() != nil {
		opts = append(opts, WithClientMaxInBufSize(*p.ClientMaxInBufSize()))
	}
	return NewOptions(opts...)
}
