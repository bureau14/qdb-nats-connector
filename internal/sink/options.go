// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package sink: QuasarDB connection & persistence
// Types: Sink, Options, OptionsProvider
// Ex: sink.NewSink(opts).Write(tables) → writes to QDB
package sink

import (
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
)

// Option: functional option for sink configuration
type Option func(Options) Options

// Options: QDB sink config with auth & worker pool settings
type Options struct {
	ClusterUri           string `json:"cluster_uri"`
	ClusterPublicKeyFile string `json:"cluster_public_key_file"`
	ClusterPublicKey     string `json:"cluster_public_key"`
	UserSecurityFile     string `json:"user_security_file"`
	UserName             string `json:"user_name"`
	UserSecret           string `json:"user_secret"`
	Encryption           *qdb.Encryption

	// Defaults to `qdb.WriterPushModeAsync`
	PushMode qdb.WriterPushMode

	// Defaults to `qdb.CompNone`
	Compression qdb.Compression

	ClientMaxParallelism *uint
	ClientMaxInBufSize   *uint

	// Worker pool configuration
	NumWriters          int            `json:"num_writers"`
	QueueSize           int            `json:"queue_size"`
	RetryAttempts       int            `json:"retry_attempts"`
	Timeout             *time.Duration `json:"timeout"`
	WorkerCreationDelay time.Duration  `json:"worker_creation_delay"`
}

// NewOptions creates sink config with defaults.
// Args:
//
//	opts: ...Option - functional option setters
//
// Returns:
//
//	Options: CompNone compression, 4 workers, queue=100
//
// Example:
//
//	NewOptions(WithClusterUri("qdb://host:2836")) // → opts
func NewOptions(opts ...Option) Options {
	options := Options{
		PushMode:            qdb.WriterPushModeAsync,
		Compression:         qdb.CompNone,
		NumWriters:          4,
		QueueSize:           100,
		RetryAttempts:       10,
		WorkerCreationDelay: 0,
	}
	for _, opt := range opts {
		options = opt(options)
	}

	return options
}

// WithClusterUri sets QDB cluster URI.
// In: uri string - qdb://host:port
// Out: Option
// Ex: WithClusterUri("qdb://127.0.0.1:2836")
func WithClusterUri(uri string) Option {
	return func(o Options) Options {
		o.ClusterUri = uri

		return o
	}
}

// WithClusterPublicKeyFile sets pubkey file path.
// In: file string - path to key file
// Out: Option
// Ex: WithClusterPublicKeyFile("/etc/qdb/cluster.pub")
func WithClusterPublicKeyFile(file string) Option {
	return func(o Options) Options {
		o.ClusterPublicKeyFile = file

		return o
	}
}

// WithClusterPublicKey sets pubkey content.
// In: key string - inline key
// Out: Option
// Ex: WithClusterPublicKey("base64key...")
func WithClusterPublicKey(key string) Option {
	return func(o Options) Options {
		o.ClusterPublicKey = key

		return o
	}
}

// WithUserSecurityFile sets user sec file.
// In: file string - path to file
// Out: Option
// Ex: WithUserSecurityFile("/etc/qdb/user.sec")
func WithUserSecurityFile(file string) Option {
	return func(o Options) Options {
		o.UserSecurityFile = file

		return o
	}
}

// WithUserName sets auth username.
// In: name string
// Out: Option
// Ex: WithUserName("admin")
func WithUserName(name string) Option {
	return func(o Options) Options {
		o.UserName = name

		return o
	}
}

// WithUserSecret sets auth secret.
// In: secret string
// Out: Option
// Ex: WithUserSecret("secret123")
func WithUserSecret(secret string) Option {
	return func(o Options) Options {
		o.UserSecret = secret

		return o
	}
}

// WithEncryption sets encryption mode.
// In: enc *qdb.Encryption - none|aes
// Out: Option
// Ex: WithEncryption(&qdb.EncryptAES)
func WithEncryption(enc *qdb.Encryption) Option {
	return func(o Options) Options {
		o.Encryption = enc

		return o
	}
}

// WithPushMode sets push mode.
// In: mode qdb.WriterPushMode - transactional|async|fast
// Out: Option
// Ex: WithPushMode(qdb.WriterPushModeTransactional)
func WithPushMode(mode qdb.WriterPushMode) Option {
	return func(o Options) Options {
		o.PushMode = mode

		return o
	}
}

// WithCompression sets compression level.
// In: c qdb.Compression - none|fast|best
// Out: Option
// Ex: WithCompression(qdb.CompFast)
func WithCompression(c qdb.Compression) Option {
	return func(o Options) Options {
		o.Compression = c

		return o
	}
}

// WithClientMaxParallelism sets max threads.
// In: par uint - parallelism limit
// Out: Option
// Ex: WithClientMaxParallelism(8)
func WithClientMaxParallelism(par uint) Option {
	return func(o Options) Options {
		o.ClientMaxParallelism = &par

		return o
	}
}

// WithClientMaxInBufSize sets buffer limit.
// In: size uint - buffer bytes
// Out: Option
// Ex: WithClientMaxInBufSize(1024)
func WithClientMaxInBufSize(size uint) Option {
	return func(o Options) Options {
		o.ClientMaxInBufSize = &size

		return o
	}
}

// WithNumWriters sets worker pool size.
// In: num int - worker count
// Out: Option
// Ex: WithNumWriters(8) // 8 parallel writers
func WithNumWriters(num int) Option {
	return func(o Options) Options {
		o.NumWriters = num

		return o
	}
}

// WithQueueSize sets message buffer size.
// In: size int - queue capacity
// Out: Option
// Ex: WithQueueSize(1000) // buffer 1k msgs
func WithQueueSize(size int) Option {
	return func(o Options) Options {
		o.QueueSize = size

		return o
	}
}

// WithRetryAttempts sets max retries.
// In: attempts int - retry limit
// Out: Option
// Ex: WithRetryAttempts(5) // retry 5x
func WithRetryAttempts(attempts int) Option {
	return func(o Options) Options {
		o.RetryAttempts = attempts

		return o
	}
}

// WithTimeout sets connection timeout.
// In: timeout time.Duration - connection timeout
// Out: Option
// Ex: WithTimeout(30*time.Second) // 30s timeout
func WithTimeout(timeout time.Duration) Option {
	return func(o Options) Options {
		o.Timeout = &timeout

		return o
	}
}

// WithWorkerCreationDelay sets delay between worker creation.
// In: delay time.Duration - delay between workers
// Out: Option
// Ex: WithWorkerCreationDelay(time.Second) // 1s delay
func WithWorkerCreationDelay(delay time.Duration) Option {
	return func(o Options) Options {
		o.WorkerCreationDelay = delay

		return o
	}
}

// OptionsProvider: interface for sink config decoupling from connector
type OptionsProvider interface {
	ClusterUri() string
	ClusterPublicKeyFile() string
	UserSecurityFile() string
	Encryption() *qdb.Encryption
	Compression() qdb.Compression
	ClientMaxParallelism() *uint
	ClientMaxInBufSize() *uint
	Timeout() *time.Duration
	WorkerCreationDelay() time.Duration
}

// FromOptionsProvider builds Options from provider.
// In: p OptionsProvider - config source
// Out: Options - built config
// Ex: FromOptionsProvider(connectorOpts) → sinkOpts
func FromOptionsProvider(p OptionsProvider) Options {
	// Build base options with required fields
	opts := []Option{
		WithClusterUri(p.ClusterUri()),
		WithClusterPublicKeyFile(p.ClusterPublicKeyFile()),
		WithUserSecurityFile(p.UserSecurityFile()),
		WithEncryption(p.Encryption()),
		WithCompression(p.Compression()),
		WithWorkerCreationDelay(p.WorkerCreationDelay()),
	}

	// Use Go 1.21+ slice operations for conditional appends
	if p.ClientMaxParallelism() != nil {
		opts = append(opts, WithClientMaxParallelism(*p.ClientMaxParallelism()))
	}
	if p.ClientMaxInBufSize() != nil {
		opts = append(opts, WithClientMaxInBufSize(*p.ClientMaxInBufSize()))
	}
	if p.Timeout() != nil {
		opts = append(opts, WithTimeout(*p.Timeout()))
	}

	return NewOptions(opts...)
}
