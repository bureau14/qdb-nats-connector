// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package source: NATS JetStream connection & batch fetching
// Types: Source, Options, OptionsProvider, MessageBatch, MessageInfo
// Ex: source.NewSource(opts).FetchBatch(ctx) → messages flow
package source

import "time"

// Option: functional option pattern for source configuration. Needed for flexible option composition.
// Who: WithX functions return, NewOptions consumes.
type Option func(Options) Options

// Options: NATS JetStream configuration for reliable message consumption. Needed for pull-based batch fetching.
// Who: connector config provides, source consumes via NewSource.
// Endpoint-ConsumerName: JetStream connection details
// CredsFile-TLSCAFile: connection security (JWT credentials, private CA)
// BatchSize-FetchTimeout: message fetching policies
type Options struct {
	Endpoint     string        `json:"endpoint"`
	StreamName   string        `json:"stream_name"`
	ConsumerName string        `json:"consumer_name"`
	CredsFile    string        `json:"creds_file"`
	TLSCAFile    string        `json:"tls_ca_file"`
	BatchSize    int           `json:"batch_size"`
	BatchTimeout time.Duration `json:"batch_timeout"`
	FetchTimeout time.Duration `json:"fetch_timeout"`
}

// NewOptions applies options to JetStream defaults.
// In: opts ...Option - functional options
// Out: Options - batch=100, timeout=1s
// Ex: NewOptions(WithEndpoint("nats://localhost")) → Options{}
func NewOptions(opts ...Option) Options {
	// Set defaults for JetStream
	options := Options{
		BatchSize:    100,
		BatchTimeout: time.Second,
		FetchTimeout: 5 * time.Second,
	}
	for _, opt := range opts {
		options = opt(options)
	}

	return options
}

// WithEndpoint sets NATS server URL.
// In: endpoint string - nats://host:port
// Out: Option
// Ex: WithEndpoint("nats://localhost:4222")
func WithEndpoint(endpoint string) Option {
	return func(o Options) Options {
		o.Endpoint = endpoint

		return o
	}
}

// WithStreamName sets JetStream stream name.
// In: streamName string - JetStream stream
// Out: Option
// Ex: WithStreamName("EVENTS")
func WithStreamName(streamName string) Option {
	return func(o Options) Options {
		o.StreamName = streamName

		return o
	}
}

// WithCredsFile sets NATS credentials (.creds) file for JWT auth.
// In: path string - credentials file
// Out: Option
// Ex: WithCredsFile("/keys/real.creds")
func WithCredsFile(path string) Option {
	return func(o Options) Options {
		o.CredsFile = path

		return o
	}
}

// WithTLSCAFile sets CA certificate file for NATS TLS.
// In: path string - PEM CA certificate file
// Out: Option
// Ex: WithTLSCAFile("/keys/ca.pem")
func WithTLSCAFile(path string) Option {
	return func(o Options) Options {
		o.TLSCAFile = path

		return o
	}
}

// WithBatchSize sets messages per fetch.
// In: batchSize int - messages per batch
// Out: Option
// Ex: WithBatchSize(100)
func WithBatchSize(batchSize int) Option {
	return func(o Options) Options {
		o.BatchSize = batchSize

		return o
	}
}

// WithBatchTimeout sets max wait for batch.
// In: timeout time.Duration - max wait
// Out: Option
// Ex: WithBatchTimeout(time.Second)
func WithBatchTimeout(timeout time.Duration) Option {
	return func(o Options) Options {
		o.BatchTimeout = timeout

		return o
	}
}

// WithFetchTimeout sets total fetch timeout.
// In: timeout time.Duration - total timeout
// Out: Option
// Ex: WithFetchTimeout(5*time.Second)
func WithFetchTimeout(timeout time.Duration) Option {
	return func(o Options) Options {
		o.FetchTimeout = timeout

		return o
	}
}

// OptionsProvider: interface for source config decoupling from connector
type OptionsProvider interface {
	// URL returns NATS server URL (e.g., "nats://host:4222")
	URL() string
	// StreamName returns JetStream stream name for subscription
	StreamName() string
	// CredsFile returns NATS credentials file path ("" = no credentials)
	CredsFile() string
	// TLSCAFile returns CA certificate file path ("" = system trust store)
	TLSCAFile() string
	// BatchSize returns max messages per fetch operation
	BatchSize() int
	// BatchTimeout returns max wait time for batch completion
	BatchTimeout() time.Duration
	// FetchTimeout returns overall timeout for fetch operation
	FetchTimeout() time.Duration
}

// FromOptionsProvider extracts source config.
// In: p OptionsProvider - config
// Out: Options - JetStream config
// Ex: FromOptionsProvider(opts) → Options{}
func FromOptionsProvider(p OptionsProvider) Options {
	opts := []Option{
		WithEndpoint(p.URL()),
		WithStreamName(p.StreamName()),
		WithCredsFile(p.CredsFile()),
		WithTLSCAFile(p.TLSCAFile()),
		WithBatchSize(p.BatchSize()),
		WithBatchTimeout(p.BatchTimeout()),
		WithFetchTimeout(p.FetchTimeout()),
	}

	return NewOptions(opts...)
}
