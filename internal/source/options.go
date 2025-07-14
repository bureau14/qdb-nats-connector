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
// BatchSize-MaxDeliver: message fetching & acknowledgment policies
type Options struct {
	Endpoint     string        `json:"endpoint"`
	Topic        string        `json:"topic"`
	StreamName   string        `json:"stream_name"`
	ConsumerName string        `json:"consumer_name"`
	BatchSize    int           `json:"batch_size"`
	BatchTimeout time.Duration `json:"batch_timeout"`
	FetchTimeout time.Duration `json:"fetch_timeout"`
	AckWait      time.Duration `json:"ack_wait"`
	MaxDeliver   int           `json:"max_deliver"`
}

// NewOptions applies options to JetStream defaults.
// In: opts ...Option - functional options
// Out: Options - batch=100, timeout=1s, ack=30s
// Ex: NewOptions(WithEndpoint("nats://localhost")) → Options{}
func NewOptions(opts ...Option) Options {
	// Set defaults for JetStream
	options := Options{
		BatchSize:    100,
		BatchTimeout: time.Second,
		FetchTimeout: 5 * time.Second,
		AckWait:      30 * time.Second,
		MaxDeliver:   3,
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

// WithTopic sets NATS subscription subject.
// In: topic string - supports wildcards
// Out: Option
// Ex: WithTopic("data.*")
func WithTopic(topic string) Option {
	return func(o Options) Options {
		o.Topic = topic

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

// WithAckWait sets message ACK timeout.
// In: timeout time.Duration - ACK timeout
// Out: Option
// Ex: WithAckWait(30*time.Second)
func WithAckWait(timeout time.Duration) Option {
	return func(o Options) Options {
		o.AckWait = timeout

		return o
	}
}

// WithMaxDeliver sets max redelivery count.
// In: maxDeliver int - max attempts
// Out: Option
// Ex: WithMaxDeliver(3)
func WithMaxDeliver(maxDeliver int) Option {
	return func(o Options) Options {
		o.MaxDeliver = maxDeliver

		return o
	}
}

// OptionsProvider: interface for source config decoupling from connector
type OptionsProvider interface {
	// URL returns NATS server URL (e.g., "nats://host:4222")
	URL() string
	// StreamName returns JetStream stream name for subscription
	StreamName() string
	// BatchSize returns max messages per fetch operation
	BatchSize() int
	// BatchTimeout returns max wait time for batch completion
	BatchTimeout() time.Duration
	// FetchTimeout returns overall timeout for fetch operation
	FetchTimeout() time.Duration
	// AckWait returns time before message redelivery
	AckWait() time.Duration
	// MaxDeliver returns max delivery attempts before message is dead-lettered
	MaxDeliver() int
}

// FromOptionsProvider extracts source config with topic override.
// In: p OptionsProvider - config, topicFilter string - worker topic
// Out: Options - JetStream config with custom topic
// Ex: FromOptionsProvider(opts, "sensors.*") → Options{topic:"sensors.*"}
func FromOptionsProvider(p OptionsProvider, topicFilter string) Options {
	opts := []Option{
		WithEndpoint(p.URL()),
		WithTopic(topicFilter),
		WithStreamName(p.StreamName()),
		WithBatchSize(p.BatchSize()),
		WithBatchTimeout(p.BatchTimeout()),
		WithFetchTimeout(p.FetchTimeout()),
		WithAckWait(p.AckWait()),
		WithMaxDeliver(p.MaxDeliver()),
	}

	return NewOptions(opts...)
}
