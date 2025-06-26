// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package source: NATS connection & subscriptions
// Types: Source, Options, OptionsProvider
// Ex: source.NewSource(opts).Subscribe(handler) → messages flow
package source

// Option: functional option for source configuration
type Option func(Options) Options

// Options: NATS connection config with endpoint & topic
type Options struct {
	Endpoint string `json:"endpoint"`
	Topic    string `json:"topic"`
}

// NewOptions creates source config.
// Args:
//
//	opts: ...Option - functional option setters
//
// Returns:
//
//	Options: NATS endpoint & topic config
//
// Example:
//
//	NewOptions(WithEndpoint("nats://localhost:4222")) // → opts
func NewOptions(opts ...Option) Options {
	options := Options{}
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

// OptionsProvider: interface for source config decoupling from connector
type OptionsProvider interface {
	Endpoint() string
	Topic() string
}

// FromOptionsProvider builds Options from provider.
// In: p OptionsProvider - config source
// Out: Options - NATS config
// Ex: FromOptionsProvider(connectorOpts) → sourceOpts
func FromOptionsProvider(p OptionsProvider) Options {
	opts := []Option{
		WithEndpoint(p.Endpoint()),
		WithTopic(p.Topic()),
	}
	return NewOptions(opts...)
}
