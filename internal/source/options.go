package source

// Option is a function that configures an Options object.
// Decision rationale:
// - Functional options pattern for flexible configuration
// - Immutable by returning new Options struct
type Option func(Options) Options

// Options configures the NATS source connection parameters.
// Key assumptions:
// - Endpoint includes protocol (nats:// or tls://)
// - Topic supports wildcards (* and >)
// - Empty endpoint defaults to nats://localhost:4222
type Options struct {
	Endpoint string `json:"endpoint"`
	Topic    string `json:"topic"`
}

// NewOptions creates a new Options object and serves as the entrypoint for the
// builder pattern.
//
// Usage example:
//
//	opts := source.NewOptions(
//	           source.WithEndpoint("nats://127.0.0.1:4222"),
//	           source.WithTopic("my.topic"),
//	         )
func NewOptions(opts ...Option) Options {
	options := Options{}
	for _, opt := range opts {
		options = opt(options)
	}
	return options
}

// WithEndpoint configures the NATS endpoint for the source.
func WithEndpoint(endpoint string) Option {
	return func(o Options) Options {
		o.Endpoint = endpoint
		return o
	}
}

// WithTopic configures the NATS topic for the source.
func WithTopic(topic string) Option {
	return func(o Options) Options {
		o.Topic = topic
		return o
	}
}

// OptionsProvider defines an interface for providing source options.
// This allows decoupling the connector's options from the source's options,
// making the configuration scalable.
type OptionsProvider interface {
	Endpoint() string
	Topic() string
}

// FromOptionsProvider creates a new Options object from a provider.
//
// Decision rationale:
// - Uses the builder pattern to construct the Options object.
func FromOptionsProvider(p OptionsProvider) Options {
	opts := []Option{
		WithEndpoint(p.Endpoint()),
		WithTopic(p.Topic()),
	}
	return NewOptions(opts...)
}
