// Package source manages NATS connections and message subscriptions.
// This internal package handles connection lifecycle, automatic reconnection,
// and message delivery to the processing pipeline.
// Decision rationale:
// - Internal package isolates NATS-specific logic
// - Supports graceful shutdown with Drain()
// - Automatic reconnection for resilience
package source

import (
	"log/slog"

	"github.com/nats-io/nats.go"
	
	"github.com/bureau14/qdb-nats-connector/internal/errors"
)

// Source manages the NATS client connection and subscription lifecycle.
// Key assumptions:
// - NATS server is reachable at configured endpoint
// - Topic exists or will be auto-created
// - Connection supports automatic reconnection
type Source struct {
	NatsConn *nats.Conn
	Options  Options
}

// NewSource establishes a connection to a NATS server based on the provided options.
// Decision rationale:
// - Connects synchronously to fail fast on invalid endpoints
// - Uses default NATS options for automatic reconnection
// - Logs connection attempts for troubleshooting
// Performance trade-offs:
// - Synchronous connection blocks startup but ensures valid config
// - Automatic reconnection adds minimal overhead
func NewSource(opts Options) (*Source, error) {
	slog.Info("Establishing connection with NATS endpoint", "nats_endpoint", opts.Endpoint)
	nc, err := nats.Connect(opts.Endpoint)
	if err != nil {
		slog.Error("Error while establishing connection", "nats_endpoint", opts.Endpoint, "error", err)
		return nil, errors.NewConnectionFailedError("source", opts.Endpoint, err)
	}

	slog.Info("Connected to NATS endpoint", "topic", opts.Topic)
	return &Source{NatsConn: nc, Options: opts}, nil
}

// Close gracefully shuts down the NATS connection.
// Key assumptions:
// - Drain() waits for pending messages to complete
// - Close() is called after Drain() completes
// - Method is idempotent
// Decision rationale:
// - Drain ensures no message loss during shutdown
// - Two-phase shutdown (drain then close) maximizes reliability
func (s *Source) Close() {
	slog.Info("Draining NATS source")
	s.NatsConn.Drain()

	slog.Info("Closing NATS source")
	s.NatsConn.Close()
}

// Subscribe sets up a subscription to the configured NATS topic.
//
// It uses the topic from the source's options and registers the provided
// message handler to process incoming messages.
//
// Key assumptions:
// - The NATS connection has been established and is healthy.
// - The provided handler is safe for concurrent execution.
func (s *Source) Subscribe(handler nats.MsgHandler) error {
	slog.Info("Subscribing to topic", "topic", s.Options.Topic)
	_, err := s.NatsConn.Subscribe(s.Options.Topic, handler)
	if err != nil {
		slog.Error("Failed to subscribe to topic", "error", err, "topic", s.Options.Topic)
		return errors.NewSubscriptionFailedError("source", s.Options.Topic, err)
	}
	return nil
}
