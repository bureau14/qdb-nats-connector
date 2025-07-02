// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package source: NATS connection & subscriptions
// Types: Source, Options, OptionsProvider
// Ex: source.NewSource(opts).Subscribe(handler) → messages flow
package source

import (
	"log/slog"

	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
)

// Source: NATS client with connection & subscription lifecycle management
type Source struct {
	NatsConn *nats.Conn
	Options  Options
}

// NewSource connects to NATS server.
// Args:
//
//	opts: Options - endpoint & topic config
//
// Returns:
//
//	*Source: connected NATS client
//	error: connection fails
//
// Example:
//
//	NewSource(opts) // → source, nil
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

// Close drains & closes NATS connection.
// Approach: drain→close for graceful shutdown
// 1. Drain - flush pending messages
// 2. Close - terminate connection
// Ex: Close() → connection closed gracefully
func (s *Source) Close() {
	slog.Info("Draining NATS source")
	// 1. Drain: wait for pending messages
	err := s.NatsConn.Drain()
	if err != nil {
		slog.Error("Failed to drain NATS connection", "error", err)
	}

	slog.Info("Closing NATS source")
	// 2. Close: terminate connection
	s.NatsConn.Close()
}

// Subscribe registers handler for topic.
// In: handler nats.MsgHandler - concurrent-safe func
// Out: error - subscription failure
// Ex: Subscribe(handleMsg) → nil
func (s *Source) Subscribe(handler nats.MsgHandler) error {
	slog.Info("Subscribing to topic", "topic", s.Options.Topic)
	_, err := s.NatsConn.Subscribe(s.Options.Topic, handler)
	if err != nil {
		slog.Error("Failed to subscribe to topic", "error", err, "topic", s.Options.Topic)

		return errors.NewSubscriptionFailedError("source", s.Options.Topic, err)
	}

	return nil
}
