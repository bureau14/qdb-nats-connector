// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package source: NATS JetStream connection & batch fetching
// Types: Source, Options, OptionsProvider, MessageBatch, MessageInfo
// Ex: source.NewSource(opts).FetchBatch(ctx) → messages flow
package source

import (
	"context"
	"log/slog"

	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
)

// Source: NATS JetStream client with batch fetching capabilities
type Source struct {
	NatsConn     *nats.Conn
	JetStream    nats.JetStreamContext
	Subscription *nats.Subscription
	Options      Options
}

// MessageInfo: wrapper for NATS message with sequence tracking
type MessageInfo struct {
	Msg      *nats.Msg
	Sequence uint64
}

// MessageBatch: collection of messages with acknowledgment functions
type MessageBatch struct {
	Messages []MessageInfo
	AckFunc  func([]uint64) error
	NackFunc func([]uint64) error
}

// NewSource validates config, defers connection to Connect().
// In: opts Options - endpoint/stream/consumer required
// Out: *Source, error - unconnected source or validation err
// Ex: NewSource(opts) → &Source{Options:opts}, nil
func NewSource(opts Options) (*Source, error) {
	// Validate required fields
	if opts.Endpoint == "" {
		return nil, errors.NewInvalidConfigError("source", "endpoint is required")
	}
	if opts.StreamName == "" {
		return nil, errors.NewInvalidConfigError("source", "stream name is required")
	}
	if opts.ConsumerName == "" {
		return nil, errors.NewInvalidConfigError("source", "consumer name is required")
	}

	return &Source{Options: opts}, nil
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

// Connect establishes NATS connection, creates JetStream consumer.
// In: ctx context.Context - cancellation
// Out: error - nil or connection/subscription failure
// Ex: Connect(ctx) → nil (connected+subscribed)
func (s *Source) Connect(ctx context.Context) error {
	slog.Info("Establishing connection with NATS endpoint", "nats_endpoint", s.Options.Endpoint)
	nc, err := nats.Connect(s.Options.Endpoint)
	if err != nil {
		slog.Error("Error while establishing connection", "nats_endpoint", s.Options.Endpoint, "error", err)

		return errors.NewConnectionFailedError("source", s.Options.Endpoint, err)
	}
	s.NatsConn = nc

	// Create JetStream context
	slog.Info("Creating JetStream context")
	js, err := nc.JetStream()
	if err != nil {
		slog.Error("Failed to create JetStream context", "error", err)

		return errors.NewConnectionFailedError("source", "JetStream", err)
	}
	s.JetStream = js

	// Create pull subscription
	slog.Info("Creating JetStream pull subscription",
		"stream", s.Options.StreamName,
		"consumer", s.Options.ConsumerName,
		"subject", s.Options.Topic)

	sub, err := js.PullSubscribe(s.Options.Topic, s.Options.ConsumerName,
		nats.BindStream(s.Options.StreamName),
		nats.AckWait(s.Options.AckWait),
		nats.MaxDeliver(s.Options.MaxDeliver),
	)
	if err != nil {
		slog.Error("Failed to create pull subscription", "error", err)

		return errors.NewSubscriptionFailedError("source", s.Options.Topic, err)
	}
	s.Subscription = sub

	slog.Info("JetStream source connected successfully")

	return nil
}

// FetchBatch pulls messages from JetStream with ACK/NACK funcs.
// In: ctx context.Context - timeout/cancellation
// Out: *MessageBatch, error - messages+funcs or fetch err
// Ex: FetchBatch(ctx) → &MessageBatch{msgs,ack,nack}, nil
func (s *Source) FetchBatch(ctx context.Context) (*MessageBatch, error) {
	if s.Subscription == nil {
		return nil, errors.NewConnectionFailedError("source", "fetch",
			nats.ErrInvalidConnection)
	}

	// Fetch messages using JetStream pull
	msgs, err := s.Subscription.Fetch(s.Options.BatchSize,
		nats.MaxWait(s.Options.BatchTimeout))
	if err != nil {
		// Timeout is expected when no messages available
		if err == nats.ErrTimeout {
			return &MessageBatch{Messages: []MessageInfo{}}, nil
		}

		return nil, errors.NewConnectionFailedError("source", "fetch", err)
	}

	// Convert to MessageInfo slice
	messageInfos := make([]MessageInfo, len(msgs))
	for i, msg := range msgs {
		// Extract sequence from JetStream metadata
		meta, err := msg.Metadata()
		var sequence uint64
		if err == nil {
			sequence = meta.Sequence.Stream
		} else if i >= 0 {
			// Fallback: use message index if metadata unavailable
			sequence = uint64(i)
		}

		messageInfos[i] = MessageInfo{
			Msg:      msg,
			Sequence: sequence,
		}
	}

	// Create batch with acknowledgment functions
	batch := &MessageBatch{
		Messages: messageInfos,
		AckFunc: func(sequences []uint64) error {
			return s.ackMessages(messageInfos, sequences)
		},
		NackFunc: func(sequences []uint64) error {
			return s.nackMessages(messageInfos, sequences)
		},
	}

	return batch, nil
}

// Deprecated: Use FetchBatch instead for JetStream
// Subscribe registers handler for topic (legacy NATS Core).
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

// ackMessages acknowledges messages by sequence number.
// In: messageInfos []MessageInfo - messages, sequences []uint64 - to ACK
// Out: error if any ACK fails
// Ex: ackMessages(msgs, []uint64{1,2,3}) → nil
func (s *Source) ackMessages(messageInfos []MessageInfo, sequences []uint64) error {
	for _, seq := range sequences {
		for _, msgInfo := range messageInfos {
			if msgInfo.Sequence == seq {
				err := msgInfo.Msg.Ack()
				if err != nil {
					slog.Error("Failed to ACK message", "sequence", seq, "error", err)

					return err
				}

				break
			}
		}
	}

	return nil
}

// nackMessages negatively acknowledges messages by sequence number.
// In: messageInfos []MessageInfo - messages, sequences []uint64 - to NACK
// Out: error if any NACK fails
// Ex: nackMessages(msgs, []uint64{4,5}) → nil
func (s *Source) nackMessages(messageInfos []MessageInfo, sequences []uint64) error {
	for _, seq := range sequences {
		for _, msgInfo := range messageInfos {
			if msgInfo.Sequence == seq {
				err := msgInfo.Msg.Nak()
				if err != nil {
					slog.Error("Failed to NACK message", "sequence", seq, "error", err)

					return err
				}

				break
			}
		}
	}

	return nil
}
