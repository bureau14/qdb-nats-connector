// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package source: NATS JetStream connection & batch fetching
// Types: Source, Options, OptionsProvider, MessageBatch, MessageInfo
// Ex: source.NewSource(opts).FetchBatch(ctx) → messages flow
package source

import (
	"context"
	stderrors "errors"
	"log/slog"
	"strings"

	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
)

// Source: JetStream client that pulls message batches. Needed for reliable message consumption.
// Who: connector creates, workers consume.
// NatsConn: underlying NATS connection
// JetStream: JS context for pull subscriptions
// Subscription: active pull subscription
// Options: connection & batch configuration
type Source struct {
	NatsConn     *nats.Conn
	JetStream    nats.JetStreamContext
	Subscription *nats.Subscription
	Options      Options
}

// MessageInfo: NATS message with sequence number. Needed for selective ACK/NACK operations.
// Who: Source creates, parsers consume.
// Msg: raw NATS message with data & metadata
// Sequence: JetStream sequence for acknowledgment
type MessageInfo struct {
	Msg      *nats.Msg
	Sequence uint64
}

// MessageBatch: message collection with ACK control. Needed for batch processing with acknowledgment.
// Who: Source creates via FetchBatch, connector processes.
// Messages: fetched messages with sequences
// AckFunc: acknowledges messages by sequence
// NackFunc: negative acknowledges by sequence
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
	// Check if connection exists before attempting to close
	if s.NatsConn == nil {
		slog.Info("NATS source already closed or not connected")

		return
	}

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
	slog.Info("Establishing NATS connection")
	nc, err := nats.Connect(s.Options.Endpoint)
	if err != nil {
		slog.Error("Failed to establish NATS connection", "error", err)

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
		"consumer", s.Options.ConsumerName)

	sub, err := js.PullSubscribe("", s.Options.ConsumerName,
		nats.BindStream(s.Options.StreamName),
	)
	if err != nil {
		slog.Error("Failed to create pull subscription", "error", err)

		return errors.NewSubscriptionFailedError("source", s.Options.ConsumerName, err)
	}
	s.Subscription = sub

	slog.Info("JetStream source connected successfully")

	return nil
}

// isExpectedFetchErr reports whether a pull-subscription Fetch error is a normal
// no-data or post-drain lifecycle condition rather than a real failure. A
// long-running puller must treat these as "no messages this round", not abort:
//   - ErrTimeout: nothing arrived within MaxWait (steady state when caught up).
//   - The pull subscription was torn down after the stream drained, after which
//     Fetch reports an invalid/closed subscription (IsValid() == false).
//   - The connection is closing/draining (graceful shutdown).
//
// Note: this does not re-establish a dropped subscription; it only keeps the
// connector healthy. Automatic re-subscription is left for a follow-up.
func isExpectedFetchErr(err error, sub *nats.Subscription) bool {
	if err == nats.ErrTimeout {
		return true
	}
	if sub != nil && !sub.IsValid() {
		return true
	}

	// ErrBadSubscription / ErrSubscriptionClosed are reported by Fetch at the
	// JetStream layer once the pull subscription is torn down after the stream
	// drains -- even while the core subscription still reports IsValid().
	if stderrors.Is(err, nats.ErrBadSubscription) ||
		stderrors.Is(err, nats.ErrSubscriptionClosed) ||
		stderrors.Is(err, nats.ErrConnectionClosed) ||
		stderrors.Is(err, nats.ErrConnectionDraining) {
		return true
	}

	// Fallback: the JetStream Fetch path combines these into an error whose
	// chain errors.Is does not reliably traverse; match the well-known benign
	// messages directly so a drained subscription is never treated as fatal.
	msg := err.Error()

	return strings.Contains(msg, "invalid subscription") ||
		strings.Contains(msg, "subscription closed")
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

	msgs, err := s.Subscription.Fetch(s.Options.BatchSize,
		nats.MaxWait(s.Options.BatchTimeout))
	if err != nil {
		// No-data and post-drain lifecycle conditions are expected for a
		// long-running puller and must not be fatal -- report them as an empty
		// batch so the fetch loop keeps polling without logging an error.
		if isExpectedFetchErr(err, s.Subscription) {
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

// ackMessages acknowledges messages by sequence number.
// In: messageInfos []MessageInfo - messages, sequences []uint64 - to ACK
// Out: error if any ACK fails
// Ex: ackMessages(msgs, []uint64{1,2,3}) → nil
func (s *Source) ackMessages(messageInfos []MessageInfo, sequences []uint64) error {
	// Build map for O(1) lookup
	msgMap := make(map[uint64]*MessageInfo, len(messageInfos))
	for i := range messageInfos {
		msgMap[messageInfos[i].Sequence] = &messageInfos[i]
	}

	// ACK messages by sequence
	for _, seq := range sequences {
		if msgInfo, ok := msgMap[seq]; ok {
			err := msgInfo.Msg.Ack()
			if err != nil {
				slog.Error("Failed to ACK message", "sequence", seq, "error", err)

				return err
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
	// Build map for O(1) lookup
	msgMap := make(map[uint64]*MessageInfo, len(messageInfos))
	for i := range messageInfos {
		msgMap[messageInfos[i].Sequence] = &messageInfos[i]
	}

	// NACK messages by sequence
	for _, seq := range sequences {
		if msgInfo, ok := msgMap[seq]; ok {
			err := msgInfo.Msg.Nak()
			if err != nil {
				slog.Error("Failed to NACK message", "sequence", seq, "error", err)

				return err
			}
		}
	}

	return nil
}
