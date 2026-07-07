// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package source: NATS JetStream connection & batch fetching
// Types: Source, Options, OptionsProvider, MessageBatch, MessageInfo
// Ex: source.NewSource(opts).FetchBatch(ctx) → messages flow
package source

import (
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"os"
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
// filterSubject: durable's FilterSubject resolved at Connect, reused by Rebind
type Source struct {
	NatsConn     *nats.Conn
	JetStream    nats.JetStreamContext
	Subscription *nats.Subscription
	Options      Options

	filterSubject string
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

// connectOptions builds nats.Options from source security config.
// In: opts Options - CredsFile/TLSCAFile ("" = unused)
// Out: []nats.Option, error - connect options or unreadable-file err
// Ex: connectOptions(opts) → [UserCredentials, RootCAs], nil
//
// Files are validated upfront: nats.go reads the creds file lazily during
// the auth handshake, so a missing file would otherwise surface as a
// confusing mid-handshake error instead of a clear config error.
func connectOptions(opts Options) ([]nats.Option, error) {
	natsOpts := []nats.Option{}

	if opts.CredsFile != "" {
		_, err := os.Stat(opts.CredsFile)
		if err != nil {
			return nil, errors.NewInvalidConfigError("source",
				fmt.Sprintf("credentials file not readable: %v", err))
		}
		natsOpts = append(natsOpts, nats.UserCredentials(opts.CredsFile))
	}

	if opts.TLSCAFile != "" {
		_, err := os.Stat(opts.TLSCAFile)
		if err != nil {
			return nil, errors.NewInvalidConfigError("source",
				fmt.Sprintf("TLS CA file not readable: %v", err))
		}
		natsOpts = append(natsOpts, nats.RootCAs(opts.TLSCAFile))
	}

	return natsOpts, nil
}

// consumerFilterSubject resolves the subject to bind the pull subscription
// with. nats.go rejects a bind whose subject differs from an existing
// durable's FilterSubject, so the filter is read from the consumer itself.
// A missing durable is a SubscriptionFailed error (wrapping
// nats.ErrConsumerNotFound): the connector never auto-creates consumers,
// because an auto-created one would pull the WHOLE stream unfiltered.
// In: js nats.JetStreamContext, stream/consumer - names to look up
// Out: string, error - durable's FilterSubject; "" when unfiltered
// Ex: consumerFilterSubject(js, "EVENTS", "qdb-connector") → "events.*.value", nil
func consumerFilterSubject(js nats.JetStreamContext, stream, consumer string) (string, error) {
	ci, err := js.ConsumerInfo(stream, consumer)
	if err != nil {
		return "", errors.NewSubscriptionFailedError("source", consumer, err)
	}

	return ci.Config.FilterSubject, nil
}

// Connect establishes the NATS connection and binds to a pre-created
// durable JetStream consumer. A missing durable fails loudly (no
// auto-creation): pre-create it with a FilterSubject, e.g.
// `nats consumer add <stream> <consumer> --pull --filter <subject>`.
// In: ctx context.Context - cancellation
// Out: error - nil or connection/subscription failure
// Ex: Connect(ctx) → nil (connected+subscribed)
func (s *Source) Connect(ctx context.Context) error {
	natsOpts, err := connectOptions(s.Options)
	if err != nil {
		return err
	}

	slog.Info("Establishing NATS connection",
		"creds_file", s.Options.CredsFile,
		"tls_ca_file", s.Options.TLSCAFile)
	nc, err := nats.Connect(s.Options.Endpoint, natsOpts...)
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

	filterSubject, err := consumerFilterSubject(js, s.Options.StreamName, s.Options.ConsumerName)
	if err != nil {
		slog.Error("Failed to resolve consumer filter subject; "+
			"the durable consumer must be pre-created with a FilterSubject "+
			"(nats consumer add) before starting the connector",
			"stream", s.Options.StreamName,
			"consumer", s.Options.ConsumerName,
			"error", err)

		return err
	}
	s.filterSubject = filterSubject

	// Create pull subscription
	slog.Info("Creating JetStream pull subscription",
		"stream", s.Options.StreamName,
		"consumer", s.Options.ConsumerName,
		"filter_subject", filterSubject)

	sub, err := s.bindPullSubscription()
	if err != nil {
		slog.Error("Failed to create pull subscription", "error", err)

		return err
	}
	s.Subscription = sub

	slog.Info("JetStream source connected successfully")

	return nil
}

// Rebind replaces a dead pull subscription by re-binding to the pre-created
// durable using the filterSubject stored at Connect. Never creates consumers
// (nats.Bind); the NATS client reconnects the connection layer itself, this
// only replaces the dead pull subscription object. Single-owner: must be
// called from the fetch goroutine only.
// In: ctx context.Context - cancellation
// Out: error - nil on success, SubscriptionFailed while durable is absent
// Ex: Rebind(ctx) → nil (subscription replaced)
func (s *Source) Rebind(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if s.Subscription != nil {
		// Best-effort teardown of the dead subscription object.
		_ = s.Subscription.Unsubscribe()
	}

	sub, err := s.bindPullSubscription()
	if err != nil {
		return err
	}
	s.Subscription = sub

	return nil
}

// isNoData reports whether a Fetch error means "no messages this round":
// nothing arrived within MaxWait (steady state when caught up). This is the
// only benign fetch error; a healthy-but-idle stream produces it forever.
func isNoData(err error) bool {
	return stderrors.Is(err, nats.ErrTimeout)
}

// isSubscriptionDead reports whether a Fetch error means the pull
// subscription no longer delivers and must be re-bound:
//   - The consumer was deleted: ErrConsumerDeleted (409 on an in-flight
//     pull) then ErrNoResponders (503 on every later pull).
//   - The pull subscription was torn down: invalid/closed/bad subscription.
//   - The connection was closed/draining outside graceful shutdown (the
//     fetch loop exits on context cancellation before these can surface
//     during shutdown).
//
// These must never be masked as empty batches: a dead subscription returns
// quickly and forever, which is indistinguishable from a quiet stream.
func isSubscriptionDead(err error, sub *nats.Subscription) bool {
	if sub != nil && !sub.IsValid() {
		return true
	}

	if stderrors.Is(err, nats.ErrConsumerDeleted) ||
		stderrors.Is(err, nats.ErrNoResponders) ||
		stderrors.Is(err, nats.ErrConsumerNotFound) ||
		stderrors.Is(err, nats.ErrBadSubscription) ||
		stderrors.Is(err, nats.ErrSubscriptionClosed) ||
		stderrors.Is(err, nats.ErrConnectionClosed) ||
		stderrors.Is(err, nats.ErrConnectionDraining) {
		return true
	}

	// Fallback: the JetStream Fetch path combines these into an error whose
	// chain errors.Is does not reliably traverse; match the well-known
	// messages directly so a dead subscription is never misclassified.
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
		// No data within MaxWait is the steady state when caught up --
		// report an empty batch so the fetch loop keeps polling quietly.
		if isNoData(err) {
			return &MessageBatch{Messages: []MessageInfo{}}, nil
		}

		// A dead pull subscription returns quickly and forever; surface it
		// as a typed error so the fetch loop can re-bind to the durable.
		if isSubscriptionDead(err, s.Subscription) {
			return nil, errors.NewSubscriptionFailedError("source", s.Options.ConsumerName, err)
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

// bindPullSubscription binds a pull subscription to the pre-created durable.
// nats.Bind (not BindStream) is essential: with only BindStream, nats.go
// auto-creates a missing durable on PullSubscribe -- violating the
// never-create contract -- whereas Bind surfaces ErrConsumerNotFound.
// In: none - uses JetStream context + filterSubject stored at Connect
// Out: *nats.Subscription, error - bound subscription or SubscriptionFailed
// Ex: bindPullSubscription() → sub, nil
func (s *Source) bindPullSubscription() (*nats.Subscription, error) {
	sub, err := s.JetStream.PullSubscribe(s.filterSubject, s.Options.ConsumerName,
		nats.Bind(s.Options.StreamName, s.Options.ConsumerName),
	)
	if err != nil {
		return nil, errors.NewSubscriptionFailedError("source", s.Options.ConsumerName, err)
	}

	return sub, nil
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
