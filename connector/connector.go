// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: NATS→QuasarDB pipeline orchestrator
// Types: Connector, Options
// Ex: connector.NewConnector(opts, parser).Run() → data flows
package connector

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/parser"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/nats-io/nats.go"
)

// Connector: NATS→QDB pipeline orchestrator, handles source/parser/sink lifecycle
type Connector struct {
	Source *source.Source
	Parser parser.Parser
	Sink   *sink.Sink
}

// NewConnector creates NATS→QDB pipeline.
// Args:
//
//	opts: *Options - endpoints & credentials
//	p: Parser - message transformer
//
// Returns:
//
//	*Connector: pipeline orchestrator
//	error: validation/connection fails
//
// Example:
//
//	NewConnector(opts, jsonParser) // → connector, nil
func NewConnector(opts *Options, p parser.Parser) (*Connector, error) {
	// Approach: validate→create in order, cleanup on fail
	// 1. Validate config - fail fast
	// 2. Create source - NATS connection
	// 3. Create sink - QDB connection

	// 1. Validate: opts & parser required
	if validationErr := ValidateOptions(opts); validationErr != nil {
		slog.Error("Options not valid", "options", opts, "error", validationErr)
		return nil, validationErr
	}

	if p == nil {
		return nil, errors.NewInvalidConfigError("connector", "parser cannot be nil")
	}

	// 2. Create source: NATS subscriber
	srcOpts := source.FromOptionsProvider(opts)
	src, err := source.NewSource(srcOpts)
	if err != nil {
		slog.Error("Failed to create source", "options", opts, "error", err)
		return nil, err
	}

	// 3. Create sink: QDB writer with cleanup
	snkOpts := sink.FromOptionsProvider(opts)
	snk, err := sink.NewSink(snkOpts)
	if err != nil {
		slog.Error("Failed to create sink", "options", snkOpts, "error", err)
		src.Close()
		return nil, err
	}

	return &Connector{
		Source: src,
		Parser: p,
		Sink:   snk,
	}, nil
}

// Run starts connector & blocks until shutdown.
// Out: error - subscription failure
// Ex: Run() → nil
func (c *Connector) Run() error {
	return c.RunWithContext(context.Background())
}

// RunWithContext runs connector with cancellation.
// Args:
//
//	ctx: context.Context - cancellation context
//
// Returns:
//
//	error: subscription/context errors
//
// Example:
//
//	RunWithContext(ctx) // blocks until SIGINT/cancel
func (c *Connector) RunWithContext(ctx context.Context) error {
	slog.Info("Starting connector")

	// Subscribe to NATS with our message handler
	if err := c.Source.Subscribe(c.handleMessage); err != nil {
		return errors.NewSubscriptionFailedError("connector", "unknown", err)
	}

	slog.Info("Connector running, processing messages...")

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait for context cancellation or interrupt signal
	select {
	case <-ctx.Done():
		slog.Info("Context cancelled, shutting down")
		return ctx.Err()
	case sig := <-sigCh:
		slog.Info("Received signal, shutting down", "signal", sig)
		return nil
	}
}

// handleMessage parses NATS msg→QDB tables
// In: msg *nats.Msg - raw message
// Approach: parse→convert→write, skip ∅
// 1. Parse msg - get tables
// 2. Convert to pointers - sink needs []*Table
// 3. Write to QDB - log errors & continue
func (c *Connector) handleMessage(msg *nats.Msg) {
	// 1. Parse msg: extract timeseries data
	tables, err := c.Parser.Parse(msg)
	if err != nil {
		slog.Error("Failed to parse message", "subject", msg.Subject, "error", err)
		return
	}

	if len(tables) == 0 {
		slog.Debug("Parser returned no tables", "subject", msg.Subject)
		return
	}

	// 2. Convert to pointers: []Table→[]*Table
	writerTables := make([]*qdb.WriterTable, len(tables))
	for i := range tables {
		writerTables[i] = &tables[i]
	}

	// 3. Write to QDB: errors logged, not fatal
	if err := c.Sink.Write(writerTables); err != nil {
		slog.Error("Failed to write to sink", "subject", msg.Subject, "num_tables", len(tables), "error", err)
		return
	}

	slog.Debug("Message processed successfully", "subject", msg.Subject, "num_tables", len(tables))
}

// Close shuts down source→sink order.
// Ex: Close() → components closed
func (c *Connector) Close() {
	slog.Info("Closing connector")
	c.Source.Close()
	c.Sink.Close()
}
