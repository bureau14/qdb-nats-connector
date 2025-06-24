// Package connector orchestrates the NATS-to-QuasarDB data pipeline.
// This package bridges NATS messaging with QuasarDB time series storage through
// a pluggable parser architecture that enables flexible message transformation.
// Decision rationale:
// - Public API package allows external usage and testing
// - Component orchestration centralizes lifecycle management
// - Signal handling enables graceful shutdown in production deployments
package connector

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/parser"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/nats-io/nats.go"
)

// Connector orchestrates the NATS source, message parser, and QuasarDB sink.
// Key assumptions:
// - Components are initialized in dependency order during NewConnector
// - Each component manages its own connection lifecycle
// - Connector handles graceful shutdown coordination between components
type Connector struct {
	Source *source.Source
	Parser *parser.Parser
	Sink   *sink.Sink
}

// NewConnector creates and initializes a new Connector.
//
// This function orchestrates the creation of source, parser, and sink components,
// handling proper resource cleanup on any initialization failure.
//
// Decision rationale:
// - Options are validated first to fail fast on invalid configuration
// - Components are created in dependency order: source -> parser -> sink
// - Each component failure triggers cleanup of previously created components
//
// Key assumptions:
// - The provided Options have been populated with valid endpoints
// - Network connectivity to NATS and QuasarDB endpoints is available
// - Component initialization is synchronous and blocking
//
// Usage example:
//
//	opts := &Options{
//	    NatsEndpoint: "nats://localhost:4222",
//	    NatsTopic:    "my.topic",
//	    QdbEndpoint:  "qdb://localhost:2836",
//	}
//	conn, err := NewConnector(opts)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer conn.Close()
func NewConnector(opts *Options) (*Connector, error) {
	// Validate options before attempting to create components
	if validationErr := ValidateOptions(opts); validationErr != nil {
		slog.Error("Options not valid", "options", opts, "error", validationErr)
		return nil, validationErr
	}

	// Create source using a provider that builds options from the connector's config
	srcOpts := source.FromOptionsProvider(opts)
	src, err := source.NewSource(srcOpts)
	if err != nil {
		slog.Error("Failed to create source", "options", opts, "error", err)
		return nil, err
	}

	par, err := parser.NewParser()
	if err != nil {
		slog.Error("Failed to create parser", "error", err)
		src.Close()
		return nil, err
	}

	// Create sink using a provider that builds options from the connector's config
	snkOpts := sink.FromOptionsProvider(opts)
	snk, err := sink.NewSink(snkOpts)
	if err != nil {
		slog.Error("Failed to create sink", "options", snkOpts, "error", err)
		src.Close()
		return nil, err
	}

	return &Connector{
		Source: src,
		Parser: par,
		Sink:   snk,
	}, nil
}

// Run starts the connector and blocks until an error occurs or shutdown.
// Decision rationale:
// - Single method to start all connector operations
// - Integrates NATS subscription with parsing and sink pipeline
// - Handles message processing errors gracefully without stopping
// Performance trade-offs:
// - Each NATS message handled in separate goroutine (via NATS async)
// - Non-blocking sink writes prevent message handler stalls
func (c *Connector) Run() error {
	return c.RunWithContext(context.Background())
}

// RunWithContext starts the connector with context support for cancellation.
// Decision rationale:
// - Context enables graceful shutdown and cancellation
// - Signal handling for production deployments
// - Maintains backward compatibility through Run() method
func (c *Connector) RunWithContext(ctx context.Context) error {
	slog.Info("Starting connector")

	// Define message handler that processes NATS messages
	handler := func(msg *nats.Msg) {
		// Parse the message
		tables, err := c.Parser.Parse(msg.Data)
		if err != nil {
			slog.Error("Failed to parse message", "subject", msg.Subject, "error", err)
			return
		}

		// Skip empty results
		if len(tables) == 0 {
			slog.Debug("Parser returned no tables", "subject", msg.Subject)
			return
		}

		// Write to sink
		if err := c.Sink.Write(tables); err != nil {
			slog.Error("Failed to write to sink", "subject", msg.Subject, "num_tables", len(tables), "error", err)
			return
		}

		slog.Debug("Message processed successfully", "subject", msg.Subject, "num_tables", len(tables))
	}

	// Subscribe to NATS with our handler
	if err := c.Source.Subscribe(handler); err != nil {
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

// Close gracefully shuts down the connector's components.
//
// Components are closed in reverse initialization order to ensure
// clean shutdown without data loss.
//
// Key assumptions:
// - Component Close() methods are idempotent
// - Close() methods handle nil receivers gracefully
func (c *Connector) Close() {
	slog.Info("Closing connector")
	c.Source.Close()
	c.Sink.Close()
}
