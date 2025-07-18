// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: NATS→QuasarDB pipeline orchestration
// Types: Connector, Worker, Options
// Ex: connector.New(opts).Run() → streams data
package connector

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector/resilience"
	"github.com/bureau14/qdb-nats-connector/internal/source"
)

// Connector: orchestrates NATS→QuasarDB pipeline, goroutine-safe
type Connector struct {
	source         *source.Source            // Single shared source
	workers        []*Worker                 // Worker pool
	workCh         chan *source.MessageBatch // Work distribution channel
	breakerManager *resilience.Manager
	wg             sync.WaitGroup
	cancel         context.CancelFunc
}

// NewConnector creates NATS→QuasarDB connector.
// Args:
//
//	opts: NATS/QuasarDB config & topic filters
//
// Returns:
//
//	*Connector: configured pipeline
//	error: invalid options/worker creation failed
//
// Example:
//
//	conn := NewConnector(&Options{...}) // → *Connector
func NewConnector(opts *Options) (*Connector, error) {
	// Validate config
	validationErr := validateOptions(opts)
	if validationErr != nil {
		slog.Error("Options not valid", "options", opts, "error", validationErr)

		return nil, validationErr
	}

	// Create circuit breaker manager
	var breakerManager *resilience.Manager
	if opts.CircuitBreakerShared {
		breakerManager = resilience.NewManager(
			resilience.WithDefaults(
				opts.CircuitBreakerFailureThreshold,
				opts.CircuitBreakerSuccessThreshold,
				opts.CircuitBreakerTimeout,
			),
			resilience.WithDefaultJitter(opts.CircuitBreakerJitterMax),
			resilience.WithDefaultHalfOpen(opts.CircuitBreakerHalfOpenBase, opts.CircuitBreakerHalfOpenMax),
			resilience.WithHookRegistry(opts.Hooks),
		)
	}

	// Create single shared source
	sourceOpts := source.FromOptionsProvider(opts)
	sourceOpts.ConsumerName = opts.ConsumerName()
	src, err := source.NewSource(sourceOpts)
	if err != nil {
		return nil, err
	}

	// Create buffered work channel (size = workers * 2)
	workCh := make(chan *source.MessageBatch, opts.GetWorkers()*2)

	// Create workers - multiple workers for horizontal scaling
	workers := make([]*Worker, opts.GetWorkers())

	for i := range opts.GetWorkers() {
		worker, err := NewWorker(i, opts, workCh, breakerManager)
		if err != nil {
			// Cleanup created workers
			for j := range i {
				_ = workers[j].shutdown()
			}
			src.Close()

			return nil, err
		}
		workers[i] = worker
	}

	return &Connector{
		source:         src,
		workers:        workers,
		workCh:         workCh,
		breakerManager: breakerManager,
	}, nil
}

// Run starts connector, blocks until shutdown/error.
// Args:
//
//	none
//
// Returns:
//
//	error: worker failure/context cancelled
//
// Example:
//
//	conn.Run() // blocks until SIGINT/error
func (c *Connector) Run() error {
	return c.RunWithContext(context.Background())
}

// RunWithContext runs connector with cancellation control.
// Args:
//
//	ctx: cancellation context
//
// Returns:
//
//	error: worker error/context cancelled
//
// Example:
//
//	RunWithContext(ctx) // blocks until ctx.Done()/error
func (c *Connector) RunWithContext(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	defer cancel()

	slog.Info("Starting connector with workers", "worker_count", len(c.workers))

	// Connect to source before starting workers
	err := c.source.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.source.Close()

	// Error channel for worker failures
	errCh := make(chan error, len(c.workers))

	// Start fetchLoop goroutine
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.fetchLoop(ctx)
	}()

	// Start workers
	for _, worker := range c.workers {
		c.wg.Add(1)
		go func(w *Worker) {
			defer c.wg.Done()
			err := w.Run(ctx)
			if err != nil && err != context.Canceled {
				errCh <- err
			}
		}(worker)
	}

	// Start health monitor
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.monitorWorkers(ctx)
	}()

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("Connector running, processing messages...")

	// Wait for error, signal, or cancellation
	select {
	case err := <-errCh:
		slog.Error("Worker error, shutting down", "error", err)
		cancel() // Stop all workers
		c.wg.Wait()

		return err
	case sig := <-sigCh:
		slog.Info("Received signal, shutting down", "signal", sig)
		cancel()
		c.wg.Wait()

		return nil
	case <-ctx.Done():
		slog.Info("Context cancelled, shutting down")
		c.wg.Wait()

		return ctx.Err()
	}
}

// Close gracefully shuts down connector & workers.
// Args:
//
//	none
//
// Returns:
//
//	none
//
// Example:
//
//	conn.Close() // cancels ctx, waits workers, frees resources
func (c *Connector) Close() {
	slog.Info("Closing connector")
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	for _, worker := range c.workers {
		_ = worker.shutdown()
	}
}

// monitorWorkers checks worker health every 30s
// In: ctx context.Context - cancellation
// Out: none - logs warnings
// Ex: monitorWorkers(ctx) → logs unhealthy
func (c *Connector) monitorWorkers(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, w := range c.workers {
				if !w.isHealthy() {
					slog.Warn("Worker unhealthy", "worker_id", w.id)
					// Workers auto-recover via consumer recreation
				}
			}
		}
	}
}

// fetchLoop continuously fetches message batches from the source and distributes them to workers
// In: ctx context.Context - cancellation
// Out: none - writes to workCh, closes it when done
// Ex: go c.fetchLoop(ctx) → distributes work to workers
func (c *Connector) fetchLoop(ctx context.Context) {
	defer close(c.workCh) // Creator closes channel

	// Exponential backoff for fetch errors
	backoffBase := 100 * time.Millisecond
	backoffMax := 5 * time.Second
	currentBackoff := backoffBase

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Fetch batch from source
			batch, err := c.source.FetchBatch(ctx)
			if err != nil {
				if ctx.Err() != nil {
					// Context was cancelled, exit gracefully
					return
				}
				// Log error and apply backoff
				slog.Error("Failed to fetch batch", "error", err, "backoff", currentBackoff)

				// Sleep with context check
				select {
				case <-time.After(currentBackoff):
					// Double backoff up to max
					currentBackoff *= 2
					if currentBackoff > backoffMax {
						currentBackoff = backoffMax
					}
				case <-ctx.Done():
					return
				}

				continue
			}

			// Reset backoff on success
			currentBackoff = backoffBase

			// Send batch to workers
			select {
			case c.workCh <- batch:
				// Successfully sent batch to worker
			case <-ctx.Done():
				// Context cancelled while sending
				return
			}
		}
	}
}
