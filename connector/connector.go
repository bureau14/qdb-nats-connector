// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: NATS→QuasarDB pipeline orchestration
// Types: Connector, Worker, Options
// Ex: connector.New(opts).Run() → streams data
package connector

import (
	"context"
	stderrors "errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector/resilience"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/source"
)

const (
	// fetchBackoffBase/Max: exponential backoff bounds for transient
	// fetch errors in fetchLoop.
	fetchBackoffBase = 100 * time.Millisecond
	fetchBackoffMax  = 5 * time.Second
	// idleSleep: pause after an empty fetch so a fast-returning empty
	// batch does not busy-spin the loop.
	idleSleep = 100 * time.Millisecond

	// rebindBackoffBase/Max/MaxAttempts: retry ladder for re-binding a
	// dead pull subscription to the pre-created durable (1s doubling to
	// 32s, ~3.2 min worst case). Past the budget the connector fails
	// fast with MaxRetriesExceeded instead of stalling silently.
	rebindBackoffBase = time.Second
	rebindBackoffMax  = 32 * time.Second
	rebindMaxAttempts = 10
)

// Connector: orchestrates NATS→QuasarDB pipeline, goroutine-safe
type Connector struct {
	source         *source.Source            // Single shared source
	workers        []*Worker                 // Worker pool
	workCh         chan *source.MessageBatch // Work distribution channel
	breakerManager *resilience.Manager
	wg             sync.WaitGroup
	cancel         context.CancelFunc

	// Health: fetch-loop liveness probe (fetch + dispatch stages),
	// transition-logged health state, and the stuck threshold shared by
	// all probes. Re-bind retry parameters are fields (not constants at
	// use sites) so in-package tests can shrink them. natsReady gates
	// NatsConnected: it orders source.Connect's NatsConn write before
	// reads from HTTP handler goroutines.
	fetchProbe     Probe
	health         *healthState
	natsReady      atomic.Bool
	stuckThreshold time.Duration
	rebindBase     time.Duration
	rebindMax      time.Duration
	rebindAttempts int
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
		health:         newHealthState(slog.Default()),
		stuckThreshold: opts.HealthStuckThreshold,
		rebindBase:     rebindBackoffBase,
		rebindMax:      rebindBackoffMax,
		rebindAttempts: rebindMaxAttempts,
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
	c.natsReady.Store(true)

	// Arm fetch liveness so a connector that never completes a single
	// fetch is flagged once the stuck threshold elapses.
	c.health.recordFetchSuccess()

	// Error channel for worker failures + one fatal fetch-loop error
	errCh := make(chan error, len(c.workers)+1)

	// Start fetchLoop goroutine
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.fetchLoop(ctx, errCh)
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
	monitor := c.newHealthMonitor()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		monitor.run(ctx)
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

// fetchLoop continuously fetches message batches from the source and
// distributes them to workers. A dead pull subscription triggers a bounded
// re-bind loop; exhausting the budget is a fatal connector error.
// In: ctx - cancellation, errCh - fatal error reporting to RunWithContext
// Out: none - writes to workCh, closes it when done
// Ex: go c.fetchLoop(ctx, errCh) → distributes work to workers
func (c *Connector) fetchLoop(ctx context.Context, errCh chan<- error) {
	defer close(c.workCh) // Creator closes channel

	// Exponential backoff for transient fetch errors
	backoff := resilience.NewBackoff(fetchBackoffBase, fetchBackoffMax)

	for ctx.Err() == nil {
		batch, err := c.fetchOnce(ctx)
		if err != nil {
			if !c.recoverFetch(ctx, err, &backoff, errCh) {
				return
			}

			continue
		}

		// Every successful fetch -- including an empty batch -- proves the
		// pipeline is live; an idle stream stays healthy indefinitely.
		backoff.Reset()
		c.health.recordFetchSuccess()

		if !c.dispatch(ctx, batch) {
			return
		}
	}
}

// recoverFetch handles one fetch error: a dead subscription enters the
// re-bind path, anything else backs off as transient.
// In: ctx - cancellation, err - FetchBatch error, backoff - transient
// ladder, errCh - fatal error reporting
// Out: bool - true to keep fetching, false to stop the loop
// Ex: recoverFetch(ctx, err, &backoff, errCh) → true (retry fetch)
func (c *Connector) recoverFetch(ctx context.Context, err error, backoff *resilience.Backoff, errCh chan<- error) bool {
	if ctx.Err() != nil {
		return false
	}

	if isSubscriptionFailed(err) {
		return c.rebindSubscription(ctx, err, errCh)
	}

	slog.Error("Failed to fetch batch", "error", err)

	return backoff.Wait(ctx) == nil
}

// fetchOnce fetches one batch under the fetch liveness probe.
// In: ctx context.Context - cancellation
// Out: *source.MessageBatch, error - one FetchBatch round
// Ex: fetchOnce(ctx) → batch, nil
func (c *Connector) fetchOnce(ctx context.Context) (*source.MessageBatch, error) {
	c.fetchProbe.Begin("fetch")
	batch, err := c.source.FetchBatch(ctx)
	c.fetchProbe.End()

	return batch, err
}

// dispatch sends a batch to the worker pool under the dispatch liveness
// probe; empty batches idle briefly instead (workers treat them as no-ops,
// and the pause keeps a fast-returning empty fetch from busy-spinning).
// A growing dispatch age doubles as the whole-pool-wedged signal: all
// workers stuck → channel fills → send blocks.
// In: ctx context.Context - cancellation, batch - fetched messages
// Out: bool - false when the context was cancelled mid-dispatch
// Ex: dispatch(ctx, batch) → true (delivered to a worker)
func (c *Connector) dispatch(ctx context.Context, batch *source.MessageBatch) bool {
	if len(batch.Messages) == 0 {
		select {
		case <-time.After(idleSleep):
			return true
		case <-ctx.Done():
			return false
		}
	}

	c.fetchProbe.Begin("dispatch")
	defer c.fetchProbe.End()

	select {
	case c.workCh <- batch:
		return true
	case <-ctx.Done():
		return false
	}
}

// isSubscriptionFailed reports whether err is a typed SubscriptionFailed
// connector error (dead pull subscription needing a re-bind).
// In: err error - FetchBatch error
// Out: bool - true for ErrCodeSubscriptionFailed
// Ex: isSubscriptionFailed(err) → true
func isSubscriptionFailed(err error) bool {
	var connErr *connectorErrors.ConnectorError

	return stderrors.As(err, &connErr) &&
		connErr.Code == connectorErrors.ErrCodeSubscriptionFailed
}

// rebindSubscription re-binds the dead pull subscription to the pre-created
// durable with a bounded retry ladder. The re-bind sleeps are deliberately
// NOT probed: they are governed by the retry budget, and probing them would
// double-report the same incident.
// In: ctx - cancellation, cause - the fetch error that triggered the
// re-bind, errCh - fatal error reporting
// Out: bool - true on success; false on cancellation or exhausted budget
// (after sending MaxRetriesExceeded on errCh)
// Ex: rebindSubscription(ctx, err, errCh) → true (subscription replaced)
func (c *Connector) rebindSubscription(ctx context.Context, cause error, errCh chan<- error) bool {
	c.health.set(condRebinding, "error", cause)

	backoff := resilience.NewBackoff(c.rebindBase, c.rebindMax)
	for attempt := 1; attempt <= c.rebindAttempts; attempt++ {
		err := c.source.Rebind(ctx)
		if err == nil {
			c.health.recordFetchSuccess()
			c.health.clear(condRebinding)

			return true
		}
		if ctx.Err() != nil {
			return false
		}

		slog.Warn("Subscription re-bind failed",
			"attempt", attempt, "max_attempts", c.rebindAttempts, "error", err)
		if attempt < c.rebindAttempts && backoff.Wait(ctx) != nil {
			return false
		}
	}

	errCh <- connectorErrors.NewMaxRetriesExceededError("source", c.rebindAttempts)

	return false
}
