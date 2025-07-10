// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package sink: QuasarDB writer pool for timeseries
// Types: Sink, Options, OptionsProvider
// Ex: sink.Write(tables) → async write
package sink

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
)

// Sink: async QuasarDB writer pool, goroutine-safe
type Sink struct {
	Options Options
	workers []*worker
	jobs    chan []qdb.WriterTable
	wg      sync.WaitGroup
	closed  atomic.Bool
}

// worker: single QDB writer with dedicated handle for thread safety
type worker struct {
	id                int
	handle            qdb.HandleType
	options           Options
	handleInitialized bool
}

// NewSink creates writer pool with workers & job queue.
// In: opts Options - cluster URI, workers, queue size
// Out: *Sink, error - pool ready or config/connection err
// Ex: NewSink(opts) → &Sink{workers:4,jobs:chan[100]}, nil
func NewSink(opts Options) (*Sink, error) {
	slog.Info("Initializing new sink", "num_workers", opts.NumWriters)

	// Defer QDB API initialization to first worker creation because
	// early initialization causes unwanted logging in tests

	err := validateSinkOptions(opts)
	if err != nil {
		return nil, err
	}

	s := createSinkInstance(opts)

	err = s.initializeWorkers()
	if err != nil {
		return nil, err
	}

	slog.Info("Sink initialized successfully", "workers", len(s.workers))

	return s, nil
}

// Connect satisfies interface, no-op (already connected).
// In: ctx context.Context - unused
// Out: error - always nil
// Ex: Connect(ctx) → nil
func (s *Sink) Connect(ctx context.Context) error {
	// Sink is already initialized with workers in NewSink
	return nil
}

// Close drains queue, waits workers, closes handles.
// In: none
// Out: none - blocks until shutdown complete
// Ex: Close() → jobs closed, workers done, handles released
func (s *Sink) Close() {
	if s.closed.Swap(true) {
		return // Already closed
	}

	slog.Info("Closing sink", "workers", len(s.workers))

	// 1. Close job queue: prevent new writes
	close(s.jobs)

	// 2. Wait workers: drain pending writes
	s.wg.Wait()

	// 3. Close handles: release QDB connections
	for _, w := range s.workers {
		w.close()
	}

	slog.Info("Sink closed successfully")
}

// Write queues tables for async QDB write.
// In: tables []WriterTable - timeseries data
// Out: error - nil or closed/empty/queue full
// Ex: Write(tables) → nil (queued for workers)
func (s *Sink) Write(tables []qdb.WriterTable) error {
	if s.closed.Load() {
		return errors.NewWriteFailedError("sink", fmt.Errorf("sink is closed"))
	}

	if len(tables) == 0 {
		return errors.NewWriteFailedError("sink", fmt.Errorf("no tables provided"))
	}

	select {
	case s.jobs <- tables:
		return nil
	default:
		return errors.NewQueueFullError("sink", len(s.jobs))
	}
}

// newWorker creates worker with QDB handle.
// In: id int, opts Options
// Out: *worker, error - worker|connection error
// Ex: newWorker(0, opts) → worker with handle
func newWorker(id int, opts Options) (*worker, error) {
	handleOpts, err := buildHandleOptions(opts)
	if err != nil {
		return nil, err
	}

	handle, err := createHandle(handleOpts)
	if err != nil {
		return nil, err
	}

	return &worker{
		id:                id,
		handle:            handle,
		options:           opts,
		handleInitialized: true,
	}, nil
}

// run processes jobs until channel closed.
// In: jobs <-chan []*qdb.WriterTable, wg *sync.WaitGroup
// Ex: run(jobs, wg) → processes until channel closed
func (w *worker) run(jobs <-chan []qdb.WriterTable, wg *sync.WaitGroup) {
	defer wg.Done()

	slog.Info("Worker started", "worker_id", w.id)
	defer slog.Info("Worker stopped", "worker_id", w.id)

	for tables := range jobs {
		err := w.processTables(tables)
		if err != nil {
			slog.Error("Failed to process tables", "worker_id", w.id, "error", err, "num_tables", len(tables))
		}
	}
}

// processTables writes tables with exp backoff.
// In: tables []*qdb.WriterTable
// Out: error - write|retry exhausted
// Ex: processTables(tables) → nil
func (w *worker) processTables(tables []qdb.WriterTable) error {
	if len(tables) == 0 {
		return nil
	}

	backoff := 3 * time.Second
	maxBackoff := 300 * time.Second

	for attempt := range w.options.RetryAttempts {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		// Push all tables
		err := w.pushTables(tables)
		if err != nil {
			if attempt < w.options.RetryAttempts-1 {
				slog.Debug("Retryable error, backing off", "worker_id", w.id, "attempt", attempt+1, "backoff", backoff, "error", err)

				continue
			}

			return errors.NewWriteFailedError("sink", err)
		}

		return nil
	}

	return errors.NewMaxRetriesExceededError("sink", w.options.RetryAttempts)
}

// pushTables writes batch to QDB.
// In: tables []*qdb.WriterTable
// Out: error - QDB write error
// Ex: pushTables(tables) → nil
func (w *worker) pushTables(tables []qdb.WriterTable) error {
	slog.Debug("Worker pushing tables to QDB", "worker_id", w.id, "num_tables", len(tables))

	// Create writer options with push mode and deduplication mode
	writerOpts := qdb.NewWriterOptions().WithPushMode(w.options.PushMode).WithDeduplicationMode(w.options.DeduplicationMode)
	writer := qdb.NewWriter(writerOpts)

	// Add all tables to the writer
	for i, table := range tables {
		slog.Debug("Adding table to writer", "worker_id", w.id, "table_index", i, "table_name", table.TableName)
		err := writer.SetTable(table)
		if err != nil {
			slog.Error("Failed to set table in writer", "worker_id", w.id, "table_name", table.TableName, "error", err)

			return err
		}
	}

	// Push to QuasarDB
	slog.Debug("Pushing data to QuasarDB", "worker_id", w.id)
	err := writer.Push(w.handle)
	if err != nil {
		slog.Error("Failed to push to QuasarDB", "worker_id", w.id, "error", err)
	} else {
		slog.Debug("Successfully pushed to QuasarDB", "worker_id", w.id)
	}

	return err
}

// close releases QuasarDB handle
// In: none - uses w.handle
// Out: none - handle closed
// Ex: close() → handle released
func (w *worker) close() {
	if w.handleInitialized {
		_ = w.handle.Close()
	}
}

// validateSinkOptions ensures configuration safety before sink creation.
// Separated from NewSink to fail fast on invalid config without side effects.
// In: opts Options - user-provided configuration
// Out: error if NumWriters ≤0 or ClusterUri empty
// Ex: validateSinkOptions(opts{NumWriters:0}) → "num_writers must be positive"
func validateSinkOptions(opts Options) error {
	if opts.NumWriters <= 0 {
		return errors.NewInvalidConfigError("sink", "num_writers must be positive")
	}

	if opts.ClusterUri == "" {
		return errors.NewInvalidConfigError("sink", "cluster_uri is required")
	}

	return nil
}

// createSinkInstance allocates sink structure without starting workers.
// Separated to enable clean error handling if worker creation fails.
// In: opts Options - validated configuration
// Out: *Sink with allocated channels/slices, no active goroutines
// Ex: createSinkInstance(opts) → &Sink{workers:[4]nil, jobs:chan[1000]}
func createSinkInstance(opts Options) *Sink {
	return &Sink{
		Options: opts,
		workers: make([]*worker, opts.NumWriters),
		jobs:    make(chan []qdb.WriterTable, opts.QueueSize),
	}
}

// initializeWorkers establishes QuasarDB connections and starts worker pool.
// Separated from NewSink to enable cleanup on partial initialization failure.
// In: receiver *Sink with allocated structures
// Out: error if any worker fails to connect, cleans up on failure
// Ex: initializeWorkers() → starts NumWriters goroutines with QDB handles
func (s *Sink) initializeWorkers() error {
	// Create workers with dedicated QDB handles for thread safety because
	// QDB handles are not goroutine-safe. Each worker needs its own handle.

	for i := range s.Options.NumWriters {
		// Create worker with dedicated QDB handle for thread safety
		w, err := newWorker(i, s.Options)
		if err != nil {
			// Clean up previously created workers
			for j := range i {
				s.workers[j].close()
			}

			return errors.NewConnectionFailedError("sink", s.Options.ClusterUri, err)
		}
		s.workers[i] = w

		// Start goroutine for concurrent batch processing
		s.wg.Add(1)
		go w.run(s.jobs, &s.wg)

		// Delay prevents connection storm to QDB cluster
		if i < s.Options.NumWriters-1 && s.Options.WorkerCreationDelay > 0 {
			time.Sleep(s.Options.WorkerCreationDelay)
		}
	}

	return nil
}

// buildHandleOptions transforms sink config into QuasarDB connection parameters.
// Centralized to ensure consistent handle configuration across all workers.
// In: opts Options - sink configuration with connection details
// Out: *qdb.HandleOptions configured, error if client params invalid
// Ex: buildHandleOptions(opts{ClusterUri:"qdb://localhost"}) → HandleOptions
func buildHandleOptions(opts Options) (*qdb.HandleOptions, error) {
	// Transform sink options to QDB handle configuration
	handleOpts := qdb.NewHandleOptions().
		WithClusterUri(opts.ClusterUri).
		WithClusterPublicKeyFile(opts.ClusterPublicKeyFile).
		WithUserSecurityFile(opts.UserSecurityFile).
		WithCompression(opts.Compression)

	// Apply encryption settings if configured
	if opts.Encryption != nil {
		handleOpts = handleOpts.WithEncryption(*opts.Encryption)
	}

	err := configureClientParameters(handleOpts, opts)
	if err != nil {
		return handleOpts, err
	}

	configureOptionalParameters(handleOpts, opts)

	return handleOpts, nil
}

// configureClientParameters applies performance tuning to QuasarDB client.
// Separated to validate numeric constraints before handle creation.
// In: handleOpts to modify, opts with optional tuning parameters
// Out: error if parallelism exceeds math.MaxInt
// Ex: configureClientParameters(h,opts{ClientMaxParallelism:&8}) → nil
func configureClientParameters(handleOpts *qdb.HandleOptions, opts Options) error {
	// Apply performance tuning parameters
	if opts.ClientMaxParallelism != nil {
		parallelism := *opts.ClientMaxParallelism
		if parallelism > math.MaxInt {
			return fmt.Errorf("client max parallelism %d exceeds maximum allowed value %d", parallelism, math.MaxInt)
		}
		handleOpts.WithClientMaxParallelism(int(parallelism))
	}
	if opts.ClientMaxInBufSize != nil {
		handleOpts.WithClientMaxInBufSize(*opts.ClientMaxInBufSize)
	}

	return nil
}

// configureOptionalParameters applies auth/timeout settings to handle.
// Separated from required params for cleaner nil-checking logic.
// In: handleOpts to modify, opts with optional settings
// Out: modifies handleOpts in-place, no error possible
// Ex: configureOptionalParameters(h,opts{Timeout:&5s}) → timeout set
func configureOptionalParameters(handleOpts *qdb.HandleOptions, opts Options) {
	// Apply connection timeout if specified
	if opts.Timeout != nil {
		handleOpts.WithTimeout(*opts.Timeout)
	}

	// Apply authentication if both credentials present
	if opts.UserName != "" && opts.UserSecret != "" {
		handleOpts.WithUserName(opts.UserName).WithUserSecret(opts.UserSecret)
	}

	// Apply cluster key authentication if configured
	if opts.ClusterPublicKey != "" {
		handleOpts.WithClusterPublicKey(opts.ClusterPublicKey)
	}
}

// createHandle establishes connection to QuasarDB cluster.
// Isolated to enable connection retry logic in future versions.
// In: handleOpts *qdb.HandleOptions - fully configured options
// Out: qdb.HandleType connected, error if connection fails
// Ex: createHandle(opts) → handle ready for operations
func createHandle(handleOpts *qdb.HandleOptions) (qdb.HandleType, error) {
	// Establish connection to QuasarDB cluster
	handle, err := qdb.NewHandleFromOptions(handleOpts)
	if err != nil {
		return qdb.HandleType{}, err
	}

	return handle, nil
}
