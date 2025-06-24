// Package sink manages the QuasarDB connection and data persistence.
// This internal package handles batching, compression, encryption, and
// retry logic for reliable time series data storage.
// Decision rationale:
// - Internal package isolates QuasarDB-specific logic
// - Batching improves write throughput
// - Configurable compression/encryption for security
package sink

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
)

// Sink manages the QuasarDB client connection and write operations.
// Decision rationale:
// - Worker pool pattern with dedicated handles prevents resource contention
// - Each worker has 1:1 mapping with QuasarDB handle for thread safety
// - Buffered channel provides backpressure control
// Key assumptions:
// - QuasarDB cluster is reachable at configured endpoint
// - Authentication credentials are valid if provided
// - Sink handles reconnection on transient failures
type Sink struct {
	Options Options
	workers []*worker
	jobs    chan []*qdb.WriterTable
	wg      sync.WaitGroup
	closed  atomic.Bool
}

// worker represents a single writer with dedicated QuasarDB handle.
// Decision rationale:
// - 1:1 handle to writer mapping ensures thread safety
// - Dedicated goroutine per worker for concurrent processing
// Performance trade-offs:
// - Higher memory usage per worker (handle + connection)
// - Better isolation and parallel write capability
type worker struct {
	id                int
	handle            qdb.HandleType
	options           Options
	handleInitialized bool
}

// NewSink establishes connections to the QuasarDB cluster with worker pool.
// Decision rationale:
// - Creates dedicated workers with individual handles for thread safety
// - Validates configuration before creating expensive resources
// - Pre-connects all workers to fail fast on connection issues
// Performance trade-offs:
// - Higher memory usage for multiple handles but better concurrency
// - Connection establishment overhead during initialization
func NewSink(opts Options) (*Sink, error) {
	slog.Info("Initializing new sink", "num_workers", opts.NumWriters)

	if opts.NumWriters <= 0 {
		return nil, errors.NewInvalidConfigError("sink", "num_writers must be positive")
	}

	if opts.ClusterUri == "" {
		return nil, errors.NewInvalidConfigError("sink", "cluster_uri is required")
	}

	s := &Sink{
		Options: opts,
		workers: make([]*worker, opts.NumWriters),
		jobs:    make(chan []*qdb.WriterTable, opts.QueueSize),
	}

	// Create workers with dedicated handles
	for i := range opts.NumWriters {
		w, err := newWorker(i, opts)
		if err != nil {
			// Clean up previously created workers
			for j := range i {
				s.workers[j].close()
			}
			return nil, errors.NewConnectionFailedError("sink", opts.ClusterUri, err)
		}
		s.workers[i] = w

		// Start worker goroutine
		s.wg.Add(1)
		go w.run(s.jobs, &s.wg)
	}

	slog.Info("Sink initialized successfully", "workers", len(s.workers))
	return s, nil
}

// Close gracefully shuts down all workers and connections.
// Decision rationale:
// - Stops accepting new work first to prevent resource leaks
// - Waits for workers to complete current tasks
// - Closes all handles to release QuasarDB resources
// Key assumptions:
// - Pending writes are completed before closure
// - Method is idempotent and safe to call multiple times
func (s *Sink) Close() {
	if s.closed.Swap(true) {
		return // Already closed
	}

	slog.Info("Closing sink", "workers", len(s.workers))

	// Stop accepting new work
	close(s.jobs)

	// Wait for all workers to finish
	s.wg.Wait()

	// Close all worker handles
	for _, w := range s.workers {
		w.close()
	}

	slog.Info("Sink closed successfully")
}

// Write enqueues writer tables for processing by worker pool.
// Decision rationale:
// - Non-blocking enqueue prevents NATS handler goroutines from stalling
// - Returns error immediately if system is overloaded
// - Worker pool handles actual QuasarDB write operations
// Performance trade-offs:
// - Additional memory allocation for channel buffering
// - Lower latency for NATS message processing
func (s *Sink) Write(tables []*qdb.WriterTable) error {
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

// newWorker creates a worker with dedicated QuasarDB handle.
// Decision rationale:
// - Individual handle per worker ensures thread safety
// - Connection established during initialization for fail-fast behavior
// - Security and compression settings applied per handle
func newWorker(id int, opts Options) (*worker, error) {
	handle, err := qdb.NewHandle()
	if err != nil {
		return nil, err
	}

	// Configure handle options
	if opts.Encryption != nil {
		if err := handle.SetEncryption(*opts.Encryption); err != nil {
			handle.Close()
			return nil, err
		}
	}
	if opts.Compression != nil {
		if err := handle.SetCompression(*opts.Compression); err != nil {
			handle.Close()
			return nil, err
		}
	}
	if opts.ClientMaxParallelism != nil {
		if err := handle.SetClientMaxParallelism(*opts.ClientMaxParallelism); err != nil {
			handle.Close()
			return nil, err
		}
	}
	if opts.ClientMaxInBufSize != nil {
		if err := handle.SetClientMaxInBufSize(*opts.ClientMaxInBufSize); err != nil {
			handle.Close()
			return nil, err
		}
	}

	// Connect to cluster
	if err := handle.Connect(opts.ClusterUri); err != nil {
		handle.Close()
		return nil, err
	}

	return &worker{
		id:                id,
		handle:            handle,
		options:           opts,
		handleInitialized: true,
	}, nil
}

// run processes write jobs in dedicated goroutine.
// Decision rationale:
// - Dedicated goroutine per worker for parallel processing
// - Retry logic handles transient QuasarDB async pipeline full errors
// - Error logging preserves diagnostic information without stopping worker
func (w *worker) run(jobs <-chan []*qdb.WriterTable, wg *sync.WaitGroup) {
	defer wg.Done()

	slog.Info("Worker started", "worker_id", w.id)
	defer slog.Info("Worker stopped", "worker_id", w.id)

	for tables := range jobs {
		if err := w.processTables(tables); err != nil {
			slog.Error("Failed to process tables", "worker_id", w.id, "error", err, "num_tables", len(tables))
		}
	}
}

// processTables writes all tables with retry logic.
// Decision rationale:
// - Exponential backoff handles async pipeline full conditions
// - Retries only on retryable errors to avoid infinite loops
// - Batch processing of all tables in single operation
func (w *worker) processTables(tables []*qdb.WriterTable) error {
	if len(tables) == 0 {
		return nil
	}

	backoff := 100 * time.Millisecond
	maxBackoff := 5 * time.Second

	for attempt := range w.options.RetryAttempts {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		// Push all tables
		if err := w.pushTables(tables); err != nil {
			if w.isRetryable(err) && attempt < w.options.RetryAttempts-1 {
				slog.Debug("Retryable error, backing off", "worker_id", w.id, "attempt", attempt+1, "backoff", backoff, "error", err)
				continue
			}
			return errors.NewWriteFailedError("sink", err)
		}

		return nil
	}

	return errors.NewMaxRetriesExceededError("sink", w.options.RetryAttempts)
}

// pushTables executes the actual QuasarDB write operation.
// Decision rationale:
// - Single push operation for all tables to minimize network calls
// - Uses dedicated handle for thread safety
func (w *worker) pushTables(tables []*qdb.WriterTable) error {
	writer := qdb.NewWriterWithDefaultOptions()

	// Add all tables to the writer
	for _, table := range tables {
		if err := writer.SetTable(*table); err != nil {
			return err
		}
	}

	// Push to QuasarDB
	return writer.Push(w.handle)
}

// isRetryable determines if an error should trigger retry logic.
// Decision rationale:
// - Async pipeline full is temporary condition worth retrying
// - Connection errors might be transient network issues
// - Data validation errors are permanent and shouldn't be retried
func (w *worker) isRetryable(err error) bool {
	// TODO: Implement proper error classification based on qdb-api-go error types
	return true
}

// close releases the worker's QuasarDB handle.
func (w *worker) close() {
	if w.handleInitialized {
		w.handle.Close()
	}
}
