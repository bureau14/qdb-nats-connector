// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package sink: QuasarDB connection & persistence
// Types: Sink, Options, OptionsProvider
// Ex: sink.NewSink(opts).Write(tables) → writes to QDB
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

// Sink: QDB writer pool with async queue & backpressure control
type Sink struct {
	Options Options
	workers []*worker
	jobs    chan []*qdb.WriterTable
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

// NewSink creates QDB writer pool.
// Args:
//
//	opts: Options - workers/queue/retry config
//
// Returns:
//
//	*Sink: QDB sink with worker pool
//	error: connection/validation fails
//
// Example:
//
//	NewSink(opts) // → sink with 4 workers
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

	// Approach: create workers with handles, start goroutines
	// 1. Create worker - QDB connection per worker
	// 2. Start goroutine - process jobs concurrently

	for i := range opts.NumWriters {
		// 1. Create worker: dedicated QDB handle
		w, err := newWorker(i, opts)
		if err != nil {
			// Clean up previously created workers
			for j := range i {
				s.workers[j].close()
			}
			return nil, errors.NewConnectionFailedError("sink", opts.ClusterUri, err)
		}
		s.workers[i] = w

		// 2. Start goroutine: concurrent processing
		s.wg.Add(1)
		go w.run(s.jobs, &s.wg)
	}

	slog.Info("Sink initialized successfully", "workers", len(s.workers))
	return s, nil
}

// Close shuts down workers & QDB handles.
// Approach: stop queue→wait workers→close handles
// 1. Close job queue - no new work
// 2. Wait workers - drain pending
// 3. Close handles - release resources
// Ex: Close() → all workers stopped
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
// In: tables []*qdb.WriterTable - timeseries data
// Out: error - queue full|closed sink
// Ex: Write(tables) → nil (queued)
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

// newWorker creates worker with QDB handle.
// In: id int, opts Options
// Out: *worker, error - worker|connection error
// Ex: newWorker(0, opts) → worker with handle
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
	if err := handle.SetCompression(opts.Compression); err != nil {
		handle.Close()
		return nil, err
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

// run processes jobs until channel closed.
// In: jobs <-chan []*qdb.WriterTable, wg *sync.WaitGroup
// Ex: run(jobs, wg) → processes until channel closed
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

// processTables writes tables with exp backoff.
// In: tables []*qdb.WriterTable
// Out: error - write|retry exhausted
// Ex: processTables(tables) → nil
func (w *worker) processTables(tables []*qdb.WriterTable) error {
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
		if err := w.pushTables(tables); err != nil {
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
func (w *worker) pushTables(tables []*qdb.WriterTable) error {
	// Create writer options with push mode
	writerOpts := qdb.NewWriterOptions().WithPushMode(w.options.PushMode)
	writer := qdb.NewWriter(writerOpts)

	// Add all tables to the writer
	for _, table := range tables {
		if err := writer.SetTable(*table); err != nil {
			return err
		}
	}

	// Push to QuasarDB
	return writer.Push(w.handle)
}

// close releases QDB handle.
// Ex: close() → handle released
func (w *worker) close() {
	if w.handleInitialized {
		w.handle.Close()
	}
}
