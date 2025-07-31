// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package sink: QuasarDB connection & persistence
// Types: Sink, Options, OptionsProvider
// Ex: sink.NewSink(opts).Write(tables) → writes to QDB
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

// Sink: synchronous QuasarDB writer with single handle. Needed for direct writes with proper backpressure.
// Who: connector creates, parser results consumed.
// Options: connection config
// handle: single QDB connection handle
// mu: protects handle during concurrent writes (handles are not goroutine-safe)
// closed: atomic shutdown flag
type Sink struct {
	Options Options
	handle  qdb.HandleType
	mu      sync.Mutex
	closed  atomic.Bool
}

// NewSink creates synchronous writer with single QDB handle.
// In: opts Options - cluster URI, connection config
// Out: *Sink, error - ready for writes or config/connection err
// Ex: NewSink(opts) → &Sink{handle}, nil
func NewSink(opts Options) (*Sink, error) {
	slog.Info("Initializing new sink")

	err := validateSinkOptions(opts)
	if err != nil {
		return nil, err
	}

	// Create single QDB handle
	handleOpts, err := buildHandleOptions(opts)
	if err != nil {
		return nil, err
	}

	handle, err := createHandle(handleOpts)
	if err != nil {
		return nil, errors.NewConnectionFailedError("sink", opts.ClusterUri, err)
	}

	s := &Sink{
		Options: opts,
		handle:  handle,
	}

	slog.Info("Sink initialized successfully")

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

// Close releases QDB handle.
// In: none
// Out: none - closes handle immediately
// Ex: Close() → handle released
func (s *Sink) Close() {
	if s.closed.Swap(true) {
		return // Already closed
	}

	slog.Info("Closing sink")

	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.handle.Close()
	if err != nil {
		slog.Error("Failed to close QDB handle", "error", err)
	}

	slog.Info("Sink closed successfully")
}

// Write performs synchronous QDB write with retry logic.
// In: ctx context.Context - cancellation context, tables []WriterTable - timeseries data
// Out: error - nil or write/retry failure
// Ex: Write(ctx, tables) → nil (data written to QDB)
func (s *Sink) Write(ctx context.Context, tables []qdb.WriterTable) error {
	if s.closed.Load() {
		return errors.NewWriteFailedError("sink", fmt.Errorf("sink is closed"))
	}

	if len(tables) == 0 {
		return errors.NewWriteFailedError("sink", fmt.Errorf("no tables provided"))
	}

	return s.writeWithRetry(ctx, tables)
}

// writeWithRetry performs synchronous write with exponential backoff.
// In: ctx context.Context - cancellation context, tables []qdb.WriterTable - data to write
// Out: error - write failure or retry exhausted
// Ex: writeWithRetry(ctx, tables) → nil (successful write)
func (s *Sink) writeWithRetry(ctx context.Context, tables []qdb.WriterTable) error {
	if len(tables) == 0 {
		return nil
	}

	backoff := 3 * time.Second
	maxBackoff := 300 * time.Second

	for attempt := range s.Options.RetryAttempts {
		if attempt > 0 {
			// Check context cancellation during backoff
			select {
			case <-ctx.Done():
				return errors.NewWriteFailedError("sink", fmt.Errorf("context cancelled during retry backoff: %w", ctx.Err()))
			case <-time.After(backoff):
				// Continue with retry
			}

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		// Check context before attempting write
		select {
		case <-ctx.Done():
			return errors.NewWriteFailedError("sink", fmt.Errorf("context cancelled before write attempt: %w", ctx.Err()))
		default:
			// Continue with write
		}

		// Push all tables
		err := s.pushTables(tables)
		if err != nil {
			// Check if error is retryable using qdb.IsRetryable()
			if !qdb.IsRetryable(err) {
				slog.Error("Non-retryable error, failing immediately", "error", err, "retryable", false)

				return errors.NewWriteFailedError("sink", fmt.Errorf("permanent failure: %w", err))
			}

			if attempt < s.Options.RetryAttempts-1 {
				slog.Debug("Retryable error, backing off", "attempt", attempt+1, "backoff", backoff, "error", err, "retryable", true)

				continue
			}

			slog.Error("Max retries exceeded for retryable error", "attempts", s.Options.RetryAttempts, "error", err, "retryable", true)

			return errors.NewMaxRetriesExceededError("sink", s.Options.RetryAttempts)
		}

		return nil
	}

	return errors.NewMaxRetriesExceededError("sink", s.Options.RetryAttempts)
}

// pushTables writes batch to QDB with mutex protection.
// In: tables []qdb.WriterTable - data to write
// Out: error - QDB write error
// Ex: pushTables(tables) → nil
func (s *Sink) pushTables(tables []qdb.WriterTable) error {
	slog.Debug("Pushing tables to QDB", "num_tables", len(tables))

	// Protect handle access with mutex since handles are not goroutine-safe
	s.mu.Lock()
	defer s.mu.Unlock()


	// Create writer options with push mode and deduplication mode
	writerOpts := qdb.NewWriterOptions().WithPushMode(s.Options.PushMode).WithDeduplicationMode(s.Options.DeduplicationMode)
	writer := qdb.NewWriter(writerOpts)

	// Add all tables to the writer
	for i, table := range tables {
		slog.Debug("Adding table to writer", "table_index", i, "table_name", table.TableName)

		// CRITICAL MEMORY PINNING CHECKPOINT:
		// ===================================
		// At this point, ALL string data in `table` MUST be pinnable by runtime.Pinner.
		// QuasarDB's writer.SetTable() and subsequent batch.Push() operations will:
		// 1. Call PinToC() on each string value
		// 2. Use unsafe.StringData() to get raw memory pointers
		// 3. Pin memory using runtime.Pinner to prevent GC movement
		// 4. Pass pointers directly to C++ code for zero-copy operations
		//
		// COMMON FAILURE POINTS:
		// - strings.Builder generated strings → SEGFAULT
		// - Strings from certain buffer pools → SEGFAULT
		// - Any non-contiguous string memory → SEGFAULT
		//
		// If you see crashes in qdb_batch_push_columns, check ALL string
		// creation methods in the parser pipeline. Use + operator for safety.
		err := writer.SetTable(table)
		if err != nil {
			slog.Error("Failed to set table in writer", "table_name", table.TableName, "error", err)

			return err
		}
	}

	// Push to QuasarDB
	slog.Debug("Pushing data to QuasarDB")

	// IMPORTANT: This function is synchronous, and **always** returns immediately.
	// It pins all the memory provided
	err := writer.Push(s.handle)
	if err != nil {
		slog.Error("Failed to push to QuasarDB", "error", err)
	} else {
		slog.Debug("Successfully pushed to QuasarDB")
	}

	return err
}

// validateSinkOptions ensures configuration safety before sink creation.
// Separated from NewSink to fail fast on invalid config without side effects.
// In: opts Options - user-provided configuration
// Out: error if ClusterUri empty
// Ex: validateSinkOptions(opts{ClusterUri:""}) → "cluster_uri is required"
func validateSinkOptions(opts Options) error {
	if opts.ClusterUri == "" {
		return errors.NewInvalidConfigError("sink", "cluster_uri is required")
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
