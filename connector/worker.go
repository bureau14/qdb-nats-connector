// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: NATS→QuasarDB pipeline orchestration
// Types: Connector, Worker, Options
// Ex: connector.New(opts).Run() → streams data
package connector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	"github.com/bureau14/qdb-nats-connector/connector/resilience"
	"github.com/bureau14/qdb-nats-connector/internal/filter"
	"github.com/bureau14/qdb-nats-connector/internal/parser"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/bureau14/qdb-nats-connector/internal/source"
)

// Worker: processes NATS topic→QuasarDB, goroutine-safe
type Worker struct {
	id             int
	workerID       string                      // Cached worker ID string for performance
	workCh         <-chan *source.MessageBatch // Work distribution channel
	parser         parser.Parser
	rowFilter      *filter.RowFilter // pre-merge row filter; nil = pass-through
	sink           *sink.Sink
	circuitBreaker *resilience.CircuitBreaker
	hooks          *hooks.HookRegistry

	// parseErrorAction: "drop" discards structurally-unusable messages
	// (ACK, no row); "fail" NACKs any message with parse errors for
	// redelivery. The parser only classifies; policy is applied here.
	parseErrorAction string

	// Liveness probe around batch processing; sampled by the health
	// monitor. Cleared (waiting for work) means healthy by definition.
	probe Probe

	// Shutdown synchronization
	activeBatches sync.WaitGroup
	closing       atomic.Bool

	// Metrics hot-path counters; snapshot via GetStats (see stats.go)
	workerCounters
}

// NewWorker creates NATS→QuasarDB worker.
// Args:
//
//	id: worker identifier ≥0
//	opts: connector configuration
//	workCh: work distribution channel
//	manager: circuit breaker manager (optional)
//
// Returns:
//
//	*Worker: configured worker with parser/sink/filter
//	error: parser/sink initialization failure
//
// Example:
//
//	w := NewWorker(0, opts, workCh, manager) // → *Worker
//
// Also obtains and stores the parser's row filter from the factory; a nil
// filter (noop parser or no filters configured) means pass-through.
func NewWorker(id int, opts *Options, workCh <-chan *source.MessageBatch, manager *resilience.Manager) (*Worker, error) {
	// Create parser using factory based on command-line options
	parserOpts := parser.ParserOptions{
		ParserType: opts.Parser,
		ConfigPath: opts.ParserConfig,
	}
	messageParser, rowFilter, err := parser.NewParserWithOptions(parserOpts)
	if err != nil {
		return nil, err
	}

	// Ensure hooks is never nil to simplify worker code
	if opts.Hooks == nil {
		opts.Hooks = hooks.NewHookRegistry()
	}

	// Create circuit breaker
	var circuitBreaker *resilience.CircuitBreaker
	workerID := fmt.Sprintf("worker-%d", id)

	if manager != nil {
		// Use shared circuit breaker from manager
		circuitBreaker = manager.ForResource("qdb-cluster", workerID)
	} else {
		// Create individual circuit breaker
		circuitBreaker = resilience.NewCircuitBreaker(
			opts.CircuitBreakerFailureThreshold,
			opts.CircuitBreakerSuccessThreshold,
			opts.CircuitBreakerTimeout,
			resilience.WithHooks(opts.Hooks, workerID, "qdb-cluster"),
			resilience.WithJitter(opts.CircuitBreakerJitterMax),
			resilience.WithHalfOpenProgression(opts.CircuitBreakerHalfOpenBase, opts.CircuitBreakerHalfOpenMax),
		)
	}

	w := &Worker{
		id:               id,
		workerID:         workerID,
		workCh:           workCh,
		parser:           messageParser,
		rowFilter:        rowFilter,
		circuitBreaker:   circuitBreaker,
		hooks:            opts.Hooks,
		parseErrorAction: opts.ParseErrorAction,
	}

	// Create sink after the worker so its retry loop can report progress to
	// the worker's liveness probe: each retry boundary refreshes the probe,
	// keeping a legitimately retrying worker distinct from a wedged one.
	// The method value binds &w.probe here, after heap allocation.
	sinkOpts := sink.FromOptionsProvider(opts)
	sinkOpts.OnRetryProgress = w.probe.Touch
	qdbSink, err := sink.NewSink(sinkOpts)
	if err != nil {
		return nil, err
	}
	w.sink = qdbSink

	return w, nil
}

// Run starts worker message processing loop.
// Args:
//
//	ctx: cancellation context
//
// Returns:
//
//	error: connection/processing failure
//
// Example:
//
//	w.Run(ctx) // blocks until ctx.Done()
func (w *Worker) Run(ctx context.Context) error {
	// Connect sink
	err := w.sink.Connect(ctx)
	if err != nil {
		return err
	}
	defer w.sink.Close()

	// Main processing loop - receive work from channel
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-w.workCh:
			if !ok {
				// Channel closed, exit gracefully
				return nil
			}
			w.probe.Begin("process")
			err := w.processBatchFromChannel(ctx, batch)
			w.probe.End()
			if err != nil {
				// Log error but continue processing
				slog.Error("Worker error", "worker_id", w.id, "error", err)
			}
		}
	}
}

// GetStats returns a snapshot of worker statistics for monitoring.
// Args: none
// Returns:
//
//	WorkerStats: point-in-time counter snapshot (see stats.go)
//
// Example:
//
//	stats := w.GetStats() // → WorkerStats{MessagesProcessed: 1000, ...}
func (w *Worker) GetStats() WorkerStats {
	return w.snapshot()
}

// processBatchFromChannel processes a batch received from the work channel.
// It parses each message, applies the configured row filter pre-merge, merges
// surviving single-row tables, NACKs parse failures, writes survivors to
// QuasarDB under the circuit breaker, and ACKs every successfully-processed
// (non-parse-failed) sequence -- including batches whose rows are all filtered
// out, which write nothing but must still be ACKed to avoid redelivery.
// In: ctx context.Context - cancellation, batch *source.MessageBatch - received batch
// Out: error - processing failure
// Ex: processBatchFromChannel(ctx, batch) → nil
func (w *Worker) processBatchFromChannel(ctx context.Context, batch *source.MessageBatch) error {
	// Track active batch processing for safe shutdown
	w.activeBatches.Add(1)
	defer w.activeBatches.Done()

	// Check if we're shutting down - return early if so
	if w.closing.Load() {
		return nil
	}
	// PreRead hook
	preReadData := &hooks.PreReadData{
		WorkerID:  w.workerID,
		Topic:     "all",
		Timestamp: time.Now(),
	}
	w.hooks.Execute(ctx, "PreRead", preReadData)

	// PostRead hook
	messageCount := 0
	batchSize := 0
	if batch != nil {
		messageCount = len(batch.Messages)
		batchSize = messageCount
	}
	postReadData := &hooks.PostReadData{
		WorkerID:     w.workerID,
		Topic:        "all",
		MessageCount: messageCount,
		BatchSize:    batchSize,
		Duration:     0, // No fetch duration since we received the batch
		Error:        nil,
		Timestamp:    time.Now(),
	}
	w.hooks.Execute(ctx, "PostRead", postReadData)

	if len(batch.Messages) == 0 {
		return nil
	}

	// 2. Parse messages - failures retry automatically
	validTables, failedSequenceNumbers := w.parseMessages(batch)

	// 2.1. Drop rows that do not pass the configured filter. Applied pre-merge,
	// where every table holds exactly one row. Nil filter = pass-through.
	validTables = w.rowFilter.Apply(validTables)

	// 2.5. Merge tables with the same name to prevent "already exists" errors
	if len(validTables) > 0 {
		mergedTables, mergeErr := qdb.MergeWriterTables(validTables)
		if mergeErr != nil {
			slog.Error("Failed to merge tables", "worker_id", w.id, "error", mergeErr, "num_tables", len(validTables))

			return mergeErr
		}
		validTables = mergedTables
	}

	// 3. NACK failed parses immediately for retry
	if len(failedSequenceNumbers) > 0 {
		nackErr := batch.NackFunc(failedSequenceNumbers)
		if nackErr != nil {
			slog.Error("Failed to NACK parse failures", "worker_id", w.id, "error", nackErr)
		} else {
			w.nacks.Add(uint64(len(failedSequenceNumbers)))
		}
	}

	// 4. Compute valid (non-parse-failed) sequences. These MUST be ACKed whether
	// they were written or entirely filtered out.
	var validSequences []uint64
	failedSeqMap := make(map[uint64]bool)
	for _, seq := range failedSequenceNumbers {
		failedSeqMap[seq] = true
	}
	for _, msgInfo := range batch.Messages {
		if !failedSeqMap[msgInfo.Sequence] {
			validSequences = append(validSequences, msgInfo.Sequence)
		}
	}

	w.messagesProcessed.Add(uint64(len(validSequences)))

	// 5. Write survivors with the circuit breaker -- only when there is something
	// to write. An all-filtered batch skips the write but still ACKs below.
	if len(validTables) > 0 {
		tables := validTables

		// PreWrite hook
		rowCount := 0
		for _, table := range tables {
			rowCount += table.RowCount()
		}
		preWriteData := &hooks.PreWriteData{
			WorkerID:   w.workerID,
			Topic:      "all",
			TableCount: len(tables),
			RowCount:   rowCount,
			Tables:     tables,
			Timestamp:  time.Now(),
		}
		w.hooks.Execute(ctx, "PreWrite", preWriteData)

		writeStart := time.Now()
		writeErr := w.circuitBreaker.Execute(func() error {
			return w.sink.Write(ctx, tables)
		})
		writeDuration := time.Since(writeStart)

		// PostWrite hook
		rowsWritten := 0
		tablesWritten := 0
		if writeErr == nil {
			for _, table := range tables {
				rowsWritten += table.RowCount()
			}
			tablesWritten = len(tables)
			w.rowsWritten.Add(uint64(rowsWritten))
		}
		postWriteData := &hooks.PostWriteData{
			WorkerID:      w.workerID,
			Topic:         "all",
			Duration:      writeDuration,
			RowsWritten:   rowsWritten,
			TablesWritten: tablesWritten,
			Error:         writeErr,
			Timestamp:     time.Now(),
		}
		w.hooks.Execute(ctx, "PostWrite", postWriteData)

		if writeErr != nil {
			return w.handleWriteFailure(ctx, batch, validSequences, writeErr)
		}
	}

	// 6. Write succeeded (or there was nothing to write) - ACK valid messages.
	if len(validSequences) == 0 {
		return nil
	}

	// PreAck hook (ACK case)
	preAckData := &hooks.PreAckData{
		WorkerID:  w.workerID,
		Topic:     "all",
		Sequences: validSequences,
		IsNack:    false,
		Count:     len(validSequences),
		Timestamp: time.Now(),
	}
	w.hooks.Execute(ctx, "PreAck", preAckData)

	ackErr := batch.AckFunc(validSequences)

	// PostAck hook (ACK case)
	ackedCount := 0
	nackedCount := 0
	if ackErr == nil {
		ackedCount = len(validSequences)
		w.acks.Add(uint64(ackedCount))
	}
	postAckData := &hooks.PostAckData{
		WorkerID:    fmt.Sprintf("worker-%d", w.id),
		Topic:       "all",
		AckedCount:  ackedCount,
		NackedCount: nackedCount,
		Error:       ackErr,
		Timestamp:   time.Now(),
	}
	w.hooks.Execute(ctx, "PostAck", postAckData)

	if ackErr != nil {
		slog.Error("Failed to ACK after write success", "worker_id", w.id, "error", ackErr)
	}

	return nil
}

// parseMessages transforms NATS→tables, applying the parse-error-action
// policy to each classified result: "fail" NACKs any message with parse
// errors for redelivery; "drop" discards structurally-unusable messages
// (counted, one WARN, ACKed without a row) and keeps sentinel-filled rows
// for partial field failures (ADR-005).
// In: batch *source.MessageBatch - messages to parse
// Out: []qdb.WriterTable - valid tables, []uint64 - failedSequenceNumbers
// Ex: parseMessages(batch) → [table1, table2], [seq3]
func (w *Worker) parseMessages(batch *source.MessageBatch) (validTables []qdb.WriterTable, failedSequenceNumbers []uint64) {
	for _, msgInfo := range batch.Messages {
		seq := msgInfo.Sequence

		// NACK for retry (no poisoning)
		nack := func(cause error) {
			slog.Warn("Message parse failed",
				"worker_id", w.id,
				"sequence", seq,
				"error", cause,
				"subject", msgInfo.Msg.Subject)
			failedSequenceNumbers = append(failedSequenceNumbers, seq)
			w.parseFailures.Add(1)
		}

		res, err := w.parser.Parse(msgInfo.Msg)
		switch {
		case err != nil:
			nack(err)
		case w.parseErrorAction == "fail" && res.Outcome != parser.OutcomeOK:
			nack(firstError(res.Errors))
		case res.Outcome == parser.OutcomeUnusable:
			slog.Warn("Message dropped: structurally unusable",
				"worker_id", w.id,
				"sequence", seq,
				"error", firstError(res.Errors),
				"error_count", len(res.Errors),
				"subject", msgInfo.Msg.Subject)
			w.messagesDropped.Add(1)
		default:
			// OK, or Partial in drop mode: sentinel-filled row per ADR-005
			validTables = append(validTables, res.Tables...)
		}
	}

	return validTables, failedSequenceNumbers
}

// firstError returns the representative error from a step-error list.
// Ex: firstError([e1, e2]) → e1
func firstError(errs []error) error {
	if len(errs) == 0 {
		return nil
	}

	return errs[0]
}

// shutdown closes source/sink connections.
// In: none
// Out: error - always nil
// Ex: shutdown() → nil
func (w *Worker) shutdown() error {
	// Signal no new work should be processed
	w.closing.Store(true)

	// Wait for in-flight batches to complete
	w.activeBatches.Wait()

	// Now safe to close resources
	if w.sink != nil {
		w.sink.Close()
	}

	return nil
}

// handleWriteFailure handles write failure scenarios with proper error handling and hooks
func (w *Worker) handleWriteFailure(ctx context.Context, batch *source.MessageBatch, validSequences []uint64, err error) error {
	// Write failed - NACK valid messages for retry
	w.writeFailures.Add(1)

	// PreAck hook (NACK case)
	preAckData := &hooks.PreAckData{
		WorkerID:  w.workerID,
		Topic:     "all",
		Sequences: validSequences,
		IsNack:    true,
		Count:     len(validSequences),
		Timestamp: time.Now(),
	}
	w.hooks.Execute(ctx, "PreAck", preAckData)

	cbErr := batch.NackFunc(validSequences)

	// PostAck hook (NACK case)
	ackedCount := 0
	nackedCount := 0
	if cbErr == nil {
		nackedCount = len(validSequences)
		w.nacks.Add(uint64(nackedCount))
	}
	postAckData := &hooks.PostAckData{
		WorkerID:    w.workerID,
		Topic:       "all",
		AckedCount:  ackedCount,
		NackedCount: nackedCount,
		Error:       cbErr,
		Timestamp:   time.Now(),
	}
	w.hooks.Execute(ctx, "PostAck", postAckData)

	if cbErr != nil {
		slog.Error("Failed to NACK after write failure", "worker_id", w.id, "error", cbErr)
	}

	// Circuit breaker opened - backpressure protection
	if w.circuitBreaker.IsOpen() {
		slog.Warn("Circuit breaker opened, NACKing all messages for retry",
			"worker_id", w.id, "circuit_state", w.circuitBreaker.GetState())
	} else {
		slog.Error("Sink write failed", "worker_id", w.id, "error", err)
	}

	return err
}
