// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: NATS→QuasarDB pipeline orchestration
// Types: Connector, Worker, Options
// Ex: connector.New(opts).Run() → streams data
package connector

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	"github.com/bureau14/qdb-nats-connector/connector/resilience"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/parser"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/bureau14/qdb-nats-connector/internal/source"
)

// Worker: processes NATS topic→QuasarDB, goroutine-safe
type Worker struct {
	id             int
	workerID       string // Cached worker ID string for performance
	topicFilter    string
	source         *source.Source
	parser         parser.Parser
	sink           *sink.Sink
	circuitBreaker *resilience.CircuitBreaker
	hooks          *hooks.HookRegistry

	// Health tracking
	mu           sync.RWMutex
	lastFetch    time.Time
	batchTimeout time.Duration

	// Metrics (using atomic for thread safety)
	messagesProcessed atomic.Uint64
	parseFailures     atomic.Uint64
	writeFailures     atomic.Uint64
}

// NewWorker creates NATS→QuasarDB worker.
// Args:
//
//	id: worker identifier ≥0
//	topicFilter: NATS subject pattern (e.g., "sensors.>")
//	opts: connector configuration
//	manager: circuit breaker manager (optional)
//
// Returns:
//
//	*Worker: configured worker with parser/sink
//	error: source/parser/sink initialization failure
//
// Example:
//
//	w := NewWorker(0, "sensors.>", opts, manager) // → *Worker
func NewWorker(id int, topicFilter string, opts Options, manager *resilience.Manager) (*Worker, error) {
	// Create source with topic-specific consumer
	consumerName := generateConsumerName(opts.NatsConsumerPrefix, topicFilter)

	// Create proper source.Options using FromOptionsProvider
	sourceOpts := source.FromOptionsProvider(&opts, topicFilter)

	// Override consumer name for topic-specific consumer
	sourceOpts.ConsumerName = consumerName

	src, err := source.NewSource(sourceOpts)
	if err != nil {
		return nil, err
	}

	// Create parser using factory based on command-line options
	parserOpts := parser.ParserOptions{
		ParserType:  opts.Parser,
		ConfigPath:  opts.ParserConfig,
		ErrorAction: opts.ParseErrorAction,
	}
	messageParser, err := parser.NewParserWithOptions(parserOpts)
	if err != nil {
		return nil, err
	}

	// Create sink
	qdbSink, err := sink.NewSink(sink.FromOptionsProvider(&opts))
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

	return &Worker{
		id:             id,
		workerID:       workerID,
		topicFilter:    topicFilter,
		source:         src,
		parser:         messageParser,
		sink:           qdbSink,
		circuitBreaker: circuitBreaker,
		hooks:          opts.Hooks,
		batchTimeout:   opts.NatsBatchTimeout,
	}, nil
}

// Run starts worker message processing loop.
// Args:
//
//	ctx: cancellation context
//
// Returns:
//
//	error: connection failure/context cancelled
//
// Example:
//
//	w.Run(ctx) // blocks until ctx.Done()
func (w *Worker) Run(ctx context.Context) error {
	// Connect components
	err := w.source.Connect(ctx)
	if err != nil {
		return err
	}
	defer w.source.Close()

	err = w.sink.Connect(ctx)
	if err != nil {
		return err
	}
	defer w.sink.Close()

	// Main processing loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err := w.processBatch(ctx)
			if err != nil {
				// Log error but continue processing
				fmt.Printf("Worker %d error: %v\n", w.id, err)
			}

			w.mu.Lock()
			w.lastFetch = time.Now()
			w.mu.Unlock()
		}
	}
}

// GetStats returns worker statistics for monitoring.
// Args: none
// Returns:
//
//	messagesProcessed: total successfully processed
//	parseFailures: total parse errors
//	writeFailures: total write errors
//
// Example:
//
//	proc, parse, write := w.GetStats() // → 1000, 5, 2
func (w *Worker) GetStats() (messagesProcessed, parseFailures, writeFailures uint64) {
	return w.messagesProcessed.Load(), w.parseFailures.Load(), w.writeFailures.Load()
}

// fetchMessages retrieves batch from NATS source.
// In: ctx context.Context - cancellation
// Out: *source.MessageBatch, error on connection failure
// Ex: fetchMessages(ctx) → batch, nil
func (w *Worker) fetchMessages(ctx context.Context) (*source.MessageBatch, error) {
	batch, err := w.source.FetchBatch(ctx)
	if err != nil {
		return nil, errors.NewConnectionFailedError("worker", w.topicFilter, err)
	}

	return batch, nil
}

// parseMessages transforms NATS→tables, tracks failures.
// In: batch *source.MessageBatch - messages to parse
// Out: []qdb.WriterTable - valid tables, []uint64 - failedSequenceNumbers
// Ex: parseMessages(batch) → [table1, table2], [seq3]
func (w *Worker) parseMessages(batch *source.MessageBatch) (validTables []qdb.WriterTable, failedSequenceNumbers []uint64) {
	// Parse messages individually
	for _, msgInfo := range batch.Messages {
		seq := msgInfo.Sequence

		// Parse individual message
		tables, err := w.parser.Parse(msgInfo.Msg)
		if err != nil {
			// Log parse error with details for debugging
			slog.Warn("Message parse failed",
				"worker_id", w.id,
				"sequence", seq,
				"error", err,
				"subject", msgInfo.Msg.Subject)

			// Simply NACK for retry (no poisoning)
			failedSequenceNumbers = append(failedSequenceNumbers, seq)
			w.parseFailures.Add(1)

			continue
		}

		// Add all parsed tables to valid results
		validTables = append(validTables, tables...)
	}

	return validTables, failedSequenceNumbers
}

// processBatch executes fetch→parse→write pipeline.
// In: ctx context.Context - cancellation
// Out: error if fetch/write fails
// Ex: processBatch(ctx) → nil
func (w *Worker) processBatch(ctx context.Context) error {
	// PreRead hook
	preReadData := &hooks.PreReadData{
		WorkerID:  w.workerID,
		Topic:     w.topicFilter,
		Timestamp: time.Now(),
	}
	err := w.hooks.Execute(ctx, "PreRead", preReadData)
	if err != nil {
		return err
	}

	// 1. Fetch
	fetchStart := time.Now()
	batch, err := w.fetchMessages(ctx)
	fetchDuration := time.Since(fetchStart)

	// PostRead hook
	messageCount := 0
	batchSize := 0
	if batch != nil {
		messageCount = len(batch.Messages)
		batchSize = messageCount
	}
	postReadData := &hooks.PostReadData{
		WorkerID:     w.workerID,
		Topic:        w.topicFilter,
		MessageCount: messageCount,
		BatchSize:    batchSize,
		Duration:     fetchDuration,
		Error:        err,
		Timestamp:    time.Now(),
	}
	err = w.hooks.Execute(ctx, "PostRead", postReadData)
	if err != nil {
		slog.Error("PostRead hook failed", "worker_id", w.id, "error", err)
	}

	if err != nil {
		return err
	}

	if len(batch.Messages) == 0 {
		return nil
	}

	// 2. Parse messages - failures retry automatically
	validTables, failedSequenceNumbers := w.parseMessages(batch)

	// 3. NACK failed parses immediately for retry
	if len(failedSequenceNumbers) > 0 {
		err := batch.NackFunc(failedSequenceNumbers)
		if err != nil {
			slog.Error("Failed to NACK parse failures", "worker_id", w.id, "error", err)
		}
	}

	// 4. If no valid messages, we're done
	if len(validTables) == 0 {
		return nil
	}

	// 5. Write valid messages with circuit breaker
	tables := validTables

	// PreWrite hook
	rowCount := 0
	for _, table := range tables {
		rowCount += table.RowCount()
	}
	preWriteData := &hooks.PreWriteData{
		WorkerID:   w.workerID,
		Topic:      w.topicFilter,
		TableCount: len(tables),
		RowCount:   rowCount,
		Tables:     tables,
		Timestamp:  time.Now(),
	}
	err = w.hooks.Execute(ctx, "PreWrite", preWriteData)
	if err != nil {
		return err
	}

	writeStart := time.Now()
	err = w.circuitBreaker.Execute(func() error {
		return w.sink.Write(tables)
	})
	writeDuration := time.Since(writeStart)

	// PostWrite hook
	rowsWritten := 0
	tablesWritten := 0
	if err == nil {
		for _, table := range tables {
			rowsWritten += table.RowCount()
		}
		tablesWritten = len(tables)
	}
	postWriteData := &hooks.PostWriteData{
		WorkerID:      w.workerID,
		Topic:         w.topicFilter,
		Duration:      writeDuration,
		RowsWritten:   rowsWritten,
		TablesWritten: tablesWritten,
		Error:         err,
		Timestamp:     time.Now(),
	}
	err = w.hooks.Execute(ctx, "PostWrite", postWriteData)
	if err != nil {
		slog.Error("PostWrite hook failed", "worker_id", w.id, "error", err)
	}

	// 6. ACK or NACK based on write result
	// Calculate valid sequences by excluding failed ones
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

	if err != nil {
		return w.handleWriteFailure(ctx, batch, validSequences, err)
	}

	// Write succeeded - ACK valid messages
	// PreAck hook (ACK case)
	preAckData := &hooks.PreAckData{
		WorkerID:  w.workerID,
		Topic:     w.topicFilter,
		Sequences: validSequences,
		IsNack:    false,
		Count:     len(validSequences),
		Timestamp: time.Now(),
	}
	err = w.hooks.Execute(ctx, "PreAck", preAckData)
	if err != nil {
		// Log the error but continue with ACK to prevent infinite redelivery
		slog.Error("PreAck hook failed, continuing with ACK", "worker_id", w.id, "error", err)
	}

	ackErr := batch.AckFunc(validSequences)

	// PostAck hook (ACK case)
	ackedCount := 0
	nackedCount := 0
	if ackErr == nil {
		ackedCount = len(validSequences)
	}
	postAckData := &hooks.PostAckData{
		WorkerID:    fmt.Sprintf("worker-%d", w.id),
		Topic:       w.topicFilter,
		AckedCount:  ackedCount,
		NackedCount: nackedCount,
		Error:       ackErr,
		Timestamp:   time.Now(),
	}
	err = w.hooks.Execute(ctx, "PostAck", postAckData)
	if err != nil {
		slog.Error("PostAck hook failed", "worker_id", w.id, "error", err)
	}

	if ackErr != nil {
		slog.Error("Failed to ACK after write success", "worker_id", w.id, "error", ackErr)
	}

	return nil
}

// isHealthy verifies processing within 2x batch timeout.
// In: none
// Out: bool - true if lastFetch < 2*batchTimeout
// Ex: isHealthy() → true
func (w *Worker) isHealthy() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Healthy if fetched within 2x batch timeout
	if time.Since(w.lastFetch) > 2*w.batchTimeout {
		return false
	}

	return true
}

// shutdown closes source/sink connections.
// In: none
// Out: error - always nil
// Ex: shutdown() → nil
func (w *Worker) shutdown() error {
	if w.source != nil {
		w.source.Close()
	}
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
		Topic:     w.topicFilter,
		Sequences: validSequences,
		IsNack:    true,
		Count:     len(validSequences),
		Timestamp: time.Now(),
	}
	hookErr := w.hooks.Execute(ctx, "PreAck", preAckData)
	if hookErr != nil {
		// Log the error but continue with NACK to prevent infinite redelivery
		slog.Error("PreAck hook failed, continuing with NACK", "worker_id", w.id, "error", hookErr)
	}

	cbErr := batch.NackFunc(validSequences)

	// PostAck hook (NACK case)
	nackedCount := 0
	ackedCount := 0
	if cbErr == nil {
		nackedCount = len(validSequences)
	}
	postAckData := &hooks.PostAckData{
		WorkerID:    w.workerID,
		Topic:       w.topicFilter,
		AckedCount:  ackedCount,
		NackedCount: nackedCount,
		Error:       cbErr,
		Timestamp:   time.Now(),
	}
	hookErr = w.hooks.Execute(ctx, "PostAck", postAckData)
	if hookErr != nil {
		slog.Error("PostAck hook failed", "worker_id", w.id, "error", hookErr)
	}

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

// generateConsumerName creates SHA256-based consumer name
// In: prefix string, topic string
// Out: string - "prefix-hash[:8]"
// Ex: generateConsumerName("qdb", "sensors") → "qdb-1a2b3c4d"
func generateConsumerName(prefix, topic string) string {
	h := sha256.Sum256([]byte(topic))

	return fmt.Sprintf("%s-%x", prefix, h[:8])
}
