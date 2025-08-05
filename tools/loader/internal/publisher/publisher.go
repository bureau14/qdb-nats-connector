package publisher

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal/batch"
)

// Publisher manages a pool of workers for parallel batch publishing
// NOTE: This is a test tool - no backpressure by design.
// Failure under load is a valid test result that reveals system limits.
type Publisher struct {
	connection *internal.Connection
	workers    int
	input      <-chan batch.Batch
	topic      string
	semaphore  chan struct{}
	// Metrics
	messagesPublished int64
	batchesProcessed  int64
	publishErrors     int64
	// Completion tracking
	workersCompleted int64
	completionChan   chan struct{}
}

// NewPublisher creates a new publisher with the specified number of workers
func NewPublisher(conn *internal.Connection, workers int, input <-chan batch.Batch, topic string) *Publisher {
	if workers <= 0 {
		workers = 4 // Default to 4 workers for safety
	}

	return &Publisher{
		connection:     conn,
		workers:        workers,
		input:          input,
		topic:          topic,
		semaphore:      make(chan struct{}, workers), // Limit concurrent publishing
		completionChan: make(chan struct{}),
	}
}

// NewPublisherWithBackpressure is deprecated but kept for compatibility
// It now just calls NewPublisher, ignoring backpressure parameters
func NewPublisherWithBackpressure(conn *internal.Connection, workers int, input <-chan batch.Batch, topic string, maxQueueDepth, circuitMaxFailures int, circuitResetTimeout time.Duration) *Publisher {
	// Ignore backpressure parameters - no longer used
	return NewPublisher(conn, workers, input, topic)
}

// Start begins the publisher worker pool and processes batches
func (p *Publisher) Start(ctx context.Context) error {
	if p.connection == nil {
		return connectorErrors.NewInvalidConfigError("publisher", "connection is nil")
	}
	if p.input == nil {
		return connectorErrors.NewInvalidConfigError("publisher", "input channel is nil")
	}

	slog.Info("Starting publisher worker pool", "workers", p.workers)

	var wg sync.WaitGroup

	// Start worker goroutines
	for i := range p.workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker := &Worker{
				id:        workerID,
				publisher: p,
			}
			worker.Run(ctx, p.input, p.topic)
		}(i)
	}

	// Start metrics logging goroutine
	go p.logMetrics(ctx)

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		slog.Info("All publisher workers completed")
		close(p.completionChan)
	}()

	return nil
}

// publishBatch publishes each message in the batch to the specified topic
// No backpressure - designed to find system limits through failure
func (p *Publisher) publishBatch(ctx context.Context, batchData batch.Batch, topic string) error {
	// Acquire semaphore to limit concurrent publishing
	select {
	case p.semaphore <- struct{}{}:
		defer func() { <-p.semaphore }()
	case <-ctx.Done():
		return ctx.Err()
	}

	// Publish all messages in the batch directly
	for _, messageData := range batchData.Messages {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := p.connection.Publish(topic, messageData)
		if err != nil {
			atomic.AddInt64(&p.publishErrors, 1)

			return connectorErrors.NewWriteFailedError("publisher", err)
		}

		atomic.AddInt64(&p.messagesPublished, 1)
	}

	atomic.AddInt64(&p.batchesProcessed, 1)

	return nil
}

// logMetrics periodically logs publishing metrics
func (p *Publisher) logMetrics(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			messages := atomic.LoadInt64(&p.messagesPublished)
			batches := atomic.LoadInt64(&p.batchesProcessed)
			errors := atomic.LoadInt64(&p.publishErrors)

			if messages > 0 || errors > 0 {
				slog.Info("Publisher metrics",
					"messages_published", messages,
					"batches_processed", batches,
					"publish_errors", errors,
					"workers", p.workers)
			}
		}
	}
}

// GetMetrics returns the current publishing metrics
func (p *Publisher) GetMetrics() (messagesPublished, batchesProcessed int64) {
	return atomic.LoadInt64(&p.messagesPublished), atomic.LoadInt64(&p.batchesProcessed)
}

// GetBackpressureMetrics is deprecated - returns zeros for compatibility
func (p *Publisher) GetBackpressureMetrics() (circuitTrips, backpressureEvents int64) {
	// No backpressure in this tool - always return 0
	return 0, 0
}

// GetCompletionChannel returns a channel that closes when all workers complete
func (p *Publisher) GetCompletionChannel() <-chan struct{} {
	return p.completionChan
}

// OnWorkerCompleted should be called when a worker completes after tombstone
func (p *Publisher) OnWorkerCompleted() {
	completed := atomic.AddInt64(&p.workersCompleted, 1)
	slog.Debug("Worker completed", "completed_count", completed, "total_workers", p.workers)
}
