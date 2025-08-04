package publisher

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/bureau14/qdb-nats-connector/tools/loader/internal/batch"
)

// Worker represents a single worker in the publisher pool
type Worker struct {
	id               int
	publisher        *Publisher
	messagesHandled  int64
	batchesHandled   int64
	totalProcessTime time.Duration
}

// Run starts the worker and processes batches from the input channel
func (w *Worker) Run(ctx context.Context, batches <-chan batch.Batch, topic string) {
	slog.Info("Starting publisher worker", "worker_id", w.id, "topic", topic)
	defer slog.Info("Publisher worker completed", "worker_id", w.id,
		"messages_handled", w.messagesHandled,
		"batches_handled", w.batchesHandled)

	for {
		select {
		case <-ctx.Done():
			slog.Info("Worker received cancellation signal", "worker_id", w.id)

			return
		case batchData, ok := <-batches:
			if !ok {
				slog.Debug("Batch channel closed", "worker_id", w.id)

				return
			}

			w.processBatch(ctx, batchData, topic)
		}
	}
}

// processBatch handles a single batch and tracks metrics
func (w *Worker) processBatch(ctx context.Context, batchData batch.Batch, topic string) {
	startTime := time.Now()
	defer func() {
		w.totalProcessTime += time.Since(startTime)
	}()

	err := w.publisher.publishBatch(ctx, batchData, topic)
	if err != nil {
		slog.Error("Failed to publish batch",
			"worker_id", w.id,
			"batch_size", batchData.Size,
			"error", err)

		return
	}

	// Update worker metrics
	atomic.AddInt64(&w.messagesHandled, int64(batchData.Size))
	atomic.AddInt64(&w.batchesHandled, 1)

	// Log progress for this worker
	if w.batchesHandled%100 == 0 {
		slog.Debug("Worker progress",
			"worker_id", w.id,
			"batches_handled", w.batchesHandled,
			"messages_handled", w.messagesHandled,
			"avg_process_time_ms", w.totalProcessTime.Milliseconds()/w.batchesHandled)
	}
}

// GetMetrics returns the current worker metrics
func (w *Worker) GetMetrics() (messagesHandled, batchesHandled int64, avgProcessTime time.Duration) {
	messages := atomic.LoadInt64(&w.messagesHandled)
	batches := atomic.LoadInt64(&w.batchesHandled)

	var avgTime time.Duration
	if batches > 0 {
		avgTime = time.Duration(w.totalProcessTime.Nanoseconds() / batches)
	}

	return messages, batches, avgTime
}
