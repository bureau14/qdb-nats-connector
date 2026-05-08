package batch

import (
	"context"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
)

// Batch represents a collection of messages ready for publishing
type Batch struct {
	Messages    [][]byte
	Size        int
	IsTombstone bool // true if this is a tombstone batch
}

// Batcher collects messages into batches based on size and timeout
type Batcher struct {
	size     int
	timeout  time.Duration
	messages [][]byte
	timer    *time.Timer
	output   chan<- Batch
	input    <-chan internal.Message
	metrics  *MetricsCollector
}

// NewBatcher creates a new batcher with the specified size and timeout
// Returns the batcher instance and a channel to receive batches
func NewBatcher(size int, timeout time.Duration, input <-chan internal.Message) (batcher *Batcher, output <-chan Batch) {
	if size <= 0 {
		size = 500 // Default batch size
	}
	if timeout <= 0 {
		timeout = 100 * time.Millisecond // Default timeout
	}

	outputChan := make(chan Batch, 10) // Buffered channel to prevent blocking
	batcher = &Batcher{
		size:     size,
		timeout:  timeout,
		messages: make([][]byte, 0, size),
		output:   outputChan,
		input:    input,
		metrics:  NewMetricsCollector(),
	}

	// Set initial batch size in metrics
	batcher.metrics.SetCurrentBatchSize(size)

	return batcher, outputChan
}

// Start begins the batching process in a goroutine
// It collects messages into batches and sends them when size is reached or timeout expires
func (b *Batcher) Start(ctx context.Context) error {
	if b.input == nil {
		return connectorErrors.NewInvalidConfigError("batcher", "input channel is nil")
	}
	if b.output == nil {
		return connectorErrors.NewInvalidConfigError("batcher", "output channel is nil")
	}

	go func() {
		defer b.cleanup()

		for {
			select {
			case <-ctx.Done():
				// Context cancelled, send remaining messages as final batch
				b.sendFinalBatch()
				// Send tombstone to signal downstream workers
				b.sendTombstoneBatch()

				return

			case message, ok := <-b.input:
				if !ok {
					// Input channel closed, send remaining messages as final batch
					b.sendFinalBatch()

					return
				}

				// Check if this is a tombstone message
				if message.Type == internal.MessageTypeTombstone {
					// Send any pending messages first
					if len(b.messages) > 0 {
						b.sendBatch()
					}

					// Send tombstone batch
					b.sendTombstoneBatch()

					return
				}

				// Track message
				b.metrics.IncrementMessages()

				// Add message data to current batch
				b.messages = append(b.messages, message.Data)

				// If this is the first message, start the timer
				if len(b.messages) == 1 {
					b.resetTimer()
				}

				// Send batch if size limit reached
				if len(b.messages) >= b.size {
					b.sendBatch()
				}

			case <-b.getTimerChannel():
				// Timeout expired, send current batch if not empty
				if len(b.messages) > 0 {
					b.sendBatch()
				}
			}
		}
	}()

	return nil
}

// GetMetrics returns current batching metrics
func (b *Batcher) GetMetrics() Metrics {
	return b.metrics.GetMetrics()
}

// sendBatch sends the current batch and resets the message buffer
//
//nolint:dupl // mirrors AdaptiveBatcher.sendAdaptiveBatch; differences in metrics ownership prevent extraction
func (b *Batcher) sendBatch() {
	if len(b.messages) == 0 {
		return
	}

	// Update batch metrics
	b.metrics.IncrementBatches()

	batch := Batch{
		Messages:    make([][]byte, len(b.messages)),
		Size:        len(b.messages),
		IsTombstone: false,
	}

	// Copy messages to avoid sharing slices
	copy(batch.Messages, b.messages)

	// Send batch to output channel - block until space available
	// No default case to prevent silent data loss
	select {
	case b.output <- batch:
		// Batch sent successfully
	case <-time.After(5 * time.Second):
		// Log error if blocked too long, but continue trying
		// This indicates backpressure issues that need investigation
		panic("batch channel blocked for 5+ seconds - indicates severe backpressure")
	}

	// Reset the message buffer
	b.messages = b.messages[:0]

	// Stop the timer if it's running
	b.stopTimer()
}

// sendFinalBatch sends any remaining messages as the final batch
func (b *Batcher) sendFinalBatch() {
	if len(b.messages) > 0 {
		b.sendBatch()
	}
}

// sendTombstoneBatch sends a tombstone batch to signal end of data
func (b *Batcher) sendTombstoneBatch() {
	batch := Batch{
		Messages:    nil,
		Size:        -1,
		IsTombstone: true,
	}

	// Send tombstone batch to output channel - must succeed
	// Block until space available since tombstone is critical for clean shutdown
	select {
	case b.output <- batch:
		// Tombstone sent successfully
	case <-time.After(5 * time.Second):
		// Log error if blocked too long, but continue trying
		panic("tombstone send blocked for 5+ seconds - critical shutdown failure")
	}
}

// resetTimer starts or resets the timeout timer
func (b *Batcher) resetTimer() {
	b.stopTimer()
	b.timer = time.NewTimer(b.timeout)
}

// stopTimer stops the timer if it's running
func (b *Batcher) stopTimer() {
	if b.timer != nil {
		if !b.timer.Stop() {
			// Timer had already fired, drain the channel
			select {
			case <-b.timer.C:
			default:
			}
		}
		b.timer = nil
	}
}

// getTimerChannel returns the timer channel or nil if timer is not active
func (b *Batcher) getTimerChannel() <-chan time.Time {
	if b.timer == nil {
		return nil
	}

	return b.timer.C
}

// cleanup closes the output channel and stops the timer
// This signals all downstream workers that no more batches are coming
func (b *Batcher) cleanup() {
	b.stopTimer()
	// Close channel after tombstone to signal all workers
	close(b.output)
}
