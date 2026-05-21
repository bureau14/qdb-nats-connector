package batch

import (
	"context"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
)

// AdaptiveBatcher adjusts batch size based on throughput and latency
type AdaptiveBatcher struct {
	*Batcher
	minSize     int
	maxSize     int
	targetRate  int // messages per second
	window      time.Duration
	lastAdjust  time.Time
	metrics     *MetricsCollector
	currentSize int64 // atomic access
}

// NewAdaptiveBatcher creates a new adaptive batcher with the specified parameters
// Returns the batcher instance and a channel to receive batches
func NewAdaptiveBatcher(minSize, maxSize, targetRate int, timeout time.Duration, input <-chan internal.Message) (batcher *AdaptiveBatcher, output <-chan Batch) {
	if minSize <= 0 {
		minSize = 10
	}
	if maxSize <= 0 {
		maxSize = 1000
	}
	if targetRate <= 0 {
		targetRate = 10000
	}
	if minSize > maxSize {
		minSize = maxSize
	}

	// Start with initial size between min and max
	initialSize := minSize + (maxSize-minSize)/4 // Start at 25% of range

	regularBatcher, output := NewBatcher(initialSize, timeout, input)

	batcher = &AdaptiveBatcher{
		Batcher:     regularBatcher,
		minSize:     minSize,
		maxSize:     maxSize,
		targetRate:  targetRate,
		window:      10 * time.Second, // Adjustment window
		lastAdjust:  time.Now(),
		metrics:     NewMetricsCollector(),
		currentSize: int64(initialSize),
	}

	// Set initial batch size in metrics
	batcher.metrics.SetCurrentBatchSize(initialSize)

	return batcher, output
}

// Start begins the adaptive batching process
func (ab *AdaptiveBatcher) Start(ctx context.Context) error {
	if ab.input == nil {
		return connectorErrors.NewInvalidConfigError("adaptive_batcher", "input channel is nil")
	}
	if ab.output == nil {
		return connectorErrors.NewInvalidConfigError("adaptive_batcher", "output channel is nil")
	}

	// Start the base batcher in a goroutine
	go func() {
		defer ab.cleanup()
		ab.runBatchingLoop(ctx)
	}()

	// Start the adaptive monitoring goroutine
	go func() {
		ab.runAdaptiveLoop(ctx)
	}()

	return nil
}

// GetMetrics returns current adaptive batching metrics
func (ab *AdaptiveBatcher) GetMetrics() Metrics {
	return ab.metrics.GetMetrics()
}

// runBatchingLoop runs the main batching logic with adaptive size
func (ab *AdaptiveBatcher) runBatchingLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			ab.sendAdaptiveFinalBatch()
			// Send tombstone to signal downstream workers
			ab.sendAdaptiveTombstoneBatch()

			return

		case message, ok := <-ab.input:
			if !ok {
				ab.sendAdaptiveFinalBatch()

				return
			}

			// Check if this is a tombstone message
			if message.Type == internal.MessageTypeTombstone {
				// Send any pending messages first
				if len(ab.messages) > 0 {
					ab.sendAdaptiveBatch()
				}

				// Send tombstone batch
				ab.sendAdaptiveTombstoneBatch()

				return
			}

			// Track message
			ab.metrics.IncrementMessages()

			// Add message data to current batch
			ab.messages = append(ab.messages, message.Data)

			// If this is the first message, start the timer
			if len(ab.messages) == 1 {
				ab.resetTimer()
			}

			// Get current adaptive batch size
			currentSize := int(atomic.LoadInt64(&ab.currentSize))

			// Send batch if size limit reached
			if len(ab.messages) >= currentSize {
				ab.sendAdaptiveBatch()
			}

		case <-ab.getTimerChannel():
			// Timeout expired, send current batch if not empty
			if len(ab.messages) > 0 {
				ab.sendAdaptiveBatch()
			}
		}
	}
}

// runAdaptiveLoop monitors performance and adjusts batch size
func (ab *AdaptiveBatcher) runAdaptiveLoop(ctx context.Context) {
	ticker := time.NewTicker(ab.window)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ab.adjustBatchSize()
		}
	}
}

// sendAdaptiveBatch sends the current batch and updates metrics
//
//nolint:dupl // mirrors Batcher.sendBatch; differences in metrics ownership prevent extraction
func (ab *AdaptiveBatcher) sendAdaptiveBatch() {
	if len(ab.messages) == 0 {
		return
	}

	// Update adaptive batch metrics
	ab.metrics.IncrementBatches()

	// Create batch manually to avoid double-counting in parent metrics
	batch := Batch{
		Messages:    make([][]byte, len(ab.messages)),
		Size:        len(ab.messages),
		IsTombstone: false,
	}

	// Copy messages to avoid sharing slices
	copy(batch.Messages, ab.messages)

	// Send batch to output channel - block until space available
	// No default case to prevent silent data loss
	select {
	case ab.output <- batch:
		// Batch sent successfully
	case <-time.After(5 * time.Second):
		// Log error if blocked too long, but continue trying
		// This indicates backpressure issues that need investigation
		panic("adaptive batch channel blocked for 5+ seconds - indicates severe backpressure")
	}

	// Reset the message buffer
	ab.messages = ab.messages[:0]

	// Stop the timer if it's running
	ab.stopTimer()
}

// sendAdaptiveFinalBatch sends any remaining messages as the final batch
func (ab *AdaptiveBatcher) sendAdaptiveFinalBatch() {
	if len(ab.messages) > 0 {
		ab.sendAdaptiveBatch()
	}
}

// sendAdaptiveTombstoneBatch sends a tombstone batch to signal end of data
func (ab *AdaptiveBatcher) sendAdaptiveTombstoneBatch() {
	batch := Batch{
		Messages:    nil,
		Size:        -1,
		IsTombstone: true,
	}

	// Send tombstone batch to output channel - must succeed
	// Block until space available since tombstone is critical for clean shutdown
	select {
	case ab.output <- batch:
		// Tombstone sent successfully
	case <-time.After(5 * time.Second):
		// Log error if blocked too long, but continue trying
		panic("adaptive tombstone send blocked for 5+ seconds - critical shutdown failure")
	}
}

// adjustBatchSize calculates and applies new batch size based on performance
func (ab *AdaptiveBatcher) adjustBatchSize() {
	now := time.Now()
	if now.Sub(ab.lastAdjust) < ab.window {
		return // Too soon to adjust
	}

	metrics := ab.metrics.GetMetrics()
	currentSize := int(atomic.LoadInt64(&ab.currentSize))

	// Calculate current throughput
	currentRate := metrics.Throughput
	targetRate := float64(ab.targetRate)

	var newSize int

	switch {
	case currentRate < targetRate*0.9:
		// Throughput too low, increase batch size to improve efficiency
		newSize = ab.min(int(float64(currentSize)*1.5), ab.maxSize)
		if newSize != currentSize {
			slog.Debug("Increasing batch size for better throughput",
				"current_rate", currentRate,
				"target_rate", targetRate,
				"old_size", currentSize,
				"new_size", newSize)
		}
	case currentRate > targetRate*1.1:
		// Throughput exceeding target, might indicate backpressure
		newSize = ab.max(int(float64(currentSize)*0.75), ab.minSize)
		if newSize != currentSize {
			slog.Debug("Decreasing batch size due to high throughput",
				"current_rate", currentRate,
				"target_rate", targetRate,
				"old_size", currentSize,
				"new_size", newSize)
		}
	default:
		// Throughput within target range, no adjustment needed
		newSize = currentSize
	}

	// Apply the new size if it changed
	if newSize != currentSize {
		atomic.StoreInt64(&ab.currentSize, int64(newSize))
		ab.metrics.SetCurrentBatchSize(newSize)
		ab.lastAdjust = now

		slog.Info("Adjusted batch size",
			"old_size", currentSize,
			"new_size", newSize,
			"throughput", currentRate,
			"target_rate", targetRate,
			"messages_processed", metrics.MessagesProcessed,
			"batches_created", metrics.BatchesCreated)
	}
}

// Helper functions for min/max
func (ab *AdaptiveBatcher) min(a, b int) int {
	return int(math.Min(float64(a), float64(b)))
}

func (ab *AdaptiveBatcher) max(a, b int) int {
	return int(math.Max(float64(a), float64(b)))
}

// cleanup closes the output channel and stops the timer for adaptive batcher
// This signals all downstream workers that no more batches are coming
func (ab *AdaptiveBatcher) cleanup() {
	ab.stopTimer()
	// Close channel after tombstone to signal all workers
	close(ab.output)
}
