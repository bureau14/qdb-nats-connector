package batch

import (
	"context"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
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
func NewAdaptiveBatcher(minSize, maxSize, targetRate int, timeout time.Duration, input <-chan []byte) (batcher *AdaptiveBatcher, output <-chan Batch) {
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

// runBatchingLoop runs the main batching logic with adaptive size
func (ab *AdaptiveBatcher) runBatchingLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			ab.sendAdaptiveFinalBatch()

			return

		case message, ok := <-ab.input:
			if !ok {
				ab.sendAdaptiveFinalBatch()

				return
			}

			// Track message
			ab.metrics.IncrementMessages()

			// Add message to current batch
			ab.messages = append(ab.messages, message)

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
func (ab *AdaptiveBatcher) sendAdaptiveBatch() {
	if len(ab.messages) == 0 {
		return
	}

	// Update adaptive batch metrics
	ab.metrics.IncrementBatches()

	// Create batch manually to avoid double-counting in parent metrics
	batch := Batch{
		Messages: make([][]byte, len(ab.messages)),
		Size:     len(ab.messages),
	}

	// Copy messages to avoid sharing slices
	copy(batch.Messages, ab.messages)

	// Send batch to output channel
	select {
	case ab.output <- batch:
		// Batch sent successfully
	default:
		// Output channel full, this shouldn't happen with proper buffering
		// but we'll handle it gracefully by continuing
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

// GetMetrics returns current adaptive batching metrics
func (ab *AdaptiveBatcher) GetMetrics() Metrics {
	return ab.metrics.GetMetrics()
}

// Helper functions for min/max
func (ab *AdaptiveBatcher) min(a, b int) int {
	return int(math.Min(float64(a), float64(b)))
}

func (ab *AdaptiveBatcher) max(a, b int) int {
	return int(math.Max(float64(a), float64(b)))
}
