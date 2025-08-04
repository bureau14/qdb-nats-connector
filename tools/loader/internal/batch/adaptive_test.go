package batch

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewAdaptiveBatcher(t *testing.T) {
	input := make(chan []byte, 10)
	batcher, output := NewAdaptiveBatcher(10, 100, 1000, 50*time.Millisecond, input)

	if batcher == nil {
		t.Fatal("Expected batcher to be created, got nil")
	}

	if output == nil {
		t.Fatal("Expected output channel to be created, got nil")
	}

	// Verify initial settings
	if batcher.minSize != 10 {
		t.Errorf("Expected minSize 10, got %d", batcher.minSize)
	}
	if batcher.maxSize != 100 {
		t.Errorf("Expected maxSize 100, got %d", batcher.maxSize)
	}
	if batcher.targetRate != 1000 {
		t.Errorf("Expected targetRate 1000, got %d", batcher.targetRate)
	}

	// Check initial batch size is within range
	currentSize := int(atomic.LoadInt64(&batcher.currentSize))
	if currentSize < batcher.minSize || currentSize > batcher.maxSize {
		t.Errorf("Initial batch size %d not within range [%d, %d]", currentSize, batcher.minSize, batcher.maxSize)
	}
}

func TestAdaptiveBatcherDefaults(t *testing.T) {
	input := make(chan []byte, 10)
	// Test with invalid parameters to verify defaults are applied
	batcher, _ := NewAdaptiveBatcher(0, -1, -1, 50*time.Millisecond, input)

	if batcher.minSize != 10 {
		t.Errorf("Expected default minSize 10, got %d", batcher.minSize)
	}
	if batcher.maxSize != 1000 {
		t.Errorf("Expected default maxSize 1000, got %d", batcher.maxSize)
	}
	if batcher.targetRate != 10000 {
		t.Errorf("Expected default targetRate 10000, got %d", batcher.targetRate)
	}
}

func TestAdaptiveBatcherMinMaxValidation(t *testing.T) {
	input := make(chan []byte, 10)
	// Test with minSize > maxSize to verify minSize is adjusted
	batcher, _ := NewAdaptiveBatcher(200, 100, 1000, 50*time.Millisecond, input)

	if batcher.minSize != 100 {
		t.Errorf("Expected minSize to be adjusted to maxSize (100), got %d", batcher.minSize)
	}
}

func TestAdaptiveBatcherMetrics(t *testing.T) {
	input := make(chan []byte, 10)
	batcher, output := NewAdaptiveBatcher(10, 100, 1000, 50*time.Millisecond, input)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := batcher.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}

	// Send a message
	testMessage := []byte("test message")
	input <- testMessage
	close(input)

	// Wait for batch to be processed
	select {
	case batch := <-output:
		if len(batch.Messages) != 1 {
			t.Errorf("Expected 1 message in batch, got %d", len(batch.Messages))
		}
		if string(batch.Messages[0]) != "test message" {
			t.Errorf("Expected 'test message', got '%s'", string(batch.Messages[0]))
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timeout waiting for batch")
	}

	// Give a small delay for metrics to be updated
	time.Sleep(10 * time.Millisecond)

	// Check metrics
	metrics := batcher.GetMetrics()
	if metrics.MessagesProcessed != 1 {
		t.Errorf("Expected 1 message processed, got %d", metrics.MessagesProcessed)
	}
	if metrics.BatchesCreated != 1 {
		t.Errorf("Expected 1 batch created, got %d", metrics.BatchesCreated)
	}
}
