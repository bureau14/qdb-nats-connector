package batch

import (
	"sync/atomic"
	"testing"

	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
)

func TestAdaptiveBatcher_MaxSizeLimit(t *testing.T) {
	input := make(chan internal.Message, 10)
	maxSize := 100
	batcher, _ := NewAdaptiveBatcher(10, maxSize, 1000, 50, input)

	// Verify batch size never exceeds maximum
	currentSize := int(atomic.LoadInt64(&batcher.currentSize))
	if currentSize > maxSize {
		t.Errorf("Batch size %d exceeds maximum %d", currentSize, maxSize)
	}
}

func TestAdaptiveBatcher_MinSizeLimit(t *testing.T) {
	input := make(chan internal.Message, 10)
	minSize := 10
	batcher, _ := NewAdaptiveBatcher(minSize, 100, 1000, 50, input)

	// Verify batch size never goes below minimum
	currentSize := int(atomic.LoadInt64(&batcher.currentSize))
	if currentSize < minSize {
		t.Errorf("Batch size %d below minimum %d", currentSize, minSize)
	}
}
