package batch

import (
	"sync/atomic"
	"time"
)

// Metrics represents batching performance metrics
type Metrics struct {
	MessagesProcessed int64
	BatchesCreated    int64
	AverageBatchSize  float64
	Throughput        float64 // messages/sec
	CurrentBatchSize  int
}

// MetricsCollector provides thread-safe metrics collection
type MetricsCollector struct {
	messagesProcessed int64
	batchesCreated    int64
	startTime         time.Time
	currentBatchSize  int64
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		startTime: time.Now(),
	}
}

// IncrementMessages atomically increments the message counter
func (m *MetricsCollector) IncrementMessages() {
	atomic.AddInt64(&m.messagesProcessed, 1)
}

// IncrementBatches atomically increments the batch counter
func (m *MetricsCollector) IncrementBatches() {
	atomic.AddInt64(&m.batchesCreated, 1)
}

// SetCurrentBatchSize atomically sets the current batch size
func (m *MetricsCollector) SetCurrentBatchSize(size int) {
	atomic.StoreInt64(&m.currentBatchSize, int64(size))
}

// GetMetrics returns current metrics snapshot
func (m *MetricsCollector) GetMetrics() Metrics {
	messages := atomic.LoadInt64(&m.messagesProcessed)
	batches := atomic.LoadInt64(&m.batchesCreated)
	currentSize := atomic.LoadInt64(&m.currentBatchSize)

	elapsed := time.Since(m.startTime).Seconds()

	var avgBatchSize float64
	if batches > 0 {
		avgBatchSize = float64(messages) / float64(batches)
	}

	var throughput float64
	if elapsed > 0 {
		throughput = float64(messages) / elapsed
	}

	return Metrics{
		MessagesProcessed: messages,
		BatchesCreated:    batches,
		AverageBatchSize:  avgBatchSize,
		Throughput:        throughput,
		CurrentBatchSize:  int(currentSize),
	}
}
