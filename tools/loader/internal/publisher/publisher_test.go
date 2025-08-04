package publisher

import (
	"testing"

	"github.com/bureau14/qdb-nats-connector/tools/loader/internal/batch"
)

func TestPublisher_NewPublisher(t *testing.T) {
	input := make(chan batch.Batch, 1)
	topic := "test.topic"

	// Test with nil connection to just verify constructor logic
	pub := NewPublisher(nil, 2, input, topic)

	if pub.workers != 2 {
		t.Errorf("Expected 2 workers, got %d", pub.workers)
	}
	if pub.topic != topic {
		t.Errorf("Expected topic %s, got %s", topic, pub.topic)
	}
	if cap(pub.semaphore) != 2 {
		t.Errorf("Expected semaphore capacity 2, got %d", cap(pub.semaphore))
	}
}

func TestPublisher_DefaultWorkers(t *testing.T) {
	input := make(chan batch.Batch, 1)
	topic := "test.topic"

	pub := NewPublisher(nil, 0, input, topic) // 0 workers should default to 4

	if pub.workers != 4 {
		t.Errorf("Expected default 4 workers, got %d", pub.workers)
	}
	if cap(pub.semaphore) != 4 {
		t.Errorf("Expected semaphore capacity 4, got %d", cap(pub.semaphore))
	}
}

func TestPublisher_NegativeWorkers(t *testing.T) {
	input := make(chan batch.Batch, 1)
	topic := "test.topic"

	pub := NewPublisher(nil, -5, input, topic) // Negative workers should default to 4

	if pub.workers != 4 {
		t.Errorf("Expected default 4 workers for negative input, got %d", pub.workers)
	}
}

func TestPublisher_GetMetrics(t *testing.T) {
	input := make(chan batch.Batch, 1)
	topic := "test.topic"

	pub := NewPublisher(nil, 2, input, topic)

	messages, batches := pub.GetMetrics()
	if messages != 0 {
		t.Errorf("Expected 0 messages initially, got %d", messages)
	}
	if batches != 0 {
		t.Errorf("Expected 0 batches initially, got %d", batches)
	}
}
