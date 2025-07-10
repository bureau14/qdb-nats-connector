package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CounterHook is a test hook that counts how many times it's called
type CounterHook struct {
	mu       sync.Mutex
	count    int
	callData []interface{}
}

func NewCounterHook() *CounterHook {
	return &CounterHook{
		callData: make([]interface{}, 0),
	}
}

func (ch *CounterHook) Hook(ctx context.Context, data interface{}) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.count++
	ch.callData = append(ch.callData, data)

	return nil
}

func (ch *CounterHook) GetCount() int {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.count
}

func (ch *CounterHook) GetCallData() []interface{} {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return append([]interface{}{}, ch.callData...)
}

func (ch *CounterHook) Reset() {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.count = 0
	ch.callData = make([]interface{}, 0)
}

func TestHookRegistry(t *testing.T) {
	registry := hooks.NewHookRegistry()
	counter := NewCounterHook()

	// Test registering hooks
	registry.Register("test", counter.Hook)
	registry.Register("test", counter.Hook) // Register same hook twice

	// Test synchronous execution
	ctx := context.Background()
	testData := "test data"

	err := registry.Execute(ctx, "test", testData)
	require.NoError(t, err)

	// Should have called the hook twice
	assert.Equal(t, 2, counter.GetCount())

	// Check that data was passed correctly
	callData := counter.GetCallData()
	assert.Len(t, callData, 2)
	assert.Equal(t, testData, callData[0])
	assert.Equal(t, testData, callData[1])
}

func TestHookRegistryAsync(t *testing.T) {
	registry := hooks.NewHookRegistry()
	counter := NewCounterHook()

	registry.Register("async-test", counter.Hook)

	ctx := context.Background()
	testData := "async test data"

	// Test execution
	err := registry.Execute(ctx, "async-test", testData)
	assert.NoError(t, err)

	assert.Equal(t, 1, counter.GetCount())
	callData := counter.GetCallData()
	assert.Len(t, callData, 1)
	assert.Equal(t, testData, callData[0])
}

func TestHookRegistryFailure(t *testing.T) {
	registry := hooks.NewHookRegistry()

	// Hook that always fails
	failingHook := func(ctx context.Context, data interface{}) error {
		return assert.AnError
	}

	passingHook := func(ctx context.Context, data interface{}) error {
		return nil
	}

	registry.Register("fail-test", passingHook)
	registry.Register("fail-test", failingHook)
	registry.Register("fail-test", passingHook) // This should not be called

	ctx := context.Background()
	err := registry.Execute(ctx, "fail-test", "test")

	// Should fail on the second hook
	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)
}

func TestHookRegistryNonExistentHook(t *testing.T) {
	registry := hooks.NewHookRegistry()

	ctx := context.Background()

	// Execute non-existent hook - should not error
	err := registry.Execute(ctx, "non-existent", "test")
	assert.NoError(t, err)

	// Execute with non-existent hook - should not error
	err = registry.Execute(ctx, "non-existent", "test")
	assert.NoError(t, err)
}

func TestHookDataTypes(t *testing.T) {
	// Test that hook data types can be created and have expected fields
	preReadData := &hooks.PreReadData{
		WorkerID:  "worker-1",
		Topic:     "test.topic",
		Timestamp: time.Now(),
	}

	postReadData := &hooks.PostReadData{
		WorkerID:     "worker-1",
		Topic:        "test.topic",
		MessageCount: 5,
		BatchSize:    10,
		Duration:     time.Second,
		Error:        nil,
		Timestamp:    time.Now(),
	}

	preWriteData := &hooks.PreWriteData{
		WorkerID:   "worker-1",
		Topic:      "test.topic",
		TableCount: 2,
		RowCount:   100,
		Tables:     nil, // Would normally contain qdb.WriterTable
		Timestamp:  time.Now(),
	}

	postWriteData := &hooks.PostWriteData{
		WorkerID:      "worker-1",
		Topic:         "test.topic",
		Duration:      time.Second,
		RowsWritten:   100,
		TablesWritten: 2,
		Error:         nil,
		Timestamp:     time.Now(),
	}

	preAckData := &hooks.PreAckData{
		WorkerID:  "worker-1",
		Topic:     "test.topic",
		Sequences: []uint64{1, 2, 3},
		IsNack:    false,
		Count:     3,
		Timestamp: time.Now(),
	}

	postAckData := &hooks.PostAckData{
		WorkerID:    "worker-1",
		Topic:       "test.topic",
		AckedCount:  3,
		NackedCount: 0,
		Error:       nil,
		Timestamp:   time.Now(),
	}

	// Verify data types have expected fields
	assert.Equal(t, "worker-1", preReadData.WorkerID)
	assert.Equal(t, "test.topic", preReadData.Topic)

	assert.Equal(t, 5, postReadData.MessageCount)
	assert.Equal(t, 10, postReadData.BatchSize)
	assert.Equal(t, time.Second, postReadData.Duration)

	assert.Equal(t, 2, preWriteData.TableCount)
	assert.Equal(t, 100, preWriteData.RowCount)

	assert.Equal(t, 100, postWriteData.RowsWritten)
	assert.Equal(t, 2, postWriteData.TablesWritten)

	assert.Equal(t, []uint64{1, 2, 3}, preAckData.Sequences)
	assert.False(t, preAckData.IsNack)
	assert.Equal(t, 3, preAckData.Count)

	assert.Equal(t, 3, postAckData.AckedCount)
	assert.Equal(t, 0, postAckData.NackedCount)
}

func TestFailureInjector(t *testing.T) {
	injector := NewFailureInjector("PreRead", 2, FailureModeError)
	registry := hooks.NewHookRegistry()

	injector.RegisterHooks(registry)

	ctx := context.Background()

	// First two calls should succeed
	err := registry.Execute(ctx, "PreRead", &hooks.PreReadData{})
	assert.NoError(t, err)
	assert.Equal(t, 1, injector.GetCallCount("PreRead"))

	err = registry.Execute(ctx, "PreRead", &hooks.PreReadData{})
	assert.NoError(t, err)
	assert.Equal(t, 2, injector.GetCallCount("PreRead"))

	// Third call should fail
	err = registry.Execute(ctx, "PreRead", &hooks.PreReadData{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "injected failure at PreRead after 2 calls")
	assert.Equal(t, 3, injector.GetCallCount("PreRead"))

	// Other hooks should not be affected
	err = registry.Execute(ctx, "PreWrite", &hooks.PreWriteData{})
	assert.NoError(t, err)
	assert.Equal(t, 1, injector.GetCallCount("PreWrite"))
}
