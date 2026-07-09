// Package hooks_test provides comprehensive unit tests for the hooks system.
// This test suite covers hook registration, execution order, data sharing,
// panic recovery, and various hook lifecycle scenarios. Hooks are
// observational: they return nothing and cannot fail the pipeline.
package hooks_test

import (
	"context"
	"fmt"
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

func (ch *CounterHook) Hook(ctx context.Context, data interface{}) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.count++
	ch.callData = append(ch.callData, data)
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

// OrderedHook tracks the order of hook calls
type OrderedHook struct {
	mu     sync.Mutex
	calls  []string
	panics bool
}

func NewOrderedHook() *OrderedHook {
	return &OrderedHook{
		calls: make([]string, 0),
	}
}

func (oh *OrderedHook) SetPanics(panics bool) {
	oh.mu.Lock()
	defer oh.mu.Unlock()
	oh.panics = panics
}

func (oh *OrderedHook) Hook(hookName string) hooks.HookFunc {
	return func(ctx context.Context, data interface{}) {
		oh.mu.Lock()
		defer oh.mu.Unlock()

		oh.calls = append(oh.calls, hookName)

		if oh.panics {
			panic(fmt.Sprintf("hook %s panicked", hookName))
		}
	}
}

func (oh *OrderedHook) GetCalls() []string {
	oh.mu.Lock()
	defer oh.mu.Unlock()

	return append([]string{}, oh.calls...)
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

	registry.Execute(ctx, "test", testData)

	// Should have called the hook twice
	assert.Equal(t, 2, counter.GetCount())

	// Check that data was passed correctly
	callData := counter.GetCallData()
	assert.Len(t, callData, 2)
	assert.Equal(t, testData, callData[0])
	assert.Equal(t, testData, callData[1])
}

func TestHookRegistryNonExistentHook(t *testing.T) {
	registry := hooks.NewHookRegistry()

	ctx := context.Background()

	// Execute non-existent hook - must be a safe no-op
	assert.NotPanics(t, func() {
		registry.Execute(ctx, "non-existent", "test")
	})
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

func TestHookExecutionOrder(t *testing.T) {
	registry := hooks.NewHookRegistry()
	orderedHook := NewOrderedHook()

	// Register hooks for all phases
	registry.Register("PreRead", orderedHook.Hook("PreRead"))
	registry.Register("PostRead", orderedHook.Hook("PostRead"))
	registry.Register("PreWrite", orderedHook.Hook("PreWrite"))
	registry.Register("PostWrite", orderedHook.Hook("PostWrite"))
	registry.Register("PreAck", orderedHook.Hook("PreAck"))
	registry.Register("PostAck", orderedHook.Hook("PostAck"))

	ctx := context.Background()

	// Simulate the expected order during normal message processing
	registry.Execute(ctx, "PreRead", &hooks.PreReadData{
		WorkerID: "worker-1", Topic: "test", Timestamp: time.Now(),
	})

	registry.Execute(ctx, "PostRead", &hooks.PostReadData{
		WorkerID: "worker-1", Topic: "test", MessageCount: 1, BatchSize: 1,
		Duration: time.Millisecond, Timestamp: time.Now(),
	})

	registry.Execute(ctx, "PreWrite", &hooks.PreWriteData{
		WorkerID: "worker-1", Topic: "test", TableCount: 1, RowCount: 1,
		Timestamp: time.Now(),
	})

	registry.Execute(ctx, "PostWrite", &hooks.PostWriteData{
		WorkerID: "worker-1", Topic: "test", Duration: time.Millisecond,
		RowsWritten: 1, TablesWritten: 1, Timestamp: time.Now(),
	})

	registry.Execute(ctx, "PreAck", &hooks.PreAckData{
		WorkerID: "worker-1", Topic: "test", Sequences: []uint64{1},
		IsNack: false, Count: 1, Timestamp: time.Now(),
	})

	registry.Execute(ctx, "PostAck", &hooks.PostAckData{
		WorkerID: "worker-1", Topic: "test", AckedCount: 1, NackedCount: 0,
		Timestamp: time.Now(),
	})

	calls := orderedHook.GetCalls()

	// Check that we have all expected calls
	assert.Contains(t, calls, "PreRead")
	assert.Contains(t, calls, "PostRead")
	assert.Contains(t, calls, "PreWrite")
	assert.Contains(t, calls, "PostWrite")
	assert.Contains(t, calls, "PreAck")
	assert.Contains(t, calls, "PostAck")

	// Check that synchronous hooks are called in order
	preReadIdx := findInSlice(calls, "PreRead")
	preWriteIdx := findInSlice(calls, "PreWrite")
	preAckIdx := findInSlice(calls, "PreAck")

	assert.True(t, preReadIdx < preWriteIdx, "PreRead should come before PreWrite")
	assert.True(t, preWriteIdx < preAckIdx, "PreWrite should come before PreAck")
}

func TestHookDataContainsExpectedFields(t *testing.T) {
	registry := hooks.NewHookRegistry()

	var capturedData interface{}
	dataCapture := func(ctx context.Context, data interface{}) {
		capturedData = data
	}

	registry.Register("PreRead", dataCapture)

	ctx := context.Background()
	testData := &hooks.PreReadData{
		WorkerID:  "worker-1",
		Topic:     "test.topic",
		Timestamp: time.Now(),
	}

	registry.Execute(ctx, "PreRead", testData)

	// Verify the data was captured correctly
	require.NotNil(t, capturedData)
	preReadData, ok := capturedData.(*hooks.PreReadData)
	require.True(t, ok)

	assert.Equal(t, "worker-1", preReadData.WorkerID)
	assert.Equal(t, "test.topic", preReadData.Topic)
	assert.False(t, preReadData.Timestamp.IsZero())
}

func TestSyncHooksBlockProcessing(t *testing.T) {
	registry := hooks.NewHookRegistry()

	slowHook := func(ctx context.Context, data interface{}) {
		time.Sleep(100 * time.Millisecond) // Slow hook
	}

	registry.Register("PostRead", slowHook)

	ctx := context.Background()
	start := time.Now()

	// This should execute synchronously and block
	registry.Execute(ctx, "PostRead", &hooks.PostReadData{
		WorkerID: "worker-1", Topic: "test", MessageCount: 1, BatchSize: 1,
		Duration: time.Millisecond, Timestamp: time.Now(),
	})

	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "Sync hook should block for its duration")
}

func TestHookPanicRecovery(t *testing.T) {
	registry := hooks.NewHookRegistry()
	counter := NewCounterHook()

	panickingHook := func(ctx context.Context, data interface{}) {
		panic("hook exploded")
	}

	// A panicking hook is recovered and logged; execution continues with
	// the remaining hooks -- callers never observe hook outcome.
	registry.Register("PreRead", panickingHook)
	registry.Register("PreRead", counter.Hook)

	ctx := context.Background()
	assert.NotPanics(t, func() {
		registry.Execute(ctx, "PreRead", &hooks.PreReadData{
			WorkerID: "worker-1", Topic: "test", Timestamp: time.Now(),
		})
	})

	assert.Equal(t, 1, counter.GetCount(), "hooks after a panicking hook must still run")
}

func TestHookDataSharing(t *testing.T) {
	registry := hooks.NewHookRegistry()

	var capturedData1, capturedData2 interface{}
	hook1 := func(ctx context.Context, data interface{}) {
		capturedData1 = data
	}
	hook2 := func(ctx context.Context, data interface{}) {
		capturedData2 = data
	}

	registry.Register("PostRead", hook1)
	registry.Register("PostRead", hook2)

	ctx := context.Background()
	originalData := &hooks.PostReadData{
		WorkerID:     "worker-1",
		Topic:        "test.topic",
		MessageCount: 5,
		BatchSize:    10,
		Duration:     time.Second,
		Timestamp:    time.Now(),
	}

	registry.Execute(ctx, "PostRead", originalData)

	// Verify both hooks got the same data
	require.NotNil(t, capturedData1)
	require.NotNil(t, capturedData2)

	data1, ok1 := capturedData1.(*hooks.PostReadData)
	data2, ok2 := capturedData2.(*hooks.PostReadData)

	require.True(t, ok1)
	require.True(t, ok2)

	// They should have the same values
	assert.Equal(t, data1.WorkerID, data2.WorkerID)
	assert.Equal(t, data1.Topic, data2.Topic)
	assert.Equal(t, data1.MessageCount, data2.MessageCount)

	// Now they should be the same instance since we're synchronous
	assert.True(t, data1 == data2, "Hook data should be the same instance for synchronous execution")
}

// Helper function to find index of element in slice
func findInSlice(slice []string, item string) int {
	for i, v := range slice {
		if v == item {
			return i
		}
	}

	return -1
}
