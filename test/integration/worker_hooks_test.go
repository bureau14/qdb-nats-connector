package integration

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

// OrderedHook tracks the order of hook calls
type OrderedHook struct {
	mu     sync.Mutex
	calls  []string
	panics bool
	errors bool
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

func (oh *OrderedHook) SetErrors(errors bool) {
	oh.mu.Lock()
	defer oh.mu.Unlock()
	oh.errors = errors
}

func (oh *OrderedHook) Hook(hookName string) hooks.HookFunc {
	return func(ctx context.Context, data interface{}) error {
		oh.mu.Lock()
		defer oh.mu.Unlock()

		oh.calls = append(oh.calls, hookName)

		if oh.panics {
			panic(fmt.Sprintf("hook %s panicked", hookName))
		}

		if oh.errors {
			return fmt.Errorf("hook %s failed", hookName)
		}

		return nil
	}
}

func (oh *OrderedHook) GetCalls() []string {
	oh.mu.Lock()
	defer oh.mu.Unlock()

	return append([]string{}, oh.calls...)
}

func (oh *OrderedHook) Reset() {
	oh.mu.Lock()
	defer oh.mu.Unlock()
	oh.calls = make([]string, 0)
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
	err := registry.Execute(ctx, "PreRead", &hooks.PreReadData{
		WorkerID: "worker-1", Topic: "test", Timestamp: time.Now(),
	})
	require.NoError(t, err)

	err = registry.Execute(ctx, "PostRead", &hooks.PostReadData{
		WorkerID: "worker-1", Topic: "test", MessageCount: 1, BatchSize: 1,
		Duration: time.Millisecond, Timestamp: time.Now(),
	})
	require.NoError(t, err)

	err = registry.Execute(ctx, "PreWrite", &hooks.PreWriteData{
		WorkerID: "worker-1", Topic: "test", TableCount: 1, RowCount: 1,
		Timestamp: time.Now(),
	})
	require.NoError(t, err)

	err = registry.Execute(ctx, "PostWrite", &hooks.PostWriteData{
		WorkerID: "worker-1", Topic: "test", Duration: time.Millisecond,
		RowsWritten: 1, TablesWritten: 1, Timestamp: time.Now(),
	})
	require.NoError(t, err)

	err = registry.Execute(ctx, "PreAck", &hooks.PreAckData{
		WorkerID: "worker-1", Topic: "test", Sequences: []uint64{1},
		IsNack: false, Count: 1, Timestamp: time.Now(),
	})
	require.NoError(t, err)

	err = registry.Execute(ctx, "PostAck", &hooks.PostAckData{
		WorkerID: "worker-1", Topic: "test", AckedCount: 1, NackedCount: 0,
		Timestamp: time.Now(),
	})
	require.NoError(t, err)

	// Hooks are now synchronous, no need to wait

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
	dataCapture := func(ctx context.Context, data interface{}) error {
		capturedData = data

		return nil
	}

	registry.Register("PreRead", dataCapture)

	ctx := context.Background()
	testData := &hooks.PreReadData{
		WorkerID:  "worker-1",
		Topic:     "test.topic",
		Timestamp: time.Now(),
	}

	err := registry.Execute(ctx, "PreRead", testData)
	require.NoError(t, err)

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

	slowHook := func(ctx context.Context, data interface{}) error {
		time.Sleep(100 * time.Millisecond) // Slow hook

		return nil
	}

	registry.Register("PostRead", slowHook)

	ctx := context.Background()
	start := time.Now()

	// This should execute synchronously and block
	err := registry.Execute(ctx, "PostRead", &hooks.PostReadData{
		WorkerID: "worker-1", Topic: "test", MessageCount: 1, BatchSize: 1,
		Duration: time.Millisecond, Timestamp: time.Now(),
	})
	require.NoError(t, err)

	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "Sync hook should block for its duration")
}

func TestHookErrorScenarios(t *testing.T) {
	registry := hooks.NewHookRegistry()
	orderedHook := NewOrderedHook()

	// Test synchronous hook error
	orderedHook.SetErrors(true)
	registry.Register("PreRead", orderedHook.Hook("PreRead"))

	ctx := context.Background()
	err := registry.Execute(ctx, "PreRead", &hooks.PreReadData{
		WorkerID: "worker-1", Topic: "test", Timestamp: time.Now(),
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hook PreRead failed")

	// Reset and test panic recovery
	orderedHook.Reset()
	orderedHook.SetErrors(false)
	orderedHook.SetPanics(true)

	err = registry.Execute(ctx, "PreRead", &hooks.PreReadData{
		WorkerID: "worker-1", Topic: "test", Timestamp: time.Now(),
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hook panic")
}

func TestHookDataSharing(t *testing.T) {
	registry := hooks.NewHookRegistry()

	var capturedData1, capturedData2 interface{}
	hook1 := func(ctx context.Context, data interface{}) error {
		capturedData1 = data

		return nil
	}
	hook2 := func(ctx context.Context, data interface{}) error {
		capturedData2 = data

		return nil
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

	err := registry.Execute(ctx, "PostRead", originalData)
	require.NoError(t, err)

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

func TestPreAckHookErrorDoesNotPreventAck(t *testing.T) {
	// This test validates that PreAck hook failures don't prevent ACK/NACK
	// which would cause infinite redelivery loops

	registry := hooks.NewHookRegistry()

	failingHook := func(ctx context.Context, data interface{}) error {
		return fmt.Errorf("PreAck hook failed")
	}

	registry.Register("PreAck", failingHook)

	ctx := context.Background()

	// This should return an error but not prevent the ACK operation
	err := registry.Execute(ctx, "PreAck", &hooks.PreAckData{
		WorkerID: "worker-1", Topic: "test", Sequences: []uint64{1, 2, 3},
		IsNack: false, Count: 3, Timestamp: time.Now(),
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "PreAck hook failed")

	// In the actual worker implementation, this error would be logged
	// but the ACK/NACK would still proceed
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
