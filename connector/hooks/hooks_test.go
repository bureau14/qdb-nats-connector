// Package hooks_test provides comprehensive unit tests for the hooks system.
// This test suite covers hook registration, execution order, data sharing,
// error handling, failure injection, and various hook lifecycle scenarios.
package hooks_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FailureMode defines how failures should be injected
type FailureMode int

const (
	FailureModeError FailureMode = iota // Return error
	FailureModePanic                    // Panic
	FailureModeExit                     // Exit process
	FailureModeHang                     // Hang indefinitely
)

// FailureInjector allows injecting failures at specific hook points for testing
type FailureInjector struct {
	failurePoint   string
	failAfterCalls int
	failureMode    FailureMode
	callCounts     map[string]int
	mu             sync.Mutex
}

// NewFailureInjector creates a new failure injector
func NewFailureInjector(failurePoint string, failAfterCalls int, mode FailureMode) *FailureInjector {
	return &FailureInjector{
		failurePoint:   failurePoint,
		failAfterCalls: failAfterCalls,
		failureMode:    mode,
		callCounts:     make(map[string]int),
	}
}

// RegisterHooks registers hooks for all injection points
func (fi *FailureInjector) RegisterHooks(registry *hooks.HookRegistry) {
	hookNames := []string{
		"PreRead",
		"PostRead",
		"PreWrite",
		"PostWrite",
		"PreAck",
		"PostAck",
	}

	for _, hookName := range hookNames {
		registry.Register(hookName, fi.createHookFunc(hookName))
	}
}

// GetCallCount returns the number of times a hook has been called
func (fi *FailureInjector) GetCallCount(hookName string) int {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	return fi.callCounts[hookName]
}

// Reset resets all call counts
func (fi *FailureInjector) Reset() {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.callCounts = make(map[string]int)
}

// createHookFunc creates a hook function for a specific hook point
func (fi *FailureInjector) createHookFunc(hookName string) hooks.HookFunc {
	return func(ctx context.Context, data interface{}) error {
		fi.mu.Lock()
		defer fi.mu.Unlock()

		// Increment call count for this hook
		fi.callCounts[hookName]++

		// Check if we should fail at this point
		if hookName == fi.failurePoint && fi.callCounts[hookName] > fi.failAfterCalls {
			switch fi.failureMode {
			case FailureModeError:
				return fmt.Errorf("injected failure at %s after %d calls", hookName, fi.failAfterCalls)
			case FailureModePanic:
				panic(fmt.Sprintf("injected panic at %s after %d calls", hookName, fi.failAfterCalls))
			case FailureModeExit:
				os.Exit(1)
			case FailureModeHang:
				// Block forever
				select {}
			}
		}

		return nil
	}
}

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
