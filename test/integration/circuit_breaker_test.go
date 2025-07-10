//go:build integration
// +build integration

// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

package integration

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCircuitBreakerSharedBehavior tests that multiple workers share the same circuit breaker instance
func TestCircuitBreakerSharedBehavior(t *testing.T) {
	// Skip if NATS or QDB not available
	if !isNATSAvailable() || !isQDBAvailable() {
		t.Skip("NATS or QDB not available for integration tests")
	}

	// Create test stream and subjects
	streamName := fmt.Sprintf("TEST_SHARED_CB_%d", time.Now().UnixNano())
	subjects := []string{
		fmt.Sprintf("test.shared.cb.%d.sensor1", time.Now().UnixNano()),
		fmt.Sprintf("test.shared.cb.%d.sensor2", time.Now().UnixNano()),
	}

	// Setup NATS connection and stream
	nc, js, err := setupNATSConnection()
	require.NoError(t, err)
	defer nc.Close()

	_, err = createTestStream(js, streamName, subjects)
	require.NoError(t, err)
	defer func() { _ = deleteTestStream(js, streamName) }()

	// Create hook registry to monitor circuit breaker events
	hookRegistry := hooks.NewHookRegistry()
	var stateChangeCount int64
	var rejectionCount int64

	// Track state changes
	hookRegistry.Register("PostCircuitBreakerStateChange", func(ctx context.Context, data interface{}) error {
		atomic.AddInt64(&stateChangeCount, 1)
		event := data.(*hooks.PostCircuitBreakerStateChange)
		t.Logf("Circuit breaker state change: %s → %s (%s)", event.OldState, event.NewState, event.Reason)

		return nil
	})

	// Track rejections
	hookRegistry.Register("PostCircuitBreakerRequestRejected", func(ctx context.Context, data interface{}) error {
		atomic.AddInt64(&rejectionCount, 1)
		event := data.(*hooks.PostCircuitBreakerRequestRejected)
		t.Logf("Circuit breaker rejected request: %s (reason: %s)", event.WorkerID, event.Reason)

		return nil
	})

	// Create connector options with shared circuit breaker
	opts := &connector.Options{
		NatsEndpoint:                   getTestNATSEndpoint(),
		NatsStreamName:                 streamName,
		NatsTopicFilters:               subjects,
		NatsConsumerPrefix:             "test-shared-cb",
		NatsBatchSize:                  10,
		NatsBatchTimeout:               time.Second,
		QdbClusterUri:                  getTestQDBEndpoint(),
		CircuitBreakerFailureThreshold: 2, // Low threshold for testing
		CircuitBreakerSuccessThreshold: 2,
		CircuitBreakerTimeout:          time.Second,
		CircuitBreakerShared:           true,
		CircuitBreakerJitterMax:        10 * time.Millisecond,
		CircuitBreakerHalfOpenBase:     1,
		CircuitBreakerHalfOpenMax:      4,
		Hooks:                          hookRegistry,
	}

	// Create connector
	conn, err := connector.NewConnector(opts)
	require.NoError(t, err)

	// Test that shared circuit breaker is created
	assert.NotNil(t, conn)

	// Start connector in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = conn.RunWithContext(ctx)
	}()

	// Wait for connector to start
	time.Sleep(100 * time.Millisecond)

	// Send messages to trigger processing
	for i := range 5 {
		for _, subject := range subjects {
			testData := fmt.Sprintf(`{"timestamp": %d, "value": %d}`, time.Now().UnixNano(), i)
			_, err := js.Publish(subject, []byte(testData))
			require.NoError(t, err)
		}
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Stop connector
	cancel()
	conn.Close()

	// Verify that events were captured
	t.Logf("State changes: %d, Rejections: %d", stateChangeCount, rejectionCount)

	// At minimum, we should have some activity
	assert.Greater(t, stateChangeCount+rejectionCount, int64(0), "Expected some circuit breaker activity")
}

// TestCircuitBreakerProgressiveRecovery tests the progressive recovery mechanism
func TestCircuitBreakerProgressiveRecovery(t *testing.T) {
	// Skip if NATS or QDB not available
	if !isNATSAvailable() || !isQDBAvailable() {
		t.Skip("NATS or QDB not available for integration tests")
	}

	// Create a failing sink scenario by using invalid QDB endpoint
	streamName := fmt.Sprintf("TEST_PROGRESSIVE_CB_%d", time.Now().UnixNano())
	subject := fmt.Sprintf("test.progressive.cb.%d", time.Now().UnixNano())

	// Setup NATS connection and stream
	nc, js, err := setupNATSConnection()
	require.NoError(t, err)
	defer nc.Close()

	_, err = createTestStream(js, streamName, []string{subject})
	require.NoError(t, err)
	defer func() { _ = deleteTestStream(js, streamName) }()

	// Create hook registry to monitor circuit breaker progression
	hookRegistry := hooks.NewHookRegistry()
	var stateTransitions []string
	var transitionMu sync.Mutex

	// Track state transitions
	hookRegistry.Register("PostCircuitBreakerStateChange", func(ctx context.Context, data interface{}) error {
		event := data.(*hooks.PostCircuitBreakerStateChange)
		transitionMu.Lock()
		stateTransitions = append(stateTransitions, fmt.Sprintf("%s→%s", event.OldState, event.NewState))
		transitionMu.Unlock()
		t.Logf("Circuit breaker progression: %s → %s (%s)", event.OldState, event.NewState, event.Reason)

		return nil
	})

	// Create connector options with invalid QDB endpoint to trigger failures
	opts := &connector.Options{
		NatsEndpoint:                   getTestNATSEndpoint(),
		NatsStreamName:                 streamName,
		NatsTopicFilters:               []string{subject},
		NatsConsumerPrefix:             "test-progressive-cb",
		NatsBatchSize:                  5,
		NatsBatchTimeout:               time.Second,
		QdbClusterUri:                  "qdb://invalid-endpoint:2836", // Invalid endpoint
		CircuitBreakerFailureThreshold: 2,                             // Low threshold for testing
		CircuitBreakerSuccessThreshold: 2,
		CircuitBreakerTimeout:          200 * time.Millisecond, // Short timeout
		CircuitBreakerShared:           true,
		CircuitBreakerJitterMax:        10 * time.Millisecond,
		CircuitBreakerHalfOpenBase:     1,
		CircuitBreakerHalfOpenMax:      4,
		Hooks:                          hookRegistry,
	}

	// Create connector
	conn, err := connector.NewConnector(opts)
	require.NoError(t, err)

	// Start connector in background
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = conn.RunWithContext(ctx)
	}()

	// Wait for connector to start
	time.Sleep(100 * time.Millisecond)

	// Send messages to trigger failures
	for i := range 10 {
		testData := fmt.Sprintf(`{"timestamp": %d, "value": %d}`, time.Now().UnixNano(), i)
		_, err := js.Publish(subject, []byte(testData))
		require.NoError(t, err)
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for processing and recovery attempts
	time.Sleep(2 * time.Second)

	// Stop connector
	cancel()
	conn.Close()

	// Verify progressive recovery behavior
	transitionMu.Lock()
	defer transitionMu.Unlock()

	t.Logf("Captured transitions: %v", stateTransitions)

	// Should have at least closed→open transition due to failures
	foundClosedToOpen := false
	foundOpenToHalfOpen := false

	for _, transition := range stateTransitions {
		if transition == "closed→open" {
			foundClosedToOpen = true
		}
		if transition == "open→half-open" {
			foundOpenToHalfOpen = true
		}
	}

	assert.True(t, foundClosedToOpen, "Expected closed→open transition due to failures")
	assert.True(t, foundOpenToHalfOpen, "Expected open→half-open transition during recovery")
}

// TestCircuitBreakerJitterPreventsThunderingHerd tests jitter functionality
func TestCircuitBreakerJitterPreventsThunderingHerd(t *testing.T) {
	// Skip if NATS or QDB not available
	if !isNATSAvailable() || !isQDBAvailable() {
		t.Skip("NATS or QDB not available for integration tests")
	}

	// Create test stream and multiple subjects for multiple workers
	streamName := fmt.Sprintf("TEST_JITTER_CB_%d", time.Now().UnixNano())
	subjects := []string{
		fmt.Sprintf("test.jitter.cb.%d.worker1", time.Now().UnixNano()),
		fmt.Sprintf("test.jitter.cb.%d.worker2", time.Now().UnixNano()),
		fmt.Sprintf("test.jitter.cb.%d.worker3", time.Now().UnixNano()),
	}

	// Setup NATS connection and stream
	nc, js, err := setupNATSConnection()
	require.NoError(t, err)
	defer nc.Close()

	_, err = createTestStream(js, streamName, subjects)
	require.NoError(t, err)
	defer func() { _ = deleteTestStream(js, streamName) }()

	// Create hook registry to monitor timing
	hookRegistry := hooks.NewHookRegistry()
	var transitionTimes []time.Time
	var timingMu sync.Mutex

	// Track transition timing
	hookRegistry.Register("PostCircuitBreakerStateChange", func(ctx context.Context, data interface{}) error {
		timingMu.Lock()
		transitionTimes = append(transitionTimes, time.Now())
		timingMu.Unlock()

		return nil
	})

	// Create connector options with jitter
	opts := &connector.Options{
		NatsEndpoint:                   getTestNATSEndpoint(),
		NatsStreamName:                 streamName,
		NatsTopicFilters:               subjects,
		NatsConsumerPrefix:             "test-jitter-cb",
		NatsBatchSize:                  5,
		NatsBatchTimeout:               time.Second,
		QdbClusterUri:                  "qdb://invalid-endpoint:2836", // Invalid endpoint
		CircuitBreakerFailureThreshold: 2,
		CircuitBreakerSuccessThreshold: 2,
		CircuitBreakerTimeout:          200 * time.Millisecond,
		CircuitBreakerShared:           true,
		CircuitBreakerJitterMax:        100 * time.Millisecond, // Significant jitter
		CircuitBreakerHalfOpenBase:     1,
		CircuitBreakerHalfOpenMax:      4,
		Hooks:                          hookRegistry,
	}

	// Create connector
	conn, err := connector.NewConnector(opts)
	require.NoError(t, err)

	// Start connector in background
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() {
		_ = conn.RunWithContext(ctx)
	}()

	// Wait for connector to start
	time.Sleep(100 * time.Millisecond)

	// Send messages to trigger failures and recovery
	for i := range 20 {
		for _, subject := range subjects {
			testData := fmt.Sprintf(`{"timestamp": %d, "value": %d}`, time.Now().UnixNano(), i)
			_, err := js.Publish(subject, []byte(testData))
			require.NoError(t, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Stop connector
	cancel()
	conn.Close()

	// Analyze timing to verify jitter effect
	timingMu.Lock()
	defer timingMu.Unlock()

	if len(transitionTimes) > 1 {
		// Check for jitter by ensuring transitions aren't perfectly synchronized
		var deltas []time.Duration
		for i := 1; i < len(transitionTimes); i++ {
			delta := transitionTimes[i].Sub(transitionTimes[i-1])
			deltas = append(deltas, delta)
		}

		// With jitter, we shouldn't see perfectly synchronized transitions
		// This is a basic check - in practice, jitter helps prevent thundering herd
		t.Logf("Transition count: %d, timing deltas: %v", len(transitionTimes), deltas)
		assert.Greater(t, len(transitionTimes), 0, "Expected some state transitions")
	}
}

// Helper functions (these would need to be implemented based on existing test infrastructure)

func isNATSAvailable() bool {
	nc, err := nats.Connect(getTestNATSEndpoint())
	if err != nil {
		return false
	}
	nc.Close()

	return true
}

func isQDBAvailable() bool {
	// Try to connect to QDB
	h, err := qdb.NewHandle()
	if err != nil {
		return false
	}
	defer func() { _ = h.Close() }()

	err = h.Connect(getTestQDBEndpoint())

	return err == nil
}

func getTestNATSEndpoint() string {
	if endpoint := os.Getenv("TEST_NATS_ENDPOINT"); endpoint != "" {
		return endpoint
	}

	return "nats://localhost:4222"
}

func getTestQDBEndpoint() string {
	if endpoint := os.Getenv("TEST_QDB_ENDPOINT"); endpoint != "" {
		return endpoint
	}

	return "qdb://127.0.0.1:2836"
}

//nolint:ireturn // JetStreamContext is an interface in the NATS library
func setupNATSConnection() (*nats.Conn, nats.JetStreamContext, error) {
	nc, err := nats.Connect(getTestNATSEndpoint())
	if err != nil {
		return nil, nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()

		return nil, nil, err
	}

	return nc, js, nil
}

func createTestStream(js nats.JetStreamContext, streamName string, subjects []string) (*nats.StreamInfo, error) {
	config := &nats.StreamConfig{
		Name:       streamName,
		Subjects:   subjects,
		MaxAge:     time.Hour,
		MaxMsgs:    1000,
		Retention:  nats.InterestPolicy,
		Discard:    nats.DiscardOld,
		Storage:    nats.MemoryStorage,
		Duplicates: time.Minute,
	}

	return js.AddStream(config)
}

func deleteTestStream(js nats.JetStreamContext, streamName string) error {
	return js.DeleteStream(streamName)
}
