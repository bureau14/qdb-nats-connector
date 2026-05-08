// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package resilience: circuit breaker comprehensive test suite
package resilience

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBasicCircuitBreakerCreation tests basic circuit breaker creation
func TestBasicCircuitBreakerCreation(t *testing.T) {
	cb := NewCircuitBreaker(3, 2, 100*time.Millisecond)
	require.NotNil(t, cb)
	assert.Equal(t, StateClosed, cb.GetState())
	assert.False(t, cb.IsOpen())
	assert.False(t, cb.IsHalfOpen())
}

// TestStateTransitions tests basic state transitions
func TestStateTransitions(t *testing.T) {
	t.Run("closed_to_open_after_failure_threshold", func(t *testing.T) {
		cb := NewCircuitBreaker(3, 2, 100*time.Millisecond)
		require.Equal(t, StateClosed, cb.GetState())

		// First 2 failures should keep it closed
		err := cb.Execute(func() error { return errors.New("fail1") })
		require.Error(t, err)
		assert.Equal(t, StateClosed, cb.GetState())

		err = cb.Execute(func() error { return errors.New("fail2") })
		require.Error(t, err)
		assert.Equal(t, StateClosed, cb.GetState())

		// Third failure should open it
		err = cb.Execute(func() error { return errors.New("fail3") })
		require.Error(t, err)
		assert.Equal(t, StateOpen, cb.GetState())
		assert.True(t, cb.IsOpen())
	})

	t.Run("open_to_half_open_after_timeout", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond, WithJitter(0))

		// Trigger circuit open
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)
		assert.Equal(t, StateOpen, cb.GetState())

		// Should reject immediately
		err = cb.Execute(func() error { return nil })
		require.Error(t, err)
		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, connectorErrors.ErrCodeConnectionFailed, connErr.Code)

		// Wait for timeout
		time.Sleep(60 * time.Millisecond)

		// Should now be half-open and allow one request
		err = cb.Execute(func() error { return nil })
		require.NoError(t, err)
		assert.Equal(t, StateHalfOpen, cb.GetState())
		assert.True(t, cb.IsHalfOpen())
	})

	t.Run("half_open_to_open_on_failure", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond, WithJitter(0))

		// Open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)
		assert.Equal(t, StateOpen, cb.GetState())

		// Wait for timeout
		time.Sleep(60 * time.Millisecond)

		// Should be half-open and allow one request
		err = cb.Execute(func() error { return nil })
		require.NoError(t, err)
		assert.Equal(t, StateHalfOpen, cb.GetState())

		// Any failure should reopen circuit
		err = cb.Execute(func() error { return errors.New("fail_again") })
		require.Error(t, err)
		assert.Equal(t, StateOpen, cb.GetState())
	})

	t.Run("reset_function", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond)

		// Open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)
		assert.Equal(t, StateOpen, cb.GetState())

		// Reset should close it
		cb.Reset()
		assert.Equal(t, StateClosed, cb.GetState())

		// Should work normally
		err = cb.Execute(func() error { return nil })
		require.NoError(t, err)
		assert.Equal(t, StateClosed, cb.GetState())
	})
}

// TestProgressiveRecovery tests the progressive recovery mechanism
func TestProgressiveRecovery(t *testing.T) {
	t.Run("progressive_recovery_1_2_4_8_16_32", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond, WithJitter(0))

		// Open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)
		assert.Equal(t, StateOpen, cb.GetState())

		// Wait for timeout
		time.Sleep(60 * time.Millisecond)

		// First request should transition to half-open and execute
		err = cb.Execute(func() error { return nil })
		require.NoError(t, err)
		assert.Equal(t, StateHalfOpen, cb.GetState())
		assert.Equal(t, int64(2), atomic.LoadInt64(&cb.halfOpen.allowedRequests)) // Should progress to level 2

		// 2 more successes move to level 4
		err = cb.Execute(func() error { return nil })
		require.NoError(t, err)
		err = cb.Execute(func() error { return nil })
		require.NoError(t, err)
		assert.Equal(t, int64(4), atomic.LoadInt64(&cb.halfOpen.allowedRequests))

		// Continue progression: 4 → 8 → 16 → 32
		for range 4 {
			err = cb.Execute(func() error { return nil })
			require.NoError(t, err)
		}
		assert.Equal(t, int64(8), atomic.LoadInt64(&cb.halfOpen.allowedRequests))

		for range 8 {
			err = cb.Execute(func() error { return nil })
			require.NoError(t, err)
		}
		assert.Equal(t, int64(16), atomic.LoadInt64(&cb.halfOpen.allowedRequests))

		for range 16 {
			err = cb.Execute(func() error { return nil })
			require.NoError(t, err)
		}
		assert.Equal(t, int64(32), atomic.LoadInt64(&cb.halfOpen.allowedRequests))
	})

	t.Run("unlimited_mode_maxAllowed_0", func(t *testing.T) {
		// With maxAllowed=0, circuit breaker should keep doubling until overflow
		// This test just verifies it doesn't break and eventually works
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond, WithJitter(0), WithHalfOpenProgression(1, 0))

		// Open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)

		// Wait for timeout
		time.Sleep(60 * time.Millisecond)

		// Execute enough requests to progress through each level
		// Start with the first request that transitions to half-open
		err = cb.Execute(func() error { return nil })
		require.NoError(t, err)

		// Now we need to execute enough requests to progress all the way to level 64
		// The progression logic needs consecutiveSuccesses >= currentLevel at each level
		// Let's execute many requests to ensure we progress through all levels
		for range 200 { // Execute enough requests to definitely progress beyond level 32
			err = cb.Execute(func() error { return nil })
			require.NoError(t, err)
		}

		// Should still be in half-open, progressing through levels
		assert.Equal(t, StateHalfOpen, cb.GetState())

		// The allowed requests should have grown beyond 32
		allowedRequests := atomic.LoadInt64(&cb.halfOpen.allowedRequests)
		if allowedRequests <= 32 {
			// Print debug info to understand what's happening
			t.Logf("Debug: allowedRequests=%d, maxAllowed=%d, baseAllowed=%d",
				allowedRequests, cb.halfOpen.maxAllowed, cb.halfOpen.baseAllowed)
		}
		assert.Greater(t, allowedRequests, int64(32), "Should have progressed beyond 32")
	})

	t.Run("capped_progression_maxAllowed_16", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond, WithJitter(0), WithHalfOpenProgression(1, 16))

		// Open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)

		// Wait for timeout
		time.Sleep(60 * time.Millisecond)

		// Progress through levels until we hit max
		levels := []int64{1, 2, 4, 8, 16}
		for _, expectedLevel := range levels {
			assert.Equal(t, expectedLevel, atomic.LoadInt64(&cb.halfOpen.allowedRequests))
			for range expectedLevel {
				err = cb.Execute(func() error { return nil })
				require.NoError(t, err)
			}
		}

		// Should close at max level
		assert.Equal(t, StateClosed, cb.GetState())
	})

	t.Run("concurrent_request_limiting", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond, WithJitter(0))

		// Open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)

		// Wait for timeout
		time.Sleep(60 * time.Millisecond)

		// At level 1, only 1 concurrent request should be allowed
		assert.Equal(t, int64(1), atomic.LoadInt64(&cb.halfOpen.allowedRequests))

		// Start a long-running request
		longRunningStarted := make(chan struct{})
		longRunningCanFinish := make(chan struct{})
		longRunningDone := make(chan struct{})
		var longRunningErr error

		go func() {
			defer close(longRunningDone)
			longRunningErr = cb.Execute(func() error {
				close(longRunningStarted)
				<-longRunningCanFinish

				return nil
			})
		}()

		// Wait for the long-running request to start
		<-longRunningStarted

		// Additional requests should be rejected
		err = cb.Execute(func() error { return nil })
		require.Error(t, err)
		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, connectorErrors.ErrCodeConnectionFailed, connErr.Code)

		// Allow the long-running request to finish and wait for it
		close(longRunningCanFinish)
		<-longRunningDone
		require.NoError(t, longRunningErr)

		// Now should allow requests again
		err = cb.Execute(func() error { return nil })
		require.NoError(t, err)
	})
}

// testHookCapture captures hook events for testing
type testHookCapture struct {
	mu                sync.RWMutex
	preStateChanges   []hooks.PreCircuitBreakerStateChange
	postStateChanges  []hooks.PostCircuitBreakerStateChange
	rejections        []hooks.PostCircuitBreakerRequestRejected
	jitterEvents      []hooks.PreCircuitBreakerJitter
	executionTimeouts []string
}

func newTestHookCapture() *testHookCapture {
	return &testHookCapture{
		preStateChanges:   make([]hooks.PreCircuitBreakerStateChange, 0),
		postStateChanges:  make([]hooks.PostCircuitBreakerStateChange, 0),
		rejections:        make([]hooks.PostCircuitBreakerRequestRejected, 0),
		jitterEvents:      make([]hooks.PreCircuitBreakerJitter, 0),
		executionTimeouts: make([]string, 0),
	}
}

func (c *testHookCapture) setupHooks(registry *hooks.HookRegistry) {
	registry.Register("PreCircuitBreakerStateChange", func(ctx context.Context, data interface{}) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		event := data.(*hooks.PreCircuitBreakerStateChange)
		c.preStateChanges = append(c.preStateChanges, *event)

		return nil
	})

	registry.Register("PostCircuitBreakerStateChange", func(ctx context.Context, data interface{}) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		event := data.(*hooks.PostCircuitBreakerStateChange)
		c.postStateChanges = append(c.postStateChanges, *event)

		return nil
	})

	registry.Register("PostCircuitBreakerRequestRejected", func(ctx context.Context, data interface{}) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		event := data.(*hooks.PostCircuitBreakerRequestRejected)
		c.rejections = append(c.rejections, *event)

		return nil
	})

	registry.Register("PreCircuitBreakerJitter", func(ctx context.Context, data interface{}) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		event := data.(*hooks.PreCircuitBreakerJitter)
		c.jitterEvents = append(c.jitterEvents, *event)

		return nil
	})
}

func (c *testHookCapture) getEvents() ([]hooks.PreCircuitBreakerStateChange, []hooks.PostCircuitBreakerStateChange, []hooks.PostCircuitBreakerRequestRejected, []hooks.PreCircuitBreakerJitter) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return append([]hooks.PreCircuitBreakerStateChange(nil), c.preStateChanges...),
		append([]hooks.PostCircuitBreakerStateChange(nil), c.postStateChanges...),
		append([]hooks.PostCircuitBreakerRequestRejected(nil), c.rejections...),
		append([]hooks.PreCircuitBreakerJitter(nil), c.jitterEvents...)
}

func (c *testHookCapture) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preStateChanges = c.preStateChanges[:0]
	c.postStateChanges = c.postStateChanges[:0]
	c.rejections = c.rejections[:0]
	c.jitterEvents = c.jitterEvents[:0]
	c.executionTimeouts = c.executionTimeouts[:0]
}

// TestJitterMechanism tests the jitter functionality
func TestJitterMechanism(t *testing.T) {
	t.Run("jitter_applied_in_open_state", func(t *testing.T) {
		hookCapture := newTestHookCapture()
		hookRegistry := hooks.NewHookRegistry()
		hookCapture.setupHooks(hookRegistry)
		cb := NewCircuitBreaker(1, 2, 100*time.Millisecond, WithJitter(50*time.Millisecond), WithHooks(hookRegistry, "test-worker", "test-resource"))

		// Open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)
		assert.Equal(t, StateOpen, cb.GetState())

		// Execute should apply jitter
		start := time.Now()
		err = cb.Execute(func() error { return nil })
		duration := time.Since(start)
		require.Error(t, err) // Should fail because circuit is open

		// Should have applied some jitter (but less than max)
		assert.True(t, duration >= 0, "Should have some delay")
		assert.True(t, duration < 50*time.Millisecond, "Should not exceed max jitter")

		// Check jitter hook was fired
		_, _, _, jitterEvents := hookCapture.getEvents()
		assert.Len(t, jitterEvents, 1)
		assert.Equal(t, "test-worker", jitterEvents[0].WorkerID)
		assert.Equal(t, "test-resource", jitterEvents[0].Resource)
		assert.Equal(t, "open", jitterEvents[0].State)
		assert.Greater(t, jitterEvents[0].JitterMs, int64(0))
	})

	t.Run("jitter_bounds_capped_at_10_percent", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 2, 100*time.Millisecond, WithJitter(50*time.Millisecond))

		// Test that effective timeout calculation caps jitter at 10%
		effectiveTimeout := cb.calculateEffectiveTimeout()

		// The jitter should be capped at 10% of timeout (10ms)
		// So effective timeout should be in range [90ms, 110ms]
		assert.True(t, effectiveTimeout >= 90*time.Millisecond, "Effective timeout should be >= 90ms")
		assert.True(t, effectiveTimeout <= 110*time.Millisecond, "Effective timeout should be <= 110ms")
	})

	t.Run("withJitter_option", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 2, 100*time.Millisecond, WithJitter(75*time.Millisecond))
		assert.Equal(t, 75*time.Millisecond, cb.jitterMax)

		// Test that custom jitter is applied
		cb2 := NewCircuitBreaker(1, 2, 100*time.Millisecond, WithJitter(0))
		assert.Equal(t, time.Duration(0), cb2.jitterMax)
	})
}

// TestHookIntegration tests the hook integration
func TestHookIntegration(t *testing.T) {
	t.Run("state_change_hooks", func(t *testing.T) {
		hookCapture := newTestHookCapture()
		hookRegistry := hooks.NewHookRegistry()
		hookCapture.setupHooks(hookRegistry)
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond, WithJitter(0), WithHooks(hookRegistry, "test-worker", "test-resource"))

		// Open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)

		// Check state change hooks were fired
		preEvents, postEvents, _, _ := hookCapture.getEvents()
		assert.Len(t, preEvents, 1)
		assert.Len(t, postEvents, 1)

		// Check pre-hook
		assert.Equal(t, "test-worker", preEvents[0].WorkerID)
		assert.Equal(t, "test-resource", preEvents[0].Resource)
		assert.Equal(t, "closed", preEvents[0].CurrentState)
		assert.Equal(t, "open", preEvents[0].NextState)

		// Check post-hook
		assert.Equal(t, "test-worker", postEvents[0].WorkerID)
		assert.Equal(t, "test-resource", postEvents[0].Resource)
		assert.Equal(t, "closed", postEvents[0].OldState)
		assert.Equal(t, "open", postEvents[0].NewState)
		assert.Greater(t, postEvents[0].FailureCount, 0)
	})

	t.Run("rejection_hooks", func(t *testing.T) {
		hookCapture := newTestHookCapture()
		hookRegistry := hooks.NewHookRegistry()
		hookCapture.setupHooks(hookRegistry)
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond, WithJitter(0), WithHooks(hookRegistry, "test-worker", "test-resource"))

		// Open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)

		// Reset hook capture to focus on rejections
		hookCapture.reset()

		// Try to execute - should be rejected
		err = cb.Execute(func() error { return nil })
		require.Error(t, err)

		// Check rejection hook was fired
		_, _, rejections, _ := hookCapture.getEvents()
		assert.Len(t, rejections, 1)
		assert.Equal(t, "test-worker", rejections[0].WorkerID)
		assert.Equal(t, "test-resource", rejections[0].Resource)
		assert.Equal(t, "open", rejections[0].State)
		assert.Equal(t, "circuit_open", rejections[0].Reason)
	})
}

// TestErrorHandling tests error handling
func TestErrorHandling(t *testing.T) {
	t.Run("connection_failed_error_when_open", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 2, 500*time.Millisecond) // Longer timeout to stay open

		// Open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)

		// Should return connection failed error when circuit is open (immediately, before timeout)
		err = cb.Execute(func() error { return nil })
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, connectorErrors.ErrCodeConnectionFailed, connErr.Code)
		assert.Equal(t, "circuit-breaker", connErr.Component)
	})

	t.Run("error_propagation_from_protected_function", func(t *testing.T) {
		cb := NewCircuitBreaker(3, 2, 50*time.Millisecond)

		originalErr := errors.New("original error")
		err := cb.Execute(func() error { return originalErr })
		require.Error(t, err)
		assert.Equal(t, originalErr, err)
	})
}

// TestConfiguration tests configuration options
func TestConfiguration(t *testing.T) {
	t.Run("withHooks_option", func(t *testing.T) {
		hookRegistry := hooks.NewHookRegistry()
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond, WithHooks(hookRegistry, "worker-1", "resource-1"))
		assert.Equal(t, hookRegistry, cb.hooks)
		assert.Equal(t, "worker-1", cb.workerID)
		assert.Equal(t, "resource-1", cb.resource)
	})

	t.Run("withHalfOpenProgression_option", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond, WithHalfOpenProgression(2, 64))
		assert.Equal(t, 2, cb.halfOpen.baseAllowed)
		assert.Equal(t, 64, cb.halfOpen.maxAllowed)
		assert.Equal(t, int64(2), cb.halfOpen.allowedRequests)
	})
}

// TestEdgeCases tests edge cases
func TestEdgeCases(t *testing.T) {
	t.Run("failure_threshold_1", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 1, 50*time.Millisecond)

		// Single failure should open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)
		assert.Equal(t, StateOpen, cb.GetState())
	})

	t.Run("log_status_in_different_states", func(t *testing.T) {
		cb := NewCircuitBreaker(1, 2, 50*time.Millisecond)

		// Should not panic in closed state
		cb.LogStatus()

		// Open circuit
		err := cb.Execute(func() error { return errors.New("fail") })
		require.Error(t, err)

		// Should not panic in open state
		cb.LogStatus()

		// Wait for timeout to get to half-open
		time.Sleep(60 * time.Millisecond)

		// Should not panic in half-open state
		cb.LogStatus()
	})
}

// TestConcurrency tests concurrent access
func TestConcurrency(t *testing.T) {
	t.Run("concurrent_execute_calls", func(t *testing.T) {
		cb := NewCircuitBreaker(5, 2, 100*time.Millisecond)

		var wg sync.WaitGroup
		var successCount int64
		var failureCount int64

		// Run concurrent operations
		for i := range 100 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := cb.Execute(func() error {
					// Simulate some work
					time.Sleep(1 * time.Millisecond)
					if i%10 == 0 {
						return errors.New("simulated failure")
					}

					return nil
				})

				if err != nil {
					atomic.AddInt64(&failureCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}(i)
		}

		wg.Wait()

		// Should have processed all requests
		assert.Equal(t, int64(100), atomic.LoadInt64(&successCount)+atomic.LoadInt64(&failureCount))
	})
}
