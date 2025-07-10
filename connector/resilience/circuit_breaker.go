// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
package resilience

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
)

// State represents the circuit breaker state
type State int

const (
	// StateClosed: normal operation, requests pass through
	StateClosed State = iota
	// StateOpen: circuit open, requests blocked
	StateOpen
	// StateHalfOpen: testing recovery, limited requests allowed
	StateHalfOpen
)

// String returns state name for logging.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// halfOpenState manages progressive recovery with atomic operations
type halfOpenState struct {
	allowedRequests int64 // Current limit for concurrent requests
	activeRequests  int64 // Number of in-flight requests
	successCount    int64 // Consecutive successes in current level
	baseAllowed     int   // Starting value (default 1)
	maxAllowed      int   // Maximum before fully closed (default 32)
}

// CircuitBreaker: resilient service communication with progressive recovery
type CircuitBreaker struct {
	mu sync.RWMutex

	// Configuration
	failureThreshold int           // consecutive failures to open circuit
	successThreshold int           // consecutive successes to close circuit
	timeout          time.Duration // recovery timeout
	jitterMax        time.Duration // max jitter for timeout

	// State tracking
	state                State
	consecutiveFailures  int
	consecutiveSuccesses int
	lastFailureTime      time.Time
	lastError            error

	// Progressive half-open recovery
	halfOpen halfOpenState

	// Hook integration
	hooks    *hooks.HookRegistry
	workerID string
	resource string

	// Jitter
	randSource *rand.Rand
}

// NewCircuitBreaker creates breaker with progressive recovery.
func NewCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration, opts ...Option) *CircuitBreaker {
	cb := &CircuitBreaker{
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		timeout:          timeout,
		state:            StateClosed,
		jitterMax:        100 * time.Millisecond, // default jitter
		halfOpen: halfOpenState{
			allowedRequests: 1,
			baseAllowed:     1,
			maxAllowed:      32,
		},
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // jitter doesn't require cryptographic randomness
	}

	// Apply options
	for _, opt := range opts {
		opt(cb)
	}

	return cb
}

// Option configures circuit breaker behavior
type Option func(*CircuitBreaker)

// WithHooks sets hook registry and identifiers
func WithHooks(registry *hooks.HookRegistry, workerID, resource string) Option {
	return func(cb *CircuitBreaker) {
		cb.hooks = registry
		cb.workerID = workerID
		cb.resource = resource
	}
}

// WithJitter sets maximum jitter for timeout
func WithJitter(maxJitter time.Duration) Option {
	return func(cb *CircuitBreaker) {
		cb.jitterMax = maxJitter
	}
}

// WithHalfOpenProgression sets half-open recovery parameters
func WithHalfOpenProgression(base, maxAllowed int) Option {
	return func(cb *CircuitBreaker) {
		cb.halfOpen.baseAllowed = base
		cb.halfOpen.maxAllowed = maxAllowed
		cb.halfOpen.allowedRequests = int64(base)
	}
}

// Execute runs fn if circuit allows, tracks result with progressive recovery.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	// Add request jitter to prevent thundering herd
	if cb.jitterMax > 0 && (cb.IsOpen() || cb.IsHalfOpen()) {
		jitter := time.Duration(cb.randSource.Int63n(int64(cb.jitterMax)))
		time.Sleep(jitter)
	}

	// Check state and admission control
	if !cb.canProceed() {
		baseErr := fmt.Errorf("circuit breaker open")

		return connectorErrors.NewConnectionFailedError("circuit-breaker", cb.resource, baseErr)
	}

	// Execute protected function
	err := fn()

	// Record result
	cb.recordResult(err)

	return err
}

// GetState returns current circuit breaker state
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return cb.state
}

// IsOpen returns true if circuit is open
func (cb *CircuitBreaker) IsOpen() bool {
	return cb.GetState() == StateOpen
}

// IsHalfOpen returns true if circuit is half-open
func (cb *CircuitBreaker) IsHalfOpen() bool {
	return cb.GetState() == StateHalfOpen
}

// Reset resets circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.resetLocked()
}

// canProceed checks if request allowed with progressive admission control
func (cb *CircuitBreaker) canProceed() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Update state based on current conditions
	cb.updateStateLocked()

	switch cb.state {
	case StateOpen:
		// Fire rejection hook
		cb.fireRejectionHookLocked("circuit_open")

		return false
	case StateHalfOpen:
		// Progressive admission control
		if !cb.canProceedHalfOpen() {
			cb.fireRejectionHookLocked("half_open_limit_exceeded")

			return false
		}

		return true
	case StateClosed:
		return true
	default:
		return true
	}
}

// canProceedHalfOpen implements progressive admission control for half-open state
func (cb *CircuitBreaker) canProceedHalfOpen() bool {
	allowed := atomic.LoadInt64(&cb.halfOpen.allowedRequests)

	// Special case: 0 means no limit
	if allowed == 0 {
		return true
	}

	// Normal progressive admission control
	for {
		active := atomic.LoadInt64(&cb.halfOpen.activeRequests)

		if active >= allowed {
			return false // Reject without modifying counter
		}

		// Try to claim a slot
		if atomic.CompareAndSwapInt64(&cb.halfOpen.activeRequests, active, active+1) {
			return true
		}
		// Retry if CAS failed due to concurrent modification
	}
}

// releaseHalfOpenSlot releases a slot in half-open state
func (cb *CircuitBreaker) releaseHalfOpenSlot() {
	atomic.AddInt64(&cb.halfOpen.activeRequests, -1)
}

// recordResult updates counters and manages progressive recovery
func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Release half-open slot if needed
	if cb.state == StateHalfOpen {
		cb.releaseHalfOpenSlot()
	}

	if err != nil {
		cb.onFailureLocked(err)
	} else {
		cb.onSuccessLocked()
	}

	// Update state after recording result
	cb.updateStateLocked()
}

// ============================================================================
// Methods below assume mutex is already held (suffix: Locked)
// ============================================================================

// resetLocked resets circuit breaker to closed state (assumes lock held)
func (cb *CircuitBreaker) resetLocked() {
	cb.state = StateClosed
	cb.consecutiveFailures = 0
	cb.consecutiveSuccesses = 0
	cb.lastFailureTime = time.Time{}
	cb.lastError = nil
	cb.resetHalfOpenStateLocked()
}

// updateStateLocked transitions circuit breaker state with progressive recovery (assumes lock held)
func (cb *CircuitBreaker) updateStateLocked() {
	switch cb.state {
	case StateClosed:
		// Open circuit after threshold failures
		if cb.consecutiveFailures >= cb.failureThreshold {
			cb.transitionToLocked(StateOpen, "failure threshold exceeded")
		}
	case StateOpen:
		// Try half-open after timeout with jitter
		if cb.shouldTransitionToHalfOpenLocked() {
			cb.transitionToLocked(StateHalfOpen, "timeout expired")
		}
	case StateHalfOpen:
		// Progressive recovery logic
		cb.updateHalfOpenStateLocked()
	}
}

// shouldTransitionToHalfOpenLocked checks if we should transition from open to half-open with jitter (assumes lock held)
func (cb *CircuitBreaker) shouldTransitionToHalfOpenLocked() bool {
	if cb.jitterMax <= 0 {
		// No jitter configured
		return time.Since(cb.lastFailureTime) > cb.timeout
	}

	// Calculate jitter with bounds
	maxJitter := cb.jitterMax
	if maxJitter > cb.timeout/10 {
		// Cap jitter at 10% of timeout to prevent excessive variance
		maxJitter = cb.timeout / 10
	}

	// Generate jitter in range [-maxJitter/2, +maxJitter/2]
	jitterRange := int64(maxJitter)
	jitter := time.Duration(cb.randSource.Int63n(jitterRange)) - maxJitter/2
	effectiveTimeout := cb.timeout + jitter

	return time.Since(cb.lastFailureTime) > effectiveTimeout
}

// updateHalfOpenStateLocked manages progressive recovery in half-open state (assumes lock held)
func (cb *CircuitBreaker) updateHalfOpenStateLocked() {
	// Single failure immediately reopens circuit
	if cb.consecutiveFailures >= 1 {
		cb.transitionToLocked(StateOpen, "failure in half-open state")

		return
	}

	// Check if we should progress to next level
	currentLevel := atomic.LoadInt64(&cb.halfOpen.allowedRequests)

	// Special case: if already at "no limit" (0), close the circuit
	if currentLevel == 0 {
		cb.transitionToLocked(StateClosed, "progressive recovery completed (no limit reached)")

		return
	}

	if cb.consecutiveSuccesses >= int(currentLevel) {
		cb.progressToNextLevelLocked(currentLevel)
	}
}

// transitionToLocked changes state and fires hooks (assumes lock held)
func (cb *CircuitBreaker) transitionToLocked(newState State, reason string) {
	// Fire pre-hook
	cb.firePreStateChangeHookLocked(newState, reason)

	// Perform transition
	start := time.Now()
	oldState := cb.state
	cb.state = newState
	duration := time.Since(start)

	// Reset state for new state
	if newState == StateHalfOpen {
		cb.resetHalfOpenStateLocked()
	}

	// Fire post-hook
	cb.firePostStateChangeHookLocked(oldState, newState, reason, duration)
}

// resetHalfOpenStateLocked resets half-open state counters (assumes lock held)
func (cb *CircuitBreaker) resetHalfOpenStateLocked() {
	atomic.StoreInt64(&cb.halfOpen.allowedRequests, int64(cb.halfOpen.baseAllowed))
	atomic.StoreInt64(&cb.halfOpen.activeRequests, 0)
	atomic.StoreInt64(&cb.halfOpen.successCount, 0)
	cb.consecutiveSuccesses = 0
}

// onFailureLocked handles failure event (assumes lock held)
func (cb *CircuitBreaker) onFailureLocked(err error) {
	cb.consecutiveFailures++
	cb.consecutiveSuccesses = 0
	cb.lastFailureTime = time.Now()
	cb.lastError = err
}

// onSuccessLocked handles success event (assumes lock held)
func (cb *CircuitBreaker) onSuccessLocked() {
	cb.consecutiveSuccesses++
	cb.consecutiveFailures = 0
	cb.lastError = nil
}

// firePreStateChangeHookLocked fires pre-state change hook (assumes lock held)
func (cb *CircuitBreaker) firePreStateChangeHookLocked(newState State, reason string) {
	if cb.hooks == nil {
		return
	}

	event := &hooks.PreCircuitBreakerStateChange{
		WorkerID:     cb.workerID,
		Resource:     cb.resource,
		CurrentState: cb.state.String(),
		NextState:    newState.String(),
		Reason:       reason,
	}

	// Use short timeout for pre-hooks
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_ = cb.hooks.Execute(ctx, "PreCircuitBreakerStateChange", event)
}

// firePostStateChangeHookLocked fires post-state change hook (assumes lock held)
func (cb *CircuitBreaker) firePostStateChangeHookLocked(oldState, newState State, reason string, duration time.Duration) {
	if cb.hooks == nil {
		return
	}

	event := &hooks.PostCircuitBreakerStateChange{
		WorkerID:           cb.workerID,
		Resource:           cb.resource,
		OldState:           oldState.String(),
		NewState:           newState.String(),
		Reason:             reason,
		TransitionDuration: duration,
	}

	// Add context based on transition
	if oldState == StateClosed && newState == StateOpen {
		event.FailureCount = cb.consecutiveFailures
		if cb.lastError != nil {
			event.Error = cb.lastError.Error()
		}
	} else if oldState == StateHalfOpen && newState == StateClosed {
		event.SuccessCount = cb.consecutiveSuccesses
	}

	// Use longer timeout for post-hooks
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_ = cb.hooks.Execute(ctx, "PostCircuitBreakerStateChange", event)
}

// fireRejectionHookLocked fires request rejection hook (assumes lock held)
func (cb *CircuitBreaker) fireRejectionHookLocked(reason string) {
	if cb.hooks == nil {
		return
	}

	event := &hooks.PostCircuitBreakerRequestRejected{
		WorkerID: cb.workerID,
		Resource: cb.resource,
		State:    cb.state.String(),
		Reason:   reason,
	}

	// Use very short timeout for rejection hooks
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	_ = cb.hooks.Execute(ctx, "PostCircuitBreakerRequestRejected", event)
}

// progressToNextLevelLocked handles progression to next level with overflow protection (assumes lock held)
func (cb *CircuitBreaker) progressToNextLevelLocked(currentLevel int64) {
	// Calculate next level with overflow protection
	nextLevel := currentLevel * 2

	// Check for overflow or if we exceed max
	if nextLevel < currentLevel || // overflow occurred
		(cb.halfOpen.maxAllowed > 0 && nextLevel > int64(cb.halfOpen.maxAllowed)) {
		// Check if maxAllowed is 0 (meaning no limit)
		if cb.halfOpen.maxAllowed == 0 {
			// Set to 0 to indicate no limit
			atomic.StoreInt64(&cb.halfOpen.allowedRequests, 0)
			cb.consecutiveSuccesses = 0
			// Don't close yet - let it run with no limit for a while
		} else {
			// We've reached the configured maximum, close the circuit
			cb.transitionToLocked(StateClosed, "progressive recovery completed")
		}
	} else {
		// Progress to next level
		atomic.StoreInt64(&cb.halfOpen.allowedRequests, nextLevel)
		atomic.StoreInt64(&cb.halfOpen.successCount, 0)
		cb.consecutiveSuccesses = 0
	}
}
