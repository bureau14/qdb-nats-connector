// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package resilience: circuit breaker prevents cascading failures
// Types: CircuitBreaker, State, Option
// Ex: cb.Execute(func() error { return service.Call() })
package resilience

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
)

// State: circuit breaker state (Closed→Open→HalfOpen→Closed)
type State int

const (
	// StateClosed: normal operation, requests pass through
	StateClosed   State = iota // Normal operation
	StateOpen                  // Blocking requests
	StateHalfOpen              // Progressive recovery
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

// halfOpenState: progressive recovery control (1→2→4→8→16→32→unlimited)
type halfOpenState struct {
	allowedRequests int64 // Current limit for concurrent requests
	activeRequests  int64 // Number of in-flight requests
	successCount    int64 // Consecutive successes in current level
	baseAllowed     int   // Starting value (default 1)
	maxAllowed      int   // Maximum before fully closed (default 32)
}

// CircuitBreaker: thread-safe failure prevention, progressive recovery, jitter
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
	lastStateChangeTime  time.Time

	// Progressive half-open recovery
	halfOpen halfOpenState

	// Hook integration
	hooks    *hooks.HookRegistry
	workerID string
	resource string

	// Jitter
	randSource *rand.Rand
}

// NewCircuitBreaker creates circuit breaker.
// Args:
//
//	failureThreshold: failures to open (≥1)
//	successThreshold: successes per level in half-open
//	timeout: recovery wait after open
//
// Returns:
//
//	*CircuitBreaker: configured breaker
//
// Example:
//
//	cb := NewCircuitBreaker(5, 10, time.Minute) // → *CircuitBreaker
func NewCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration, opts ...Option) *CircuitBreaker {
	cb := &CircuitBreaker{
		failureThreshold:    failureThreshold,
		successThreshold:    successThreshold,
		timeout:             timeout,
		state:               StateClosed,
		jitterMax:           100 * time.Millisecond, // default jitter
		lastStateChangeTime: time.Now(),
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

// Execute runs function with circuit protection.
// Args:
//
//	fn: protected function (typically expensive batch operation)
//
// Returns:
//
//	error: fn error or circuit open error
//
// Behavior:
//   - Adds jitter when degraded (prevents thundering herd)
//   - Rejects when open/admission control exceeded
//   - Tracks success/failure for state transitions
//
// Note: fn should be a substantial operation (e.g., batch DB write,
// API call with multiple records). Don't use for individual message
// processing - the overhead isn't justified for microsecond operations.
//
// Example:
//
//	err := cb.Execute(func() error {
//	    return db.BatchInsert(500_records) // Good: batch operation
//	})
func (cb *CircuitBreaker) Execute(fn func() error) error {
	// Jitter prevents thundering herd when recovering from outage.
	// Applied only in degraded states, not logged (reduces noise).
	// Hook provides visibility for capacity planning during recovery.
	cb.applyJitterDelay()

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

// LogStatus logs breaker state for monitoring.
// Call periodically to track degraded states.
// Only logs when open/half-open (reduces noise).
func (cb *CircuitBreaker) LogStatus() {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	cb.logStatusLocked()
}

// applyJitterDelay applies jitter sleep when in degraded states
func (cb *CircuitBreaker) applyJitterDelay() {
	if cb.jitterMax > 0 && (cb.IsOpen() || cb.IsHalfOpen()) {
		jitter := time.Duration(cb.randSource.Int63n(int64(cb.jitterMax)))

		// Fire hook only when jitter>0 because:
		// - Zero delays create noise without operational value
		// - Hooks are for actionable insights (capacity/tuning)
		// - Batch context: 500-msg writes dwarf jitter overhead
		if cb.hooks != nil && jitter > 0 {
			event := &hooks.PreCircuitBreakerJitter{
				WorkerID:  cb.workerID,
				Resource:  cb.resource,
				State:     cb.GetState().String(),
				JitterMs:  jitter.Milliseconds(),
				Timestamp: time.Now(),
			}

			// 5ms timeout because hook should just channel data, not process.
			// Provides visibility without logs (cleaner for high-volume ops).
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
			defer cancel()

			_ = cb.hooks.Execute(ctx, "PreCircuitBreakerJitter", event)
		}

		time.Sleep(jitter)
	}
}

// calculateEffectiveTimeout adds jitter to base timeout for recovery
func (cb *CircuitBreaker) calculateEffectiveTimeout() time.Duration {
	if cb.jitterMax <= 0 {
		return cb.timeout
	}

	// Calculate jitter with bounds
	maxJitter := cb.jitterMax
	if maxJitter > cb.timeout/10 {
		// Cap jitter at 10% because:
		// - Prevents pathological cases (e.g., 1min timeout + 30s jitter = too variable)
		// - Ensures recovery attempts stay reasonably predictable
		// - 10% provides enough randomization to prevent thundering herd
		maxJitter = cb.timeout / 10
	}

	// Generate jitter in range [-maxJitter/2, +maxJitter/2] because:
	// - Symmetric distribution prevents systematic bias
	// - Some recover early, some late → spreads load naturally
	// - Mean recovery time remains cb.timeout (no drift)
	jitterRange := int64(maxJitter)
	jitter := time.Duration(cb.randSource.Int63n(jitterRange)) - maxJitter/2

	return cb.timeout + jitter
}

// canProceed checks if request allowed with progressive admission control
func (cb *CircuitBreaker) canProceed() bool {
	// Write lock required for entire operation because:
	// 1. updateStateLocked() may change state (closed→open)
	// 2. Must prevent race where state changes between update and check
	// 3. Ensures atomic update-then-check operation
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
		if !cb.canProceedHalfOpenLocked() {
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

// canProceedHalfOpenLocked: admission control in half-open (lock held)
// Progressive recovery: limits concurrent requests (1→2→4→...)
// Prevents overload during recovery testing
func (cb *CircuitBreaker) canProceedHalfOpenLocked() bool {
	allowed := atomic.LoadInt64(&cb.halfOpen.allowedRequests)

	// Special case: 0 means no limit
	if allowed == 0 {
		return true
	}

	// Atomic admission control prevents race conditions.
	// CAS loop ensures exactly 'allowed' concurrent requests because:
	// - Multiple goroutines may check simultaneously
	// - Without CAS, could exceed limit (e.g., 3 goroutines see active=0, all increment → 3 > allowed=1)
	// - CAS guarantees only one succeeds per slot
	// - Thread-safe without holding main mutex (reduces contention)
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

// releaseHalfOpenSlotLocked releases a slot in half-open state (assumes lock held)
func (cb *CircuitBreaker) releaseHalfOpenSlotLocked() {
	atomic.AddInt64(&cb.halfOpen.activeRequests, -1)
}

// recordResult updates counters and manages progressive recovery
func (cb *CircuitBreaker) recordResult(err error) {
	// Lock ordering critical: acquire lock BEFORE releasing half-open slot because:
	// 1. Must read cb.state atomically (prevents state change during check)
	// 2. Ensures consistent view when updating failure/success counters
	// 3. Prevents slot leak if state transitions during operation
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Release half-open slot if needed
	if cb.state == StateHalfOpen {
		cb.releaseHalfOpenSlotLocked()
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
	effectiveTimeout := cb.calculateEffectiveTimeout()

	return time.Since(cb.lastFailureTime) > effectiveTimeout
}

// updateHalfOpenStateLocked: progressive recovery logic (lock held)
// Single failure → reopen (safety first)
// Enough successes → next level (gradual trust)
func (cb *CircuitBreaker) updateHalfOpenStateLocked() {
	// Any failure in half-open immediately reopens because
	// service likely still unhealthy. Prevents cascading failures.
	if cb.consecutiveFailures >= 1 {
		cb.transitionToLocked(StateOpen, "failure in half-open state")

		return
	}

	// Check if we should progress to next level
	currentLevel := atomic.LoadInt64(&cb.halfOpen.allowedRequests)

	// Level 0 = unlimited requests. If stable here,
	// service fully recovered → close circuit.
	if currentLevel == 0 {
		cb.transitionToLocked(StateClosed, "progressive recovery completed (no limit reached)")

		return
	}

	if cb.consecutiveSuccesses >= int(currentLevel) {
		cb.progressToNextLevelLocked(currentLevel)
	}
}

// transitionToLocked changes state and fires hooks (assumes lock held)
// State transitions must be atomic because:
// - Concurrent requests check state without lock (IsOpen/IsHalfOpen)
// - Mid-transition visibility could cause incorrect decisions
// - All state-dependent fields must update together
func (cb *CircuitBreaker) transitionToLocked(newState State, reason string) {
	// Fire pre-hook
	cb.firePreStateChangeHookLocked(newState, reason)

	// Perform transition
	start := time.Now()
	oldState := cb.state
	cb.state = newState // Single atomic write ensures no partial state visible
	duration := time.Since(start)

	// State transitions logged at Info for operational visibility.
	// Operators can track circuit behavior during incidents.
	slog.Info("Circuit breaker state transition",
		"resource", cb.resource,
		"transition", fmt.Sprintf("%s→%s", oldState, newState),
		"reason", reason,
		"duration_in_state", time.Since(cb.lastStateChangeTime))

	// Additional context when opening
	if newState == StateOpen && cb.lastError != nil {
		slog.Info("Circuit breaker opened",
			"resource", cb.resource,
			"last_error", cb.lastError.Error(),
			"consecutive_failures", cb.consecutiveFailures)
	}

	// Update last state change time
	cb.lastStateChangeTime = time.Now()

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

// firePreStateChangeHookLocked: pre-transition hook (lock held)
// Enables custom metrics/alerting integration
// Short timeout prevents hook from blocking state changes
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

// progressToNextLevelLocked: doubles allowed requests (lock held)
// Progression: 1→2→4→8→16→32→unlimited→closed
// Gradual recovery prevents re-overload
func (cb *CircuitBreaker) progressToNextLevelLocked(currentLevel int64) {
	// Double allowed requests each successful level.
	// This exponential growth balances safety with recovery speed.
	nextLevel := currentLevel * 2

	// Check for overflow or if we exceed max
	if nextLevel < currentLevel || // overflow occurred (int64 wraparound protection)
		(cb.halfOpen.maxAllowed > 0 && nextLevel > int64(cb.halfOpen.maxAllowed)) {
		// maxAllowed=0 means "unlimited after max progression" because:
		// - Allows testing with no artificial ceiling
		// - Final proof service can handle full load
		// - Still requires success at this level before closing
		if cb.halfOpen.maxAllowed == 0 {
			// Set to 0 to indicate no limit
			atomic.StoreInt64(&cb.halfOpen.allowedRequests, 0)
			cb.consecutiveSuccesses = 0
			// Log recovery progression
			slog.Info("Circuit breaker recovery progression",
				"resource", cb.resource,
				"allowed_requests", fmt.Sprintf("%d→unlimited", currentLevel),
				"is_unlimited", true)
			// Don't close yet - let it run with no limit for a while
		} else {
			// maxAllowed>0: close at configured maximum because:
			// - User set explicit recovery ceiling
			// - Sufficient proof of health at this level
			cb.transitionToLocked(StateClosed, "progressive recovery completed")
		}
	} else {
		// Progress to next level
		atomic.StoreInt64(&cb.halfOpen.allowedRequests, nextLevel)
		atomic.StoreInt64(&cb.halfOpen.successCount, 0)
		cb.consecutiveSuccesses = 0
		// Log recovery progression
		slog.Info("Circuit breaker recovery progression",
			"resource", cb.resource,
			"allowed_requests", fmt.Sprintf("%d→%d", currentLevel, nextLevel),
			"is_unlimited", false)
	}
}

// logStatusLocked logs current circuit breaker status if in degraded state (assumes lock held)
func (cb *CircuitBreaker) logStatusLocked() {
	if cb.state == StateOpen || cb.state == StateHalfOpen {
		slog.Info("Circuit breaker status",
			"resource", cb.resource,
			"state", cb.state.String(),
			"duration_in_state", time.Since(cb.lastStateChangeTime))
	}
}
