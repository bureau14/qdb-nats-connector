// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
package resilience

import (
	"sync"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector/hooks"
)

// Manager manages circuit breakers for different resources
type Manager struct {
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker

	// Default configuration
	defaultFailureThreshold int
	defaultSuccessThreshold int
	defaultTimeout          time.Duration
	defaultJitterMax        time.Duration
	defaultHalfOpenBase     int
	defaultHalfOpenMax      int

	// Hook registry
	hooks *hooks.HookRegistry
}

// NewManager creates a new circuit breaker manager
func NewManager(opts ...ManagerOption) *Manager {
	m := &Manager{
		breakers: make(map[string]*CircuitBreaker),
		// Set defaults
		defaultFailureThreshold: 5,
		defaultSuccessThreshold: 2,
		defaultTimeout:          30 * time.Second,
		defaultJitterMax:        100 * time.Millisecond,
		defaultHalfOpenBase:     1,
		defaultHalfOpenMax:      32,
	}

	// Apply options
	for _, opt := range opts {
		opt(m)
	}

	return m
}

// ManagerOption configures the manager
type ManagerOption func(*Manager)

// WithDefaults sets default circuit breaker parameters
func WithDefaults(failureThreshold, successThreshold int, timeout time.Duration) ManagerOption {
	return func(m *Manager) {
		m.defaultFailureThreshold = failureThreshold
		m.defaultSuccessThreshold = successThreshold
		m.defaultTimeout = timeout
	}
}

// WithDefaultJitter sets default jitter parameters
func WithDefaultJitter(maxJitter time.Duration) ManagerOption {
	return func(m *Manager) {
		m.defaultJitterMax = maxJitter
	}
}

// WithDefaultHalfOpen sets default half-open progression parameters
func WithDefaultHalfOpen(base, maxAllowed int) ManagerOption {
	return func(m *Manager) {
		m.defaultHalfOpenBase = base
		m.defaultHalfOpenMax = maxAllowed
	}
}

// WithHookRegistry sets the hook registry
func WithHookRegistry(registry *hooks.HookRegistry) ManagerOption {
	return func(m *Manager) {
		m.hooks = registry
	}
}

// ForResource returns the circuit breaker for a resource, creating it if necessary
func (m *Manager) ForResource(resource, workerID string) *CircuitBreaker {
	// First try read lock for fast path
	m.mu.RLock()
	if breaker, exists := m.breakers[resource]; exists {
		m.mu.RUnlock()

		return breaker
	}
	m.mu.RUnlock()

	// Need to create, acquire write lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if breaker, exists := m.breakers[resource]; exists {
		return breaker
	}

	// Create new circuit breaker with default configuration
	breaker := NewCircuitBreaker(
		m.defaultFailureThreshold,
		m.defaultSuccessThreshold,
		m.defaultTimeout,
		WithHooks(m.hooks, workerID, resource),
		WithJitter(m.defaultJitterMax),
		WithHalfOpenProgression(m.defaultHalfOpenBase, m.defaultHalfOpenMax),
	)

	m.breakers[resource] = breaker

	return breaker
}

// GetBreaker returns the circuit breaker for a resource without creating it
func (m *Manager) GetBreaker(resource string) (*CircuitBreaker, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	breaker, exists := m.breakers[resource]

	return breaker, exists
}

// ListResources returns all resource names with circuit breakers
func (m *Manager) ListResources() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	resources := make([]string, 0, len(m.breakers))
	for resource := range m.breakers {
		resources = append(resources, resource)
	}

	return resources
}

// ResetAll resets all circuit breakers to closed state
func (m *Manager) ResetAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, breaker := range m.breakers {
		breaker.Reset()
	}
}

// ResetResource resets the circuit breaker for a specific resource
func (m *Manager) ResetResource(resource string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if breaker, exists := m.breakers[resource]; exists {
		breaker.Reset()

		return true
	}

	return false
}

// GetStats returns statistics for all circuit breakers
func (m *Manager) GetStats() map[string]CircuitBreakerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]CircuitBreakerStats)
	for resource, breaker := range m.breakers {
		stats[resource] = CircuitBreakerStats{
			Resource: resource,
			State:    breaker.GetState().String(),
			IsOpen:   breaker.IsOpen(),
		}
	}

	return stats
}

// CircuitBreakerStats provides statistics for a circuit breaker
type CircuitBreakerStats struct {
	Resource string
	State    string
	IsOpen   bool
}
