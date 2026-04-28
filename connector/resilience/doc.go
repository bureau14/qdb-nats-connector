// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package resilience: circuit breaker and recovery management
// Types: CircuitBreaker, Manager, State
// Ex: manager.ForResource("qdb-cluster") → *CircuitBreaker
//
// Philosophy: Progressive recovery with shared state to prevent worker stampedes.
// Circuit breakers are shared across workers by resource name.
// Half-open state allows progressive recovery: 1→2→4→8→16→32 requests.
// Jitter prevents thundering herd patterns during recovery.
// Hooks provide visibility into state changes and request rejections.
package resilience
