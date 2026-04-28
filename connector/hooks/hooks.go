// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package hooks: synchronous lifecycle callbacks
// Types: HookFunc, HookRegistry
// Ex: r.Register("PreRead", func) → r.Execute(ctx, "PreRead", data)
//
// Philosophy: All hooks execute synchronously in the worker thread.
// Hook implementors MUST ensure fast execution to avoid blocking.
// Use channels/buffering for expensive operations (metrics, logging).
// Pre* hooks: 100μs threshold. Post* hooks: 500μs threshold.
// Timing violations logged as warnings, not errors.
package hooks

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Timing thresholds for hook execution warnings
const (
	PreHookWarningThreshold  = 100 * time.Microsecond
	PostHookWarningThreshold = 500 * time.Microsecond
)

// HookFunc signature - all hooks must match this.
// IMPORTANT: Hook functions must execute quickly to avoid blocking the worker.
// See timing thresholds above for warning levels.
type HookFunc func(ctx context.Context, data interface{}) error

// HookRegistry manages all registered hooks
type HookRegistry struct {
	mu    sync.RWMutex
	hooks map[string][]HookFunc // map of hook name -> list of functions
}

// NewHookRegistry initializes an empty hooks registry
func NewHookRegistry() *HookRegistry {
	return &HookRegistry{
		hooks: make(map[string][]HookFunc),
	}
}

// Register adds a hook function to the registry for the given hook name
func (r *HookRegistry) Register(name string, fn HookFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.hooks[name] = append(r.hooks[name], fn)
}

// Execute runs all registered hooks for the given name synchronously with timing measurement.
// Logs warnings if hooks exceed timing thresholds (see constants above).
// Returns error on first hook failure (fail-fast)
func (r *HookRegistry) Execute(ctx context.Context, name string, data interface{}) error {
	r.mu.RLock()
	hooks, exists := r.hooks[name]
	r.mu.RUnlock()

	if !exists {
		return nil
	}

	// Determine warning threshold based on hook type
	threshold := r.getWarningThreshold(name)

	// Measure total execution time
	totalStart := time.Now()

	for i, hook := range hooks {
		// Measure individual hook execution time
		hookStart := time.Now()
		err := r.executeHookWithRecovery(ctx, hook, data)
		hookDuration := time.Since(hookStart)

		// Log warning if hook exceeds threshold
		if hookDuration > threshold {
			slog.Warn("Hook execution exceeded threshold",
				"hook_name", name,
				"hook_index", i,
				"execution_time", hookDuration,
				"threshold", threshold)
		}

		if err != nil {
			return err
		}
	}

	// Log warning for total execution time if multiple hooks
	if len(hooks) > 1 {
		totalDuration := time.Since(totalStart)
		if totalDuration > threshold {
			slog.Warn("Total hook execution exceeded threshold",
				"hook_name", name,
				"total_execution_time", totalDuration,
				"threshold", threshold,
				"hook_count", len(hooks))
		}
	}

	return nil
}

// executeHookWithRecovery executes a hook with panic recovery
func (r *HookRegistry) executeHookWithRecovery(ctx context.Context, hook HookFunc, data interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("hook panic: %v", r)
			slog.Error("Hook panic recovered", "panic", r)
		}
	}()

	return hook(ctx, data)
}

// getWarningThreshold returns the appropriate warning threshold based on hook name
func (r *HookRegistry) getWarningThreshold(name string) time.Duration {
	switch name {
	case "PreRead", "PreWrite", "PreAck", "PreCircuitBreakerStateChange", "PreCircuitBreakerJitter":
		return PreHookWarningThreshold
	case "PostRead", "PostWrite", "PostAck", "PostCircuitBreakerStateChange":
		return PostHookWarningThreshold
	case "PostCircuitBreakerRequestRejected":
		// Request rejections must be very fast to avoid blocking
		return 5 * time.Microsecond
	default:
		// Default to pre-hook threshold for unknown hook types
		return PreHookWarningThreshold
	}
}
