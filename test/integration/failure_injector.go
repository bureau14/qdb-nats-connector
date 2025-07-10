package integration

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/bureau14/qdb-nats-connector/connector/hooks"
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
