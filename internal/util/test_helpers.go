// Package util provides testing utilities for the qdb-nats-connector project.
// These helpers are designed to be generic and reusable across all test suites.
package util

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssertError checks if an error occurred and optionally validates error content.
// It accepts any test struct that has an errContains field for content validation.
// This is a generic helper that can be used across all test suites.
func AssertError[T any](t *testing.T, tt T, err error) {
	require.Error(t, err)

	// Use reflection to access errContains field
	if v, ok := any(tt).(struct{ errContains string }); ok && v.errContains != "" {
		assert.Contains(t, err.Error(), v.errContains)
	}
}

// WaitForMessages waits for messages with a timeout using a WaitGroup.
// This is useful for synchronizing tests that involve concurrent message processing.
// It will fail the test if the timeout is reached before all messages are processed.
func WaitForMessages(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("timeout waiting for messages")
	}
}
