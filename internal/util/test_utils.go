// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package util provides testing utilities for the qdb-nats-connector project.
// These helpers are designed to be generic and reusable across all test suites.
package util

import (
	"bytes"
	"errors"
	"math/rand" // #nosec G404 - This is for test data generation only
	"sync"
	"testing"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// latin: safe alphanumeric chars for IDs, avoids ambiguous chars
const latin = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// RandomAlias generates 16-char alphanumeric ID.
// Out: string - [A-Za-z0-9]{16}
// Ex: RandomAlias() → "a8Bc3dEf9GhI2jKl"
func RandomAlias() string {
	const size = 16
	var buffer bytes.Buffer
	for range size {
		buffer.WriteString(string(latin[rand.Intn(len(latin))])) // #nosec G404 - This is for test data generation only
	}

	return buffer.String()
}

// RandomTopicName creates test topic name.
// Out: string - 16-char topic
// Ex: RandomTopicName() → "topic1a2b3c4d5e6"
func RandomTopicName() string {
	return RandomAlias()
}

// RandomNumber generates a random integer for testing.
// Out: int - random number 0-999999
// Ex: RandomNumber() → 42857
func RandomNumber() int {
	return rand.Intn(1000000) // #nosec G404 - This is for test data generation only
}

// SetupNATS creates a test NATS connection using the default URL.
// Returns a connected NATS connection for testing.
// The connection should be closed by the caller using defer conn.Close().
func SetupNATS(t *testing.T) *nats.Conn {
	t.Helper()

	conn, err := nats.Connect(nats.DefaultURL)
	require.NoError(t, err, "failed to connect to NATS server for testing")
	require.True(t, conn.IsConnected(), "NATS connection should be active")

	return conn
}

// DefaultNATSEndpoint returns the default NATS endpoint for testing.
func DefaultNATSEndpoint() string {
	return nats.DefaultURL
}

// DefaultQDBEndpoint returns the default QuasarDB endpoint for testing.
func DefaultQDBEndpoint() string {
	return "qdb://127.0.0.1:2836"
}

// TestSinkConfig returns conservative sink configuration for testing.
func TestSinkConfig() (clusterUri string, numWriters, queueSize, retryAttempts int) {
	return "qdb://127.0.0.1:2836", 2, 50, 2
}

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

// AssertSubscriptionError checks subscription-specific error details
func AssertSubscriptionError[T any](t *testing.T, tt T, err error, topic string) {
	AssertError(t, tt, err)

	var connErr *connectorErrors.ConnectorError
	require.True(t, errors.As(err, &connErr), "error should be a ConnectorError")
	assert.Equal(t, "source", connErr.Component)
	assert.Equal(t, connectorErrors.ErrCodeSubscriptionFailed, connErr.Code)
	assert.Equal(t, topic, connErr.Metadata["topic"])
}
