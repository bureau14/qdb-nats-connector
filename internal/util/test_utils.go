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
// In: none
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

// RandomTopicName delegates to RandomAlias for topics.
// In: none
// Out: string - 16-char alphanumeric
// Ex: RandomTopicName() → "topic1a2b3c4d5e6"
func RandomTopicName() string {
	return RandomAlias()
}

// RandomNumber generates test integer 0-999999.
// In: none
// Out: int - random [0,999999]
// Ex: RandomNumber() → 42857
func RandomNumber() int {
	return rand.Intn(1000000) // #nosec G404 - This is for test data generation only
}

// SetupNATS connects to test NATS server.
// In: t *testing.T - test context
// Out: *nats.Conn - connected, caller must close
// Ex: SetupNATS(t) → &Conn{connected:true}
func SetupNATS(t *testing.T) *nats.Conn {
	t.Helper()

	conn, err := nats.Connect(nats.DefaultURL)
	require.NoError(t, err, "failed to connect to NATS server for testing")
	require.True(t, conn.IsConnected(), "NATS connection should be active")

	return conn
}

// DefaultNATSEndpoint returns test NATS URL.
// In: none
// Out: string - nats.DefaultURL
// Ex: DefaultNATSEndpoint() → "nats://localhost:4222"
func DefaultNATSEndpoint() string {
	return nats.DefaultURL
}

// DefaultQDBEndpoint returns test QuasarDB URL.
// In: none
// Out: string - local QDB endpoint
// Ex: DefaultQDBEndpoint() → "qdb://127.0.0.1:2836"
func DefaultQDBEndpoint() string {
	return "qdb://127.0.0.1:2836"
}

// TestSinkConfig returns conservative test sink params.
// In: none
// Out: uri string, writers/queue/retries int
// Ex: TestSinkConfig() → "qdb://127.0.0.1:2836",2,50,2
func TestSinkConfig() (clusterUri string, numWriters, queueSize, retryAttempts int) {
	return "qdb://127.0.0.1:2836", 2, 50, 2
}

// AssertError validates error & optional content match.
// In: t *testing.T, tt T - test case, err error
// Out: none - asserts error exists & contains text
// Ex: AssertError(t, tt{errContains:"fail"}, err)
func AssertError[T any](t *testing.T, tt T, err error) {
	require.Error(t, err)

	// Use reflection to access errContains field
	if v, ok := any(tt).(struct{ errContains string }); ok && v.errContains != "" {
		assert.Contains(t, err.Error(), v.errContains)
	}
}

// WaitForMessages blocks until WaitGroup done or timeout.
// In: t *testing.T, wg *sync.WaitGroup, timeout Duration
// Out: none - returns or t.Fatal on timeout
// Ex: WaitForMessages(t, wg, 5*time.Second)
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

// AssertSubscriptionError validates subscription error fields.
// In: t *testing.T, tt T, err error, topic string
// Out: none - asserts ConnectorError with code 1003
// Ex: AssertSubscriptionError(t, tt, err, "data.*")
func AssertSubscriptionError[T any](t *testing.T, tt T, err error, topic string) {
	AssertError(t, tt, err)

	var connErr *connectorErrors.ConnectorError
	require.True(t, errors.As(err, &connErr), "error should be a ConnectorError")
	assert.Equal(t, "source", connErr.Component)
	assert.Equal(t, connectorErrors.ErrCodeSubscriptionFailed, connErr.Code)
	assert.Equal(t, topic, connErr.Metadata["topic"])
}
