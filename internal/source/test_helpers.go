package source

import (
	"errors"
	"sync"
	"testing"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertError is a wrapper around util.AssertError for backwards compatibility
func assertError[T any](t *testing.T, tt T, err error) {
	util.AssertError(t, tt, err)
}

// assertSubscriptionError checks subscription-specific error details
func assertSubscriptionError[T any](t *testing.T, tt T, err error, source *Source) {
	assertError(t, tt, err)

	var connErr *connectorErrors.ConnectorError
	require.True(t, errors.As(err, &connErr), "error should be a ConnectorError")
	assert.Equal(t, "source", connErr.Component)
	assert.Equal(t, connectorErrors.ErrCodeSubscriptionFailed, connErr.Code)
	assert.Equal(t, source.Options.Topic, connErr.Metadata["topic"])
}

// publishTestMessages publishes a specified number of test messages
func publishTestMessages(t *testing.T, source *Source, count int) {
	for range count {
		err := source.NatsConn.Publish(source.Options.Topic, []byte("test message"))
		require.NoError(t, err)
	}
	require.NoError(t, source.NatsConn.Flush())
}

// waitForMessages is a wrapper around util.WaitForMessages for backwards compatibility
func waitForMessages(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	util.WaitForMessages(t, wg, timeout)
}
