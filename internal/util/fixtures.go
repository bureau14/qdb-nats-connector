package util

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

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
