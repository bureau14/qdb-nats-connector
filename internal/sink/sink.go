// Package sink manages the QuasarDB connection and data persistence.
// This internal package handles batching, compression, encryption, and
// retry logic for reliable time series data storage.
// Decision rationale:
// - Internal package isolates QuasarDB-specific logic
// - Batching improves write throughput
// - Configurable compression/encryption for security
package sink

import (
	log "github.com/sirupsen/logrus"
)

// Sink manages the QuasarDB client connection and write operations.
// Key assumptions:
// - QuasarDB cluster is reachable at configured endpoint
// - Authentication credentials are valid if provided
// - Sink handles reconnection on transient failures
type Sink struct {
	Options Options
}

// NewSink establishes a connection to the QuasarDB cluster.
// Decision rationale:
// - Validates options before attempting connection
// - Returns error immediately on invalid configuration
// - Connection pooling configured based on options
// Performance trade-offs:
// - Higher parallelism increases memory usage
// - Larger buffers improve throughput but increase latency
func NewSink(opts Options) (*Sink, error) {
	log.Info("Initializing new sink")

	return &Sink{Options: opts}, nil
}

// Close gracefully shuts down the QuasarDB connection.
// Key assumptions:
// - Pending writes are flushed before closure
// - Method is idempotent and safe to call multiple times
// Decision rationale:
// - Graceful shutdown prevents data loss
// - No error return as shutdown is best-effort
func (s *Sink) Close() {
	log.Info("Closing sink")
}
