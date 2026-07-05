// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: worker statistics
// Types: WorkerStats, workerCounters
// Ex: w.GetStats() → WorkerStats{MessagesProcessed: 1000, ...}
package connector

import "sync/atomic"

// WorkerStats: point-in-time snapshot of a worker's message counters.
// Plain values, safe to copy, compare, and serialize.
type WorkerStats struct {
	// MessagesProcessed: messages that completed the pipeline without a
	// parse failure (written, filtered out, or dropped -- all ACKed).
	MessagesProcessed uint64
	// ParseFailures: messages NACKed for redelivery due to parse errors.
	ParseFailures uint64
	// WriteFailures: batch write errors against QuasarDB.
	WriteFailures uint64
	// MessagesDropped: structurally-unusable messages discarded in drop
	// mode (ACKed without a row).
	MessagesDropped uint64
}

// workerCounters: atomic counters updated on the message hot path.
// Embedded in Worker; read via snapshot().
type workerCounters struct {
	messagesProcessed atomic.Uint64
	parseFailures     atomic.Uint64
	writeFailures     atomic.Uint64
	messagesDropped   atomic.Uint64
}

// snapshot returns a point-in-time copy of the counters.
// Each counter is loaded atomically; the set is not read as one atomic unit.
func (c *workerCounters) snapshot() WorkerStats {
	return WorkerStats{
		MessagesProcessed: c.messagesProcessed.Load(),
		ParseFailures:     c.parseFailures.Load(),
		WriteFailures:     c.writeFailures.Load(),
		MessagesDropped:   c.messagesDropped.Load(),
	}
}
