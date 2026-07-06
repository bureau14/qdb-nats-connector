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

// accumulateStats sums per-worker snapshots into a single aggregate.
func accumulateStats(stats []WorkerStats) WorkerStats {
	var total WorkerStats
	for _, s := range stats {
		total.MessagesProcessed += s.MessagesProcessed
		total.ParseFailures += s.ParseFailures
		total.WriteFailures += s.WriteFailures
		total.MessagesDropped += s.MessagesDropped
	}

	return total
}

// Stats snapshots every worker's counters and returns their sum. Workers
// keep sole write ownership of their counters (lock-free hot path, no
// shared state); this is read-time reduction only. Per-worker snapshots
// are taken at slightly different instants, so the aggregate is
// monitoring-grade rather than a globally-atomic view; it is exact once
// workers have quiesced (RunWithContext returned).
func (c *Connector) Stats() WorkerStats {
	stats := make([]WorkerStats, len(c.workers))
	for i, w := range c.workers {
		stats[i] = w.GetStats()
	}

	return accumulateStats(stats)
}
