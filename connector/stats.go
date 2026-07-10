// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: worker and fetch-loop statistics
// Types: WorkerStats, WorkerStatsSnapshot, workerCounters, FetchStats,
// fetchCounters
// Ex: w.GetStats() → WorkerStats{MessagesProcessed: 1000, ...}
package connector

import (
	"context"
	"sync/atomic"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
)

// pendingMaxAge: tolerance for the fetch-fed pending cache. An active
// stream refreshes it every fetch (<= 1s BatchTimeout); older than this
// means idle or stalled, so ConsumerPending polls ConsumerInfo instead.
const pendingMaxAge = 10 * time.Second

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
	// ParsesZeroRows: successful (OK/Partial) parses that produced zero
	// tables -- e.g. a terminal explode over an empty sample array. ACKed
	// like any valid parse; counted so silent data disappearance stays
	// observable.
	ParsesZeroRows uint64
	// RowsWritten: rows successfully written to QuasarDB.
	RowsWritten uint64
	// Acks: messages acknowledged after a successful pipeline pass.
	Acks uint64
	// Nacks: messages negatively acknowledged (parse or write failure).
	Nacks uint64
}

// workerCounters: atomic counters updated on the message hot path.
// Embedded in Worker; read via snapshot().
type workerCounters struct {
	messagesProcessed atomic.Uint64
	parseFailures     atomic.Uint64
	writeFailures     atomic.Uint64
	messagesDropped   atomic.Uint64
	parsesZeroRows    atomic.Uint64
	rowsWritten       atomic.Uint64
	acks              atomic.Uint64
	nacks             atomic.Uint64
}

// snapshot returns a point-in-time copy of the counters.
// Each counter is loaded atomically; the set is not read as one atomic unit.
func (c *workerCounters) snapshot() WorkerStats {
	return WorkerStats{
		MessagesProcessed: c.messagesProcessed.Load(),
		ParseFailures:     c.parseFailures.Load(),
		WriteFailures:     c.writeFailures.Load(),
		MessagesDropped:   c.messagesDropped.Load(),
		ParsesZeroRows:    c.parsesZeroRows.Load(),
		RowsWritten:       c.rowsWritten.Load(),
		Acks:              c.acks.Load(),
		Nacks:             c.nacks.Load(),
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
		total.ParsesZeroRows += s.ParsesZeroRows
		total.RowsWritten += s.RowsWritten
		total.Acks += s.Acks
		total.Nacks += s.Nacks
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

// WorkerStatsSnapshot: one worker's counters with its stable identity
// ("worker-0", ...), matching HealthSummary.BreakerStates keys so
// per-worker metric series join across families.
type WorkerStatsSnapshot struct {
	Worker string
	Stats  WorkerStats
}

// PerWorkerStats snapshots every worker's counters individually, in
// worker order. Same monitoring-grade semantics as Stats.
// In: none
// Out: []WorkerStatsSnapshot - one entry per worker
// Ex: c.PerWorkerStats()[0] -> {Worker: "worker-0", Stats: {...}}
func (c *Connector) PerWorkerStats() []WorkerStatsSnapshot {
	snapshots := make([]WorkerStatsSnapshot, len(c.workers))
	for i, w := range c.workers {
		snapshots[i] = WorkerStatsSnapshot{
			Worker: w.workerID,
			Stats:  w.GetStats(),
		}
	}

	return snapshots
}

// TableWrite: one table's contribution to a successfully written
// batch. Msgs counts pre-merge single-row table contributions, not
// distinct messages -- a message writing to two tables counts once in
// each. Consumed by the console stats top-10 window via the
// Options.OnTableWrites callback.
type TableWrite struct {
	Table string
	Rows  int
	Msgs  int
}

// FetchStats: point-in-time snapshot of the fetch-loop counters.
// Plain values, safe to copy, compare, and serialize.
type FetchStats struct {
	// MessagesFetched: messages pulled from JetStream, pre-parse.
	MessagesFetched uint64
	// TransientErrors: fetch errors retried with backoff.
	TransientErrors uint64
	// SubscriptionErrors: dead-subscription errors that entered re-bind.
	SubscriptionErrors uint64
	// RebindAttempts: individual re-bind tries across all incidents.
	RebindAttempts uint64
}

// fetchCounters: atomic counters updated by the fetch loop.
// Embedded in Connector; read via FetchStats(). rebindsAttempted is
// named to avoid colliding with the rebindAttempts budget field.
type fetchCounters struct {
	messagesFetched    atomic.Uint64
	transientErrors    atomic.Uint64
	subscriptionErrors atomic.Uint64
	rebindsAttempted   atomic.Uint64
}

// FetchStats returns a point-in-time copy of the fetch-loop counters.
// Each counter is loaded atomically; the set is not one atomic unit.
// In: none
// Out: FetchStats - fetch-side counter snapshot
// Ex: c.FetchStats() -> FetchStats{MessagesFetched: 5000, ...}
func (c *Connector) FetchStats() FetchStats {
	return FetchStats{
		MessagesFetched:    c.messagesFetched.Load(),
		TransientErrors:    c.transientErrors.Load(),
		SubscriptionErrors: c.subscriptionErrors.Load(),
		RebindAttempts:     c.rebindsAttempted.Load(),
	}
}

// WorkChannelDepth returns the number of batches queued for workers.
// A persistently full channel signals worker-pool backpressure.
// In: none
// Out: int - len of the work channel at this instant
// Ex: c.WorkChannelDepth() -> 3
func (c *Connector) WorkChannelDepth() int {
	return len(c.workCh)
}

// ConsumerPending returns the durable consumer's pending-message count,
// served from the fetch-fed cache when fresh and refreshed via
// ConsumerInfo when idle/stalled. Gated on natsReady: the same
// happens-before edge that orders source.Connect's field writes before
// reads from scrape goroutines.
// In: ctx context.Context - bounds the refresh RPC
// Out: uint64, error - pending count (last-known on error)
// Ex: c.ConsumerPending(ctx) -> 1523, nil
func (c *Connector) ConsumerPending(ctx context.Context) (uint64, error) {
	if !c.natsReady.Load() {
		return 0, connectorErrors.NewConnectionFailedError("connector",
			"consumer-info", nats.ErrInvalidConnection)
	}

	return c.source.PendingMessages(ctx, pendingMaxAge)
}
