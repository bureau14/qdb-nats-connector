// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: worker statistics aggregation tests
package connector

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

// bumpedWorker returns a Worker whose counters have been advanced by the
// given stats, mimicking hot-path atomic increments.
func bumpedWorker(id int, stats WorkerStats) *Worker {
	w := &Worker{workerID: fmt.Sprintf("worker-%d", id)}
	w.messagesProcessed.Add(stats.MessagesProcessed)
	w.parseFailures.Add(stats.ParseFailures)
	w.writeFailures.Add(stats.WriteFailures)
	w.messagesDropped.Add(stats.MessagesDropped)
	w.rowsWritten.Add(stats.RowsWritten)
	w.acks.Add(stats.Acks)
	w.nacks.Add(stats.Nacks)

	return w
}

// genWorkerStats draws an arbitrary WorkerStats for property tests.
func genWorkerStats(rt *rapid.T, label string) WorkerStats {
	gen := rapid.Uint64Range(0, 1<<40)

	return WorkerStats{
		MessagesProcessed: gen.Draw(rt, label+"-processed"),
		ParseFailures:     gen.Draw(rt, label+"-parse"),
		WriteFailures:     gen.Draw(rt, label+"-write"),
		MessagesDropped:   gen.Draw(rt, label+"-dropped"),
		RowsWritten:       gen.Draw(rt, label+"-rows"),
		Acks:              gen.Draw(rt, label+"-acks"),
		Nacks:             gen.Draw(rt, label+"-nacks"),
	}
}

func TestAccumulateStats(t *testing.T) {
	t.Run("empty slice is zero", func(t *testing.T) {
		assert.Equal(t, WorkerStats{}, accumulateStats(nil))
	})

	t.Run("sums per field", func(t *testing.T) {
		total := accumulateStats([]WorkerStats{
			{
				MessagesProcessed: 10, ParseFailures: 1, WriteFailures: 2,
				MessagesDropped: 3, RowsWritten: 20, Acks: 9, Nacks: 1,
			},
			{
				MessagesProcessed: 5, ParseFailures: 4, WriteFailures: 0,
				MessagesDropped: 7, RowsWritten: 5, Acks: 5, Nacks: 4,
			},
		})

		assert.Equal(t, WorkerStats{
			MessagesProcessed: 15,
			ParseFailures:     5,
			WriteFailures:     2,
			MessagesDropped:   10,
			RowsWritten:       25,
			Acks:              14,
			Nacks:             5,
		}, total)
	})
}

func TestConnectorStats(t *testing.T) {
	t.Run("zero workers", func(t *testing.T) {
		c := &Connector{}
		assert.Equal(t, WorkerStats{}, c.Stats())
	})

	t.Run("aggregates across workers", func(t *testing.T) {
		c := &Connector{workers: []*Worker{
			bumpedWorker(0, WorkerStats{MessagesProcessed: 100, ParseFailures: 2, MessagesDropped: 4, Acks: 98}),
			bumpedWorker(1, WorkerStats{MessagesProcessed: 50, WriteFailures: 1, RowsWritten: 49, Nacks: 1}),
			bumpedWorker(2, WorkerStats{MessagesDropped: 6}),
		}}

		assert.Equal(t, WorkerStats{
			MessagesProcessed: 150,
			ParseFailures:     2,
			WriteFailures:     1,
			MessagesDropped:   10,
			RowsWritten:       49,
			Acks:              98,
			Nacks:             1,
		}, c.Stats())
	})
}

func TestPerWorkerStats(t *testing.T) {
	t.Run("zero workers", func(t *testing.T) {
		c := &Connector{}
		assert.Empty(t, c.PerWorkerStats())
	})

	t.Run("labels follow worker order", func(t *testing.T) {
		c := &Connector{workers: []*Worker{
			bumpedWorker(0, WorkerStats{MessagesProcessed: 7}),
			bumpedWorker(1, WorkerStats{ParseFailures: 3}),
		}}

		snapshots := c.PerWorkerStats()
		assert.Equal(t, []WorkerStatsSnapshot{
			{Worker: "worker-0", Stats: WorkerStats{MessagesProcessed: 7}},
			{Worker: "worker-1", Stats: WorkerStats{ParseFailures: 3}},
		}, snapshots)
	})
}

// Property: the aggregate Stats() equals the field-wise sum of
// PerWorkerStats() for arbitrary counter values -- the invariant the
// Prometheus collector relies on (sum() over the worker label).
func TestStatsMatchPerWorkerSum(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		count := rapid.IntRange(0, 8).Draw(rt, "workers")

		workers := make([]*Worker, count)
		for i := range workers {
			workers[i] = bumpedWorker(i, genWorkerStats(rt, fmt.Sprintf("w%d", i)))
		}
		c := &Connector{workers: workers}

		var expected WorkerStats
		for _, snap := range c.PerWorkerStats() {
			expected.MessagesProcessed += snap.Stats.MessagesProcessed
			expected.ParseFailures += snap.Stats.ParseFailures
			expected.WriteFailures += snap.Stats.WriteFailures
			expected.MessagesDropped += snap.Stats.MessagesDropped
			expected.RowsWritten += snap.Stats.RowsWritten
			expected.Acks += snap.Stats.Acks
			expected.Nacks += snap.Stats.Nacks
		}

		assert.Equal(t, expected, c.Stats())
	})
}
