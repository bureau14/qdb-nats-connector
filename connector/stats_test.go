// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: worker statistics aggregation tests
package connector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// bumpedWorker returns a Worker whose counters have been advanced by the
// given amounts, mimicking hot-path atomic increments.
func bumpedWorker(processed, parseFail, writeFail, dropped uint64) *Worker {
	w := &Worker{}
	w.messagesProcessed.Add(processed)
	w.parseFailures.Add(parseFail)
	w.writeFailures.Add(writeFail)
	w.messagesDropped.Add(dropped)

	return w
}

func TestAccumulateStats(t *testing.T) {
	t.Run("empty slice is zero", func(t *testing.T) {
		assert.Equal(t, WorkerStats{}, accumulateStats(nil))
	})

	t.Run("sums per field", func(t *testing.T) {
		total := accumulateStats([]WorkerStats{
			{MessagesProcessed: 10, ParseFailures: 1, WriteFailures: 2, MessagesDropped: 3},
			{MessagesProcessed: 5, ParseFailures: 4, WriteFailures: 0, MessagesDropped: 7},
		})

		assert.Equal(t, WorkerStats{
			MessagesProcessed: 15,
			ParseFailures:     5,
			WriteFailures:     2,
			MessagesDropped:   10,
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
			bumpedWorker(100, 2, 0, 4),
			bumpedWorker(50, 0, 1, 0),
			bumpedWorker(0, 0, 0, 6),
		}}

		assert.Equal(t, WorkerStats{
			MessagesProcessed: 150,
			ParseFailures:     2,
			WriteFailures:     1,
			MessagesDropped:   10,
		}, c.Stats())
	})
}
