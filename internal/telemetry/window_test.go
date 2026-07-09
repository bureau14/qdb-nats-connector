// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
package telemetry

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// fakeClock: settable clock for deterministic window tests.
type fakeClock struct {
	at time.Time
}

func (c *fakeClock) now() time.Time {
	return c.at
}

func (c *fakeClock) advance(d time.Duration) {
	c.at = c.at.Add(d)
}

func newFakeClock() *fakeClock {
	return &fakeClock{at: time.Unix(1_700_000_040, 0)}
}

func TestWindowMergesAcrossLiveBuckets(t *testing.T) {
	clock := newFakeClock()
	w := newTableWindow(clock.now)

	w.add("a", 10, 10)
	clock.advance(time.Minute)
	w.add("a", 5, 5)
	w.add("b", 1, 1)

	r := w.report(10)
	assert.Equal(t, uint64(16), r.TotalRows)
	assert.Equal(t, 2, r.Distinct)
	require.Len(t, r.Top, 2)
	assert.Equal(t, tableShare{Table: "a", Rows: 15, Msgs: 15, SharePct: 93.75}, r.Top[0])
	assert.Equal(t, "b", r.Top[1].Table)
}

func TestWindowExpiresAfterFiveMinutes(t *testing.T) {
	clock := newFakeClock()
	w := newTableWindow(clock.now)

	w.add("a", 10, 10)

	// 4 minutes later the entry is still inside the 5-bucket window.
	clock.advance(4 * time.Minute)
	assert.Equal(t, uint64(10), w.report(10).TotalRows)

	// 5 minutes after insert the bucket has aged out.
	clock.advance(time.Minute)
	assert.Equal(t, uint64(0), w.report(10).TotalRows)
	assert.Empty(t, w.report(10).Top)
}

func TestWindowSlotReuseNeverDoubleCounts(t *testing.T) {
	clock := newFakeClock()
	w := newTableWindow(clock.now)

	w.add("a", 10, 10)

	// Same ring slot (minute M and M+5 share minute%5) must reset.
	clock.advance(5 * time.Minute)
	w.add("a", 1, 1)

	r := w.report(10)
	assert.Equal(t, uint64(1), r.TotalRows)
}

func TestWindowCapOverflowsToOther(t *testing.T) {
	clock := newFakeClock()
	w := newTableWindow(clock.now)

	for i := range maxDistinctPerBucket {
		w.add(fmt.Sprintf("t%05d", i), 1, 1)
	}
	// Bucket is full: new names spill into "other"...
	w.add("straggler-1", 3, 3)
	w.add("straggler-2", 4, 4)
	// ...but existing names keep accumulating normally.
	w.add("t00000", 1, 1)

	r := w.report(maxDistinctPerBucket + 5)
	assert.Equal(t, maxDistinctPerBucket, r.Distinct)
	assert.Equal(t, uint64(maxDistinctPerBucket+8), r.TotalRows)
	require.Equal(t, overflowKey, r.Top[0].Table)
	assert.Equal(t, uint64(7), r.Top[0].Rows)
	assert.Equal(t, uint64(2), r.Top[1].Rows) // t00000
}

func TestWindowTopNOrderingAndShares(t *testing.T) {
	clock := newFakeClock()
	w := newTableWindow(clock.now)

	w.add("big", 80, 80)
	w.add("mid", 15, 15)
	w.add("tie-b", 5, 5)
	w.add("tie-a", 5, 5)

	r := w.report(3)
	require.Len(t, r.Top, 3)
	assert.Equal(t, "big", r.Top[0].Table)
	assert.InDelta(t, 76.19, r.Top[0].SharePct, 0.01)
	assert.Equal(t, "mid", r.Top[1].Table)
	// Ties break by name ascending.
	assert.Equal(t, "tie-a", r.Top[2].Table)
	assert.Equal(t, 4, r.Distinct)
	assert.Equal(t, uint64(105), r.TotalRows)
}

func TestWindowTopNLargerThanDistinct(t *testing.T) {
	clock := newFakeClock()
	w := newTableWindow(clock.now)

	w.add("only", 1, 1)

	r := w.report(10)
	require.Len(t, r.Top, 1)
	assert.Equal(t, 100.0, r.Top[0].SharePct)
}

func TestWindowTotalRowsMatchesLiveInserts(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		clock := newFakeClock()
		w := newTableWindow(clock.now)

		// Inserts within the last 5 minutes count; older ones age out.
		var expected uint64
		steps := rapid.IntRange(1, 40).Draw(rt, "steps")
		type insert struct {
			minute int64
			rows   uint64
		}
		inserts := make([]insert, 0, steps)
		for range steps {
			rows := rapid.Uint64Range(0, 1000).Draw(rt, "rows")
			table := rapid.SampledFrom([]string{"a", "b", "c"}).Draw(rt, "table")
			w.add(table, rows, rows)
			inserts = append(inserts, insert{clock.at.Unix() / 60, rows})

			gap := rapid.Int64Range(0, 3).Draw(rt, "gap")
			clock.advance(time.Duration(gap) * time.Minute)
		}

		oldest := clock.at.Unix()/60 - windowBucketCount + 1
		for _, in := range inserts {
			if in.minute >= oldest {
				expected += in.rows
			}
		}

		require.Equal(rt, expected, w.report(10).TotalRows)
	})
}
