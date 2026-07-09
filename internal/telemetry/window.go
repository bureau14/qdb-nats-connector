// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Per-table rolling window: five 1-minute buckets of per-table
// msg/row counts, rotated lazily on insert and merged at read time.
// Memory is bounded regardless of table cardinality: each bucket caps
// its distinct keys and overflows into a single "other" entry.
// Single-goroutine by design (owned by the stats logger's run loop) --
// no locks anywhere.
// Types: tableCount, windowBucket, tableWindow, tableShare, windowReport
// Ex: w := newTableWindow(time.Now); w.add("skf/b831", 12, 12)
package telemetry

import (
	"sort"
	"time"
)

const (
	// windowBucketCount x windowBucketWidth = the 5-minute window; fixed
	// regardless of --stats-interval.
	windowBucketCount = 5
	windowBucketWidth = time.Minute
	// maxDistinctPerBucket bounds per-bucket memory; past it, new tables
	// accumulate under overflowKey.
	maxDistinctPerBucket = 10_000
	// overflowKey is a regular entry and may reach the top-10 -- its
	// appearance there IS the cap-overflow signal.
	overflowKey = "other"
)

// tableCount: per-table tallies within one bucket.
type tableCount struct {
	Msgs uint64
	Rows uint64
}

// windowBucket: one minute of per-table counts. minute stamps which
// wall-clock minute the data belongs to; a stale stamp means the slot
// is reused after a reset.
type windowBucket struct {
	minute int64
	counts map[string]tableCount
}

// tableWindow: ring of windowBucketCount 1-minute buckets keyed by
// unix minute modulo the ring size. The clock is injectable for tests.
type tableWindow struct {
	now     func() time.Time
	buckets [windowBucketCount]windowBucket
}

// newTableWindow builds an empty window over the given clock.
// In: now func() time.Time - clock; time.Now in production
// Out: *tableWindow - ready for add/report
// Ex: w := newTableWindow(time.Now)
func newTableWindow(now func() time.Time) *tableWindow {
	return &tableWindow{now: now}
}

// add records rows/msgs for one table in the current minute's bucket,
// resetting the slot first when it still holds an older minute.
// In: table string - table name, rows, msgs uint64 - tallies to add
// Out: none
// Ex: w.add("skf/b831", 12, 12)
func (w *tableWindow) add(table string, rows, msgs uint64) {
	minute := w.now().Unix() / 60
	bucket := &w.buckets[minute%windowBucketCount]
	if bucket.minute != minute || bucket.counts == nil {
		bucket.minute = minute
		bucket.counts = make(map[string]tableCount)
	}

	key := table
	_, seen := bucket.counts[key]
	if !seen && len(bucket.counts) >= maxDistinctPerBucket {
		key = overflowKey
	}

	c := bucket.counts[key]
	c.Rows += rows
	c.Msgs += msgs
	bucket.counts[key] = c
}

// tableShare: one table's window totals with its share of all rows.
type tableShare struct {
	Table    string  `json:"table"`
	Rows     uint64  `json:"rows"`
	Msgs     uint64  `json:"msgs"`
	SharePct float64 `json:"share_pct"`
}

// windowReport: merged view over the live window. Distinct excludes
// the overflow entry, so it is a lower bound once a bucket overflows.
type windowReport struct {
	Top       []tableShare
	Distinct  int
	TotalRows uint64
}

// report merges the live buckets and returns the topN tables by rows
// (desc, name asc on ties) with share-% of the window's total rows.
// In: topN int - maximum entries to return
// Out: windowReport - merged window view
// Ex: w.report(10) -> {Top: [...], Distinct: 1523, TotalRows: 88210}
func (w *tableWindow) report(topN int) windowReport {
	oldest := w.now().Unix()/60 - windowBucketCount + 1

	merged := make(map[string]tableCount)
	var total uint64
	for i := range w.buckets {
		bucket := &w.buckets[i]
		if bucket.minute < oldest || bucket.counts == nil {
			continue
		}
		for table, c := range bucket.counts {
			m := merged[table]
			m.Rows += c.Rows
			m.Msgs += c.Msgs
			merged[table] = m
			total += c.Rows
		}
	}

	shares := make([]tableShare, 0, len(merged))
	distinct := 0
	for table, c := range merged {
		if table != overflowKey {
			distinct++
		}
		share := tableShare{Table: table, Rows: c.Rows, Msgs: c.Msgs}
		if total > 0 {
			share.SharePct = float64(c.Rows) / float64(total) * 100
		}
		shares = append(shares, share)
	}
	sort.Slice(shares, func(i, j int) bool {
		if shares[i].Rows != shares[j].Rows {
			return shares[i].Rows > shares[j].Rows
		}

		return shares[i].Table < shares[j].Table
	})
	if len(shares) > topN {
		shares = shares[:topN]
	}

	return windowReport{Top: shares, Distinct: distinct, TotalRows: total}
}
