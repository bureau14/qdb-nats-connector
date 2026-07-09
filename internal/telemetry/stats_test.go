// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureHandler: slog.Handler that records every log call for
// assertions (same pattern as connector/health_test.go).
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, r.Clone())

	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

// snapshotRecords returns a copy of the captured records.
func (h *captureHandler) snapshotRecords() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]slog.Record(nil), h.records...)
}

// recordAttr returns a record attribute's value, nil if absent.
func recordAttr(r slog.Record, key string) any {
	var found any
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			found = a.Value.Resolve().Any()

			return false
		}

		return true
	})

	return found
}

// tickedLogger builds a StatsLogger whose window clock is controllable
// and whose output lands in the returned capture handler.
func tickedLogger(interval time.Duration) (*StatsLogger, *captureHandler) {
	capture := &captureHandler{}

	return NewStatsLogger(nil, interval, slog.New(capture)), capture
}

func TestStatsLogTickRates(t *testing.T) {
	sl, capture := tickedLogger(time.Minute)
	src := greenStatsSource()

	base := time.Unix(1_700_000_000, 0)
	prev := statsSnapshot{at: base}
	cur := statsSnapshot{
		at: base.Add(10 * time.Second),
		worker: connector.WorkerStats{
			MessagesProcessed: 150, RowsWritten: 950,
			ParseFailures: 20, MessagesDropped: 30, WriteFailures: 5,
		},
		fetch: connector.FetchStats{MessagesFetched: 155},
	}

	sl.logTick(src, prev, cur)

	records := capture.snapshotRecords()
	require.Len(t, records, 1)
	r := records[0]
	assert.Equal(t, "Connector stats", r.Message)
	assert.InDelta(t, 10.0, recordAttr(r, "interval_s"), 1e-9)
	assert.InDelta(t, 15.0, recordAttr(r, "msgs_per_s"), 1e-9)
	assert.InDelta(t, 95.0, recordAttr(r, "rows_per_s"), 1e-9)
	assert.InDelta(t, 2.0, recordAttr(r, "parse_fail_per_s"), 1e-9)
	assert.InDelta(t, 3.0, recordAttr(r, "drops_per_s"), 1e-9)
	assert.InDelta(t, 0.5, recordAttr(r, "write_fail_per_s"), 1e-9)
	assert.InDelta(t, 15.5, recordAttr(r, "fetched_per_s"), 1e-9)
}

func TestStatsLogTickGauges(t *testing.T) {
	sl, capture := tickedLogger(time.Minute)
	src := greenStatsSource()

	base := time.Unix(1_700_000_000, 0)
	sl.logTick(src, statsSnapshot{at: base}, statsSnapshot{at: base.Add(time.Minute)})

	records := capture.snapshotRecords()
	require.Len(t, records, 1)
	r := records[0]
	assert.Equal(t, uint64(42), recordAttr(r, "pending"))
	assert.Equal(t, int64(3), recordAttr(r, "work_channel_depth"))
	assert.Equal(t, map[string]int{"closed": 2}, recordAttr(r, "breakers"))
	assert.Equal(t, int64(0), recordAttr(r, "tables_distinct"))
	assert.Equal(t, uint64(0), recordAttr(r, "table_events_dropped"))
}

func TestStatsLogTickPendingError(t *testing.T) {
	sl, capture := tickedLogger(time.Minute)
	src := greenStatsSource()
	src.pendingErr = context.DeadlineExceeded

	base := time.Unix(1_700_000_000, 0)
	sl.logTick(src, statsSnapshot{at: base}, statsSnapshot{at: base.Add(time.Minute)})

	r := capture.snapshotRecords()[0]
	assert.Nil(t, recordAttr(r, "pending"))
	assert.NotEmpty(t, recordAttr(r, "pending_err"))
}

func TestStatsLagAttrsOmittedWithoutMetrics(t *testing.T) {
	sl, capture := tickedLogger(time.Minute)

	base := time.Unix(1_700_000_000, 0)
	sl.logTick(greenStatsSource(), statsSnapshot{at: base}, statsSnapshot{at: base.Add(time.Minute)})

	r := capture.snapshotRecords()[0]
	assert.Nil(t, recordAttr(r, "lag_samples"))
	assert.Nil(t, recordAttr(r, "lag_p50_s"))
}

func TestStatsLagAttrsIdleInterval(t *testing.T) {
	sl, capture := tickedLogger(time.Minute)

	base := time.Unix(1_700_000_000, 0)
	snap := statsSnapshot{at: base, lagOK: true}
	idle := statsSnapshot{at: base.Add(time.Minute), lagOK: true}
	sl.logTick(greenStatsSource(), snap, idle)

	r := capture.snapshotRecords()[0]
	assert.Equal(t, uint64(0), recordAttr(r, "lag_samples"))
	assert.Nil(t, recordAttr(r, "lag_p50_s"))
}

func TestStatsRecordDropsWhenFull(t *testing.T) {
	sl, _ := tickedLogger(time.Minute)

	events := []connector.TableWrite{{Table: "a", Rows: 1, Msgs: 1}}
	// Fill the buffered channel; no consumer is running.
	for range eventChanCap {
		sl.Record(events)
	}
	assert.Equal(t, uint64(0), sl.dropped.Load())

	completed := make(chan struct{})
	go func() {
		sl.Record(events)
		sl.Record([]connector.TableWrite{{Table: "b", Rows: 1, Msgs: 1}, {Table: "c", Rows: 1, Msgs: 1}})
		close(completed)
	}()

	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("Record blocked on a full channel")
	}
	assert.Equal(t, uint64(3), sl.dropped.Load())
}

func TestStatsLoggerLifecycle(t *testing.T) {
	sl, capture := tickedLogger(10 * time.Millisecond)
	sl.Record([]connector.TableWrite{{Table: "hot", Rows: 90, Msgs: 90}})
	sl.Record([]connector.TableWrite{{Table: "cold", Rows: 10, Msgs: 10}})

	sl.Start(greenStatsSource())

	// Wait for a tick that has already absorbed both events: the run
	// loop's select may fire a tick before draining the channel.
	require.Eventually(t, func() bool {
		records := capture.snapshotRecords()
		if len(records) == 0 {
			return false
		}

		return recordAttr(records[len(records)-1], "tables_distinct") == int64(2)
	}, 5*time.Second, 5*time.Millisecond)

	sl.Stop()
	after := len(capture.snapshotRecords())
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, after, len(capture.snapshotRecords()), "no records after Stop")

	// The recorded events flowed into the window and the tick ranking.
	var last slog.Record
	records := capture.snapshotRecords()
	last = records[len(records)-1]
	top, ok := recordAttr(last, "top_tables").([]tableShare)
	require.True(t, ok)
	require.Len(t, top, 2)
	assert.Equal(t, "hot", top[0].Table)
	assert.InDelta(t, 90.0, top[0].SharePct, 1e-9)
	assert.Equal(t, int64(2), recordAttr(last, "tables_distinct"))
}
