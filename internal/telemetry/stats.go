// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Console stats logger: one structured log line per interval with
// delta-based rates, an interval lag summary, backlog/breaker/channel
// gauges, and the rolling top-10 tables by insert volume. Reads the
// SAME atomic counters and histograms as /metrics (single substrate).
// Owned by main like the probe server; the connector stays
// telemetry-free and feeds per-table events through a non-blocking
// callback that never stalls the worker hot path.
// Types: StatsLogger
// Ex: sl := NewStatsLogger(m, time.Minute, slog.Default());
// opts.OnTableWrites = sl.Record; sl.Start(conn); defer sl.Stop()
package telemetry

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/connector/resilience"
	"github.com/bureau14/qdb-nats-connector/internal/metrics"
)

const (
	// eventChanCap buffers per-batch event slices between workers and
	// the stats goroutine; a full channel drops (counted), never blocks.
	eventChanCap = 256
	// topTablesN entries appear in the per-tick table ranking.
	topTablesN = 10
)

// statsSnapshot: one instant of every delta-tracked input.
type statsSnapshot struct {
	at     time.Time
	worker connector.WorkerStats
	fetch  connector.FetchStats
	lag    metrics.HistogramSnapshot
	lagOK  bool
}

// StatsLogger: periodic console stats over a StatsSource. Two-phase:
// construction wires the Record callback (needed before the connector
// exists), Start launches the tick goroutine once it does. The table
// window is owned exclusively by the run goroutine -- no locks.
type StatsLogger struct {
	metrics  *metrics.Metrics // nil = no lag summary
	interval time.Duration
	logger   *slog.Logger
	window   *tableWindow
	events   chan []connector.TableWrite
	dropped  atomic.Uint64
	stop     chan struct{}
	done     chan struct{}
}

// NewStatsLogger builds a stats logger; Start it after the connector
// exists.
// In: m *metrics.Metrics - lag histogram source (nil skips the lag
// summary), interval time.Duration - tick period, logger *slog.Logger
// - tick line destination
// Out: *StatsLogger - ready for Record wiring and Start
// Ex: sl := NewStatsLogger(m, time.Minute, slog.Default())
func NewStatsLogger(m *metrics.Metrics, interval time.Duration, logger *slog.Logger) *StatsLogger {
	return &StatsLogger{
		metrics:  m,
		interval: interval,
		logger:   logger,
		window:   newTableWindow(time.Now),
		events:   make(chan []connector.TableWrite, eventChanCap),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Record forwards per-table write events to the stats goroutine.
// Non-blocking by contract (worker hot path): a full channel drops the
// slice and counts it. Safe to call before Start.
// In: events []connector.TableWrite - one batch's per-table tallies
// Out: none
// Ex: opts.OnTableWrites = sl.Record
func (s *StatsLogger) Record(events []connector.TableWrite) {
	if len(events) == 0 {
		return
	}

	select {
	case s.events <- events:
	default:
		s.dropped.Add(uint64(len(events)))
	}
}

// Start launches the tick goroutine over src.
// In: src StatsSource - counter/gauge snapshots (*connector.Connector)
// Out: none
// Ex: sl.Start(conn)
func (s *StatsLogger) Start(src StatsSource) {
	go s.run(src)
}

// Stop terminates the tick goroutine and waits for it to exit. Only
// valid after Start.
// In: none
// Out: none
// Ex: defer sl.Stop()
func (s *StatsLogger) Stop() {
	close(s.stop)
	<-s.done
}

// run drains table events and logs one stats line per tick. The first
// line appears one interval after start: the baseline snapshot has no
// predecessor to delta against.
func (s *StatsLogger) run(src StatsSource) {
	defer close(s.done)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	prev := s.snapshot(src, time.Now())
	for {
		select {
		case <-s.stop:
			return
		case events := <-s.events:
			for _, e := range events {
				s.window.add(e.Table, uint64(max(e.Rows, 0)), uint64(max(e.Msgs, 0))) //nolint:gosec // negatives clamped
			}
		case <-ticker.C:
			cur := s.snapshot(src, time.Now())
			s.logTick(src, prev, cur)
			prev = cur
		}
	}
}

// snapshot captures every delta-tracked input at one instant.
func (s *StatsLogger) snapshot(src StatsSource, now time.Time) statsSnapshot {
	snap := statsSnapshot{
		at:    now,
		fetch: src.FetchStats(),
	}
	for _, w := range src.PerWorkerStats() {
		snap.worker.MessagesProcessed += w.Stats.MessagesProcessed
		snap.worker.ParseFailures += w.Stats.ParseFailures
		snap.worker.WriteFailures += w.Stats.WriteFailures
		snap.worker.MessagesDropped += w.Stats.MessagesDropped
		snap.worker.ParsesZeroRows += w.Stats.ParsesZeroRows
		snap.worker.RowsWritten += w.Stats.RowsWritten
		snap.worker.Acks += w.Stats.Acks
		snap.worker.Nacks += w.Stats.Nacks
	}
	snap.lag, snap.lagOK = s.metrics.LagSnapshot()

	return snap
}

// logTick emits the stats line for one interval.
func (s *StatsLogger) logTick(src StatsSource, prev, cur statsSnapshot) {
	attrs := []slog.Attr{slog.Float64("interval_s", cur.at.Sub(prev.at).Seconds())}
	attrs = append(attrs, rateAttrs(prev, cur)...)
	attrs = append(attrs, s.lagAttrs(prev, cur)...)
	attrs = append(attrs, gaugeAttrs(src)...)
	attrs = append(attrs, s.tableAttrs()...)

	s.logger.LogAttrs(context.Background(), slog.LevelInfo, "Connector stats", attrs...)
}

// rateAttrs converts counter deltas into per-second rates over the
// measured (not nominal) elapsed time.
func rateAttrs(prev, cur statsSnapshot) []slog.Attr {
	elapsed := cur.at.Sub(prev.at).Seconds()
	if elapsed <= 0 {
		return nil
	}

	rate := func(cur, prev uint64) float64 {
		if cur < prev {
			return 0
		}

		return float64(cur-prev) / elapsed
	}

	return []slog.Attr{
		slog.Float64("msgs_per_s", rate(cur.worker.MessagesProcessed, prev.worker.MessagesProcessed)),
		slog.Float64("rows_per_s", rate(cur.worker.RowsWritten, prev.worker.RowsWritten)),
		slog.Float64("parse_fail_per_s", rate(cur.worker.ParseFailures, prev.worker.ParseFailures)),
		slog.Float64("drops_per_s", rate(cur.worker.MessagesDropped, prev.worker.MessagesDropped)),
		slog.Float64("write_fail_per_s", rate(cur.worker.WriteFailures, prev.worker.WriteFailures)),
		slog.Float64("fetched_per_s", rate(cur.fetch.MessagesFetched, prev.fetch.MessagesFetched)),
	}
}

// lagAttrs summarizes the interval's lag distribution. No metrics =
// no attrs; metrics but an idle interval = lag_samples 0 only.
func (s *StatsLogger) lagAttrs(prev, cur statsSnapshot) []slog.Attr {
	if !prev.lagOK || !cur.lagOK {
		return nil
	}

	summary, ok := lagInterval(prev.lag, cur.lag)
	if !ok {
		return []slog.Attr{slog.Uint64("lag_samples", 0)}
	}

	return []slog.Attr{
		slog.Uint64("lag_samples", summary.Samples),
		slog.Float64("lag_p50_s", summary.P50),
		slog.Float64("lag_p99_s", summary.P99),
		slog.Float64("lag_max_le_s", summary.MaxLE),
	}
}

// gaugeAttrs reads the point-in-time gauges: pending backlog, breaker
// state counts, work channel depth.
func gaugeAttrs(src StatsSource) []slog.Attr {
	attrs := []slog.Attr{
		slog.Int("work_channel_depth", src.WorkChannelDepth()),
		slog.Any("breakers", breakerCounts(src.BreakerStates())),
	}

	ctx, cancel := context.WithTimeout(context.Background(), pendingScrapeTimeout)
	defer cancel()

	pending, err := src.ConsumerPending(ctx)
	if err != nil {
		return append(attrs, slog.String("pending_err", err.Error()))
	}

	return append(attrs, slog.Uint64("pending", pending))
}

// breakerCounts folds per-worker breaker states into state -> count.
func breakerCounts(states map[string]resilience.State) map[string]int {
	counts := make(map[string]int, 3)
	for _, state := range states {
		counts[state.String()]++
	}

	return counts
}

// tableAttrs reports the rolling top-N tables by rows with share-%.
func (s *StatsLogger) tableAttrs() []slog.Attr {
	report := s.window.report(topTablesN)

	return []slog.Attr{
		slog.Any("top_tables", report.Top),
		slog.Int("tables_distinct", report.Distinct),
		slog.Uint64("table_events_dropped", s.dropped.Load()),
	}
}
