// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for the Prometheus collector: lint-clean exposition,
// exact counter/gauge values, breaker-state encoding, pending error
// resilience, ready-gauge parity with /readyz, and the sum-over-workers
// invariant against Connector.Stats semantics.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/connector/resilience"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// fakeStatsSource: canned StatsSource for collector tests.
type fakeStatsSource struct {
	fakeSource

	workers       []connector.WorkerStatsSnapshot
	fetch         connector.FetchStats
	channelDepth  int
	breakers      map[string]resilience.State
	pending       uint64
	pendingErr    error
	pendingBlocks bool // block until ctx cancels before returning
}

func (f fakeStatsSource) PerWorkerStats() []connector.WorkerStatsSnapshot { return f.workers }
func (f fakeStatsSource) FetchStats() connector.FetchStats                { return f.fetch }
func (f fakeStatsSource) WorkChannelDepth() int                           { return f.channelDepth }
func (f fakeStatsSource) BreakerStates() map[string]resilience.State      { return f.breakers }

func (f fakeStatsSource) ConsumerPending(ctx context.Context) (uint64, error) {
	if f.pendingBlocks {
		<-ctx.Done()

		return f.pending, ctx.Err()
	}

	return f.pending, f.pendingErr
}

// greenStatsSource returns a healthy fake with distinctive counters.
func greenStatsSource() fakeStatsSource {
	return fakeStatsSource{
		fakeSource: greenSource(),
		workers: []connector.WorkerStatsSnapshot{
			{Worker: "worker-0", Stats: connector.WorkerStats{
				MessagesProcessed: 100, ParseFailures: 2, WriteFailures: 1,
				MessagesDropped: 3, RowsWritten: 95, Acks: 97, Nacks: 3,
			}},
			{Worker: "worker-1", Stats: connector.WorkerStats{
				MessagesProcessed: 50, Acks: 50,
			}},
		},
		fetch: connector.FetchStats{
			MessagesFetched: 155, TransientErrors: 4,
			SubscriptionErrors: 1, RebindAttempts: 2,
		},
		channelDepth: 3,
		breakers: map[string]resilience.State{
			"worker-0": resilience.StateClosed,
			"worker-1": resilience.StateClosed,
		},
		pending: 42,
	}
}

// gatherValue finds one sample value by family name and label pairs.
func gatherValue(t *testing.T, reg *prometheus.Registry, family string, labels map[string]string) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range families {
		if mf.GetName() != family {
			continue
		}
	metric:
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue metric
				}
			}
			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("no sample for %s %v", family, labels)

	return 0
}

// registered wraps src in a Collector on a fresh pedantic registry.
func registered(t *testing.T, src StatsSource) *prometheus.Registry {
	t.Helper()

	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(NewCollector(src, testLogger())))

	return reg
}

func testLogger() *slog.Logger { return slog.Default() }

func TestCollectorLintClean(t *testing.T) {
	problems, err := testutil.GatherAndLint(registered(t, greenStatsSource()))
	require.NoError(t, err)
	assert.Empty(t, problems)
}

func TestCollectorFetchCounters(t *testing.T) {
	reg := registered(t, greenStatsSource())

	assert.Equal(t, float64(155), gatherValue(t, reg, "qdb_nats_connector_messages_fetched_total", nil))
	assert.Equal(t, float64(4), gatherValue(t, reg, "qdb_nats_connector_fetch_errors_total", map[string]string{"kind": "transient"}))
	assert.Equal(t, float64(1), gatherValue(t, reg, "qdb_nats_connector_fetch_errors_total", map[string]string{"kind": "subscription"}))
	assert.Equal(t, float64(2), gatherValue(t, reg, "qdb_nats_connector_rebind_attempts_total", nil))
}

func TestCollectorWorkerCounters(t *testing.T) {
	reg := registered(t, greenStatsSource())
	w0 := map[string]string{"worker": "worker-0"}

	assert.Equal(t, float64(100), gatherValue(t, reg, "qdb_nats_connector_messages_processed_total", w0))
	assert.Equal(t, float64(2), gatherValue(t, reg, "qdb_nats_connector_parse_failures_total", w0))
	assert.Equal(t, float64(3), gatherValue(t, reg, "qdb_nats_connector_messages_dropped_total", w0))
	assert.Equal(t, float64(1), gatherValue(t, reg, "qdb_nats_connector_write_failures_total", w0))
	assert.Equal(t, float64(95), gatherValue(t, reg, "qdb_nats_connector_rows_written_total", w0))
	assert.Equal(t, float64(97), gatherValue(t, reg, "qdb_nats_connector_acks_total", w0))
	assert.Equal(t, float64(3), gatherValue(t, reg, "qdb_nats_connector_nacks_total", w0))
}

func TestCollectorGauges(t *testing.T) {
	src := greenStatsSource()
	lastFetch := time.Now().Add(-time.Minute)
	src.health.LastFetchSuccess = lastFetch
	reg := registered(t, src)

	assert.Equal(t, float64(1), gatherValue(t, reg, "qdb_nats_connector_healthy", nil))
	assert.Equal(t, float64(1), gatherValue(t, reg, "qdb_nats_connector_ready", nil))
	assert.Equal(t, float64(3), gatherValue(t, reg, "qdb_nats_connector_work_channel_depth", nil))
	assert.Equal(t, float64(42), gatherValue(t, reg, "qdb_nats_connector_consumer_pending_messages", nil))
	assert.InDelta(t, float64(lastFetch.UnixNano())/1e9,
		gatherValue(t, reg, "qdb_nats_connector_last_fetch_success_timestamp_seconds", nil), 0.001)
}

func TestCollectorZeroLastFetchIsZero(t *testing.T) {
	reg := registered(t, greenStatsSource())

	assert.Equal(t, float64(0),
		gatherValue(t, reg, "qdb_nats_connector_last_fetch_success_timestamp_seconds", nil))
}

func TestCollectorBreakerStateEncoding(t *testing.T) {
	src := greenStatsSource()
	src.breakers = map[string]resilience.State{
		"worker-0": resilience.StateClosed,
		"worker-1": resilience.StateHalfOpen,
		"worker-2": resilience.StateOpen,
	}
	reg := registered(t, src)

	assert.Equal(t, float64(0), gatherValue(t, reg, "qdb_nats_connector_breaker_state", map[string]string{"worker": "worker-0"}))
	assert.Equal(t, float64(1), gatherValue(t, reg, "qdb_nats_connector_breaker_state", map[string]string{"worker": "worker-1"}))
	assert.Equal(t, float64(2), gatherValue(t, reg, "qdb_nats_connector_breaker_state", map[string]string{"worker": "worker-2"}))
}

func TestCollectorPendingErrorEmitsLastKnown(t *testing.T) {
	src := greenStatsSource()
	src.pending = 17
	src.pendingErr = context.DeadlineExceeded
	reg := registered(t, src)

	assert.Equal(t, float64(17),
		gatherValue(t, reg, "qdb_nats_connector_consumer_pending_messages", nil))
}

// TestCollectorPendingBlockedSourceIsBounded proves a scrape cannot hang
// on a NATS outage: a ConsumerPending that only returns on ctx
// cancellation still lets Gather finish within the scrape timeout.
func TestCollectorPendingBlockedSourceIsBounded(t *testing.T) {
	src := greenStatsSource()
	src.pendingBlocks = true
	reg := registered(t, src)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = reg.Gather()
	}()

	select {
	case <-done:
	case <-time.After(pendingScrapeTimeout + 3*time.Second):
		t.Fatal("Gather did not finish; scrape hangs on a blocked pending source")
	}
}

// TestCollectorReadyParityWithReadyz pins that the ready gauge and the
// /readyz verdict flip together for every readiness input.
func TestCollectorReadyParityWithReadyz(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*fakeStatsSource)
	}{
		{"green", func(_ *fakeStatsSource) {}},
		{"nats down", func(s *fakeStatsSource) { s.natsUp = false }},
		{"fetch unhealthy", func(s *fakeStatsSource) { s.health.FetchHealthy = false }},
		{"breaker open", func(s *fakeStatsSource) { s.health.BreakerOpen = true }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := greenStatsSource()
			tc.mutate(&src)

			code, _ := probeCode(t, handleReadyz(src.fakeSource))
			gauge := gatherValue(t, registered(t, src), "qdb_nats_connector_ready", nil)

			if code == http.StatusOK {
				assert.Equal(t, float64(1), gauge)
			} else {
				assert.Equal(t, float64(0), gauge)
			}
		})
	}
}

// Property: per-family sums over the worker label equal the field-wise
// sums of the snapshots -- the exact quantity Connector.Stats computes.
func TestCollectorWorkerSumsMatchStats(t *testing.T) {
	families := map[string]func(connector.WorkerStats) uint64{
		"qdb_nats_connector_messages_processed_total": func(s connector.WorkerStats) uint64 { return s.MessagesProcessed },
		"qdb_nats_connector_parse_failures_total":     func(s connector.WorkerStats) uint64 { return s.ParseFailures },
		"qdb_nats_connector_messages_dropped_total":   func(s connector.WorkerStats) uint64 { return s.MessagesDropped },
		"qdb_nats_connector_write_failures_total":     func(s connector.WorkerStats) uint64 { return s.WriteFailures },
		"qdb_nats_connector_rows_written_total":       func(s connector.WorkerStats) uint64 { return s.RowsWritten },
		"qdb_nats_connector_acks_total":               func(s connector.WorkerStats) uint64 { return s.Acks },
		"qdb_nats_connector_nacks_total":              func(s connector.WorkerStats) uint64 { return s.Nacks },
	}

	rapid.Check(t, func(rt *rapid.T) {
		count := rapid.IntRange(0, 8).Draw(rt, "workers")
		gen := rapid.Uint64Range(0, 1<<32)

		src := greenStatsSource()
		src.workers = nil
		for i := range count {
			src.workers = append(src.workers, connector.WorkerStatsSnapshot{
				Worker: fmt.Sprintf("worker-%d", i),
				Stats: connector.WorkerStats{
					MessagesProcessed: gen.Draw(rt, fmt.Sprintf("w%d-processed", i)),
					ParseFailures:     gen.Draw(rt, fmt.Sprintf("w%d-parse", i)),
					WriteFailures:     gen.Draw(rt, fmt.Sprintf("w%d-write", i)),
					MessagesDropped:   gen.Draw(rt, fmt.Sprintf("w%d-dropped", i)),
					RowsWritten:       gen.Draw(rt, fmt.Sprintf("w%d-rows", i)),
					Acks:              gen.Draw(rt, fmt.Sprintf("w%d-acks", i)),
					Nacks:             gen.Draw(rt, fmt.Sprintf("w%d-nacks", i)),
				},
			})
		}

		reg := prometheus.NewPedanticRegistry()
		err := reg.Register(NewCollector(src, testLogger()))
		if err != nil {
			rt.Fatalf("register failed: %v", err)
		}
		mfs, err := reg.Gather()
		if err != nil {
			rt.Fatalf("gather failed: %v", err)
		}

		for family, field := range families {
			var expected uint64
			for _, snap := range src.workers {
				expected += field(snap.Stats)
			}

			var got float64
			for _, mf := range mfs {
				if mf.GetName() != family {
					continue
				}
				for _, m := range mf.GetMetric() {
					got += m.GetCounter().GetValue()
				}
			}

			if got != float64(expected) {
				rt.Fatalf("%s: sum over workers = %v, want %d", family, got, expected)
			}
		}
	})
}

// TestCollectorExpositionContainsAllFamilies smoke-checks the wire
// format via the text exposition.
func TestCollectorExpositionContainsAllFamilies(t *testing.T) {
	reg := registered(t, greenStatsSource())

	exposition, err := testutil.GatherAndCount(reg)
	require.NoError(t, err)
	assert.Positive(t, exposition)

	families, err := reg.Gather()
	require.NoError(t, err)

	var names []string
	for _, mf := range families {
		names = append(names, mf.GetName())
	}
	joined := strings.Join(names, "\n")

	for _, want := range []string{
		"messages_fetched_total", "fetch_errors_total", "rebind_attempts_total",
		"messages_processed_total", "parse_failures_total", "messages_dropped_total",
		"write_failures_total", "rows_written_total", "acks_total", "nacks_total",
		"consumer_pending_messages", "breaker_state", "work_channel_depth",
		"last_fetch_success_timestamp_seconds", "ready", "healthy",
	} {
		assert.Contains(t, joined, "qdb_nats_connector_"+want)
	}
}
