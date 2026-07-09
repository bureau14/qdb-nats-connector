// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for connector health: idle stays green, transitions log
// exactly once per direction, rebinding supersedes fetch-stalled, and the
// health summary keeps breaker state separate from fetch liveness.
package connector

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector/resilience"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureHandler: slog.Handler that records every log call for assertions.
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

// byLevel returns captured records with the given level.
func (h *captureHandler) byLevel(level slog.Level) []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()

	var out []slog.Record
	for i := range h.records {
		if h.records[i].Level == level {
			out = append(out, h.records[i])
		}
	}

	return out
}

// attrValue returns the string form of a record attribute, "" if absent.
func attrValue(r slog.Record, key string) string {
	value := ""
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			value = a.Value.String()

			return false
		}

		return true
	})

	return value
}

// newTestMonitor builds a monitor over the given probes with a capturing
// logger and the given stuck threshold.
func newTestMonitor(threshold time.Duration, probes ...probeRef) (*healthMonitor, *captureHandler) {
	capture := &captureHandler{}
	health := newHealthState(slog.New(capture))
	health.recordFetchSuccess()

	return &healthMonitor{
		health:         health,
		probes:         probes,
		stuckThreshold: threshold,
		interval:       monitorInterval,
	}, capture
}

func TestHealthIdleConnectorLogsNothing(t *testing.T) {
	var workerProbe, fetchProbe Probe
	monitor, capture := newTestMonitor(5*time.Minute,
		probeRef{owner: "fetch-loop", probe: &fetchProbe},
		probeRef{owner: "worker-0", probe: &workerProbe})

	// Many evaluation rounds with fresh fetch successes and cleared
	// probes: an idle-but-fetching connector is healthy indefinitely.
	for range 10 {
		monitor.health.recordFetchSuccess()
		monitor.evaluate(time.Now())
	}

	assert.Empty(t, capture.records)

	conditions, _ := monitor.health.snapshot()
	assert.Empty(t, conditions)
}

func TestHealthStuckProbeLogsOncePerTransition(t *testing.T) {
	var probe Probe
	monitor, capture := newTestMonitor(5*time.Minute,
		probeRef{owner: "worker-3", probe: &probe})

	// Blocked in "process" for 6 minutes (rewind since; fetch success
	// stays fresh so only the stuck condition can fire).
	probe.Begin("process")
	probe.mu.Lock()
	probe.since = time.Now().Add(-6 * time.Minute)
	probe.mu.Unlock()

	monitor.evaluate(time.Now())
	monitor.evaluate(time.Now()) // unchanged condition: no re-log

	errors := capture.byLevel(slog.LevelError)
	require.Len(t, errors, 1)
	assert.Equal(t, condStuckPrefix+"worker-3", attrValue(errors[0], "reason"))
	assert.Equal(t, "process", attrValue(errors[0], "stage"))
	assert.Equal(t, "worker-3", attrValue(errors[0], "owner"))
	assert.NotEmpty(t, attrValue(errors[0], "age"))

	// Completing the blocked operation recovers exactly once.
	probe.End()
	monitor.evaluate(time.Now())
	monitor.evaluate(time.Now())

	infos := capture.byLevel(slog.LevelInfo)
	require.Len(t, infos, 1)
	assert.Equal(t, condStuckPrefix+"worker-3", attrValue(infos[0], "reason"))
	assert.NotEmpty(t, attrValue(infos[0], "downtime"))
}

func TestHealthRetryProgressPreventsStageStuck(t *testing.T) {
	var probe Probe
	monitor, capture := newTestMonitor(5*time.Minute,
		probeRef{owner: "worker-0", probe: &probe})

	// A worker deep in a >threshold sink retry ladder: entered "process"
	// long ago, but the retry loop keeps reporting progress via Touch
	// (exactly what sink.Options.OnRetryProgress does). Never stuck.
	probe.Begin("process")
	for range 3 {
		probe.mu.Lock()
		probe.since = time.Now().Add(-6 * time.Minute)
		probe.mu.Unlock()

		probe.Touch()
		monitor.health.recordFetchSuccess()
		monitor.evaluate(time.Now())
	}

	assert.Empty(t, capture.byLevel(slog.LevelError))

	conditions, _ := monitor.health.snapshot()
	assert.Empty(t, conditions)
}

func TestHealthBlockedWriteWithoutProgressStillTrips(t *testing.T) {
	var probe Probe
	monitor, capture := newTestMonitor(5*time.Minute,
		probeRef{owner: "worker-0", probe: &probe})

	// Same shape, but a hard-wedged write never reaches a retry boundary:
	// no Touch, so the stuck condition must fire at the threshold.
	probe.Begin("process")
	probe.mu.Lock()
	probe.since = time.Now().Add(-6 * time.Minute)
	probe.mu.Unlock()

	monitor.health.recordFetchSuccess()
	monitor.evaluate(time.Now())

	errors := capture.byLevel(slog.LevelError)
	require.Len(t, errors, 1)
	assert.Equal(t, condStuckPrefix+"worker-0", attrValue(errors[0], "reason"))
}

func TestHealthIdleProbeNeverFlagged(t *testing.T) {
	var probe Probe
	monitor, capture := newTestMonitor(time.Nanosecond,
		probeRef{owner: "worker-0", probe: &probe})

	// Cleared probe (waiting for work) is healthy regardless of how long
	// ago it last ran -- even with an absurdly small threshold. Only the
	// stuck conditions matter here; fetch staleness is tested separately.
	monitor.health.recordFetchSuccess()
	monitor.evaluate(time.Now())

	for _, r := range capture.byLevel(slog.LevelError) {
		assert.NotContains(t, attrValue(r, "reason"), condStuckPrefix)
	}
}

func TestHealthFetchStalledTransitions(t *testing.T) {
	monitor, capture := newTestMonitor(5 * time.Minute)

	stale := time.Now().Add(6 * time.Minute)
	monitor.evaluate(stale)
	monitor.evaluate(stale) // unchanged: no re-log

	errors := capture.byLevel(slog.LevelError)
	require.Len(t, errors, 1)
	assert.Equal(t, condFetchStalled, attrValue(errors[0], "reason"))

	monitor.health.recordFetchSuccess()
	monitor.evaluate(time.Now())

	infos := capture.byLevel(slog.LevelInfo)
	require.Len(t, infos, 1)
	assert.Equal(t, condFetchStalled, attrValue(infos[0], "reason"))
}

func TestHealthRebindingSupersedesFetchStalled(t *testing.T) {
	monitor, capture := newTestMonitor(5 * time.Minute)

	monitor.health.set(condRebinding)
	monitor.evaluate(time.Now().Add(time.Hour))

	// One ERROR for the re-bind itself, none for fetch-stalled: one
	// incident, one condition.
	errors := capture.byLevel(slog.LevelError)
	require.Len(t, errors, 1)
	assert.Equal(t, condRebinding, attrValue(errors[0], "reason"))
}

func TestHealthSetClearIdempotent(t *testing.T) {
	capture := &captureHandler{}
	health := newHealthState(slog.New(capture))

	health.set(condRebinding)
	health.set(condRebinding)
	assert.Len(t, capture.byLevel(slog.LevelError), 1)

	health.clear(condRebinding)
	health.clear(condRebinding)
	assert.Len(t, capture.byLevel(slog.LevelInfo), 1)

	// Clearing a never-set condition logs nothing.
	health.clear(condFetchStalled)
	assert.Len(t, capture.byLevel(slog.LevelInfo), 1)
}

func TestHealthSummaryKeepsBreakerAndFetchSeparate(t *testing.T) {
	breaker := resilience.NewCircuitBreaker(1, 1, time.Minute)
	c := &Connector{
		workers: []*Worker{{workerID: "worker-0", circuitBreaker: breaker}},
		health:  newHealthState(slog.New(&captureHandler{})),
	}
	c.health.recordFetchSuccess()

	require.True(t, c.Healthy())

	// Open the breaker: overall unhealthy, but fetch side stays green
	// and the summary shows exactly why.
	err := breaker.Execute(func() error {
		return connectorErrors.NewWriteFailedError("sink", nil)
	})
	require.Error(t, err)
	require.True(t, breaker.IsOpen())

	summary := c.Health()
	assert.False(t, summary.Healthy)
	assert.True(t, summary.FetchHealthy)
	assert.True(t, summary.BreakerOpen)
	assert.Contains(t, summary.BreakerStates, "worker-0")
	assert.False(t, c.Healthy())

	// Fetch-side condition also degrades overall health.
	c.health.set(condFetchStalled)
	summary = c.Health()
	assert.False(t, summary.FetchHealthy)
	assert.Contains(t, summary.Conditions, condFetchStalled)
}
