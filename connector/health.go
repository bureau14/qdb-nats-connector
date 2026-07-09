// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: connector-level health state & monitoring
// Types: healthState, healthMonitor, HealthSummary
// Ex: c.Healthy() → true (fetching, no stuck stages, breakers closed)
package connector

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector/resilience"
)

const (
	// condRebinding: the fetch loop is re-binding a dead pull subscription.
	condRebinding = "subscription-rebinding"
	// condFetchStalled: no successful fetch (including empty) within the
	// stuck threshold -- fetch attempts are failing, not merely idle.
	condFetchStalled = "fetch-stalled"
	// condStuckPrefix: a goroutine has been blocked inside a probed
	// pipeline stage past the stuck threshold.
	condStuckPrefix = "stage-stuck:"

	// monitorInterval: health evaluation cadence. Not configuration;
	// tests inject a shorter interval on healthMonitor directly.
	monitorInterval = 30 * time.Second
)

// healthState: connector-level health with per-condition edge-triggered
// transition logging. A condition key is active from set() to clear();
// setting an active key or clearing an inactive one is a no-op, so each
// transition logs exactly once (one ERROR going unhealthy, one INFO on
// recovery) -- never re-logged on unchanged ticks. Goroutine-safe.
type healthState struct {
	mu               sync.Mutex
	logger           *slog.Logger
	lastFetchSuccess time.Time
	active           map[string]time.Time // condition → since
}

// newHealthState creates health state logging transitions via logger.
// In: logger *slog.Logger - transition log destination
// Out: *healthState - no active conditions
// Ex: newHealthState(slog.Default()) → healthy state
func newHealthState(logger *slog.Logger) *healthState {
	return &healthState{
		logger: logger,
		active: make(map[string]time.Time),
	}
}

// recordFetchSuccess marks a successful FetchBatch return, empty batches
// included: a healthy-but-idle deployment stays green indefinitely.
// In: none
// Out: none
// Ex: recordFetchSuccess() → fetch liveness refreshed
func (h *healthState) recordFetchSuccess() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastFetchSuccess = time.Now()
}

// fetchSuccessAge returns time since the last successful fetch.
// In: now time.Time - evaluation clock
// Out: time.Duration - age of last success
// Ex: fetchSuccessAge(now) → 1.2s
func (h *healthState) fetchSuccessAge(now time.Time) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()

	return now.Sub(h.lastFetchSuccess)
}

// set activates a condition; logs one ERROR on the inactive→active edge.
// In: reason string - condition key, attrs ...any - extra log attributes
// Out: none
// Ex: set(condRebinding, "consumer", name) → one "Connector unhealthy" ERROR
func (h *healthState) set(reason string, attrs ...any) {
	h.mu.Lock()
	if _, exists := h.active[reason]; exists {
		h.mu.Unlock()

		return
	}
	h.active[reason] = time.Now()
	h.mu.Unlock()

	logAttrs := append([]any{"reason", reason}, attrs...)
	h.logger.Error("Connector unhealthy", logAttrs...)
}

// clear deactivates a condition; logs one INFO on the active→inactive edge.
// In: reason string - condition key
// Out: none
// Ex: clear(condRebinding) → one "Connector recovered" INFO with downtime
func (h *healthState) clear(reason string) {
	h.mu.Lock()
	since, exists := h.active[reason]
	if !exists {
		h.mu.Unlock()

		return
	}
	delete(h.active, reason)
	h.mu.Unlock()

	h.logger.Info("Connector recovered", "reason", reason, "downtime", time.Since(since))
}

// isActive reports whether a condition is currently active.
// In: reason string - condition key
// Out: bool - true if active
// Ex: isActive(condRebinding) → false
func (h *healthState) isActive(reason string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, exists := h.active[reason]

	return exists
}

// snapshot returns the active condition keys and last fetch success.
// In: none
// Out: []string, time.Time - sorted-insertion-order-free copy of conditions
// Ex: snapshot() → ["fetch-stalled"], 2026-07-07T08:55:29Z
func (h *healthState) snapshot() ([]string, time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()

	conditions := make([]string, 0, len(h.active))
	for reason := range h.active {
		conditions = append(conditions, reason)
	}

	return conditions, h.lastFetchSuccess
}

// probeRef: a named liveness probe registered with the monitor.
// owner identifies the goroutine ("fetch-loop", "worker-3") in logs.
type probeRef struct {
	owner string
	probe *Probe
}

// healthMonitor: periodically evaluates liveness probes and fetch
// staleness against the stuck threshold, driving healthState conditions.
// Liveness means "not stuck", not "recently active": a cleared probe and
// a fresh fetch success are healthy no matter how idle the stream is.
type healthMonitor struct {
	health         *healthState
	probes         []probeRef
	stuckThreshold time.Duration
	interval       time.Duration
}

// newHealthMonitor builds the monitor over the fetch-loop probe and one
// probe per worker.
// In: none - uses connector state
// Out: *healthMonitor - ready for run(ctx)
// Ex: monitor := c.newHealthMonitor(); go monitor.run(ctx)
func (c *Connector) newHealthMonitor() *healthMonitor {
	probes := make([]probeRef, 0, len(c.workers)+1)
	probes = append(probes, probeRef{owner: "fetch-loop", probe: &c.fetchProbe})
	for _, w := range c.workers {
		probes = append(probes, probeRef{owner: w.workerID, probe: &w.probe})
	}

	return &healthMonitor{
		health:         c.health,
		probes:         probes,
		stuckThreshold: c.stuckThreshold,
		interval:       monitorInterval,
	}
}

// run evaluates health on every tick until the context is cancelled.
// In: ctx context.Context - cancellation
// Out: none
// Ex: go monitor.run(ctx)
func (m *healthMonitor) run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.evaluate(time.Now())
		}
	}
}

// evaluate drives conditions from the injected clock; deterministic for
// tests (no time.Now / time.Since inside).
// In: now time.Time - evaluation instant
// Out: none - transitions logged via healthState
// Ex: evaluate(time.Now()) → flags probes stuck past threshold
func (m *healthMonitor) evaluate(now time.Time) {
	for _, ref := range m.probes {
		condition := condStuckPrefix + ref.owner
		stage, age, active := ref.probe.Sample(now)
		if active && age > m.stuckThreshold {
			m.health.set(condition,
				"stage", stage, "owner", ref.owner, "age", age)
		} else {
			m.health.clear(condition)
		}
	}

	// Fetch staleness only counts when not already reported as a re-bind
	// in progress -- one incident, one condition.
	stalled := m.health.fetchSuccessAge(now) > m.stuckThreshold &&
		!m.health.isActive(condRebinding)
	if stalled {
		m.health.set(condFetchStalled, "threshold", m.stuckThreshold)
	} else {
		m.health.clear(condFetchStalled)
	}
}

// HealthSummary: point-in-time connector health, keeping fetch liveness
// and write-side circuit-breaker state visibly separate so the two
// signals cannot be conflated.
type HealthSummary struct {
	// Healthy: FetchHealthy && !BreakerOpen.
	Healthy bool `json:"healthy"`
	// FetchHealthy: no active unhealthy conditions (fetch side).
	FetchHealthy bool `json:"fetch_healthy"`
	// Conditions: active condition keys (empty when FetchHealthy);
	// stage-stuck entries carry the "stage-stuck:" prefix.
	Conditions []string `json:"conditions"`
	// LastFetchSuccess: last successful FetchBatch return (incl. empty).
	LastFetchSuccess time.Time `json:"last_fetch_success"`
	// BreakerOpen: any worker's circuit breaker is not closed.
	BreakerOpen bool `json:"breaker_open"`
	// BreakerStates: per-worker breaker state ("worker-0" → "CLOSED").
	BreakerStates map[string]string `json:"breaker_states"`
}

// StageStuck reports whether any stage-stuck condition is active.
// In: none
// Out: bool - true when a probed goroutine is wedged past the threshold
// Ex: HealthSummary{Conditions: []string{"stage-stuck:worker-0"}}.StageStuck() → true
func (s HealthSummary) StageStuck() bool {
	for _, c := range s.Conditions {
		if strings.HasPrefix(c, condStuckPrefix) {
			return true
		}
	}

	return false
}

// Health returns the aggregate health summary.
// In: none
// Out: HealthSummary - fetch liveness + breaker state
// Ex: c.Health() → HealthSummary{Healthy: true, ...}
func (c *Connector) Health() HealthSummary {
	conditions, lastFetch := c.health.snapshot()

	breakerStates := make(map[string]string, len(c.workers))
	breakerOpen := false
	for _, w := range c.workers {
		state := w.circuitBreaker.GetState()
		breakerStates[w.workerID] = state.String()
		if state != resilience.StateClosed {
			breakerOpen = true
		}
	}

	fetchHealthy := len(conditions) == 0

	return HealthSummary{
		Healthy:          fetchHealthy && !breakerOpen,
		FetchHealthy:     fetchHealthy,
		Conditions:       conditions,
		LastFetchSuccess: lastFetch,
		BreakerOpen:      breakerOpen,
		BreakerStates:    breakerStates,
	}
}

// Healthy reports overall connector health.
// In: none
// Out: bool - fetch healthy and all breakers closed
// Ex: c.Healthy() → true
func (c *Connector) Healthy() bool {
	return c.Health().Healthy
}
