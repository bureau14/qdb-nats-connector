// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Probe semantics: /livez detects process wedges only (never dependency
// state -- a failing liveness probe means "restart me"); /readyz reports
// end-to-end usefulness (NATS connected, fetch healthy, breakers closed).
// Types: liveness, readiness
// Ex: GET /readyz → 503 {"ready":false,"nats_connected":false,...}
package telemetry

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector"
)

const (
	// heartbeatStaleAfter: /livez fails when the health monitor's last
	// completed evaluation is older than this (monitor goroutine dead).
	// 3x the monitor cadence tolerates two missed ticks under load.
	heartbeatStaleAfter = 3 * connector.MonitorInterval

	// reasonHeartbeatStale/reasonStageStuck: liveness failure reasons.
	reasonHeartbeatStale = "heartbeat-stale"
	reasonStageStuck     = "stage-stuck"
)

// liveness: /livez body -- verdict plus reason when failing.
type liveness struct {
	Alive      bool     `json:"alive"`
	Reason     string   `json:"reason,omitempty"`
	Conditions []string `json:"conditions,omitempty"`
}

// readiness: /readyz body -- verdict plus its inputs, self-explaining.
type readiness struct {
	Ready         bool                    `json:"ready"`
	NatsConnected bool                    `json:"nats_connected"`
	Health        connector.HealthSummary `json:"health"`
}

// evaluateLiveness decides /livez: 200 unless the monitor heartbeat is
// stale or a stage-stuck condition is active. Never consults NATS/QDB.
// A zero heartbeat is alive: the HTTP server starts before
// RunWithContext launches the monitor (startup grace).
// In: src HealthSource - probe inputs, now time.Time - evaluation clock
// Out: liveness - verdict with reason when failing
// Ex: evaluateLiveness(src, time.Now()) → liveness{Alive: true}
func evaluateLiveness(src HealthSource, now time.Time) liveness {
	last := src.LastHealthEvaluate()
	if !last.IsZero() && now.Sub(last) > heartbeatStaleAfter {
		return liveness{Alive: false, Reason: reasonHeartbeatStale}
	}

	health := src.Health()
	if health.StageStuck() {
		return liveness{
			Alive:      false,
			Reason:     reasonStageStuck,
			Conditions: health.Conditions,
		}
	}

	return liveness{Alive: true}
}

// evaluateReadiness decides /readyz: ready iff NATS is connected, the
// fetch side is healthy, and all circuit breakers are closed --
// Connector.Healthy() plus the live connection check.
// In: src HealthSource - probe inputs
// Out: readiness - verdict plus its inputs
// Ex: evaluateReadiness(src) → readiness{Ready: true, ...}
func evaluateReadiness(src HealthSource) readiness {
	natsConnected := src.NatsConnected()
	health := src.Health()

	return readiness{
		Ready:         natsConnected && health.FetchHealthy && !health.BreakerOpen,
		NatsConnected: natsConnected,
		Health:        health,
	}
}

// handleLivez serves /livez: 200 alive, 503 wedged.
// In: src HealthSource - probe inputs
// Out: http.HandlerFunc - JSON liveness verdict
// Ex: GET /livez → 200 {"alive":true}
func handleLivez(src HealthSource) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		verdict := evaluateLiveness(src, time.Now())
		writeJSON(w, probeStatus(verdict.Alive), verdict)
	}
}

// handleReadyz serves /readyz: 200 ready, 503 not ready.
// In: src HealthSource - probe inputs
// Out: http.HandlerFunc - JSON readiness verdict
// Ex: GET /readyz → 503 {"ready":false,"nats_connected":false,...}
func handleReadyz(src HealthSource) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		verdict := evaluateReadiness(src)
		writeJSON(w, probeStatus(verdict.Ready), verdict)
	}
}

// probeStatus maps a probe verdict to its HTTP status code.
// In: ok bool - probe verdict
// Out: int - 200 when ok, 503 otherwise
// Ex: probeStatus(false) → 503
func probeStatus(ok bool) int {
	if ok {
		return http.StatusOK
	}

	return http.StatusServiceUnavailable
}

// writeJSON writes a JSON response with the given status code.
// In: w http.ResponseWriter, status int - HTTP status, body any - payload
// Out: none - encode failures are unrecoverable mid-response
// Ex: writeJSON(w, 200, liveness{Alive: true})
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
