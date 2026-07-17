// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for probe semantics: dependency outages never fail /livez,
// every readiness input flips /readyz on its own, bodies stay snake_case.
package telemetry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSource: canned HealthSource for handler tests.
type fakeSource struct {
	health   connector.HealthSummary
	lastEval time.Time
	natsUp   bool
}

func (f fakeSource) Health() connector.HealthSummary { return f.health }
func (f fakeSource) LastHealthEvaluate() time.Time   { return f.lastEval }
func (f fakeSource) NatsConnected() bool             { return f.natsUp }

// greenSource returns a fully healthy fake.
func greenSource() fakeSource {
	return fakeSource{
		health: connector.HealthSummary{
			Healthy:      true,
			FetchHealthy: true,
			Conditions:   []string{},
		},
		lastEval: time.Now(),
		natsUp:   true,
	}
}

// probeCode runs handler against a GET request, returns status + body.
func probeCode(t *testing.T, handler http.HandlerFunc) (code int, body []byte) {
	t.Helper()

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	return rec.Code, rec.Body.Bytes()
}

func TestLivezAliveWhenGreen(t *testing.T) {
	code, _ := probeCode(t, handleLivez(greenSource()))
	assert.Equal(t, http.StatusOK, code)
}

func TestLivezAliveOnZeroHeartbeat(t *testing.T) {
	// Startup grace: the HTTP server starts before RunWithContext
	// launches the monitor; a zero heartbeat is not a wedge.
	src := greenSource()
	src.lastEval = time.Time{}

	code, _ := probeCode(t, handleLivez(src))
	assert.Equal(t, http.StatusOK, code)
}

func TestLivezIgnoresDependencyOutages(t *testing.T) {
	// NATS down, breaker open, fetch-side conditions: none of these are
	// process wedges, so /livez must stay 200 throughout.
	src := greenSource()
	src.natsUp = false
	src.health.Healthy = false
	src.health.FetchHealthy = false
	src.health.BreakerOpen = true
	src.health.Conditions = []string{"fetch-stalled", "subscription-rebinding"}

	code, _ := probeCode(t, handleLivez(src))
	assert.Equal(t, http.StatusOK, code)
}

func TestLivezFailsOnStaleHeartbeat(t *testing.T) {
	src := greenSource()
	src.lastEval = time.Now().Add(-heartbeatStaleAfter - time.Second)

	code, body := probeCode(t, handleLivez(src))
	assert.Equal(t, http.StatusServiceUnavailable, code)

	var verdict liveness
	require.NoError(t, json.Unmarshal(body, &verdict))
	assert.False(t, verdict.Alive)
	assert.Equal(t, reasonHeartbeatStale, verdict.Reason)
}

func TestLivezFailsOnStageStuck(t *testing.T) {
	src := greenSource()
	src.health.FetchHealthy = false
	src.health.Conditions = []string{"stage-stuck:worker-1"}

	code, body := probeCode(t, handleLivez(src))
	assert.Equal(t, http.StatusServiceUnavailable, code)

	var verdict liveness
	require.NoError(t, json.Unmarshal(body, &verdict))
	assert.Equal(t, reasonStageStuck, verdict.Reason)
	assert.Contains(t, verdict.Conditions, "stage-stuck:worker-1")
}

func TestAliveMatchesLivezSemantics(t *testing.T) {
	// Alive is the /livez verdict for non-HTTP consumers (systemd
	// watchdog); pin it to the exact handler semantics.
	cases := map[string]struct {
		degrade    func(*fakeSource)
		wantAlive  bool
		wantReason string
	}{
		"green": {
			degrade:   func(*fakeSource) {},
			wantAlive: true,
		},
		"zero heartbeat startup grace": {
			degrade:   func(s *fakeSource) { s.lastEval = time.Time{} },
			wantAlive: true,
		},
		"stale heartbeat": {
			degrade: func(s *fakeSource) {
				s.lastEval = time.Now().Add(-heartbeatStaleAfter - time.Second)
			},
			wantAlive:  false,
			wantReason: reasonHeartbeatStale,
		},
		"stage stuck": {
			degrade: func(s *fakeSource) {
				s.health.FetchHealthy = false
				s.health.Conditions = []string{"stage-stuck:worker-1"}
			},
			wantAlive:  false,
			wantReason: reasonStageStuck,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			src := greenSource()
			tc.degrade(&src)

			alive, reason := Alive(src, time.Now())
			assert.Equal(t, tc.wantAlive, alive)
			assert.Equal(t, tc.wantReason, reason)
		})
	}
}

func TestReadyzReadyWhenGreen(t *testing.T) {
	code, body := probeCode(t, handleReadyz(greenSource()))
	assert.Equal(t, http.StatusOK, code)

	// Pin the wire format end-to-end: wrapped verdict, snake_case keys.
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, true, decoded["ready"])
	assert.Equal(t, true, decoded["nats_connected"])

	health, ok := decoded["health"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, health["fetch_healthy"])
}

func TestReadyzFailsPerInput(t *testing.T) {
	cases := map[string]func(*fakeSource){
		"nats disconnected": func(s *fakeSource) { s.natsUp = false },
		"fetch unhealthy":   func(s *fakeSource) { s.health.FetchHealthy = false },
		"breaker open":      func(s *fakeSource) { s.health.BreakerOpen = true },
	}

	for name, degrade := range cases {
		t.Run(name, func(t *testing.T) {
			src := greenSource()
			degrade(&src)

			code, body := probeCode(t, handleReadyz(src))
			assert.Equal(t, http.StatusServiceUnavailable, code)

			var verdict readiness
			require.NoError(t, json.Unmarshal(body, &verdict))
			assert.False(t, verdict.Ready)
		})
	}
}
