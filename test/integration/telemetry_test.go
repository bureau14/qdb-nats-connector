//go:build integration

// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Probe integration test: a full connector against a hermetic JetStream
// plus a real telemetry server, with the NATS process killed and restored
// mid-run. Pins the probe semantics: /readyz flips on the outage and
// recovers with the reconnect, /livez stays 200 throughout (dependency
// outages are never liveness failures).
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/bureau14/qdb-nats-connector/internal/telemetry"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	telemetryStreamName   = "TELEMETRY_IT"
	telemetryConsumerName = "telemetry-it-consumer"
	telemetrySubject      = "telemetry.it.events"
	telemetryQDBEndpoint  = "qdb://127.0.0.1:2836"
)

// natsHandle: killable/restartable nats-server pinned to one port and one
// store dir, so stream and durable survive a restart.
type natsHandle struct {
	bin      string
	confFile string
	url      string
	cmd      *exec.Cmd
}

// startRestartableNats spawns a JetStream nats-server whose config file is
// retained for later restarts on the same port and store_dir.
func startRestartableNats(t *testing.T, dir string) *natsHandle {
	t.Helper()

	bin, err := exec.LookPath("nats-server")
	if err != nil {
		skipMissingNats(t)
	}

	port := freeTCPPort(t)
	conf := fmt.Sprintf(`
listen: 127.0.0.1:%d
jetstream { store_dir: %q }
`, port, filepath.Join(dir, "js"))

	confFile := filepath.Join(dir, "nats.conf")
	err = os.WriteFile(confFile, []byte(conf), 0o600)
	require.NoError(t, err)

	h := &natsHandle{
		bin:      bin,
		confFile: confFile,
		url:      fmt.Sprintf("nats://127.0.0.1:%d", port),
	}
	h.restart(t)

	return h
}

// kill terminates the nats-server process (simulated outage).
func (h *natsHandle) kill(t *testing.T) {
	t.Helper()

	require.NoError(t, h.cmd.Process.Kill())
	_, _ = h.cmd.Process.Wait()
	h.cmd = nil
}

// restart launches nats-server with the retained config and waits for it.
func (h *natsHandle) restart(t *testing.T) {
	t.Helper()

	cmd := exec.Command(h.bin, "-c", h.confFile) //nolint:gosec // bin from LookPath, confFile from t.TempDir()
	require.NoError(t, cmd.Start())
	h.cmd = cmd
	t.Cleanup(func() {
		if h.cmd != nil {
			_ = h.cmd.Process.Kill()
			_, _ = h.cmd.Process.Wait()
		}
	})

	waitForNatsServer(t, h.url)
}

// probeGet fetches a probe endpoint, returns status code and decoded body.
func probeGet(t *testing.T, url string) (code int, body map[string]any) {
	t.Helper()

	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

	return resp.StatusCode, body
}

// pollProbeStatus polls url until it returns wantCode, failing on deadline
// or early connector exit.
func pollProbeStatus(t *testing.T, url string, wantCode int, done <-chan error, deadline time.Duration) {
	t.Helper()

	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		select {
		case runErr := <-done:
			t.Fatalf("connector exited early: %v", runErr)
		default:
		}

		code, _ := probeGet(t, url)
		if code == wantCode {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("probe %s did not reach %d within %s", url, wantCode, deadline)
}

func TestTelemetryProbesNatsOutage(t *testing.T) {
	h := startRestartableNats(t, t.TempDir())

	js := jetStreamContext(t, h.url)
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     telemetryStreamName,
		Subjects: []string{telemetrySubject},
	})
	require.NoError(t, err)
	addDurablePullConsumer(t, js, telemetryStreamName, telemetryConsumerName, telemetrySubject)

	conn, done, stop := startConnector(t, []string{
		"--nats", h.url,
		"--stream", telemetryStreamName,
		"--consumer", telemetryConsumerName,
		"--qdb", telemetryQDBEndpoint,
		"--workers", "1",
		"--parser", "noop",
		"--http-addr", "", // server owned by the test, as by main
	})

	srv, err := telemetry.NewServer("127.0.0.1:0", conn, nil, slog.Default())
	require.NoError(t, err)
	srv.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	livezURL := fmt.Sprintf("http://%s/livez", srv.Addr())
	readyzURL := fmt.Sprintf("http://%s/readyz", srv.Addr())

	// Startup: ready once Connect completes; alive throughout.
	pollProbeStatus(t, readyzURL, http.StatusOK, done, 30*time.Second)
	code, _ := probeGet(t, livezURL)
	assert.Equal(t, http.StatusOK, code)

	// Outage: readiness flips, liveness must not.
	h.kill(t)
	pollProbeStatus(t, readyzURL, http.StatusServiceUnavailable, done, 10*time.Second)

	code, body := probeGet(t, readyzURL)
	require.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, false, body["ready"])
	assert.Equal(t, false, body["nats_connected"])
	health, ok := body["health"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, health["fetch_healthy"])

	for range 3 {
		code, _ = probeGet(t, livezURL)
		assert.Equal(t, http.StatusOK, code)
		time.Sleep(200 * time.Millisecond)
	}

	// Recovery: the NATS client reconnects on its own (default 2s wait).
	h.restart(t)
	pollProbeStatus(t, readyzURL, http.StatusOK, done, 30*time.Second)
	code, _ = probeGet(t, livezURL)
	assert.Equal(t, http.StatusOK, code)

	stop()
}
