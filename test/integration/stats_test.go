//go:build integration

// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Console stats integration test: a full connector against hermetic
// JetStream and real QuasarDB with skewed per-table traffic, asserting
// the periodic "Connector stats" line -- rates, lag summary, and the
// rolling top-10 table ranking -- through a JSON slog handler, exactly
// as an operator would read it.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/internal/metrics"
	"github.com/bureau14/qdb-nats-connector/internal/telemetry"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	statsStreamName   = "STATS_IT"
	statsConsumerName = "stats-it-consumer"
	statsSubject      = "stats.it.events"
	statsQDBEndpoint  = "qdb://127.0.0.1:2836"
)

// statsParserConfig writes a yaml parser config with DYNAMIC table
// routing: the target table is read from each message's "table" field.
func statsParserConfig(t *testing.T) string {
	t.Helper()

	config := `
output:
  columns:
    - name: "value"
      type: "double"

transformations:
  - step: "parse_json"
    config: {}

  - step: "extract_table"
    config:
      source: "table"

  - step: "extract_index"
    config:
      source: "timestamp"
      format: "rfc3339nano"

  - step: "extract_field"
    config:
      source: "value"
      target: "value"
      type: "float64"
`

	path := filepath.Join(t.TempDir(), "parser.yaml")
	require.NoError(t, os.WriteFile(path, []byte(config), 0o600))

	return path
}

// createStatsTables provisions QuasarDB target tables, returned in the
// order given.
func createStatsTables(t *testing.T, names []string) {
	t.Helper()

	handle, err := qdb.NewHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })
	require.NoError(t, handle.Connect(statsQDBEndpoint))

	for _, name := range names {
		table := handle.Table(name)
		require.NoError(t, table.Create(24*time.Hour,
			qdb.NewTsColumnInfo("value", qdb.TsColumnDouble)))
		t.Cleanup(func() { _ = table.Remove() })
	}
}

// syncBuffer: goroutine-safe buffer for the JSON slog handler (the
// stats goroutine writes while the test reads).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	var out []string
	for _, line := range bytes.Split(b.buf.Bytes(), []byte("\n")) {
		if len(line) > 0 {
			out = append(out, string(line))
		}
	}

	return out
}

// statsLines parses every captured "Connector stats" JSON line.
func statsLines(buf *syncBuffer) []map[string]any {
	var out []map[string]any
	for _, line := range buf.lines() {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record["msg"] == "Connector stats" {
			out = append(out, record)
		}
	}

	return out
}

// topTable returns the rank-0 entry of a record's top_tables, nil if absent.
func topTable(record map[string]any) map[string]any {
	top, ok := record["top_tables"].([]any)
	if !ok || len(top) == 0 {
		return nil
	}
	entry, ok := top[0].(map[string]any)
	if !ok {
		return nil
	}

	return entry
}

func TestStatsTickReflectsSkewedTraffic(t *testing.T) {
	h := startRestartableNats(t, t.TempDir())
	js := jetStreamContext(t, h.url)
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     statsStreamName,
		Subjects: []string{statsSubject},
	})
	require.NoError(t, err)
	addDurablePullConsumer(t, js, statsStreamName, statsConsumerName, statsSubject)

	suffix := time.Now().Format("20060102150405")
	tableA := "test_stats_it_a_" + suffix
	tableB := "test_stats_it_b_" + suffix
	tableC := "test_stats_it_c_" + suffix
	createStatsTables(t, []string{tableA, tableB, tableC})

	// Skewed traffic: 80/15/5 across A/B/C, published before start.
	nc, err := nats.Connect(h.url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	var messages [][]byte
	for table, count := range map[string]int{tableA: 80, tableB: 15, tableC: 5} {
		for i := range count {
			messages = append(messages, fmt.Appendf(nil,
				`{"timestamp": %q, "table": %q, "value": %d.5}`,
				time.Now().Format(time.RFC3339Nano), table, i))
		}
	}
	require.NoError(t, PublishJSONMessages(nc, statsSubject, messages))

	// Wire metrics + stats logger exactly as main does, with a JSON
	// handler standing in for the console.
	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg, metrics.Config{
		Lag:   metrics.BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 20},
		Write: metrics.BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 20},
	})
	require.NoError(t, err)

	buf := &syncBuffer{}
	sl := telemetry.NewStatsLogger(m, time.Second, slog.New(slog.NewJSONHandler(buf, nil)))

	conn, done, stop := startConnector(t, []string{
		"--nats", h.url,
		"--stream", statsStreamName,
		"--consumer", statsConsumerName,
		"--qdb", statsQDBEndpoint,
		"--workers", "1",
		"--parser", "yaml",
		"--parser-config", statsParserConfig(t),
		"--http-addr", "",
	}, func(o *connector.Options) {
		o.Metrics = m
		o.OnTableWrites = sl.Record
	})
	defer stop()

	sl.Start(conn)
	t.Cleanup(sl.Stop)

	// Wait for a tick that has absorbed all three tables' events.
	require.Eventually(t, func() bool {
		select {
		case runErr := <-done:
			t.Fatalf("connector exited early: %v", runErr)
		default:
		}

		records := statsLines(buf)
		if len(records) == 0 {
			return false
		}
		last := records[len(records)-1]

		return last["tables_distinct"] == float64(3)
	}, 20*time.Second, 200*time.Millisecond)

	records := statsLines(buf)

	// The window ranking reflects the skew: A dominates with ~80% share.
	last := records[len(records)-1]
	top := topTable(last)
	require.NotNil(t, top)
	assert.Equal(t, tableA, top["table"])
	assert.Equal(t, float64(80), top["rows"])
	assert.InDelta(t, 80.0, top["share_pct"], 20.0)
	assert.Equal(t, float64(0), last["table_events_dropped"])

	// Some tick saw the burst: positive rates and lag samples.
	var sawRate, sawLag bool
	for _, r := range records {
		if rate, ok := r["msgs_per_s"].(float64); ok && rate > 0 {
			sawRate = true
		}
		if samples, ok := r["lag_samples"].(float64); ok && samples > 0 {
			sawLag = true
		}
	}
	assert.True(t, sawRate, "no tick reported a positive msgs_per_s")
	assert.True(t, sawLag, "no tick reported lag samples")
}
