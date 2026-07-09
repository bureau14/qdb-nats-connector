//go:build integration

// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Metrics integration tests: a full connector against hermetic JetStream
// and real QuasarDB, scraped over HTTP exactly as Prometheus would.
// TestMetricsEndToEnd pins the whole /metrics surface under real load and
// the scrape-vs-Stats parity once quiesced; TestSourcePendingRefresh pins
// the idle ConsumerInfo refresh path the end-to-end test cannot isolate.
package integration

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/internal/metrics"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/bureau14/qdb-nats-connector/internal/telemetry"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	metricsStreamName   = "METRICS_IT"
	metricsConsumerName = "metrics-it-consumer"
	metricsSubject      = "metrics.it.events"
	metricsQDBEndpoint  = "qdb://127.0.0.1:2836"
	metricsMessageCount = 50
)

// metricsParserConfig writes a yaml parser config producing one double
// row per message into tableName; returns the config path.
func metricsParserConfig(t *testing.T, tableName string) string {
	t.Helper()

	config := fmt.Sprintf(`
output:
  columns:
    - name: "value"
      type: "double"

transformations:
  - step: "parse_json"
    config: {}

  - step: "extract_table"
    config:
      value: "%s"

  - step: "extract_index"
    config:
      source: "timestamp"
      format: "rfc3339nano"

  - step: "extract_field"
    config:
      source: "value"
      target: "value"
      type: "float64"
`, tableName)

	path := filepath.Join(t.TempDir(), "parser.yaml")
	require.NoError(t, os.WriteFile(path, []byte(config), 0o600))

	return path
}

// createMetricsTable provisions the QuasarDB target table.
func createMetricsTable(t *testing.T) string {
	t.Helper()

	handle, err := qdb.NewHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })
	require.NoError(t, handle.Connect(metricsQDBEndpoint))

	tableName := "test_metrics_it_" + time.Now().Format("20060102150405")
	table := handle.Table(tableName)
	err = table.Create(24*time.Hour, qdb.NewTsColumnInfo("value", qdb.TsColumnDouble))
	require.NoError(t, err)
	t.Cleanup(func() { _ = table.Remove() })

	return tableName
}

// scrapeFamilies GETs /metrics and parses the text exposition.
func scrapeFamilies(t *testing.T, url string) map[string]*dto.MetricFamily {
	t.Helper()

	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	require.NoError(t, err)

	return families
}

// sumFamily sums every sample of a counter/gauge family; 0 when absent.
func sumFamily(families map[string]*dto.MetricFamily, name string) float64 {
	mf, ok := families[name]
	if !ok {
		return 0
	}

	total := float64(0)
	for _, m := range mf.GetMetric() {
		switch {
		case m.GetCounter() != nil:
			total += m.GetCounter().GetValue()
		case m.GetGauge() != nil:
			total += m.GetGauge().GetValue()
		}
	}

	return total
}

// histogramCount returns the sample count of a histogram family.
func histogramCount(families map[string]*dto.MetricFamily, name string) uint64 {
	mf, ok := families[name]
	if !ok {
		return 0
	}

	var count uint64
	for _, m := range mf.GetMetric() {
		count += m.GetHistogram().GetSampleCount()
	}

	return count
}

func TestMetricsEndToEnd(t *testing.T) {
	testStart := time.Now()

	h := startRestartableNats(t, t.TempDir())
	js := jetStreamContext(t, h.url)
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     metricsStreamName,
		Subjects: []string{metricsSubject},
	})
	require.NoError(t, err)
	addDurablePullConsumer(t, js, metricsStreamName, metricsConsumerName, metricsSubject)

	tableName := createMetricsTable(t)

	// Publish before the connector starts so the first fetches are full.
	nc, err := nats.Connect(h.url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	messages := make([][]byte, metricsMessageCount)
	for i := range messages {
		messages[i] = fmt.Appendf(nil, `{"timestamp": %q, "value": %d.5}`,
			time.Now().Format(time.RFC3339Nano), i)
	}
	require.NoError(t, PublishJSONMessages(nc, metricsSubject, messages))

	// Wire registry + histograms + collector + server exactly as main does.
	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg, metrics.Config{
		Lag:   metrics.BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 20},
		Write: metrics.BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 20},
	})
	require.NoError(t, err)
	require.NoError(t, metrics.RegisterBuildInfo(reg, "it-version", "it-commit"))

	conn, done, stop := startConnector(t, []string{
		"--nats", h.url,
		"--stream", metricsStreamName,
		"--consumer", metricsConsumerName,
		"--qdb", metricsQDBEndpoint,
		"--workers", "1",
		"--parser", "yaml",
		"--parser-config", metricsParserConfig(t, tableName),
		"--http-addr", "", // server owned by the test, as by main
	}, func(o *connector.Options) { o.Metrics = m })

	require.NoError(t, reg.Register(telemetry.NewCollector(conn, slog.Default())))

	srv, err := telemetry.NewServer("127.0.0.1:0", conn, reg, slog.Default())
	require.NoError(t, err)
	srv.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	metricsURL := fmt.Sprintf("http://%s/metrics", srv.Addr())

	// Scrape until every message is ACKed (pipeline fully drained).
	var families map[string]*dto.MetricFamily
	deadline := time.Now().Add(30 * time.Second)
	for {
		select {
		case runErr := <-done:
			t.Fatalf("connector exited early: %v", runErr)
		default:
		}

		families = scrapeFamilies(t, metricsURL)
		if sumFamily(families, "qdb_nats_connector_acks_total") == metricsMessageCount {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("acks never reached %d; stats %+v", metricsMessageCount, conn.Stats())
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Counters under real load.
	assert.GreaterOrEqual(t, sumFamily(families, "qdb_nats_connector_messages_fetched_total"),
		float64(metricsMessageCount))
	assert.Equal(t, float64(metricsMessageCount),
		sumFamily(families, "qdb_nats_connector_messages_processed_total"))
	assert.Equal(t, float64(metricsMessageCount),
		sumFamily(families, "qdb_nats_connector_rows_written_total"))
	assert.Zero(t, sumFamily(families, "qdb_nats_connector_parse_failures_total"))
	assert.Zero(t, sumFamily(families, "qdb_nats_connector_nacks_total"))

	// Histograms populated with sane values.
	assert.Equal(t, uint64(metricsMessageCount),
		histogramCount(families, "qdb_nats_connector_message_lag_seconds"))
	lagSum := families["qdb_nats_connector_message_lag_seconds"].GetMetric()[0].GetHistogram().GetSampleSum()
	assert.Positive(t, lagSum)
	assert.Less(t, lagSum, float64(metricsMessageCount*60), "per-message lag should be seconds, not minutes")
	assert.GreaterOrEqual(t, histogramCount(families, "qdb_nats_connector_write_duration_seconds"), uint64(1))
	assert.GreaterOrEqual(t, histogramCount(families, "qdb_nats_connector_fetch_batch_size"), uint64(1))

	// Gauges: healthy, ready, drained, breaker closed, fresh fetch stamp.
	assert.Equal(t, float64(1), sumFamily(families, "qdb_nats_connector_healthy"))
	assert.Equal(t, float64(1), sumFamily(families, "qdb_nats_connector_ready"))
	assert.Zero(t, sumFamily(families, "qdb_nats_connector_consumer_pending_messages"))
	assert.Zero(t, sumFamily(families, "qdb_nats_connector_work_channel_depth"))
	assert.Zero(t, sumFamily(families, "qdb_nats_connector_breaker_state"))
	assert.Equal(t, float64(1), sumFamily(families, "qdb_nats_connector_build_info"))

	lastFetch := sumFamily(families, "qdb_nats_connector_last_fetch_success_timestamp_seconds")
	assert.GreaterOrEqual(t, lastFetch, float64(testStart.Unix()))
	assert.LessOrEqual(t, lastFetch, float64(time.Now().Unix()+1))

	// Parity: quiesced scrape sums equal the connector's own snapshots.
	stats := conn.Stats()
	assert.Equal(t, float64(stats.MessagesProcessed),
		sumFamily(families, "qdb_nats_connector_messages_processed_total"))
	assert.Equal(t, float64(stats.RowsWritten),
		sumFamily(families, "qdb_nats_connector_rows_written_total"))
	assert.Equal(t, float64(stats.Acks),
		sumFamily(families, "qdb_nats_connector_acks_total"))

	fetchStats := conn.FetchStats()
	assert.Equal(t, float64(fetchStats.MessagesFetched),
		sumFamily(families, "qdb_nats_connector_messages_fetched_total"))
	assert.Equal(t, float64(fetchStats.TransientErrors+fetchStats.SubscriptionErrors),
		sumFamily(families, "qdb_nats_connector_fetch_errors_total"))

	stop()
}

// TestSourcePendingRefresh pins the pending-count paths deterministically:
// a never-fetched consumer forces the ConsumerInfo refresh; after a fetch
// the cache serves without any RPC (proven by closing the connection).
func TestSourcePendingRefresh(t *testing.T) {
	h := startRestartableNats(t, t.TempDir())
	js := jetStreamContext(t, h.url)
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     metricsStreamName,
		Subjects: []string{metricsSubject},
	})
	require.NoError(t, err)
	addDurablePullConsumer(t, js, metricsStreamName, metricsConsumerName, metricsSubject)

	nc, err := nats.Connect(h.url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	messages := make([][]byte, 5)
	for i := range messages {
		messages[i] = fmt.Appendf(nil, `{"n": %d}`, i)
	}
	require.NoError(t, PublishJSONMessages(nc, metricsSubject, messages))

	opts := source.NewOptions(
		source.WithEndpoint(h.url),
		source.WithStreamName(metricsStreamName),
		source.WithBatchTimeout(time.Second),
	)
	opts.ConsumerName = metricsConsumerName

	src, err := source.NewSource(opts)
	require.NoError(t, err)
	t.Cleanup(src.Close)
	require.NoError(t, src.Connect(context.Background()))

	// Never fetched: maxAge 0 forces the ConsumerInfo path.
	pending, err := src.PendingMessages(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), pending)

	// One fetch drains the stream and feeds the cache for free.
	batch, err := src.FetchBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, batch.Messages, 5)

	// Cache hit: no RPC possible on a closed connection, so a nil error
	// proves the fetch-fed cache answered.
	src.NatsConn.Close()
	pending, err = src.PendingMessages(context.Background(), time.Hour)
	require.NoError(t, err)
	assert.Zero(t, pending)
}
