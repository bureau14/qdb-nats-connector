// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Prometheus collector: reads the connector's atomic counters and
// health state at scrape time (node_exporter pattern) -- zero parallel
// bookkeeping, zero hot-path cost beyond the increments that already
// exist. Cardinality policy: the only label is the bounded worker
// identity; no data-derived label values.
// Types: StatsSource, Collector
// Ex: reg.Register(telemetry.NewCollector(conn, slog.Default()))
package telemetry

import (
	"context"
	"log/slog"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/connector/resilience"
	"github.com/bureau14/qdb-nats-connector/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// pendingScrapeTimeout bounds the ConsumerInfo refresh so a scrape can
// never hang during a NATS outage; the last-known value is served.
const pendingScrapeTimeout = 2 * time.Second

// StatsSource: connector-side surface the collector consumes.
// Satisfied by *connector.Connector.
type StatsSource interface {
	HealthSource
	PerWorkerStats() []connector.WorkerStatsSnapshot
	FetchStats() connector.FetchStats
	WorkChannelDepth() int
	BreakerStates() map[string]resilience.State
	ConsumerPending(ctx context.Context) (uint64, error)
}

// Collector: custom prometheus.Collector over connector snapshots.
type Collector struct {
	src    StatsSource
	logger *slog.Logger

	messagesFetched   *prometheus.Desc
	fetchErrors       *prometheus.Desc
	rebindAttempts    *prometheus.Desc
	messagesProcessed *prometheus.Desc
	parseFailures     *prometheus.Desc
	messagesDropped   *prometheus.Desc
	writeFailures     *prometheus.Desc
	rowsWritten       *prometheus.Desc
	acks              *prometheus.Desc
	nacks             *prometheus.Desc
	consumerPending   *prometheus.Desc
	breakerState      *prometheus.Desc
	workChannelDepth  *prometheus.Desc
	lastFetchSuccess  *prometheus.Desc
	ready             *prometheus.Desc
	healthy           *prometheus.Desc
}

// NewCollector builds the collector over src.
// In: src StatsSource - counter/health snapshots, logger *slog.Logger -
// destination for per-scrape emission problems
// Out: *Collector - ready to register
// Ex: reg.Register(NewCollector(conn, slog.Default()))
func NewCollector(src StatsSource, logger *slog.Logger) *Collector {
	fqName := func(name string) string { return metrics.Namespace + "_" + name }
	workerLabel := []string{"worker"}

	return &Collector{
		src:    src,
		logger: logger,

		messagesFetched: prometheus.NewDesc(fqName("messages_fetched_total"),
			"Messages pulled from JetStream, pre-parse.", nil, nil),
		fetchErrors: prometheus.NewDesc(fqName("fetch_errors_total"),
			"Fetch errors by kind (transient|subscription).", []string{"kind"}, nil),
		rebindAttempts: prometheus.NewDesc(fqName("rebind_attempts_total"),
			"Individual subscription re-bind tries.", nil, nil),
		messagesProcessed: prometheus.NewDesc(fqName("messages_processed_total"),
			"Messages that completed the pipeline without a parse failure.", workerLabel, nil),
		parseFailures: prometheus.NewDesc(fqName("parse_failures_total"),
			"Messages NACKed for redelivery due to parse errors.", workerLabel, nil),
		messagesDropped: prometheus.NewDesc(fqName("messages_dropped_total"),
			"Structurally-unusable messages discarded in drop mode.", workerLabel, nil),
		writeFailures: prometheus.NewDesc(fqName("write_failures_total"),
			"Batch write errors against QuasarDB.", workerLabel, nil),
		rowsWritten: prometheus.NewDesc(fqName("rows_written_total"),
			"Rows successfully written to QuasarDB.", workerLabel, nil),
		acks: prometheus.NewDesc(fqName("acks_total"),
			"Messages acknowledged after a successful pipeline pass.", workerLabel, nil),
		nacks: prometheus.NewDesc(fqName("nacks_total"),
			"Messages negatively acknowledged (parse or write failure).", workerLabel, nil),
		consumerPending: prometheus.NewDesc(fqName("consumer_pending_messages"),
			"Messages pending on the durable consumer (backlog).", nil, nil),
		breakerState: prometheus.NewDesc(fqName("breaker_state"),
			"Circuit breaker state: 0 closed, 1 half-open, 2 open.", workerLabel, nil),
		workChannelDepth: prometheus.NewDesc(fqName("work_channel_depth"),
			"Batches queued for the worker pool.", nil, nil),
		lastFetchSuccess: prometheus.NewDesc(fqName("last_fetch_success_timestamp_seconds"),
			"Unix time of the last successful fetch; 0 before the first.", nil, nil),
		ready: prometheus.NewDesc(fqName("ready"),
			"1 when /readyz would return 200.", nil, nil),
		healthy: prometheus.NewDesc(fqName("healthy"),
			"1 when fetch is healthy and all breakers are closed.", nil, nil),
	}
}

// Describe sends every metric descriptor.
// In: ch chan<- *prometheus.Desc - descriptor sink
// Out: none
// Ex: called by Registry.Register
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.messagesFetched, c.fetchErrors, c.rebindAttempts,
		c.messagesProcessed, c.parseFailures, c.messagesDropped,
		c.writeFailures, c.rowsWritten, c.acks, c.nacks,
		c.consumerPending, c.breakerState, c.workChannelDepth,
		c.lastFetchSuccess, c.ready, c.healthy,
	} {
		ch <- d
	}
}

// Collect snapshots the connector and emits every family.
// In: ch chan<- prometheus.Metric - metric sink
// Out: none
// Ex: called per scrape by the registry
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.collectFetch(ch)
	c.collectWorkers(ch)
	c.collectGauges(ch)
	c.collectPending(ch)
}

// collectFetch emits the fetch-loop counter families.
func (c *Collector) collectFetch(ch chan<- prometheus.Metric) {
	fetch := c.src.FetchStats()

	c.emit(ch, c.messagesFetched, prometheus.CounterValue, float64(fetch.MessagesFetched))
	c.emit(ch, c.fetchErrors, prometheus.CounterValue, float64(fetch.TransientErrors), "transient")
	c.emit(ch, c.fetchErrors, prometheus.CounterValue, float64(fetch.SubscriptionErrors), "subscription")
	c.emit(ch, c.rebindAttempts, prometheus.CounterValue, float64(fetch.RebindAttempts))
}

// collectWorkers emits the per-worker counter families.
func (c *Collector) collectWorkers(ch chan<- prometheus.Metric) {
	for _, snap := range c.src.PerWorkerStats() {
		c.emit(ch, c.messagesProcessed, prometheus.CounterValue, float64(snap.Stats.MessagesProcessed), snap.Worker)
		c.emit(ch, c.parseFailures, prometheus.CounterValue, float64(snap.Stats.ParseFailures), snap.Worker)
		c.emit(ch, c.messagesDropped, prometheus.CounterValue, float64(snap.Stats.MessagesDropped), snap.Worker)
		c.emit(ch, c.writeFailures, prometheus.CounterValue, float64(snap.Stats.WriteFailures), snap.Worker)
		c.emit(ch, c.rowsWritten, prometheus.CounterValue, float64(snap.Stats.RowsWritten), snap.Worker)
		c.emit(ch, c.acks, prometheus.CounterValue, float64(snap.Stats.Acks), snap.Worker)
		c.emit(ch, c.nacks, prometheus.CounterValue, float64(snap.Stats.Nacks), snap.Worker)
	}
}

// collectGauges emits health/readiness gauges and breaker states.
// The ready gauge reuses evaluateReadiness, guaranteeing parity with
// the /readyz verdict by construction.
func (c *Collector) collectGauges(ch chan<- prometheus.Metric) {
	health := c.src.Health()

	c.emit(ch, c.healthy, prometheus.GaugeValue, boolValue(health.Healthy))
	c.emit(ch, c.ready, prometheus.GaugeValue, boolValue(evaluateReadiness(c.src).Ready))
	c.emit(ch, c.workChannelDepth, prometheus.GaugeValue, float64(c.src.WorkChannelDepth()))

	lastFetch := float64(0)
	if !health.LastFetchSuccess.IsZero() {
		lastFetch = float64(health.LastFetchSuccess.UnixNano()) / 1e9
	}
	c.emit(ch, c.lastFetchSuccess, prometheus.GaugeValue, lastFetch)

	for worker, state := range c.src.BreakerStates() {
		value, known := breakerStateValue(state)
		if !known {
			c.logger.Debug("Skipping unknown breaker state", "worker", worker, "state", state)

			continue
		}
		c.emit(ch, c.breakerState, prometheus.GaugeValue, value, worker)
	}
}

// collectPending emits the consumer backlog gauge. The refresh RPC is
// bounded; on error the last-known value is still emitted -- gauges are
// last-known by nature and a scrape must never hang on a NATS outage.
func (c *Collector) collectPending(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), pendingScrapeTimeout)
	defer cancel()

	pending, err := c.src.ConsumerPending(ctx)
	if err != nil {
		c.logger.Debug("Consumer pending refresh failed; emitting last-known", "error", err)
	}
	c.emit(ch, c.consumerPending, prometheus.GaugeValue, float64(pending))
}

// emit sends one const metric, logging instead of panicking on error.
func (c *Collector) emit(ch chan<- prometheus.Metric, desc *prometheus.Desc,
	valueType prometheus.ValueType, value float64, labels ...string,
) {
	m, err := prometheus.NewConstMetric(desc, valueType, value, labels...)
	if err != nil {
		c.logger.Debug("Metric emission failed", "desc", desc.String(), "error", err)

		return
	}
	ch <- m
}

// breakerStateValue maps the resilience enum onto the documented gauge
// encoding (0 closed, 1 half-open, 2 open) -- severity order, which
// differs from the enum's iota order. Never cast the enum.
// In: state resilience.State - breaker state
// Out: float64, bool - gauge value, known state
// Ex: breakerStateValue(resilience.StateOpen) -> 2, true
func breakerStateValue(state resilience.State) (float64, bool) {
	switch state {
	case resilience.StateClosed:
		return 0, true
	case resilience.StateHalfOpen:
		return 1, true
	case resilience.StateOpen:
		return 2, true
	default:
		return 0, false
	}
}

// boolValue converts a verdict to a 0/1 gauge value.
func boolValue(b bool) float64 {
	if b {
		return 1
	}

	return 0
}
