// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package metrics: nil-safe Prometheus histograms for hot-path
// observation. Leaf package (imports only prometheus + errors) so
// source, connector, telemetry, and main can all depend on it without
// cycles. A nil *Metrics is a valid no-op receiver -- telemetry
// disabled means every Observe* call returns immediately.
// Types: BucketConfig, Config, Metrics
// Ex: m, _ := metrics.New(reg, cfg); m.ObserveLag(120 * time.Millisecond)
package metrics

import (
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Namespace prefixes every metric family exposed by the connector.
const Namespace = "qdb_nats_connector"

// fetchBatchSize buckets are fixed 1..2048 (default batch is 100);
// batch size is bounded by config, not workload, so no knobs.
const (
	fetchBatchBucketStart  = 1
	fetchBatchBucketFactor = 2.0
	fetchBatchBucketCount  = 12
)

// BucketConfig: knobs for one exponential bucket series, mapping
// directly to prometheus.ExponentialBuckets(start, factor, count).
type BucketConfig struct {
	Start  time.Duration
	Factor float64
	Count  int
}

// buckets expands the config into histogram bucket boundaries (seconds).
// In: none
// Out: []float64 - exponential series start*factor^i, i in [0,count)
// Ex: BucketConfig{time.Millisecond, 2.0, 3}.buckets() -> [0.001 0.002 0.004]
func (b BucketConfig) buckets() []float64 {
	return prometheus.ExponentialBuckets(b.Start.Seconds(), b.Factor, b.Count)
}

// Config: bucket configuration for the two operator-tunable histograms.
type Config struct {
	Lag   BucketConfig
	Write BucketConfig
}

// BucketCount: one cumulative histogram bucket -- observations at or
// below UpperBound.
type BucketCount struct {
	UpperBound      float64
	CumulativeCount uint64
}

// HistogramSnapshot: cumulative histogram state at one instant. Buckets
// hold ascending finite bounds; the implicit +Inf bucket is Count.
type HistogramSnapshot struct {
	Count   uint64
	Sum     float64
	Buckets []BucketCount
}

// Metrics: hot-path histograms. Observe is an atomic add -- safe to
// share one instance across all workers and the fetch loop. All
// methods are nil-safe no-ops so call sites never branch on whether
// telemetry is enabled.
type Metrics struct {
	messageLag     prometheus.Histogram
	writeDuration  prometheus.Histogram
	fetchBatchSize prometheus.Histogram
}

// New builds the histograms and registers them on reg.
// In: reg *prometheus.Registry - explicit registry (no globals),
// cfg Config - bucket series for lag and write duration
// Out: *Metrics, error - registered metrics or InvalidConfig/Unexpected
// Ex: New(reg, Config{Lag: BucketConfig{time.Millisecond, 2.0, 20}, ...})
func New(reg *prometheus.Registry, cfg Config) (*Metrics, error) {
	m := &Metrics{
		messageLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "message_lag_seconds",
			Help:      "Broker store time to connector pickup time, per message.",
			Buckets:   cfg.Lag.buckets(),
		}),
		writeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "write_duration_seconds",
			Help:      "QuasarDB batch write duration, including retries and breaker.",
			Buckets:   cfg.Write.buckets(),
		}),
		fetchBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "fetch_batch_size",
			Help:      "Messages per non-empty JetStream fetch.",
			Buckets: prometheus.ExponentialBuckets(fetchBatchBucketStart,
				fetchBatchBucketFactor, fetchBatchBucketCount),
		}),
	}

	for _, collector := range []prometheus.Collector{
		m.messageLag, m.writeDuration, m.fetchBatchSize,
	} {
		err := reg.Register(collector)
		if err != nil {
			return nil, connectorErrors.NewUnexpectedError("metrics",
				"histogram registration failed", err)
		}
	}

	return m, nil
}

// ObserveLag records broker-to-pickup lag for one message. Negative
// lag (broker/client clock skew) is clamped to zero here so every
// call site inherits the same policy.
// In: lag time.Duration - now minus broker store time
// Out: none
// Ex: m.ObserveLag(now.Sub(meta.Timestamp))
func (m *Metrics) ObserveLag(lag time.Duration) {
	if m == nil {
		return
	}

	if lag < 0 {
		lag = 0
	}
	m.messageLag.Observe(lag.Seconds())
}

// ObserveWriteDuration records one QuasarDB batch write duration.
// In: d time.Duration - wall time of the write incl. retries + breaker
// Out: none
// Ex: m.ObserveWriteDuration(writeDuration)
func (m *Metrics) ObserveWriteDuration(d time.Duration) {
	if m == nil {
		return
	}

	m.writeDuration.Observe(d.Seconds())
}

// ObserveFetchBatchSize records the size of one non-empty fetch.
// Callers skip empty batches -- an idle stream polls ~1/s and zeros
// would swamp the distribution.
// In: n int - messages in the batch
// Out: none
// Ex: m.ObserveFetchBatchSize(len(batch.Messages))
func (m *Metrics) ObserveFetchBatchSize(n int) {
	if m == nil {
		return
	}

	m.fetchBatchSize.Observe(float64(n))
}

// LagSnapshot returns the lag histogram's cumulative state, the same
// cells /metrics serves -- consumers diff consecutive snapshots for
// interval summaries (single substrate, no parallel bookkeeping).
// In: none
// Out: HistogramSnapshot, bool - snapshot; false on nil receiver or
// encode failure
// Ex: snap, ok := m.LagSnapshot() -> {Count: 42, ...}, true
func (m *Metrics) LagSnapshot() (HistogramSnapshot, bool) {
	if m == nil {
		return HistogramSnapshot{}, false
	}

	var pb dto.Metric
	err := m.messageLag.Write(&pb)
	if err != nil || pb.Histogram == nil {
		return HistogramSnapshot{}, false
	}

	h := pb.Histogram
	snap := HistogramSnapshot{
		Count:   h.GetSampleCount(),
		Sum:     h.GetSampleSum(),
		Buckets: make([]BucketCount, 0, len(h.GetBucket())),
	}
	for _, b := range h.GetBucket() {
		snap.Buckets = append(snap.Buckets, BucketCount{
			UpperBound:      b.GetUpperBound(),
			CumulativeCount: b.GetCumulativeCount(),
		})
	}

	return snap, true
}

// RegisterBuildInfo exposes ldflags build metadata as a constant gauge.
// In: reg *prometheus.Registry - target registry,
// version, commit string - ldflags-injected build metadata
// Out: error - nil or Unexpected on registration failure
// Ex: RegisterBuildInfo(reg, "3.15.0.dev0", "442408d...") -> build_info 1
func RegisterBuildInfo(reg *prometheus.Registry, version, commit string) error {
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   Namespace,
		Name:        "build_info",
		Help:        "Build metadata; value is always 1.",
		ConstLabels: prometheus.Labels{"version": version, "commit": commit},
	})
	gauge.Set(1)

	err := reg.Register(gauge)
	if err != nil {
		return connectorErrors.NewUnexpectedError("metrics",
			"build_info registration failed", err)
	}

	return nil
}
