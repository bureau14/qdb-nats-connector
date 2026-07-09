// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultConfig mirrors the production flag defaults.
func defaultConfig() Config {
	return Config{
		Lag:   BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 20},
		Write: BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 20},
	}
}

// histogramFor gathers reg and returns the histogram for name.
func histogramFor(t *testing.T, reg *prometheus.Registry, name string) *dto.Histogram {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range families {
		if mf.GetName() == name {
			require.Len(t, mf.GetMetric(), 1)

			return mf.GetMetric()[0].GetHistogram()
		}
	}
	t.Fatalf("metric family %q not found", name)

	return nil
}

func TestNilMetricsMethodsAreNoOps(t *testing.T) {
	var m *Metrics

	assert.NotPanics(t, func() {
		m.ObserveLag(time.Second)
		m.ObserveWriteDuration(time.Second)
		m.ObserveFetchBatchSize(100)
	})
}

func TestNewRegistersAllHistograms(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m, err := New(reg, defaultConfig())
	require.NoError(t, err)

	m.ObserveLag(5 * time.Millisecond)
	m.ObserveWriteDuration(3 * time.Millisecond)
	m.ObserveFetchBatchSize(100)

	for _, name := range []string{
		"qdb_nats_connector_message_lag_seconds",
		"qdb_nats_connector_write_duration_seconds",
		"qdb_nats_connector_fetch_batch_size",
	} {
		h := histogramFor(t, reg, name)
		assert.Equal(t, uint64(1), h.GetSampleCount(), name)
	}
}

func TestNewRejectsDuplicateRegistration(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	_, err := New(reg, defaultConfig())
	require.NoError(t, err)

	_, err = New(reg, defaultConfig())
	assert.Error(t, err)
}

func TestObserveLagClampsNegative(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m, err := New(reg, defaultConfig())
	require.NoError(t, err)

	m.ObserveLag(-time.Second)

	h := histogramFor(t, reg, "qdb_nats_connector_message_lag_seconds")
	assert.Equal(t, uint64(1), h.GetSampleCount())
	assert.Equal(t, float64(0), h.GetSampleSum())
}

func TestBucketConfigSeries(t *testing.T) {
	b := BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 3}

	assert.Equal(t, []float64{0.001, 0.002, 0.004}, b.buckets())
}

func TestFetchBatchSizeBucketsFixed(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m, err := New(reg, defaultConfig())
	require.NoError(t, err)

	m.ObserveFetchBatchSize(1)

	h := histogramFor(t, reg, "qdb_nats_connector_fetch_batch_size")
	require.Len(t, h.GetBucket(), 12)
	assert.Equal(t, float64(1), h.GetBucket()[0].GetUpperBound())
	assert.Equal(t, float64(2048), h.GetBucket()[11].GetUpperBound())
}

func TestLagSnapshotNilReceiver(t *testing.T) {
	var m *Metrics

	_, ok := m.LagSnapshot()
	assert.False(t, ok)
}

func TestLagSnapshotMatchesObservations(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m, err := New(reg, Config{
		Lag:   BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 3},
		Write: BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 20},
	})
	require.NoError(t, err)

	// Buckets: 0.001, 0.002, 0.004. Observe one value per bucket.
	m.ObserveLag(time.Millisecond)     // <= 0.001
	m.ObserveLag(2 * time.Millisecond) // <= 0.002
	m.ObserveLag(3 * time.Millisecond) // <= 0.004

	snap, ok := m.LagSnapshot()
	require.True(t, ok)

	assert.Equal(t, uint64(3), snap.Count)
	assert.InDelta(t, 0.006, snap.Sum, 1e-9)
	require.Len(t, snap.Buckets, 3)
	assert.Equal(t, []BucketCount{
		{UpperBound: 0.001, CumulativeCount: 1},
		{UpperBound: 0.002, CumulativeCount: 2},
		{UpperBound: 0.004, CumulativeCount: 3},
	}, snap.Buckets)
}

func TestLagSnapshotOverflowOnlyRaisesCount(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m, err := New(reg, Config{
		Lag:   BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 3},
		Write: BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 20},
	})
	require.NoError(t, err)

	m.ObserveLag(time.Second) // past the 0.004 top bound -> implicit +Inf

	snap, ok := m.LagSnapshot()
	require.True(t, ok)

	assert.Equal(t, uint64(1), snap.Count)
	for _, b := range snap.Buckets {
		assert.Equal(t, uint64(0), b.CumulativeCount)
	}
}

func TestMetricsLintClean(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m, err := New(reg, defaultConfig())
	require.NoError(t, err)
	require.NoError(t, RegisterBuildInfo(reg, "1.2.3", "abc"))

	m.ObserveLag(time.Millisecond)

	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	assert.Empty(t, problems)
}

func TestRegisterBuildInfo(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, RegisterBuildInfo(reg, "3.15.0.dev0", "deadbeef"))

	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)

	mf := families[0]
	assert.Equal(t, "qdb_nats_connector_build_info", mf.GetName())
	require.Len(t, mf.GetMetric(), 1)

	metric := mf.GetMetric()[0]
	assert.Equal(t, float64(1), metric.GetGauge().GetValue())

	labels := map[string]string{}
	for _, lp := range metric.GetLabel() {
		labels[lp.GetName()] = lp.GetValue()
	}
	assert.Equal(t, "3.15.0.dev0", labels["version"])
	assert.Equal(t, "deadbeef", labels["commit"])
}
