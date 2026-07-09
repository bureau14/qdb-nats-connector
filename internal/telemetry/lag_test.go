// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
package telemetry

import (
	"testing"

	"github.com/bureau14/qdb-nats-connector/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// snapshotOf builds a cumulative snapshot from per-bucket interval
// counts plus an +Inf tail count, over the given finite bounds.
func snapshotOf(bounds []float64, counts []uint64, infTail uint64) metrics.HistogramSnapshot {
	snap := metrics.HistogramSnapshot{
		Buckets: make([]metrics.BucketCount, len(bounds)),
	}

	var cumulative uint64
	for i, bound := range bounds {
		cumulative += counts[i]
		snap.Buckets[i] = metrics.BucketCount{
			UpperBound:      bound,
			CumulativeCount: cumulative,
		}
	}
	snap.Count = cumulative + infTail

	return snap
}

func TestLagIntervalAllInOneBucket(t *testing.T) {
	bounds := []float64{0.001, 0.002, 0.004}
	prev := snapshotOf(bounds, []uint64{0, 0, 0}, 0)
	cur := snapshotOf(bounds, []uint64{0, 10, 0}, 0)

	s, ok := lagInterval(prev, cur)
	require.True(t, ok)

	assert.Equal(t, uint64(10), s.Samples)
	// All samples in (0.001, 0.002]: quantiles interpolate inside it.
	assert.Greater(t, s.P50, 0.001)
	assert.LessOrEqual(t, s.P50, 0.002)
	assert.Greater(t, s.P99, s.P50)
	assert.LessOrEqual(t, s.P99, 0.002)
	assert.Equal(t, 0.002, s.MaxLE)
}

func TestLagIntervalMidBucketInterpolation(t *testing.T) {
	bounds := []float64{0.001, 0.002}
	// 1 sample <= 0.001, 1 sample in (0.001, 0.002].
	prev := snapshotOf(bounds, []uint64{0, 0}, 0)
	cur := snapshotOf(bounds, []uint64{1, 1}, 0)

	s, ok := lagInterval(prev, cur)
	require.True(t, ok)

	// target rank for p50 = 1.0 -> exactly fills the first bucket.
	assert.InDelta(t, 0.001, s.P50, 1e-9)
	// p99 target = 1.98 -> 0.98 into the second bucket.
	assert.InDelta(t, 0.001+0.001*0.98, s.P99, 1e-9)
	assert.Equal(t, 0.002, s.MaxLE)
}

func TestLagIntervalFirstBucketInterpolatesFromZero(t *testing.T) {
	bounds := []float64{0.004}
	prev := snapshotOf(bounds, []uint64{0}, 0)
	cur := snapshotOf(bounds, []uint64{2}, 0)

	s, ok := lagInterval(prev, cur)
	require.True(t, ok)

	// p50 target = 1 of 2 -> halfway from 0 to 0.004.
	assert.InDelta(t, 0.002, s.P50, 1e-9)
}

func TestLagIntervalOverflowUsesLargestFiniteBound(t *testing.T) {
	bounds := []float64{0.001, 0.002}
	prev := snapshotOf(bounds, []uint64{0, 0}, 0)
	cur := snapshotOf(bounds, []uint64{0, 0}, 5) // all in +Inf

	s, ok := lagInterval(prev, cur)
	require.True(t, ok)

	assert.Equal(t, 0.002, s.P50)
	assert.Equal(t, 0.002, s.P99)
	assert.Equal(t, 0.002, s.MaxLE)
}

func TestLagIntervalZeroSamples(t *testing.T) {
	bounds := []float64{0.001}
	snap := snapshotOf(bounds, []uint64{7}, 1)

	_, ok := lagInterval(snap, snap)
	assert.False(t, ok)
}

func TestLagIntervalMismatchedLayout(t *testing.T) {
	prev := snapshotOf([]float64{0.001}, []uint64{0}, 0)
	cur := snapshotOf([]float64{0.001, 0.002}, []uint64{1, 1}, 0)

	_, ok := lagInterval(prev, cur)
	assert.False(t, ok)

	shifted := snapshotOf([]float64{0.005}, []uint64{1}, 0)
	_, ok = lagInterval(prev, shifted)
	assert.False(t, ok)
}

func TestLagIntervalRegressedCounts(t *testing.T) {
	bounds := []float64{0.001}
	prev := snapshotOf(bounds, []uint64{5}, 0)
	cur := snapshotOf(bounds, []uint64{2}, 0)

	_, ok := lagInterval(prev, cur)
	assert.False(t, ok)
}

func TestLagQuantilesMonotoneAndBounded(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		bucketCount := rapid.IntRange(1, 8).Draw(rt, "buckets")
		bounds := make([]float64, bucketCount)
		bound := rapid.Float64Range(0.0001, 0.01).Draw(rt, "start")
		for i := range bounds {
			bounds[i] = bound
			bound *= 2
		}

		counts := make([]uint64, bucketCount+1)
		var total uint64
		for i := range counts {
			counts[i] = rapid.Uint64Range(0, 100).Draw(rt, "count")
			total += counts[i]
		}
		if total == 0 {
			counts[0] = 1
		}

		prev := metrics.HistogramSnapshot{Buckets: make([]metrics.BucketCount, bucketCount)}
		for i := range prev.Buckets {
			prev.Buckets[i].UpperBound = bounds[i]
		}
		cur := snapshotOf(bounds, counts[:bucketCount], counts[bucketCount])

		s, ok := lagInterval(prev, cur)
		require.True(rt, ok)

		assert.LessOrEqual(rt, s.P50, s.P99)
		assert.LessOrEqual(rt, s.P99, bounds[bucketCount-1])
		assert.LessOrEqual(rt, s.MaxLE, bounds[bucketCount-1])
		assert.GreaterOrEqual(rt, s.P50, 0.0)
	})
}
