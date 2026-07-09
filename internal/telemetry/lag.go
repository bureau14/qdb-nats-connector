// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Interval lag estimation: diffs two cumulative lag-histogram snapshots
// (the same cells /metrics serves) into per-interval bucket counts and
// estimates quantiles by linear interpolation within bucket bounds --
// the histogram_quantile approach, applied to an interval delta. All
// values are estimates bounded by the bucket layout; with the default
// factor-2.0 buckets they are within 2x of truth.
// Types: lagSummary
// Ex: s, ok := lagInterval(prev, cur) -> {Samples: 42, P50: 0.003, ...}
package telemetry

import (
	"github.com/bureau14/qdb-nats-connector/internal/metrics"
)

// lagSummary: interval lag distribution estimate from bucket deltas.
// MaxLE is the upper bound of the highest non-empty bucket -- a
// bucket-bound estimate, not an exact maximum.
type lagSummary struct {
	Samples uint64
	P50     float64
	P99     float64
	MaxLE   float64
}

// bucketDeltas diffs two cumulative snapshots into per-bucket interval
// counts, including the implicit +Inf tail as the final element.
// In: prev, cur metrics.HistogramSnapshot - consecutive snapshots of
// the same histogram
// Out: []uint64, bool - len(cur.Buckets)+1 counts; false on layout
// mismatch or a non-monotonic pair (defensive: never true in-process)
// Ex: bucketDeltas(prev, cur) -> [0 3 1 0], true
func bucketDeltas(prev, cur metrics.HistogramSnapshot) ([]uint64, bool) {
	if len(prev.Buckets) != len(cur.Buckets) || cur.Count < prev.Count {
		return nil, false
	}

	counts := make([]uint64, len(cur.Buckets)+1)
	var prevCumulative uint64
	for i, b := range cur.Buckets {
		p := prev.Buckets[i]
		if p.UpperBound != b.UpperBound || b.CumulativeCount < p.CumulativeCount {
			return nil, false
		}

		cumulative := b.CumulativeCount - p.CumulativeCount
		if cumulative < prevCumulative {
			return nil, false
		}
		counts[i] = cumulative - prevCumulative
		prevCumulative = cumulative
	}

	total := cur.Count - prev.Count
	if total < prevCumulative {
		return nil, false
	}
	counts[len(cur.Buckets)] = total - prevCumulative

	return counts, true
}

// bucketQuantile estimates quantile q by linear interpolation within
// the bucket holding the target rank. A rank landing in the +Inf tail
// returns the largest finite bound (histogram_quantile behavior).
// In: q float64 - quantile in (0, 1], bounds []float64 - ascending
// finite bucket bounds, counts []uint64 - per-bucket interval counts,
// len(bounds)+1 with the +Inf tail last
// Out: float64 - estimated quantile; 0 when counts are all zero
// Ex: bucketQuantile(0.5, [0.001 0.002], [1 1 0]) -> 0.001
func bucketQuantile(q float64, bounds []float64, counts []uint64) float64 {
	var total uint64
	for _, c := range counts {
		total += c
	}
	if total == 0 || len(bounds) == 0 {
		return 0
	}

	target := q * float64(total)
	var cumulative float64
	for i, c := range counts {
		if c == 0 {
			continue
		}

		next := cumulative + float64(c)
		if next >= target {
			if i >= len(bounds) {
				return bounds[len(bounds)-1]
			}

			lower := 0.0
			if i > 0 {
				lower = bounds[i-1]
			}

			return lower + (bounds[i]-lower)*(target-cumulative)/float64(c)
		}
		cumulative = next
	}

	return bounds[len(bounds)-1]
}

// maxBound returns the upper bound of the highest non-empty bucket; a
// non-empty +Inf tail maps to the largest finite bound.
// In: bounds []float64 - ascending finite bucket bounds, counts
// []uint64 - per-bucket interval counts incl. the +Inf tail
// Out: float64 - bucket-bound estimate of the interval maximum
// Ex: maxBound([0.001 0.002], [1 0 0]) -> 0.001
func maxBound(bounds []float64, counts []uint64) float64 {
	for i := len(counts) - 1; i >= 0; i-- {
		if counts[i] == 0 {
			continue
		}
		if i >= len(bounds) {
			return bounds[len(bounds)-1]
		}

		return bounds[i]
	}

	return 0
}

// lagInterval diffs two cumulative snapshots into an interval summary.
// In: prev, cur metrics.HistogramSnapshot - consecutive snapshots of
// the lag histogram
// Out: lagSummary, bool - false on layout mismatch or zero interval
// samples (an idle interval has no distribution to summarize)
// Ex: lagInterval(prev, cur) -> {Samples: 42, P50: 0.003, ...}, true
func lagInterval(prev, cur metrics.HistogramSnapshot) (lagSummary, bool) {
	counts, ok := bucketDeltas(prev, cur)
	if !ok {
		return lagSummary{}, false
	}

	samples := cur.Count - prev.Count
	if samples == 0 {
		return lagSummary{}, false
	}

	bounds := make([]float64, len(cur.Buckets))
	for i, b := range cur.Buckets {
		bounds[i] = b.UpperBound
	}

	return lagSummary{
		Samples: samples,
		P50:     bucketQuantile(0.50, bounds, counts),
		P99:     bucketQuantile(0.99, bounds, counts),
		MaxLE:   maxBound(bounds, counts),
	}, true
}
