package metrics

import (
	"math"
	"sort"
	"time"
)

var defaultDurationBuckets = []time.Duration{
	0, time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond,
	time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second,
	30 * time.Second, time.Minute, 5 * time.Minute, time.Hour, 24 * time.Hour,
}

type durationHistogram struct {
	bounds []time.Duration
	counts []uint64
	count  uint64
	total  time.Duration
	min    time.Duration
	max    time.Duration
}

func newDurationHistogram() durationHistogram {
	bounds := append([]time.Duration(nil), defaultDurationBuckets...)
	return durationHistogram{bounds: bounds, counts: make([]uint64, len(bounds)+1)}
}

func (h *durationHistogram) observe(value time.Duration) {
	index := sort.Search(len(h.bounds), func(i int) bool { return value <= h.bounds[i] })
	h.counts[index]++
	h.count++
	h.total += value
	if h.count == 1 || value < h.min {
		h.min = value
	}
	if h.count == 1 || value > h.max {
		h.max = value
	}
}

func (h durationHistogram) snapshot() DurationDistribution {
	if h.count == 0 {
		return DurationDistribution{}
	}
	return DurationDistribution{
		Count: h.count,
		Min:   h.min,
		Max:   h.max,
		Mean:  time.Duration(int64(h.total) / int64(h.count)),
		P50:   h.quantile(0.50),
		P95:   h.quantile(0.95),
		P99:   h.quantile(0.99),
	}
}

func (h durationHistogram) quantile(q float64) time.Duration {
	target := uint64(math.Ceil(q * float64(h.count)))
	var cumulative uint64
	for index, count := range h.counts {
		cumulative += count
		if cumulative < target {
			continue
		}
		if index == len(h.bounds) {
			return h.max
		}
		return h.bounds[index]
	}
	return h.max
}
