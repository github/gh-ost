package statsd

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)

/*
Those are metrics type that can be aggregated on the client side:
  - Gauge
  - Count
  - Set
*/

type countMetric struct {
	value       int64
	name        string
	tags        []string
	cardinality Cardinality
}

func newCountMetric(name string, value int64, tags []string, cardinality Cardinality) *countMetric {
	return &countMetric{
		value:       value,
		name:        name,
		tags:        copySlice(tags),
		cardinality: cardinality,
	}
}

func (c *countMetric) sample(v int64) {
	atomic.AddInt64(&c.value, v)
}

func (c *countMetric) flushUnsafe() metric {
	return metric{
		metricType:  count,
		name:        c.name,
		tags:        c.tags,
		rate:        1,
		ivalue:      c.value,
		cardinality: c.cardinality,
	}
}

// Gauge

type gaugeMetric struct {
	value       uint64
	name        string
	tags        []string
	cardinality Cardinality
}

func newGaugeMetric(name string, value float64, tags []string, cardinality Cardinality) *gaugeMetric {
	return &gaugeMetric{
		value:       math.Float64bits(value),
		name:        name,
		tags:        copySlice(tags),
		cardinality: cardinality,
	}
}

func (g *gaugeMetric) sample(v float64) {
	atomic.StoreUint64(&g.value, math.Float64bits(v))
}

func (g *gaugeMetric) flushUnsafe() metric {
	return metric{
		metricType:  gauge,
		name:        g.name,
		tags:        g.tags,
		rate:        1,
		fvalue:      math.Float64frombits(g.value),
		cardinality: g.cardinality,
	}
}

// Set

type setMetric struct {
	data        map[string]struct{}
	name        string
	tags        []string
	cardinality Cardinality
	sync.Mutex
}

func newSetMetric(name string, value string, tags []string, cardinality Cardinality) *setMetric {
	set := &setMetric{
		data:        map[string]struct{}{},
		name:        name,
		tags:        copySlice(tags),
		cardinality: cardinality,
	}
	set.data[value] = struct{}{}
	return set
}

func (s *setMetric) sample(v string) {
	s.Lock()
	defer s.Unlock()
	s.data[v] = struct{}{}
}

// Sets are aggregated on the agent side too. We flush the keys so a set from
// multiple application can be correctly aggregated on the agent side.
func (s *setMetric) flushUnsafe() []metric {
	if len(s.data) == 0 {
		return nil
	}

	metrics := make([]metric, len(s.data))
	i := 0
	for value := range s.data {
		metrics[i] = metric{
			metricType:  set,
			name:        s.name,
			tags:        s.tags,
			rate:        1,
			svalue:      value,
			cardinality: s.cardinality,
		}
		i++
	}
	return metrics
}

// Histograms, Distributions and Timings

type bufferedMetric struct {
	sync.Mutex

	// Kept samples (after sampling)
	data []float64
	// Total stored samples (after sampling)
	storedSamples int64
	// Total number of observed samples (before sampling). This is used to keep
	// the sampling rate correct.
	totalSamples int64

	name string
	// Histograms and Distributions store tags as one string since we need
	// to compute its size multiple time when serializing.
	tags  string
	mtype metricType

	// maxSamples is the maximum number of samples we keep in memory
	maxSamples int64

	// The first observed user-specified sample rate. When specified
	// it is used because we don't know better.
	specifiedRate float64

	cardinality Cardinality
}

func (s *bufferedMetric) sample(v float64) {
	s.Lock()
	defer s.Unlock()
	s.sampleUnsafe(v)
}

func (s *bufferedMetric) sampleUnsafe(v float64) {
	s.data = append(s.data, v)
	s.storedSamples++
	// Total samples needs to be incremented though an atomic because it can be accessed without the lock.
	atomic.AddInt64(&s.totalSamples, 1)
}

func (s *bufferedMetric) maybeKeepSample(v float64, rand *rand.Rand, randLock *sync.Mutex) {
	s.Lock()
	defer s.Unlock()
	if s.maxSamples > 0 {
		if s.storedSamples >= s.maxSamples {
			// We reached the maximum number of samples we can keep in memory, so we randomly
			// replace a sample.
			randLock.Lock()
			i := rand.Int63n(atomic.LoadInt64(&s.totalSamples))
			randLock.Unlock()
			if i < s.maxSamples {
				s.data[i] = v
			}
		} else {
			s.data[s.storedSamples] = v
			s.storedSamples++
		}
		s.totalSamples++
	} else {
		// This code path appends to the slice since we did not pre-allocate memory in this case.
		s.sampleUnsafe(v)
	}
}

func (s *bufferedMetric) skipSample() {
	atomic.AddInt64(&s.totalSamples, 1)
}

func (s *bufferedMetric) flushUnsafe() metric {
	totalSamples := atomic.LoadInt64(&s.totalSamples)
	var rate float64

	// If the user had a specified rate send it because we don't know better.
	// This code should be removed once we can also remove the early return at the top of
	// `bufferedMetricContexts.sample`
	if s.specifiedRate != 1.0 {
		rate = s.specifiedRate
	} else {
		rate = float64(s.storedSamples) / float64(totalSamples)
	}

	return metric{
		metricType:  s.mtype,
		name:        s.name,
		stags:       s.tags,
		rate:        rate,
		fvalues:     s.data[:s.storedSamples],
		cardinality: s.cardinality,
	}
}

type histogramMetric = bufferedMetric

func newHistogramMetric(name string, value float64, stringTags string, maxSamples int64, rate float64, cardinality Cardinality) *histogramMetric {
	return &histogramMetric{
		data:          newData(value, maxSamples),
		totalSamples:  1,
		storedSamples: 1,
		name:          name,
		tags:          stringTags,
		mtype:         histogramAggregated,
		maxSamples:    maxSamples,
		specifiedRate: rate,
		cardinality:   cardinality,
	}
}

type distributionMetric = bufferedMetric

func newDistributionMetric(name string, value float64, stringTags string, maxSamples int64, rate float64, cardinality Cardinality) *distributionMetric {
	return &distributionMetric{
		data:          newData(value, maxSamples),
		totalSamples:  1,
		storedSamples: 1,
		name:          name,
		tags:          stringTags,
		mtype:         distributionAggregated,
		maxSamples:    maxSamples,
		specifiedRate: rate,
		cardinality:   cardinality,
	}
}

type timingMetric = bufferedMetric

func newTimingMetric(name string, value float64, stringTags string, maxSamples int64, rate float64, cardinality Cardinality) *timingMetric {
	return &timingMetric{
		data:          newData(value, maxSamples),
		totalSamples:  1,
		storedSamples: 1,
		name:          name,
		tags:          stringTags,
		mtype:         timingAggregated,
		maxSamples:    maxSamples,
		specifiedRate: rate,
		cardinality:   cardinality,
	}
}

// newData creates a new slice of float64 with the given capacity. If maxSample
// is less than or equal to 0, it returns a slice with the given value as the
// only element.
func newData(value float64, maxSample int64) []float64 {
	if maxSample <= 0 {
		return []float64{value}
	} else {
		data := make([]float64, maxSample)
		data[0] = value
		return data
	}
}
