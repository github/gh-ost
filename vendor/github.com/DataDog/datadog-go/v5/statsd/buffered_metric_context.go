package statsd

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// bufferedMetricContexts represent the contexts for Histograms, Distributions
// and Timing. Since those 3 metric types behave the same way and are sampled
// with the same type they're represented by the same class.
type bufferedMetricContexts struct {
	nbContext uint64
	mutex     sync.RWMutex
	values    bufferedMetricMap
	newMetric func(string, float64, string, float64, Cardinality) *bufferedMetric

	// Each bufferedMetricContexts uses its own random source and random
	// lock to prevent goroutines from contending for the lock on the
	// "math/rand" package-global random source (e.g. calls like
	// "rand.Float64()" must acquire a shared lock to get the next
	// pseudorandom number).
	random     *rand.Rand
	randomLock sync.Mutex
}

func newBufferedContexts(newMetric func(string, float64, string, int64, float64, Cardinality) *bufferedMetric, maxSamples int64) bufferedMetricContexts {
	return bufferedMetricContexts{
		values: bufferedMetricMap{},
		newMetric: func(name string, value float64, stringTags string, rate float64, cardinality Cardinality) *bufferedMetric {
			return newMetric(name, value, stringTags, maxSamples, rate, cardinality)
		},
		// Note that calling "time.Now().UnixNano()" repeatedly quickly may return
		// very similar values. That's fine for seeding the worker-specific random
		// source because we just need an evenly distributed stream of float values.
		// Do not use this random source for cryptographic randomness.
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (bc *bufferedMetricContexts) flush(metrics []metric) []metric {
	bc.mutex.Lock()
	values := bc.values
	bc.values = bufferedMetricMap{}
	bc.mutex.Unlock()

	for _, d := range values {
		d.Lock()
		metrics = append(metrics, d.flushUnsafe())
		d.Unlock()
	}
	atomic.AddUint64(&bc.nbContext, uint64(len(values)))
	return metrics
}

func (bc *bufferedMetricContexts) sample(name string, value float64, tags []string, rate float64, cardinality Cardinality) error {
	keepingSample := shouldSample(rate, bc.random, &bc.randomLock)

	// If we don't keep the sample, return early. If we do keep the sample
	// we end up storing the *first* observed sampling rate in the metric.
	// This is the *wrong* behavior but it's the one we had before and the alternative would increase lock contention too
	// much with the current code.
	// TODO: change this behavior in the future, probably by introducing thread-local storage and lockless stuctures.
	// If this code is removed, also remove the observed sampling rate in the metric and fix `bufferedMetric.flushUnsafe()`
	if !keepingSample {
		return nil
	}

	context, stringTags := getContextAndTags(name, tags, cardinality)
	var v *bufferedMetric

	bc.mutex.RLock()
	v, _ = bc.values[context]
	bc.mutex.RUnlock()

	// Create it if it wasn't found
	if v == nil {
		bc.mutex.Lock()
		// It might have been created by another goroutine since last call
		v, _ = bc.values[context]
		if v == nil {
			// If we might keep a sample that we should have skipped, but that should not drastically affect performances.
			bc.values[context] = bc.newMetric(name, value, stringTags, rate, cardinality)
			// We added a new value, we need to unlock the mutex and quit
			bc.mutex.Unlock()
			return nil
		}
		bc.mutex.Unlock()
	}

	// Now we can keep the sample or skip it
	if keepingSample {
		v.maybeKeepSample(value, bc.random, &bc.randomLock)
	} else {
		v.skipSample()
	}

	return nil
}

func (bc *bufferedMetricContexts) getNbContext() uint64 {
	return atomic.LoadUint64(&bc.nbContext)
}
