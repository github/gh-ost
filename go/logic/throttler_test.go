/*
   Copyright 2022 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/github/gh-ost/go/base"
)

func newTestThrottler() *Throttler {
	migrationContext := base.NewMigrationContext()
	return NewThrottler(migrationContext, NewApplier(migrationContext), NewInspector(migrationContext), "test")
}

func TestThrottleReturnsWhenNotThrottled(t *testing.T) {
	thlr := newTestThrottler()
	// not throttled by default — should return immediately
	done := make(chan struct{})
	go func() {
		thlr.throttle(nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("throttle() did not return when not throttled")
	}
}

func TestThrottleBlocksWhileThrottled(t *testing.T) {
	thlr := newTestThrottler()
	thlr.migrationContext.SetThrottled(true, "test", base.NoThrottleReasonHint)

	done := make(chan struct{})
	go func() {
		thlr.throttle(nil)
		close(done)
	}()

	// should still be blocking after 300ms
	select {
	case <-done:
		t.Fatal("throttle() returned while still throttled")
	case <-time.After(300 * time.Millisecond):
	}

	// unthrottle — should unblock within the next 250ms sleep cycle
	thlr.migrationContext.SetThrottled(false, "", base.NoThrottleReasonHint)
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("throttle() did not return after unthrottling")
	}
}

func TestThrottleReturnsOnContextCancellation(t *testing.T) {
	thlr := newTestThrottler()
	thlr.migrationContext.SetThrottled(true, "test", base.NoThrottleReasonHint)

	done := make(chan struct{})
	go func() {
		thlr.throttle(nil)
		close(done)
	}()

	// should be blocked
	select {
	case <-done:
		t.Fatal("throttle() returned before context was cancelled")
	case <-time.After(300 * time.Millisecond):
	}

	thlr.migrationContext.CancelContext()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("throttle() did not return after context cancellation")
	}
}

func TestThrottleCallsOnThrottledCallback(t *testing.T) {
	thlr := newTestThrottler()
	thlr.migrationContext.SetThrottled(true, "test", base.NoThrottleReasonHint)

	var callCount atomic.Int32
	done := make(chan struct{})
	go func() {
		thlr.throttle(func() { callCount.Add(1) })
		close(done)
	}()

	// wait long enough for at least two callback invocations
	time.Sleep(700 * time.Millisecond)
	assert.GreaterOrEqual(t, callCount.Load(), int32(2))

	thlr.migrationContext.CancelContext()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("throttle() did not return after context cancellation")
	}
}

type throttleMetricsSpy struct {
	gaugeNames      []string
	gaugeValues     []float64
	histogramNames  []string
	histogramValues []float64
	countNames      []string
	countValues     []int64
	tags            [][]string
}

func (s *throttleMetricsSpy) Gauge(name string, value float64, tags ...string) {
	s.gaugeNames = append(s.gaugeNames, name)
	s.gaugeValues = append(s.gaugeValues, value)
	s.tags = append(s.tags, append([]string(nil), tags...))
}

func (s *throttleMetricsSpy) Count(name string, value int64, tags ...string) {
	s.countNames = append(s.countNames, name)
	s.countValues = append(s.countValues, value)
	s.tags = append(s.tags, append([]string(nil), tags...))
}

func (s *throttleMetricsSpy) Histogram(name string, value float64, tags ...string) {
	s.histogramNames = append(s.histogramNames, name)
	s.histogramValues = append(s.histogramValues, value)
	s.tags = append(s.tags, append([]string(nil), tags...))
}

func TestRecordThrottleMetricsEmitsOneIntervalMetricOnThrottleExit(t *testing.T) {
	thlr := newTestThrottler()
	spy := &throttleMetricsSpy{}
	thlr.migrationContext.Metrics = spy
	startedAt := time.Unix(100, 0)

	thlr.recordThrottleMetrics(false, true, "commanded by user", startedAt)
	thlr.recordThrottleMetrics(true, true, "commanded by user", startedAt.Add(500*time.Millisecond))
	thlr.recordThrottleMetrics(true, true, "commanded by user", startedAt.Add(1100*time.Millisecond))
	thlr.recordThrottleMetrics(true, false, "", startedAt.Add(2500*time.Millisecond))

	require.Equal(t, []string{"throttle.active", "throttle.active", "throttle.active"}, spy.gaugeNames)
	require.Equal(t, []float64{1, 1, 0}, spy.gaugeValues)
	require.Equal(t, []string{"throttle.duration_milliseconds"}, spy.histogramNames)
	require.Equal(t, []float64{2500}, spy.histogramValues)
	require.Equal(t, []string{"throttle.events_total"}, spy.countNames)
	require.Equal(t, []int64{1}, spy.countValues)
	require.Len(t, spy.tags, 5)
	assert.Equal(t, []string{"reason:commanded by user"}, spy.tags[3])
	assert.Equal(t, []string{"reason:commanded by user"}, spy.tags[4])
}
