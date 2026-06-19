/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"testing"
	"time"
)

type gaugeSpy struct {
	names  []string
	values []float64
	tags   [][]string
}

func (g *gaugeSpy) Gauge(name string, value float64, tags ...string) {
	g.names = append(g.names, name)
	g.values = append(g.values, value)
	g.tags = append(g.tags, append([]string(nil), tags...))
}

func (g *gaugeSpy) Count(name string, value int64, tags ...string) {
}

func (g *gaugeSpy) Histogram(name string, value float64, tags ...string) {
}

func TestEmitProgressGauges(t *testing.T) {
	spy := &gaugeSpy{}
	EmitProgressGauges(spy, 1000, 5000, 42)

	wantNames := []string{
		"row_copy.rows_copied",
		"row_copy.rows_estimate",
		"dml.events_applied",
	}
	wantVals := []float64{1000, 5000, 42}

	if len(spy.names) != len(wantNames) {
		t.Fatalf("got %d gauges, want %d", len(spy.names), len(wantNames))
	}
	for i := range wantNames {
		if spy.names[i] != wantNames[i] || spy.values[i] != wantVals[i] {
			t.Fatalf("[%d] got %s=%v want %s=%v", i, spy.names[i], spy.values[i], wantNames[i], wantVals[i])
		}
	}
}

func TestEmitProgressGauges_nilSafe(t *testing.T) {
	EmitProgressGauges(nil, 1, 2, 3)
}

func TestEmitBinlogBacklogGauges(t *testing.T) {
	spy := &gaugeSpy{}
	EmitBinlogBacklogGauges(spy, 250, 1000)

	wantNames := []string{
		"binlog.backlog_size",
		"binlog.backlog_capacity",
		"binlog.backlog_utilization",
	}
	wantVals := []float64{250, 1000, 0.25}

	if len(spy.names) != len(wantNames) {
		t.Fatalf("got %d gauges, want %d", len(spy.names), len(wantNames))
	}
	for i := range wantNames {
		if spy.names[i] != wantNames[i] || spy.values[i] != wantVals[i] {
			t.Fatalf("[%d] got %s=%v want %s=%v", i, spy.names[i], spy.values[i], wantNames[i], wantVals[i])
		}
	}
}

func TestEmitBinlogBacklogGauges_nilSafe(t *testing.T) {
	EmitBinlogBacklogGauges(nil, 1, 2)
}

func TestBinlogBacklogUtilization(t *testing.T) {
	tests := []struct {
		size, capacity int
		want           float64
	}{
		{0, 1000, 0},
		{250, 1000, 0.25},
		{1000, 1000, 1},
		{1500, 1000, 1},
		{-1, 1000, 0},
		{10, 0, 0},
	}
	for _, tt := range tests {
		got := binlogBacklogUtilization(tt.size, tt.capacity)
		if got != tt.want {
			t.Fatalf("utilization(%d, %d) = %v, want %v", tt.size, tt.capacity, got, tt.want)
		}
	}
}

func TestEmitLagGauges_notThrottled(t *testing.T) {
	spy := &gaugeSpy{}
	EmitLagGauges(spy, 2.5, 1.25, false)

	wantNames := []string{"lag.replication_seconds", "lag.heartbeat_seconds"}
	wantVals := []float64{2.5, 1.25}
	wantTags := []string{"throttled:false"}

	if len(spy.names) != len(wantNames) {
		t.Fatalf("got %d gauges, want %d", len(spy.names), len(wantNames))
	}
	for i := range wantNames {
		if spy.names[i] != wantNames[i] || spy.values[i] != wantVals[i] {
			t.Fatalf("[%d] got %s=%v want %s=%v", i, spy.names[i], spy.values[i], wantNames[i], wantVals[i])
		}
		if len(spy.tags[i]) != 1 || spy.tags[i][0] != wantTags[0] {
			t.Fatalf("[%d] got tags %v want [%s]", i, spy.tags[i], wantTags[0])
		}
	}
}

func TestEmitLagGauges_throttled(t *testing.T) {
	spy := &gaugeSpy{}
	EmitLagGauges(spy, 4.0, 3.0, true)

	if len(spy.names) != 2 {
		t.Fatalf("got %d gauges, want 2", len(spy.names))
	}
	for i := range spy.names {
		if len(spy.tags[i]) != 1 || spy.tags[i][0] != "throttled:true" {
			t.Fatalf("[%d] got tags %v want [throttled:true]", i, spy.tags[i])
		}
	}
}

func TestEmitLagGauges_nilSafe(t *testing.T) {
	EmitLagGauges(nil, 1, 2, false)
}

func TestEmitGoRuntimeGauges(t *testing.T) {
	spy := &gaugeSpy{}
	m := &runtime.MemStats{
		Alloc:        100,
		Sys:          200,
		HeapInuse:    300,
		NumGC:        7,
		PauseTotalNs: 42,
	}
	EmitGoRuntimeGauges(spy, m, 123)

	wantNames := []string{
		"go_runtime.alloc_bytes",
		"go_runtime.sys_bytes",
		"go_runtime.heap_inuse_bytes",
		"go_runtime.num_gc",
		"go_runtime.gc_pause_total_ns",
		"go_runtime.goroutines",
	}
	wantVals := []float64{100, 200, 300, 7, 42, 123}

	if len(spy.names) != len(wantNames) {
		t.Fatalf("got %d gauges, want %d", len(spy.names), len(wantNames))
	}
	for i := range wantNames {
		if spy.names[i] != wantNames[i] || spy.values[i] != wantVals[i] {
			t.Fatalf("[%d] got %s=%v want %s=%v", i, spy.names[i], spy.values[i], wantNames[i], wantVals[i])
		}
	}
}

func TestEmitGoRuntimeGauges_nilSafe(t *testing.T) {
	EmitGoRuntimeGauges(nil, &runtime.MemStats{}, 1)
	EmitGoRuntimeGauges(&gaugeSpy{}, nil, 1)
}

func TestStartGoRuntimeReporter_stopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{} // sd nil, so the reporter should not start.
	StartGoRuntimeReporter(ctx, c, time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)
}

type throttleSpy struct {
	gaugeNames      []string
	gaugeValues     []float64
	histogramNames  []string
	histogramValues []float64
	countNames      []string
	countValues     []int64
	tags            [][]string
}

func (s *throttleSpy) Gauge(name string, value float64, tags ...string) {
	s.gaugeNames = append(s.gaugeNames, name)
	s.gaugeValues = append(s.gaugeValues, value)
	s.tags = append(s.tags, append([]string(nil), tags...))
}

func (s *throttleSpy) Count(name string, value int64, tags ...string) {
	s.countNames = append(s.countNames, name)
	s.countValues = append(s.countValues, value)
	s.tags = append(s.tags, append([]string(nil), tags...))
}

func (s *throttleSpy) Histogram(name string, value float64, tags ...string) {
	s.histogramNames = append(s.histogramNames, name)
	s.histogramValues = append(s.histogramValues, value)
	s.tags = append(s.tags, append([]string(nil), tags...))
}

func TestEmitThrottleActiveGauge(t *testing.T) {
	spy := &throttleSpy{}

	EmitThrottleActiveGauge(spy, true)
	EmitThrottleActiveGauge(spy, false)

	if len(spy.gaugeNames) != 2 {
		t.Fatalf("got %d gauges, want 2", len(spy.gaugeNames))
	}
	if spy.gaugeNames[0] != "throttle.active" || spy.gaugeValues[0] != 1 {
		t.Fatalf("got %s=%v, want throttle.active=1", spy.gaugeNames[0], spy.gaugeValues[0])
	}
	if spy.gaugeNames[1] != "throttle.active" || spy.gaugeValues[1] != 0 {
		t.Fatalf("got %s=%v, want throttle.active=0", spy.gaugeNames[1], spy.gaugeValues[1])
	}
}

func TestEmitThrottleInterval(t *testing.T) {
	spy := &throttleSpy{}

	EmitThrottleInterval(spy, 1500*time.Millisecond, "commanded by user")

	if len(spy.histogramNames) != 1 {
		t.Fatalf("got %d histograms, want 1", len(spy.histogramNames))
	}
	if spy.histogramNames[0] != "throttle.duration_milliseconds" || spy.histogramValues[0] != 1500 {
		t.Fatalf("got %s=%v, want throttle.duration_milliseconds=1500", spy.histogramNames[0], spy.histogramValues[0])
	}
	if len(spy.countNames) != 1 {
		t.Fatalf("got %d counts, want 1", len(spy.countNames))
	}
	if spy.countNames[0] != "throttle.events_total" || spy.countValues[0] != 1 {
		t.Fatalf("got %s=%v, want throttle.events_total=1", spy.countNames[0], spy.countValues[0])
	}
	if len(spy.tags) != 2 || len(spy.tags[0]) != 1 || spy.tags[0][0] != "reason:commanded by user" ||
		len(spy.tags[1]) != 1 || spy.tags[1][0] != "reason:commanded by user" {
		t.Fatalf("got tags %v, want [reason:commanded by user]", spy.tags)
	}
}

func TestEmitThrottleIntervalNilSafe(t *testing.T) {
	EmitThrottleActiveGauge(nil, true)
	EmitThrottleInterval(nil, time.Second, "test")
	EmitThrottleInterval(&gaugeSpy{}, time.Second, "test")
}

type cutOverSpy struct {
	histogramNames  []string
	histogramValues []float64
	histogramTags   [][]string
	countNames      []string
	countValues     []int64
	countTags       [][]string
}

func (s *cutOverSpy) Gauge(_ string, _ float64, _ ...string) {}

func (s *cutOverSpy) Histogram(name string, value float64, tags ...string) {
	s.histogramNames = append(s.histogramNames, name)
	s.histogramValues = append(s.histogramValues, value)
	s.histogramTags = append(s.histogramTags, tags)
}

func (s *cutOverSpy) Count(name string, value int64, tags ...string) {
	s.countNames = append(s.countNames, name)
	s.countValues = append(s.countValues, value)
	s.countTags = append(s.countTags, tags)
}

func TestRecordCutOverMetrics(t *testing.T) {
	spy := &cutOverSpy{}

	RecordCutOverPhase(spy, CutOverPhaseMagicLock, 1500*time.Millisecond, nil)
	RecordCutOverAttempt(spy, CutOverOutcomeSuccess)
	RecordCutOverTotal(spy, 2*time.Second, errors.New("boom"))

	if len(spy.histogramNames) != 2 {
		t.Fatalf("got %d histograms, want 2", len(spy.histogramNames))
	}
	if spy.histogramNames[0] != "cut_over.phase_duration_milliseconds" || spy.histogramValues[0] != 1500 {
		t.Fatalf("got first histogram %s=%v", spy.histogramNames[0], spy.histogramValues[0])
	}
	if !slices.Equal(spy.histogramTags[0], []string{"phase:magic_lock", "outcome:success"}) {
		t.Fatalf("got phase tags %#v", spy.histogramTags[0])
	}
	if spy.histogramNames[1] != "cut_over.total_duration_milliseconds" || spy.histogramValues[1] != 2000 {
		t.Fatalf("got second histogram %s=%v", spy.histogramNames[1], spy.histogramValues[1])
	}
	if !slices.Equal(spy.histogramTags[1], []string{"outcome:abort"}) {
		t.Fatalf("got total tags %#v", spy.histogramTags[1])
	}
	if len(spy.countNames) != 1 || spy.countNames[0] != "cut_over.attempts_total" || spy.countValues[0] != 1 {
		t.Fatalf("got counts %#v values %#v", spy.countNames, spy.countValues)
	}
	if !slices.Equal(spy.countTags[0], []string{"outcome:success"}) {
		t.Fatalf("got count tags %#v", spy.countTags[0])
	}
}

func TestRecordCutOverMetricsNilSafe(t *testing.T) {
	RecordCutOverPhase(nil, CutOverPhaseMagicLock, time.Second, nil)
	RecordCutOverAttempt(nil, CutOverOutcomeSuccess)
	RecordCutOverTotal(nil, time.Second, nil)
}

type histogramSpy struct {
	names  []string
	values []float64
	tags   [][]string
}

func (h *histogramSpy) Gauge(_ string, _ float64, _ ...string) {}

func (h *histogramSpy) Count(_ string, _ int64, _ ...string) {}

func (h *histogramSpy) Histogram(name string, value float64, tags ...string) {
	h.names = append(h.names, name)
	h.values = append(h.values, value)
	h.tags = append(h.tags, tags)
}

func TestRecordQueryDuration(t *testing.T) {
	spy := &histogramSpy{}

	RecordQueryDuration(spy, "source", "row_count", 1500*time.Millisecond, nil)
	RecordQueryDuration(spy, "target", "binlog_apply", 2*time.Second, errors.New("boom"))

	if len(spy.names) != 2 {
		t.Fatalf("got %d histograms, want 2", len(spy.names))
	}
	if spy.names[0] != "query.duration_milliseconds" || spy.values[0] != 1500 {
		t.Fatalf("got %s=%v, want query.duration_milliseconds=1500", spy.names[0], spy.values[0])
	}
	if !slices.Equal(spy.tags[0], []string{"side:source", "kind:row_count", "outcome:ok"}) {
		t.Fatalf("got tags %#v", spy.tags[0])
	}
	if spy.values[1] != 2000 || !slices.Equal(spy.tags[1], []string{"side:target", "kind:binlog_apply", "outcome:error"}) {
		t.Fatalf("got second metric value=%v tags=%#v", spy.values[1], spy.tags[1])
	}
}

func TestRecordQueryDurationNilSafe(t *testing.T) {
	RecordQueryDuration(nil, "source", "row_count", time.Second, nil)
	RecordQueryDuration(&histogramSpy{}, "", "row_count", time.Second, nil)
	RecordQueryDuration(&histogramSpy{}, "source", "", time.Second, nil)
	RecordQueryDuration(&histogramSpy{}, "source", "row_count", -time.Second, nil)
}

type sleepSpy struct {
	histogramNames  []string
	histogramValues []float64
	histogramTags   [][]string
	countNames      []string
	countValues     []int64
	countTags       [][]string
}

func (s *sleepSpy) Gauge(_ string, _ float64, _ ...string) {}

func (s *sleepSpy) Histogram(name string, value float64, tags ...string) {
	s.histogramNames = append(s.histogramNames, name)
	s.histogramValues = append(s.histogramValues, value)
	s.histogramTags = append(s.histogramTags, tags)
}

func (s *sleepSpy) Count(name string, value int64, tags ...string) {
	s.countNames = append(s.countNames, name)
	s.countValues = append(s.countValues, value)
	s.countTags = append(s.countTags, tags)
}

func TestRecordSleep(t *testing.T) {
	spy := &sleepSpy{}

	RecordSleep(spy, "retry_backoff", 2*time.Second)

	if len(spy.histogramNames) != 1 {
		t.Fatalf("got %d histograms, want 1", len(spy.histogramNames))
	}
	if spy.histogramNames[0] != "sleep.duration_milliseconds" || spy.histogramValues[0] != 2000 {
		t.Fatalf("got histogram %s=%v, want sleep.duration_milliseconds=2000", spy.histogramNames[0], spy.histogramValues[0])
	}
	if !slices.Equal(spy.histogramTags[0], []string{"stage:retry_backoff"}) {
		t.Fatalf("got histogram tags %#v", spy.histogramTags[0])
	}
	if len(spy.countNames) != 1 {
		t.Fatalf("got %d counts, want 1", len(spy.countNames))
	}
	if spy.countNames[0] != "sleep.total_milliseconds" || spy.countValues[0] != 2000 {
		t.Fatalf("got count %s=%v, want sleep.total_milliseconds=2000", spy.countNames[0], spy.countValues[0])
	}
	if !slices.Equal(spy.countTags[0], []string{"stage:retry_backoff"}) {
		t.Fatalf("got count tags %#v", spy.countTags[0])
	}
}

func TestRecordSleepSubSecond(t *testing.T) {
	spy := &sleepSpy{}

	RecordSleep(spy, "replica_wait", 500*time.Millisecond)

	if spy.histogramNames[0] != "sleep.duration_milliseconds" || spy.histogramValues[0] != 500 {
		t.Fatalf("got histogram %s=%v, want sleep.duration_milliseconds=500", spy.histogramNames[0], spy.histogramValues[0])
	}
	if spy.countNames[0] != "sleep.total_milliseconds" || spy.countValues[0] != 500 {
		t.Fatalf("got count %s=%v, want sleep.total_milliseconds=500", spy.countNames[0], spy.countValues[0])
	}
}

func TestRecordSleepNilSafe(t *testing.T) {
	RecordSleep(nil, "retry_backoff", time.Second)
	RecordSleep(&sleepSpy{}, "", time.Second)
	RecordSleep(&sleepSpy{}, "retry_backoff", -time.Second)
}
