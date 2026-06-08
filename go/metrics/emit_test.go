/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"context"
	"runtime"
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
