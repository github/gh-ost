/*
   Copyright 2022 GitHub Inc.
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
}

func (g *gaugeSpy) Gauge(name string, value float64, _ ...string) {
	g.names = append(g.names, name)
	g.values = append(g.values, value)
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
	c := &Client{} // sd nil — should not start
	StartGoRuntimeReporter(ctx, c, time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)
}
