/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"context"
	"runtime"
	"time"
)

// MemStatsGaugeEmitter is implemented by *Client; used for tests without UDP.
type MemStatsGaugeEmitter interface {
	Gauge(name string, value float64, tags ...string)
}

// EmitGoRuntimeGauges emits gh_ost.go_runtime.* gauges (namespace is applied by the client).
// m and numGoroutine are typically from runtime.ReadMemStats and runtime.NumGoroutine.
func EmitGoRuntimeGauges(emit MemStatsGaugeEmitter, m *runtime.MemStats, numGoroutine int) {
	if emit == nil || m == nil {
		return
	}
	emit.Gauge("go_runtime.alloc_bytes", float64(m.Alloc))
	emit.Gauge("go_runtime.sys_bytes", float64(m.Sys))
	emit.Gauge("go_runtime.heap_inuse_bytes", float64(m.HeapInuse))
	emit.Gauge("go_runtime.num_gc", float64(m.NumGC))
	emit.Gauge("go_runtime.gc_pause_total_ns", float64(m.PauseTotalNs))
	emit.Gauge("go_runtime.goroutines", float64(numGoroutine))
}

// StartGoRuntimeReporter periodically samples runtime memory and goroutines and emits gauges
// until ctx is cancelled. It is a no-op when interval <= 0, client is nil, or StatsD is disabled
// (noop client).
func StartGoRuntimeReporter(ctx context.Context, client *Client, interval time.Duration) {
	if ctx == nil || client == nil || interval <= 0 || client.sd == nil {
		return
	}

	emit := func() {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		EmitGoRuntimeGauges(client, &m, runtime.NumGoroutine())
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		emit()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				emit()
			}
		}
	}()
}
