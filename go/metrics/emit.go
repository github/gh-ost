/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"context"
	"runtime"
	"time"
)

// EmitProgressGauges emits row-copy and DML progress gauges (namespace is applied by the client):
// gh_ost.row_copy.rows_copied, gh_ost.row_copy.rows_estimate, gh_ost.dml.events_applied.
func EmitProgressGauges(emit Emitter, rowsCopied, rowsEstimate, dmlEventsApplied int64) {
	if emit == nil {
		return
	}
	emit.Gauge("row_copy.rows_copied", float64(rowsCopied))
	emit.Gauge("row_copy.rows_estimate", float64(rowsEstimate))
	emit.Gauge("dml.events_applied", float64(dmlEventsApplied))
}

// EmitBinlogBacklogGauges emits apply-events queue depth gauges (namespace is applied by the client):
// gh_ost.binlog.backlog_size, gh_ost.binlog.backlog_capacity, gh_ost.binlog.backlog_utilization.
func EmitBinlogBacklogGauges(emit Emitter, backlogSize, backlogCapacity int) {
	if emit == nil {
		return
	}
	emit.Gauge("binlog.backlog_size", float64(backlogSize))
	emit.Gauge("binlog.backlog_capacity", float64(backlogCapacity))
	emit.Gauge("binlog.backlog_utilization", binlogBacklogUtilization(backlogSize, backlogCapacity))
}

func binlogBacklogUtilization(backlogSize, backlogCapacity int) float64 {
	if backlogCapacity <= 0 {
		return 0
	}
	utilization := float64(backlogSize) / float64(backlogCapacity)
	if utilization > 1 {
		return 1
	}
	if utilization < 0 {
		return 0
	}
	return utilization
}

// EmitGoRuntimeGauges emits gh_ost.go_runtime.* gauges (namespace is applied by the client).
// m and numGoroutine are typically from runtime.ReadMemStats and runtime.NumGoroutine.
func EmitGoRuntimeGauges(emit Emitter, m *runtime.MemStats, numGoroutine int) {
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
