/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"context"
	"fmt"
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

// EmitLagGauges emits replication and heartbeat lag gauges (namespace is applied by the client):
// gh_ost.lag.replication_seconds, gh_ost.lag.heartbeat_seconds, each tagged throttled:true|false.
//
// These are point-in-time readings each status tick (not a distribution), so gauges are used
// rather than histograms; DogStatsD histogram aggregation exposes count/max series that do not
// match the log line lag values in Prometheus/Grafana.
func EmitLagGauges(emit Emitter, replicationLagSeconds, heartbeatLagSeconds float64, throttled bool) {
	if emit == nil {
		return
	}
	tags := []string{fmt.Sprintf("throttled:%t", throttled)}
	emit.Gauge("lag.replication_seconds", replicationLagSeconds, tags...)
	emit.Gauge("lag.heartbeat_seconds", heartbeatLagSeconds, tags...)
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

// EmitThrottleActiveGauge emits whether throttling is currently active.
func EmitThrottleActiveGauge(emit Emitter, active bool) {
	if emit == nil {
		return
	}
	if active {
		emit.Gauge("throttle.active", 1)
		return
	}
	emit.Gauge("throttle.active", 0)
}

// EmitThrottleInterval emits one event and one duration sample for a completed
// throttled interval.
func EmitThrottleInterval(emit Emitter, duration time.Duration, reason string) {
	if emit == nil {
		return
	}
	tags := []string{fmt.Sprintf("reason:%s", reason)}
	emit.Histogram("throttle.duration_milliseconds", float64(duration.Milliseconds()), tags...)
	emit.Count("throttle.events_total", 1, tags...)
}

const (
	CutOverOutcomeSuccess = "success"
	CutOverOutcomeRetry   = "retry"
	CutOverOutcomeAbort   = "abort"

	CutOverPhaseMagicLock         = "magic_lock"
	CutOverPhaseOriginalTableLock = "original_table_lock"
	CutOverPhaseMagicRename       = "magic_rename"
	CutOverPhaseUnlock            = "unlock"
)

// RecordCutOverPhase emits gh_ost.cut_over.phase_duration_milliseconds.
func RecordCutOverPhase(emit Emitter, phase string, duration time.Duration, err error) {
	if emit == nil || phase == "" || duration < 0 {
		return
	}
	emit.Histogram("cut_over.phase_duration_milliseconds", float64(duration.Milliseconds()), "phase:"+phase, "outcome:"+cutOverOutcomeFromError(err))
}

// RecordCutOverAttempt emits gh_ost.cut_over.attempts_total.
func RecordCutOverAttempt(emit Emitter, outcome string) {
	if emit == nil || outcome == "" {
		return
	}
	emit.Count("cut_over.attempts_total", 1, "outcome:"+outcome)
}

// RecordCutOverTotal emits gh_ost.cut_over.total_duration_milliseconds for terminal cut-over outcomes.
func RecordCutOverTotal(emit Emitter, duration time.Duration, err error) {
	if emit == nil || duration < 0 {
		return
	}
	emit.Histogram("cut_over.total_duration_milliseconds", float64(duration.Milliseconds()), "outcome:"+cutOverOutcomeFromError(err))
}

func cutOverOutcomeFromError(err error) string {
	if err != nil {
		return CutOverOutcomeAbort
	}
	return CutOverOutcomeSuccess
}

// RecordQueryDuration emits gh_ost.query.duration_milliseconds with side/kind/outcome tags.
func RecordQueryDuration(emit Emitter, side string, kind string, duration time.Duration, err error) {
	if emit == nil || side == "" || kind == "" || duration < 0 {
		return
	}
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	emit.Histogram("query.duration_milliseconds", float64(duration.Milliseconds()), "side:"+side, "kind:"+kind, "outcome:"+outcome)
}
