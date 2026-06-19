/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"sync/atomic"
	"time"
)

// migrationProgressSnapshot captures row-copy, DML, backlog, and lag at a single point in time
// for status output.
type migrationProgressSnapshot struct {
	totalRowsCopied        int64
	rowsEstimate           int64
	progressPct            float64
	dmlApplied             int64
	applyEventsBacklog     int
	applyEventsCapacity    int
	state                  string
	eta                    string
	etaDuration            time.Duration
	elapsedTime            time.Duration
	elapsedRowCopyTime     time.Duration
	elapsedSeconds         int64
	streamerBinlogPosition string
	replicationLagSeconds  float64
	heartbeatLagSeconds    float64
}

func (mgtr *Migrator) migrationProgressSnapshot() migrationProgressSnapshot {
	totalRowsCopied := mgtr.migrationContext.GetTotalRowsCopied()
	rowsEstimate := atomic.LoadInt64(&mgtr.migrationContext.RowsEstimate) + atomic.LoadInt64(&mgtr.migrationContext.RowsDeltaEstimate)
	if atomic.LoadInt64(&mgtr.rowCopyCompleteFlag) == 1 {
		// Done copying rows. The totalRowsCopied value is the de-facto number of rows,
		// and there is no further need to keep updating the value.
		rowsEstimate = totalRowsCopied
	}
	progressPct := mgtr.getProgressPercent(rowsEstimate)
	state, eta, etaDuration := mgtr.getMigrationStateAndETA(rowsEstimate)
	elapsedTime := mgtr.migrationContext.ElapsedTime()

	streamerBinlogPosition := ""
	if mgtr.eventsStreamer != nil {
		streamerBinlogPosition = mgtr.eventsStreamer.GetCurrentBinlogCoordinates().DisplayString()
	}

	return migrationProgressSnapshot{
		totalRowsCopied:        totalRowsCopied,
		rowsEstimate:           rowsEstimate,
		progressPct:            progressPct,
		dmlApplied:             atomic.LoadInt64(&mgtr.migrationContext.TotalDMLEventsApplied),
		applyEventsBacklog:     len(mgtr.applyEventsQueue),
		applyEventsCapacity:    cap(mgtr.applyEventsQueue),
		state:                  state,
		eta:                    eta,
		etaDuration:            etaDuration,
		elapsedTime:            elapsedTime,
		elapsedRowCopyTime:     mgtr.migrationContext.ElapsedRowCopyTime(),
		elapsedSeconds:         int64(elapsedTime.Seconds()),
		streamerBinlogPosition: streamerBinlogPosition,
		replicationLagSeconds:  mgtr.migrationContext.GetCurrentLagDuration().Seconds(),
		heartbeatLagSeconds:    mgtr.migrationContext.TimeSinceLastHeartbeatOnChangelog().Seconds(),
	}
}

func (mgtr *Migrator) sampleMigrationProgress() migrationProgressSnapshot {
	snap := mgtr.migrationProgressSnapshot()
	mgtr.migrationContext.SetProgressPct(snap.progressPct)
	mgtr.migrationContext.SetETADuration(snap.etaDuration)
	return snap
}
