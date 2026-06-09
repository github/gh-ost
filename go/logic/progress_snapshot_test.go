/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"bytes"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/github/gh-ost/go/base"
)

func TestMigrationProgressSnapshotCapturesBacklogAndLag(t *testing.T) {
	ctx := base.NewMigrationContext()
	migrator := NewMigrator(ctx, "test")
	atomic.StoreInt64(&ctx.TotalRowsCopied, 100)
	atomic.StoreInt64(&ctx.CurrentLag, int64(3*time.Second))
	ctx.SetLastHeartbeatOnChangelogTime(time.Now().Add(-2 * time.Second))

	snap := migrator.migrationProgressSnapshot()
	assert.Equal(t, 0, snap.applyEventsBacklog)
	assert.Equal(t, cap(migrator.applyEventsQueue), snap.applyEventsCapacity)
	assert.InDelta(t, 3.0, snap.replicationLagSeconds, 0.01)
	assert.InDelta(t, 2.0, snap.heartbeatLagSeconds, 0.5)
	assert.Empty(t, snap.streamerBinlogPosition)
}

func TestMigrationProgressSnapshotWhenRowCopyComplete(t *testing.T) {
	ctx := base.NewMigrationContext()
	migrator := NewMigrator(ctx, "test")
	atomic.StoreInt64(&ctx.TotalRowsCopied, 5000)
	atomic.StoreInt64(&ctx.RowsEstimate, 10000)
	atomic.StoreInt64(&migrator.rowCopyCompleteFlag, 1)

	snap := migrator.migrationProgressSnapshot()
	assert.Equal(t, int64(5000), snap.totalRowsCopied)
	assert.Equal(t, int64(5000), snap.rowsEstimate)
	assert.InDelta(t, 100.0, snap.progressPct, 0.01)
}

func TestReportStatusSamplesProgress(t *testing.T) {
	ctx := base.NewMigrationContext()
	atomic.StoreInt64(&ctx.TotalRowsCopied, 1000)
	atomic.StoreInt64(&ctx.RowsEstimate, 5000)

	migrator := NewMigrator(ctx, "test")
	migrator.reportStatus(NoPrintStatusRule, io.Discard)
	assert.InDelta(t, 20.0, ctx.GetProgressPct(), 0.01)
}

func TestReportStatusUpdatesContextWhenPrintSuppressed(t *testing.T) {
	ctx := base.NewMigrationContext()
	ctx.StartTime = time.Now().Add(-99 * time.Second)
	atomic.StoreInt64(&ctx.TotalRowsCopied, 1000)
	atomic.StoreInt64(&ctx.RowsEstimate, 5000)
	atomic.StoreInt64(&ctx.EtaRowsPerSecond, 1)

	migrator := NewMigrator(ctx, "test")
	snap := migrator.migrationProgressSnapshot()
	require.False(t, migrator.shouldPrintStatus(HeuristicPrintStatusRule, snap.elapsedSeconds, snap.etaDuration))

	var buf bytes.Buffer
	migrator.printStatus(HeuristicPrintStatusRule, snap, &buf)
	assert.Empty(t, buf.String(), "heuristic rule should suppress status line output")

	migrator.reportStatus(HeuristicPrintStatusRule, io.Discard)
	assert.InDelta(t, 20.0, ctx.GetProgressPct(), 0.01)
}
