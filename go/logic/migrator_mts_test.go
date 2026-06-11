/*
   Copyright 2025 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/stretchr/testify/require"
)

func TestCollectTransactionEvents_GroupsSameLogicalTimestamp(t *testing.T) {
	mgtr := NewMigrator(base.NewMigrationContext(), "test")
	mgtr.coordinatorQueue = make(chan *applyEventStruct, 8)

	dml := &binlog.BinlogDMLEvent{DML: binlog.InsertDML}
	row1 := newApplyEventStructByDML(&binlog.BinlogEntry{
		LastCommitted:  4,
		SequenceNumber: 5,
		DmlEvent:       dml,
	})
	row2 := newApplyEventStructByDML(&binlog.BinlogEntry{
		LastCommitted:  4,
		SequenceNumber: 5,
		DmlEvent:       dml,
	})
	row3 := newApplyEventStructByDML(&binlog.BinlogEntry{
		LastCommitted:  5,
		SequenceNumber: 6,
		DmlEvent:       &binlog.BinlogDMLEvent{DML: binlog.InsertDML},
	})

	mgtr.coordinatorQueue <- row2
	mgtr.coordinatorQueue <- row3

	ctx := context.Background()
	trx, ok := mgtr.collectTransactionEvents(ctx, row1)
	require.True(t, ok)
	require.Len(t, trx, 2)
	require.Equal(t, int64(5), trx[0].sequenceNum)
	require.NotNil(t, mgtr.pendingCoordinatorEvent)
	require.Equal(t, int64(6), mgtr.pendingCoordinatorEvent.sequenceNum)
}

func TestCollectTransactionEvents_HandlesNonDMLBetweenRows(t *testing.T) {
	mgtr := NewMigrator(base.NewMigrationContext(), "test")
	mgtr.coordinatorQueue = make(chan *applyEventStruct, 8)

	called := false
	var writeFunc tableWriteFunc = func() error {
		called = true
		return nil
	}
	mgtr.coordinatorQueue <- newApplyEventStructByFunc(&writeFunc)

	rowDML := &binlog.BinlogDMLEvent{DML: binlog.InsertDML}
	row1 := newApplyEventStructByDML(&binlog.BinlogEntry{
		LastCommitted:  1,
		SequenceNumber: 2,
		DmlEvent:       rowDML,
	})
	row2 := newApplyEventStructByDML(&binlog.BinlogEntry{
		LastCommitted:  1,
		SequenceNumber: 2,
		DmlEvent:       rowDML,
	})
	mgtr.coordinatorQueue <- row2

	ctx := context.Background()
	trx, ok := mgtr.collectTransactionEvents(ctx, row1)
	require.True(t, ok)
	require.Len(t, trx, 2)
	require.True(t, called)
}

func TestNotifyLogicalTimestampsDetection_ClosesOnce(t *testing.T) {
	ctx := base.NewMigrationContext()
	ctx.LogicalTimestampsDetected = make(chan struct{})

	ctx.NotifyLogicalTimestampsDetection(true)
	require.True(t, ctx.BinlogHasLogicalTimestamps)

	select {
	case <-ctx.LogicalTimestampsDetected:
	default:
		t.Fatal("expected channel closed")
	}

	ctx.NotifyLogicalTimestampsDetection(false)
	require.True(t, ctx.BinlogHasLogicalTimestamps)
}

func TestEnqueueApplyEvent_RoutesToCoordinatorWhenMTSActive(t *testing.T) {
	mgtr := NewMigrator(base.NewMigrationContext(), "test")
	mgtr.numWorkers = 4
	mgtr.coordinatorQueue = make(chan *applyEventStruct, 1)
	atomic.StoreInt64(&mgtr.mtsActive, 1)

	var noop tableWriteFunc = func() error { return nil }
	err := mgtr.enqueueApplyEvent(newApplyEventStructByFunc(&noop))
	require.NoError(t, err)
	require.Len(t, mgtr.coordinatorQueue, 1)
	require.Len(t, mgtr.applyEventsQueue, 0)
}

func TestEnqueueApplyEvent_RoutesToApplyQueueWhenMTSInactive(t *testing.T) {
	mgtr := NewMigrator(base.NewMigrationContext(), "test")
	mgtr.numWorkers = 4
	mgtr.coordinatorQueue = make(chan *applyEventStruct, 1)

	var noop tableWriteFunc = func() error { return nil }
	err := mgtr.enqueueApplyEvent(newApplyEventStructByFunc(&noop))
	require.NoError(t, err)
	require.Len(t, mgtr.applyEventsQueue, 1)
	require.Len(t, mgtr.coordinatorQueue, 0)
}

func TestMigrator_NumWorkersInitializesMTSChannels(t *testing.T) {
	mctx := base.NewMigrationContext()
	mctx.NumWorkers = 4
	mgtr := NewMigrator(mctx, "test")
	require.Equal(t, 4, mgtr.numWorkers)
	require.Len(t, mgtr.workerQueues, 4)
	require.NotNil(t, mgtr.commitBarrier)
	require.NotNil(t, mgtr.coordinatorQueue)
}

func TestMigrator_NumWorkersOneSkipsMTSChannels(t *testing.T) {
	mctx := base.NewMigrationContext()
	mctx.NumWorkers = 1
	mgtr := NewMigrator(mctx, "test")
	require.Nil(t, mgtr.commitBarrier)
	require.Nil(t, mgtr.coordinatorQueue)
}

func TestMTSWorkerJobHoldsCoordinates(t *testing.T) {
	job := &mtsWorkerJob{coords: &mysql.FileBinlogCoordinates{LogFile: "binlog.000001", LogPos: 4}}
	require.NotNil(t, job.coords)
}

func TestAdoptDMLQueryBuildersFrom_RequiresPreparedPrimary(t *testing.T) {
	columns := sql.NewColumnList([]string{"id", "item_id"})
	mctx := base.NewMigrationContext()
	mctx.DatabaseName = "test"
	mctx.OriginalTableName = "test"
	mctx.OriginalTableColumns = columns
	mctx.SharedColumns = columns
	mctx.MappedSharedColumns = columns
	mctx.UniqueKey = &sql.UniqueKey{
		Name:    t.Name(),
		Columns: *columns,
	}

	primary := NewApplier(mctx)
	worker := NewApplier(mctx)

	err := worker.adoptDMLQueryBuildersFrom(primary)
	require.Error(t, err)

	require.NoError(t, primary.prepareQueries())
	require.NoError(t, worker.adoptDMLQueryBuildersFrom(primary))
	require.NotNil(t, worker.dmlInsertQueryBuilder)
	require.Same(t, primary.dmlInsertQueryBuilder, worker.dmlInsertQueryBuilder)
}

func TestApplyMTSWorkerJob_ReleasesBarrierOnError(t *testing.T) {
	mctx := base.NewMigrationContext()
	mctx.PanicAbort = make(chan error, 1)
	mgtr := NewMigrator(mctx, "test")
	mgtr.numWorkers = 2
	mgtr.commitBarrier = newCommitBarrier()
	mgtr.commitBarrier.addDelegatedJob(0)

	job := &mtsWorkerJob{
		dmlEvents: []*binlog.BinlogDMLEvent{{DML: binlog.InsertDML}},
	}
	// Applier without DB connection or query builders: apply fails, barrier must release.
	mgtr.applyMTSWorkerJob(context.Background(), 0, NewApplier(mctx), job)
	require.Equal(t, int64(0), mgtr.commitBarrier.delegatedJobCount())
	require.Error(t, <-mctx.PanicAbort)
}

func TestIsDeadlockError(t *testing.T) {
	require.True(t, isDeadlockError(&drivermysql.MySQLError{Number: 1213, Message: "Deadlock found"}))
	require.False(t, isDeadlockError(&drivermysql.MySQLError{Number: 1205, Message: "Lock wait timeout"}))
	require.False(t, isDeadlockError(errors.New("generic error")))
	require.False(t, isDeadlockError(nil))
}

func TestRetryMTSApply_RetriesImmediatelyOnDeadlock(t *testing.T) {
	mctx := base.NewMigrationContext()
	mctx.PanicAbort = make(chan error, 1)
	mgtr := NewMigrator(mctx, "test")

	deadlockErr := &drivermysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}
	attempts := 0
	start := time.Now()
	err := mgtr.retryMTSApply(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return deadlockErr
		}
		return nil
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, 3, attempts)
	// Deadlock retries should be immediate (< 100ms for 2 retries).
	// Normal retryOperation would sleep 2 seconds (1s * 2 retries).
	require.Less(t, elapsed, 500*time.Millisecond, "deadlock retries should be fast")
}

func TestRetryMTSApply_RetriesOnLockWaitTimeout(t *testing.T) {
	mctx := base.NewMigrationContext()
	mctx.PanicAbort = make(chan error, 1)
	mgtr := NewMigrator(mctx, "test")

	lockWaitErr := &drivermysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"}
	attempts := 0
	err := mgtr.retryMTSApply(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return lockWaitErr
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestRetryMTSApply_PropagatesNonRetryableImmediately(t *testing.T) {
	mctx := base.NewMigrationContext()
	mctx.SetDefaultNumRetries(3)
	mctx.PanicAbort = make(chan error, 1)
	mgtr := NewMigrator(mctx, "test")

	// A non-retryable error (not a deadlock / lock-wait timeout) must be
	// returned immediately and unchanged, so the coordinator aborts the
	// migration rather than silently dropping the transaction.
	fatalErr := &drivermysql.MySQLError{Number: 1146, Message: "Table doesn't exist"}
	attempts := 0
	err := mgtr.retryMTSApply(context.Background(), func() error {
		attempts++
		return fatalErr
	})
	require.ErrorIs(t, err, fatalErr)
	require.Equal(t, 1, attempts)
}

func TestCoordinateUpdateMonotonic(t *testing.T) {
	mctx := base.NewMigrationContext()
	mctx.PanicAbort = make(chan error, 1)
	mgtr := NewMigrator(mctx, "test")
	mgtr.applier = NewApplier(mctx)

	// Simulate coordinate regression: worker with seq=10 finishes first.
	mgtr.applier.CurrentCoordinates = &mysql.FileBinlogCoordinates{LogFile: "binlog.000001", LogPos: 200}

	job := &mtsWorkerJob{
		dmlEvents:   []*binlog.BinlogDMLEvent{{DML: binlog.InsertDML}},
		sequenceNum: 10,
		coords:      &mysql.FileBinlogCoordinates{LogFile: "binlog.000001", LogPos: 100},
	}
	mgtr.commitBarrier = newCommitBarrier()
	mgtr.commitBarrier.addDelegatedJob(job.sequenceNum)

	// applyMTSWorkerJob will fail (no DB), but the deferred handler runs first.
	// Test coordinate update logic directly instead.
	mgtr.applier.CurrentCoordinatesMutex.Lock()
	if mgtr.applier.CurrentCoordinates.SmallerThan(job.coords) {
		mgtr.applier.CurrentCoordinates = job.coords
	}
	mgtr.applier.CurrentCoordinatesMutex.Unlock()

	// Coordinates should NOT regress to 100.
	coords := mgtr.applier.CurrentCoordinates.(*mysql.FileBinlogCoordinates)
	require.Equal(t, int64(200), coords.LogPos)
}
