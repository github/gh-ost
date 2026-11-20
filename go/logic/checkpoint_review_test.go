package logic

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/stretchr/testify/require"
)

func TestMoveTablesCutOverCheckpointUsesAppliedBoundary(t *testing.T) {
	var written []driver.NamedValue
	apl := newCheckpointExecApplier(t, func(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
		written = append([]driver.NamedValue(nil), args...)
		return driver.RowsAffected(2), nil
	})
	mc := apl.migrationContext
	defer mc.CancelContext()
	mc.InitMoveTableContainers()
	m := NewMigrator(mc, "test")
	m.applier = apl
	start, err := mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, "00000000-0000-0000-0000-000000000001:1")
	require.NoError(t, err)
	partial, err := mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, "00000000-0000-0000-0000-000000000001:1-2")
	require.NoError(t, err)
	apl.CurrentCoordinates = partial.Clone()
	apl.AppliedTransactionCoordinates = start.Clone()
	// Recovery must be recorded even after cancellation, and row-level GTID
	// progress must never be used as the resume boundary.
	mc.CancelContext()
	require.NoError(t, m.persistMoveTablesCutOverCheckpoint(partial, false))
	require.Len(t, written, 2*len(moveTablesCheckpointColumns))
	for i := 0; i < 2; i++ {
		require.Equal(t, start.String(), written[i*len(moveTablesCheckpointColumns)+2].Value)
		require.Equal(t, true, written[i*len(moveTablesCheckpointColumns)+7].Value)
		require.Equal(t, partial.String(), written[i*len(moveTablesCheckpointColumns)+8].Value)
	}
	apl.AppliedTransactionCoordinates = nil
	require.ErrorContains(t, m.persistMoveTablesCutOverCheckpoint(partial, false), "no applied transaction boundary")
}

func TestMoveTablesCheckpointDoesNotBlockApplyAndOrdersRecovery(t *testing.T) {
	entered := make(chan struct{})
	recoveryEntered := make(chan struct{})
	release := make(chan struct{})
	writes := make(chan []driver.NamedValue, 2)
	var calls int32
	apl := newCheckpointExecApplier(t, func(ctx context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
		writes <- append([]driver.NamedValue(nil), args...)
		if atomic.AddInt32(&calls, 1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		} else {
			close(recoveryEntered)
		}
		return driver.RowsAffected(2), nil
	})
	mc := apl.migrationContext
	defer mc.CancelContext()
	mc.InitMoveTableContainers()
	m := NewMigrator(mc, "test")
	m.applier = apl
	start := mysql.NewFileBinlogCoordinates("mysql-bin.000001", 100)
	end := mysql.NewFileBinlogCoordinates("mysql-bin.000001", 200)
	apl.AppliedTransactionCoordinates = start.Clone()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	periodic := make(chan error, 1)
	go func() { _, err := m.Checkpoint(ctx); periodic <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("periodic checkpoint did not enter write")
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	applied := make(chan error, 1)
	go func() { applied <- m.onApplyEventStruct(&applyEventStruct{coords: end, transactionComplete: true}) }()
	select {
	case err := <-applied:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("checkpoint SQL blocked transaction acknowledgement")
	}
	atomic.StoreInt64(&mc.InCutOverCriticalSectionFlag, 1)
	recovery := make(chan error, 1)
	recoveryStarted := make(chan struct{})
	go func() {
		close(recoveryStarted)
		recovery <- m.persistMoveTablesCutOverCheckpoint(end, false)
	}()
	<-recoveryStarted
	// A canceled waiter must not be trapped behind the outstanding write.
	waitCtx, waitCancel := context.WithCancel(ctx)
	waitCancel()
	_, err := m.Checkpoint(waitCtx)
	require.ErrorIs(t, err, context.Canceled)
	select {
	case <-recoveryEntered:
		t.Fatal("recovery write overtook the in-flight periodic checkpoint")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-periodic)
	require.NoError(t, <-recovery)
	first, second := <-writes, <-writes
	require.Equal(t, start.String(), first[2].Value)
	require.Equal(t, false, first[7].Value)
	require.Equal(t, end.String(), second[2].Value)
	require.Equal(t, true, second[7].Value)
	_, err = m.Checkpoint(ctx)
	require.ErrorContains(t, err, "cutover started")
}

func TestMoveTablesDrainUsesAppliedBoundaryWithBacklog(t *testing.T) {
	mc := newTestMigrationContext()
	defer mc.CancelContext()
	mc.CutOverLockTimeoutSeconds = 1
	m := NewMigrator(mc, "test")
	m.applier = NewApplier(mc)
	drain, err := mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, "00000000-0000-0000-0000-000000000001:1-2")
	require.NoError(t, err)
	m.applier.AppliedTransactionCoordinates = drain.Clone()
	m.applyEventsQueue <- &applyEventStruct{coords: drain, transactionComplete: true}
	require.NoError(t, m.drainMoveTablesCutOver(drain))
	require.Len(t, m.applyEventsQueue, 1)
}

func TestWaitForGhostTableMigratedReturnsListenerFailure(t *testing.T) {
	mc := newTestMigrationContext()
	defer mc.CancelContext()
	m := NewMigrator(mc, "test")
	streamer := NewEventsStreamer(mc)
	failure := errors.New("changelog listener failed")
	require.NoError(t, streamer.AddListener(false, mc.DatabaseName, mc.GetChangelogTableName(), func(*binlog.BinlogEntry) error {
		return failure
	}))
	done := make(chan error, 1)
	go func() { done <- m.waitForGhostTableMigrated() }()
	err := streamer.notifyListeners(&binlog.BinlogEntry{DmlEvent: binlog.NewBinlogDMLEvent(mc.DatabaseName, mc.GetChangelogTableName(), binlog.InsertDML)})
	require.ErrorIs(t, err, failure)
	select {
	case err := <-done:
		require.ErrorIs(t, err, failure)
	case <-time.After(time.Second):
		t.Fatal("startup did not stop after listener failure")
	}
}

func TestWaitForGhostTableMigratedSuccessAndCancellation(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mc := newTestMigrationContext()
		defer mc.CancelContext()
		m := NewMigrator(mc, "test")
		go func() { m.ghostTableMigrated <- true }()
		require.NoError(t, m.waitForGhostTableMigrated())
	})
	t.Run("canceled", func(t *testing.T) {
		mc := newTestMigrationContext()
		m := NewMigrator(mc, "test")
		mc.CancelContext()
		require.ErrorIs(t, m.waitForGhostTableMigrated(), context.Canceled)
	})
}
