package logic

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/metrics"
	"github.com/github/gh-ost/go/sql"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Signal when Checkpoint is waiting for its sampled position to be applied,
// without relying on sleeps to arrange the cancellation interleaving.
type checkpointWaitMetrics struct {
	metrics.Emitter
	waiting chan struct{}
	once    sync.Once
}

func (m *checkpointWaitMetrics) Histogram(name string, _ float64, tags ...string) {
	if name == "sleep.duration_milliseconds" && len(tags) == 1 && tags[0] == "stage:replica_wait" {
		m.once.Do(func() { close(m.waiting) })
	}
}

// The caller applied the INSERT and seeded an older durable checkpoint. The
// UPDATE, DELETE and completion marker are still pending. Replay their delivery
// under cancellation to exercise the failed-enqueue/marker interleaving.
//
//nolint:contextcheck // Migrator and dispatcher APIs use MigrationContext's internal context.
func assertCanceledTransactionCheckpoint(t *testing.T, ctx context.Context, migrationContext *base.MigrationContext, migrator *Migrator, seedID int64) {
	t.Helper()
	var pending []*applyEventStruct
	for len(migrator.applyEventsQueue) > 0 {
		pending = append(pending, <-migrator.applyEventsQueue)
	}
	require.Len(t, pending, 3)
	marker := pending[2]
	require.True(t, marker.transactionComplete)
	previous := migrator.applier.AppliedTransactionCoordinates.Clone()

	wait := &checkpointWaitMetrics{Emitter: metrics.Noop, waiting: make(chan struct{})}
	migrationContext.Metrics = wait
	checkpointCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := migrator.Checkpoint(checkpointCtx)
		_ = base.SendWithContext(ctx, done, err)
	}()
	select {
	case <-wait.waiting:
	case <-checkpointCtx.Done():
		t.Fatal("checkpoint never waited for the pending transaction")
	}

	migrationContext.CancelContext()
	for _, event := range pending[:2] {
		err := migrator.eventsStreamer.notifyListeners(&binlog.BinlogEntry{Coordinates: event.coords, DmlEvent: event.dmlEvent})
		require.ErrorIs(t, err, context.Canceled)
	}
	require.ErrorIs(t, migrator.eventsStreamer.notifyListeners(&binlog.BinlogEntry{Coordinates: marker.coords, TransactionComplete: true}), context.Canceled)
	require.Empty(t, migrator.applyEventsQueue)
	// Also reject a marker that was already dequeued before cancellation,
	// e.g. by a worker that passed its loop's abort check before throttling.
	require.ErrorIs(t, migrator.onApplyEventStruct(marker), context.Canceled)
	require.True(t, previous.Equals(migrator.applier.AppliedTransactionCoordinates))
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-checkpointCtx.Done():
		t.Fatal("migration cancellation did not interrupt the pending checkpoint")
	}
	require.NoError(t, checkpointCtx.Err(), "checkpoint's caller has not canceled")
	_, err := migrator.Checkpoint(checkpointCtx)
	require.ErrorIs(t, err, context.Canceled, "new checkpoints must also reject an aborted migration")
	stored, err := migrator.applier.ReadLastCheckpoint()
	require.NoError(t, err)
	require.Equal(t, seedID, stored.Id)
	require.True(t, stored.LastTrxCoords.Equals(previous))
	var remaining int
	require.NoError(t, migrator.applier.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", getTestGhostTableName())).Scan(&remaining))
	require.Equal(t, 1, remaining, "the unapplied DELETE must still be replayed from the safe checkpoint")
}

// Block the production checkpoint INSERT with a real MySQL table lock. Unlike
// cancellation during polling, this requires the migration cancellation bridge
// to reach an already-running ExecContext with an independent caller context.
func assertCanceledCheckpointInsert(t *testing.T, ctx context.Context, migrationContext *base.MigrationContext, migrator *Migrator, seedID int64) {
	t.Helper()
	previous, err := migrator.applier.ReadLastCheckpoint()
	require.NoError(t, err)
	require.Equal(t, seedID, previous.Id)
	checkpointTable := fmt.Sprintf("%s.%s", sql.EscapeName(migrationContext.DatabaseName), sql.EscapeName(migrationContext.GetCheckpointTableName()))
	blocker, err := migrator.applier.db.Conn(ctx)
	require.NoError(t, err)
	defer blocker.Close()
	_, err = blocker.ExecContext(ctx, "LOCK TABLES "+checkpointTable+" READ")
	require.NoError(t, err)

	checkpointCtx, cancelCheckpoint := context.WithTimeout(ctx, 30*time.Second)
	var checkpointConnectionID int64
	done := make(chan error, 1)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		_, err := migrator.Checkpoint(checkpointCtx)
		_ = base.SendWithContext(ctx, done, err)
	}()
	defer func() {
		// Release both waits on assertion failures, including mutation tests
		// that remove cancellation propagation into the SQL operation.
		cancelCheckpoint()
		// Driver cancellation closes the client connection, but MySQL may
		// retain a lock-blocked statement. Kill our test connection before
		// unlocking so that cleanup cannot accidentally execute its INSERT.
		if checkpointConnectionID != 0 {
			_, err := migrator.applier.db.ExecContext(ctx, fmt.Sprintf("KILL CONNECTION %d", checkpointConnectionID))
			var mysqlErr *drivermysql.MySQLError
			if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1094 { // ER_NO_SUCH_THREAD: already disconnected
				assert.NoError(t, err)
			}
		}
		_, err := blocker.ExecContext(ctx, "UNLOCK TABLES")
		assert.NoError(t, err)
		select {
		case <-workerDone:
		case <-ctx.Done():
			t.Error("checkpoint worker did not exit during cleanup")
		}
	}()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		// Observing the server-side lock wait proves that Checkpoint passed
		// its abort/polling checks and the INSERT is actually executing.
		err := migrator.applier.db.QueryRowContext(ctx, `
			SELECT ID FROM information_schema.PROCESSLIST
			WHERE DB = ? AND INFO LIKE '%insert /* gh-ost */%'
			  AND LOCATE(?, INFO) > 0 AND STATE = 'Waiting for table metadata lock'`,
			migrationContext.DatabaseName, checkpointTable).Scan(&checkpointConnectionID)
		assert.NoError(collect, err)
	}, 5*time.Second, 10*time.Millisecond, "checkpoint INSERT never entered the table-lock wait")
	select {
	case err := <-done:
		t.Fatalf("checkpoint returned before cancellation: %v", err)
	default:
	}

	migrationContext.CancelContext()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("migration cancellation did not interrupt the blocked checkpoint INSERT")
	}
	require.NoError(t, checkpointCtx.Err(), "checkpoint's caller must remain live")

	// READ permits this SELECT while still preventing the pending INSERT.
	// Check before cleanup: neither the lock nor the caller has been canceled
	// to make the checkpoint call return.
	stored, err := migrator.applier.ReadLastCheckpoint()
	require.NoError(t, err)
	require.Equal(t, previous.Id, stored.Id)
	require.True(t, previous.LastTrxCoords.Equals(stored.LastTrxCoords))
}

func TestCheckpointLoopStopsOnCancellation(t *testing.T) {
	migrationContext := newTestMigrationContext()
	defer migrationContext.CancelContext()
	migrationContext.CheckpointIntervalSeconds = 3600
	migrator := NewMigrator(migrationContext, "test")
	done := make(chan struct{})
	go func() {
		defer close(done)
		migrator.checkpointLoop()
	}()
	migrationContext.CancelContext()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("checkpoint loop waited for its ticker after cancellation")
	}
}
