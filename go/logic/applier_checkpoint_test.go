/*
   Copyright 2025 GitHub Inc.
         See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/stretchr/testify/require"
)

// checkpointExecConnector exercises database/sql context handling without a
// server. Each Exec is a server round trip, so tests can detect per-table writes.
type checkpointExecConnector struct {
	exec func(context.Context, string, []driver.NamedValue) (driver.Result, error)
}

func (c *checkpointExecConnector) Connect(context.Context) (driver.Conn, error) {
	return &checkpointExecConn{connector: c}, nil
}
func (c *checkpointExecConnector) Driver() driver.Driver { return checkpointExecDriver{} }

type checkpointExecDriver struct{}

func (checkpointExecDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use checkpointExecConnector")
}

type checkpointExecConn struct{ connector *checkpointExecConnector }

func (*checkpointExecConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (*checkpointExecConn) Close() error { return nil }
func (*checkpointExecConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected Begin")
}
func (c *checkpointExecConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.connector.exec(ctx, query, args)
}

func newCheckpointExecApplier(t *testing.T, exec func(context.Context, string, []driver.NamedValue) (driver.Result, error)) *Applier {
	t.Helper()
	mc := base.NewMigrationContext()
	mc.DatabaseName = "source"
	mc.MoveTables.TableNames = []string{"first", "last"}
	mc.MoveTables.TargetDatabase = "target"
	apl := NewApplier(mc)
	apl.moveTablesTargetDB = gosql.OpenDB(&checkpointExecConnector{exec: exec})
	t.Cleanup(func() { require.NoError(t, apl.moveTablesTargetDB.Close()) })
	return apl
}

func TestApplierMoveTableCheckpointSnapshotSingleStatement(t *testing.T) {
	coords, err := mysql.ParseFileBinlogCoordinates("mysql-bin.000007:123")
	require.NoError(t, err)
	drain, err := mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, "00000000-0000-0000-0000-000000000001:1-20")
	require.NoError(t, err)
	rows := []*Checkpoint{
		{
			TableName: "first", LastTrxCoords: coords, Iteration: 3, RowsCopied: 100, DMLApplied: 20,
			MoveTablesCutOverStarted: true, MoveTablesCutOverDrainGTID: drain,
			IterationRangeMin: sql.ToColumnValues([]interface{}{1, "a"}),
			IterationRangeMax: sql.ToColumnValues([]interface{}{9, "君"}),
		},
		{
			TableName: "last", LastTrxCoords: coords, Iteration: 5, RowsCopied: 200, DMLApplied: 20,
			IsCutover: true, MoveTablesCutOverStarted: true, MoveTablesCutOverDrainGTID: drain,
		},
	}
	calls := 0
	var timestamp interface{}
	var apl *Applier
	apl = newCheckpointExecApplier(t, func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		calls++
		require.NoError(t, ctx.Err())
		require.Contains(t, query, "into `target`."+sql.EscapeName(apl.migrationContext.GetCheckpointTableName()))
		require.Equal(t, len(rows)*len(moveTablesCheckpointColumns), strings.Count(query, "?"))
		require.Contains(t, query, "), (")
		require.Len(t, args, len(rows)*len(moveTablesCheckpointColumns))
		timestamp = args[0].Value
		for i, row := range rows {
			offset := i * len(moveTablesCheckpointColumns)
			got := make([]interface{}, len(moveTablesCheckpointColumns))
			for j := range got {
				got[j] = args[offset+j].Value
			}
			require.Equal(t, []interface{}{
				timestamp, row.TableName, coords.String(), row.Iteration, row.RowsCopied, row.DMLApplied,
				row.IsCutover, row.MoveTablesCutOverStarted, drain.String(),
				serializeRangeValues(row.IterationRangeMin), serializeRangeValues(row.IterationRangeMax),
			}, got)
		}
		return driver.RowsAffected(len(rows)), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), checkpointTimeout)
	defer cancel()
	require.NoError(t, apl.writeMoveTableCheckpoints(ctx, rows))
	require.Equal(t, 1, calls, "all tables must share one round trip, not consume the deadline in table order")
	require.InDelta(t, time.Now().Unix(), timestamp, 1)
}

func TestApplierMoveTableCutOverCheckpointSurvivesCancellation(t *testing.T) {
	for _, cancelBefore := range []bool{true, false} {
		name := "during-write"
		if cancelBefore {
			name = "before-write"
		}
		t.Run(name, func(t *testing.T) {
			calls := 0
			var apl *Applier
			var writeCtx context.Context
			apl = newCheckpointExecApplier(t, func(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
				calls++
				writeCtx = ctx
				apl.migrationContext.CancelContext()
				require.ErrorIs(t, apl.migrationContext.GetContext().Err(), context.Canceled)
				require.NoError(t, ctx.Err(), "recovery marker must not inherit migration cancellation")
				deadline, ok := ctx.Deadline()
				require.True(t, ok, "recovery writes must still be bounded")
				require.WithinDuration(t, time.Now().Add(moveTablesCutOverCheckpointTimeout), deadline, time.Second)
				return driver.RowsAffected(2), nil
			})
			if cancelBefore {
				apl.migrationContext.CancelContext()
			}
			require.NoError(t, apl.WriteMoveTableCheckpoints([]*Checkpoint{
				{TableName: "first", MoveTablesCutOverStarted: true},
				{TableName: "last", IsCutover: true, MoveTablesCutOverStarted: true},
			}))
			require.Equal(t, 1, calls)
			require.ErrorIs(t, writeCtx.Err(), context.Canceled, "release the independent context after the write")
		})
	}
}

func TestApplierMoveTablePeriodicCheckpointCancellation(t *testing.T) {
	t.Run("already canceled", func(t *testing.T) {
		apl := newCheckpointExecApplier(t, func(context.Context, string, []driver.NamedValue) (driver.Result, error) {
			t.Fatal("canceled periodic checkpoint must not reach the database")
			return nil, nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, apl.writeMoveTableCheckpoints(ctx, []*Checkpoint{{TableName: "first"}}), context.Canceled)
	})
	t.Run("in flight", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		apl := newCheckpointExecApplier(t, func(writeCtx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
			cancel()
			<-writeCtx.Done()
			return nil, writeCtx.Err()
		})
		require.ErrorIs(t, apl.writeMoveTableCheckpoints(ctx, []*Checkpoint{{TableName: "first"}}), context.Canceled)
	})
	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		apl := newCheckpointExecApplier(t, func(writeCtx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
			<-writeCtx.Done()
			return nil, writeCtx.Err()
		})
		require.ErrorIs(t, apl.writeMoveTableCheckpoints(ctx, []*Checkpoint{{TableName: "first"}}), context.DeadlineExceeded)
	})
}

func TestApplierMoveTableCheckpointErrorAndEmptySnapshot(t *testing.T) {
	writeErr := errors.New("checkpoint insert failed")
	calls := 0
	apl := newCheckpointExecApplier(t, func(context.Context, string, []driver.NamedValue) (driver.Result, error) {
		calls++
		return nil, writeErr
	})
	require.NoError(t, apl.WriteMoveTableCheckpoints(nil))
	require.NoError(t, apl.writeMoveTableCheckpoints(context.Background(), []*Checkpoint{}))
	require.Zero(t, calls)
	require.ErrorIs(t, apl.WriteMoveTableCheckpoints([]*Checkpoint{{TableName: "first"}, {TableName: "last"}}), writeErr)
	require.Equal(t, 1, calls)
}
