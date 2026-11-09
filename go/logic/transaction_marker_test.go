package logic

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/stretchr/testify/require"
)

func TestTransactionMarkersFollowSynchronousDMLListeners(t *testing.T) {
	ctx := newTestMigrationContext()
	streamer := NewEventsStreamer(ctx)
	migrator := NewMigrator(ctx, "test")
	migrator.eventsStreamer = streamer
	migrator.applier = NewApplier(ctx)
	require.NoError(t, migrator.addDMLEventsListener())
	coords := mysql.NewFileBinlogCoordinates("mysql-bin.000001", 1234)
	for i := 0; i < 3; i++ {
		require.NoError(t, streamer.notifyListeners(&binlog.BinlogEntry{
			Coordinates: coords,
			DmlEvent:    binlog.NewBinlogDMLEvent(ctx.DatabaseName, ctx.OriginalTableName, binlog.InsertDML),
		}))
	}
	require.NoError(t, streamer.notifyListeners(&binlog.BinlogEntry{Coordinates: coords, TransactionComplete: true}))
	require.Len(t, migrator.applyEventsQueue, 4)
	require.Nil(t, migrator.applier.AppliedTransactionCoordinates)
	for i := 0; i < 3; i++ {
		event := <-migrator.applyEventsQueue
		require.NotNil(t, event.dmlEvent)
		require.False(t, event.transactionComplete)
	}
	marker := <-migrator.applyEventsQueue
	require.True(t, marker.transactionComplete)
	require.Nil(t, marker.dmlEvent)
}

func TestTransactionMarkerRejectedAfterAbort(t *testing.T) {
	for _, abortErr := range []error{context.Canceled, errors.New("migration aborted")} {
		t.Run(abortErr.Error(), func(t *testing.T) {
			ctx := newTestMigrationContext()
			defer ctx.CancelContext()
			migrator := NewMigrator(ctx, "test")
			migrator.applier = NewApplier(ctx)
			previous := mysql.NewFileBinlogCoordinates("mysql-bin.000001", 100)
			migrator.applier.AppliedTransactionCoordinates = previous.Clone()
			marker := &applyEventStruct{coords: mysql.NewFileBinlogCoordinates("mysql-bin.000001", 200), transactionComplete: true}
			if !errors.Is(abortErr, context.Canceled) {
				ctx.SetAbortError(abortErr)
			}
			ctx.CancelContext()
			require.ErrorIs(t, migrator.onApplyEventStruct(marker), abortErr)
			require.True(t, previous.Equals(migrator.applier.AppliedTransactionCoordinates))
		})
	}
}

func TestFailedSynchronousListenerStopsTransactionDelivery(t *testing.T) {
	for _, failMarker := range []bool{false, true} {
		t.Run(fmt.Sprintf("marker=%t", failMarker), func(t *testing.T) {
			ctx := newTestMigrationContext()
			defer ctx.CancelContext()
			streamer := NewEventsStreamer(ctx)
			failure := errors.New("listener rejected event")
			called := 0
			fail := func(*binlog.BinlogEntry) error { called++; return failure }
			unexpected := func(*binlog.BinlogEntry) error { t.Error("delivered after listener failure"); return nil }
			entry := &binlog.BinlogEntry{TransactionComplete: failMarker}
			if failMarker {
				streamer.AddTransactionCompleteListener(fail)
			} else {
				entry.DmlEvent = binlog.NewBinlogDMLEvent(ctx.DatabaseName, ctx.OriginalTableName, binlog.DeleteDML)
				require.NoError(t, streamer.AddListener(false, ctx.DatabaseName, ctx.OriginalTableName, fail))
				require.NoError(t, streamer.AddListener(false, ctx.DatabaseName, ctx.OriginalTableName, unexpected))
			}
			streamer.AddTransactionCompleteListener(unexpected)
			require.ErrorIs(t, streamer.notifyListeners(entry), failure)
			require.ErrorIs(t, ctx.GetAbortError(), failure)
			require.ErrorIs(t, ctx.GetContext().Err(), context.Canceled)
			require.ErrorIs(t, streamer.notifyListeners(&binlog.BinlogEntry{TransactionComplete: true}), failure)
			require.Equal(t, 1, called)
		})
	}
}

func TestCanceledDispatcherRejectsReadyQueue(t *testing.T) {
	ctx := newTestMigrationContext()
	migrator := NewMigrator(ctx, "test")
	migrator.eventsStreamer = NewEventsStreamer(ctx)
	require.NoError(t, migrator.addDMLEventsListener())
	ctx.CancelContext()
	coords := mysql.NewFileBinlogCoordinates("mysql-bin.000001", 100)
	for _, entry := range []*binlog.BinlogEntry{
		{Coordinates: coords, DmlEvent: binlog.NewBinlogDMLEvent(ctx.DatabaseName, ctx.OriginalTableName, binlog.DeleteDML)},
		{Coordinates: coords, TransactionComplete: true},
	} {
		require.ErrorIs(t, migrator.eventsStreamer.notifyListeners(entry), context.Canceled)
	}
	require.Empty(t, migrator.applyEventsQueue)
}

func TestTransactionMarkerClonesGTIDCoordinates(t *testing.T) {
	for _, test := range []struct {
		flavor    string
		committed string
		next      string
	}{
		{mysql.MySQLFlavor, "00000000-0000-0000-0000-000000000001:1", "00000000-0000-0000-0000-000000000001:2"},
		{mysql.MariaDBFlavor, "0-1-1", "0-1-2"},
	} {
		t.Run(test.flavor, func(t *testing.T) {
			ctx := newTestMigrationContext()
			ctx.UseGTIDs = true
			migrator := NewMigrator(ctx, "test")
			migrator.applier = NewApplier(ctx)
			coords, err := mysql.NewGTIDBinlogCoordinates(test.flavor, test.committed)
			require.NoError(t, err)
			require.NoError(t, migrator.onTransactionComplete(&binlog.BinlogEntry{Coordinates: coords, TransactionComplete: true}))
			require.Nil(t, migrator.applier.AppliedTransactionCoordinates, "enqueueing is not applying")
			marker := <-migrator.applyEventsQueue
			require.NoError(t, coords.GTIDSet.Update(test.next))
			require.NoError(t, migrator.onApplyEventStruct(marker))
			require.Equal(t, test.committed, migrator.applier.AppliedTransactionCoordinates.String())
			// The acknowledged watermark also owns its own copy of the marker.
			require.NoError(t, marker.coords.(*mysql.GTIDBinlogCoordinates).GTIDSet.Update(test.next))
			require.Equal(t, test.committed, migrator.applier.AppliedTransactionCoordinates.String())
		})
	}
}

func TestHeartbeatDoesNotAcknowledgeItsTransaction(t *testing.T) {
	ctx := newTestMigrationContext()
	migrator := NewMigrator(ctx, "test")
	migrator.applier = NewApplier(ctx)
	coords := mysql.NewFileBinlogCoordinates("mysql-bin.000001", 1234)
	heartbeat := &binlog.BinlogEntry{
		Coordinates: coords,
		DmlEvent: &binlog.BinlogDMLEvent{
			DML:             binlog.InsertDML,
			NewColumnValues: sql.ToColumnValues([]interface{}{1, 0, "heartbeat", time.Now().Format(time.RFC3339Nano)}),
		},
	}
	require.NoError(t, migrator.onChangelogHeartbeatEvent(heartbeat))
	require.NoError(t, migrator.onApplyEventStruct(<-migrator.applyEventsQueue))
	require.True(t, migrator.applier.CurrentCoordinates.Equals(coords))
	require.Nil(t, migrator.applier.AppliedTransactionCoordinates, "a heartbeat row may precede more rows in the same transaction")
	require.NoError(t, migrator.onTransactionComplete(&binlog.BinlogEntry{Coordinates: coords, TransactionComplete: true}))
	require.NoError(t, migrator.onApplyEventStruct(<-migrator.applyEventsQueue))
	require.True(t, migrator.applier.AppliedTransactionCoordinates.Equals(coords))
}
