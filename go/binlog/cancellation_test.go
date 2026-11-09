package binlog

import (
	"context"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func TestTransactionMarkerSendCanceled(t *testing.T) {
	assertTransactionMarkerSendCanceled(t, nil)
}

func assertTransactionMarkerSendCanceled(t *testing.T, wrapEvents func([]*replication.BinlogEvent) []*replication.BinlogEvent) {
	t.Helper()
	reader := transactionTestReader(false)
	defer reader.migrationContext.CancelContext()
	previous := mysql.NewFileBinlogCoordinates("mysql-bin.000001", 4)
	reader.LastTrxCoords = previous.Clone()
	row := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2, LogPos: 200},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{Schema: []byte("testdb"), Table: []byte("testing")},
			Rows:  [][]interface{}{{1}},
		},
	}
	commit := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XID_EVENT, LogPos: 220},
		Event:  &replication.XIDEvent{},
	}
	events := []*replication.BinlogEvent{row, commit}
	if wrapEvents != nil {
		events = wrapEvents(events)
	}
	for _, event := range events {
		require.NoError(t, reader.binlogStreamer.AddEventToStreamer(event))
	}
	entries := make(chan *BinlogEntry)
	done := make(chan error, 1)
	go func() {
		err := reader.StreamEvents(func() bool { return false }, entries)
		_ = base.SendWithContext(t.Context(), done, err)
	}()
	select {
	case entry := <-entries:
		require.NotNil(t, entry.DmlEvent)
	case <-time.After(time.Second):
		t.Fatal("row was not delivered")
	}
	// Wait until the commit position has been read, then stop consuming.
	// The reader must propagate a canceled completion-marker send.
	require.Eventually(t, func() bool {
		return reader.GetCurrentBinlogCoordinates().(*mysql.FileBinlogCoordinates).LogPos == 220
	}, time.Second, time.Millisecond)
	reader.migrationContext.CancelContext()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("completion marker send ignored cancellation")
	}
	require.True(t, reader.transactionHasRows, "failed marker delivery must retain pending state")
	require.True(t, previous.Equals(reader.LastTrxCoords), "failed delivery must not advance reconnect coordinates")
}

func TestRowsSendCanceled(t *testing.T) {
	reader := transactionTestReader(false)
	defer reader.migrationContext.CancelContext()
	event := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{Schema: []byte("testdb"), Table: []byte("testing")},
			Rows:  [][]interface{}{{1}, {2}},
		},
	}
	require.NoError(t, reader.binlogStreamer.AddEventToStreamer(event))
	entries := make(chan *BinlogEntry)
	done := make(chan error, 1)
	go func() {
		err := reader.StreamEvents(func() bool { return false }, entries)
		_ = base.SendWithContext(t.Context(), done, err)
	}()
	select {
	case <-entries:
	case <-time.After(time.Second):
		t.Fatal("first row was not delivered")
	}
	reader.migrationContext.CancelContext()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("blocked row send ignored cancellation")
	}
}

func TestIdleReaderCanceled(t *testing.T) {
	reader := transactionTestReader(false)
	reader.migrationContext.CancelContext()
	done := make(chan error, 1)
	go func() {
		err := reader.StreamEvents(func() bool { return false }, make(chan *BinlogEntry))
		_ = base.SendWithContext(t.Context(), done, err)
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("idle reader ignored cancellation")
	}
}
