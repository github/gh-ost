package binlog

import (
	"testing"
	"time"

	"github.com/github/gh-ost/go/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func filteredRowsEvent(t *testing.T, pos uint32) *replication.BinlogEvent {
	t.Helper()
	rows := newTestRowsEvent("testdb", "ignored_table")
	decode := newRowsEventDecodeFunc(func(_, _ string) bool { return false })
	require.NoError(t, decode(rows, newRowsEventDataWithInvalidRowPayload()))
	require.Empty(t, rows.Rows)
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv1, LogPos: pos},
		Event:  rows,
	}
}

func TestFilteredTransactionCompletionMarker(t *testing.T) {
	for _, test := range []struct {
		name, flavor, committed, next string
		xidGTIDSet                    bool
	}{
		{name: "file"},
		{"mysql", mysql.MySQLFlavor, "00000000-0000-0000-0000-000000000001:1", "00000000-0000-0000-0000-000000000001:2", true},
		{"mysql fallback", mysql.MySQLFlavor, "00000000-0000-0000-0000-000000000001:1", "00000000-0000-0000-0000-000000000001:2", false},
		{"mariadb", mysql.MariaDBFlavor, "0-1-1", "0-1-2", true},
		{"mariadb fallback", mysql.MariaDBFlavor, "0-1-1", "0-1-2", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := transactionTestReader(test.flavor != "")
			defer reader.migrationContext.CancelContext()
			xid := &replication.XIDEvent{}
			var committed mysql.BinlogCoordinates = mysql.NewFileBinlogCoordinates("mysql-bin.000001", 220)
			if test.flavor != "" {
				coords, err := mysql.NewGTIDBinlogCoordinates(test.flavor, test.committed)
				require.NoError(t, err)
				reader.currentCoordinates = coords
				committed = coords.Clone()
				if test.xidGTIDSet {
					xid.GSet = coords.GTIDSet
				}
			}

			// Filtering the only row event must not acknowledge an open transaction.
			require.Empty(t, transactionTestStream(t, reader, filteredRowsEvent(t, 200)))
			require.Nil(t, reader.LastTrxCoords)
			entries := transactionTestStream(t, reader, &replication.BinlogEvent{
				Header: &replication.EventHeader{EventType: replication.XID_EVENT, LogPos: 220},
				Event:  xid,
			})
			require.Len(t, entries, 1)
			require.True(t, entries[0].TransactionComplete)
			require.Nil(t, entries[0].DmlEvent)
			require.True(t, committed.Equals(entries[0].Coordinates))
			require.True(t, committed.Equals(reader.LastTrxCoords))

			// Later events (and the syncer's XID set) must not mutate the marker.
			if test.flavor != "" {
				require.NoError(t, reader.currentCoordinates.(*mysql.GTIDBinlogCoordinates).GTIDSet.Update(test.next))
			}
			require.True(t, committed.Equals(entries[0].Coordinates))
			require.True(t, committed.Equals(reader.LastTrxCoords))
		})
	}
}

func TestFilteredTransactionMarkersPreserveDMLOrder(t *testing.T) {
	reader := transactionTestReader(false)
	defer reader.migrationContext.CancelContext()
	rows := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2, LogPos: 100},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{Schema: []byte("testdb"), Table: []byte("testing")},
			Rows:  [][]interface{}{{1}, {2}},
		},
	}
	commit := func(pos uint32) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.XID_EVENT, LogPos: pos},
			Event:  &replication.XIDEvent{},
		}
	}
	// Filtered transactions immediately after a marker are coalesced. A later
	// relevant transaction still gets a marker behind its own DML.
	nextRows := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2, LogPos: 600},
		Event:  rows.Event,
	}
	entries := transactionTestStream(t, reader,
		rows, filteredRowsEvent(t, 200), commit(220),
		filteredRowsEvent(t, 300), commit(320),
		filteredRowsEvent(t, 400), commit(420),
		commit(520), nextRows, commit(620),
	)
	require.Len(t, entries, 6)
	for _, index := range []int{0, 1, 3, 4} {
		require.NotNil(t, entries[index].DmlEvent)
		require.False(t, entries[index].TransactionComplete)
	}
	for index, pos := range map[int]int64{2: 220, 5: 620} {
		entry := entries[index]
		require.True(t, entry.TransactionComplete)
		require.Nil(t, entry.DmlEvent)
		require.Equal(t, pos, entry.Coordinates.(*mysql.FileBinlogCoordinates).LogPos)
	}
	require.Equal(t, int64(620), reader.LastTrxCoords.(*mysql.FileBinlogCoordinates).LogPos)

	// Under sustained unrelated traffic, another ordered marker eventually
	// advances the applied watermark without producing one for every XID.
	reader.lastTransactionMarkerTime = time.Now().Add(-filteredTransactionMarkerInterval)
	entries = transactionTestStream(t, reader, filteredRowsEvent(t, 700), commit(720))
	require.Len(t, entries, 1)
	require.True(t, entries[0].TransactionComplete)
	require.Equal(t, int64(720), entries[0].Coordinates.(*mysql.FileBinlogCoordinates).LogPos)
}
