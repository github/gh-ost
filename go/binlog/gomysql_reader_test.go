package binlog

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func setUnexportedField(target interface{}, fieldName string, value interface{}) {
	field := reflect.ValueOf(target).Elem().FieldByName(fieldName)
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}

func newTestRowsEvent(schemaName, tableName string) *replication.RowsEvent {
	const tableID uint64 = 7

	rowsEvent := &replication.RowsEvent{
		Version: 1,
	}
	setUnexportedField(rowsEvent, "tableIDSize", 6)
	setUnexportedField(rowsEvent, "needBitmap2", false)
	setUnexportedField(rowsEvent, "eventType", replication.WRITE_ROWS_EVENTv1)
	setUnexportedField(rowsEvent, "tables", map[uint64]*replication.TableMapEvent{
		tableID: {
			TableID:     tableID,
			Schema:      []byte(schemaName),
			Table:       []byte(tableName),
			ColumnCount: 1,
			ColumnType:  []byte{gomysql.MYSQL_TYPE_LONG},
			ColumnMeta:  []uint16{0},
			NullBitmap:  []byte{0x00},
		},
	})
	return rowsEvent
}

func newRowsEventDataWithInvalidRowPayload() []byte {
	// RowsEvent v1 header:
	// - 6 byte little-endian table id
	// - 2 byte flags
	// - length-encoded column count = 1
	// - 1 byte column bitmap
	// The final byte is an intentionally truncated row payload. DecodeHeader can
	// parse the table/schema/name, but DecodeData will fail if it is invoked.
	return []byte{7, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0}
}

func TestRowsEventDecodeFuncSkipsRowDataForFilteredTable(t *testing.T) {
	decodeFunc := newRowsEventDecodeFunc(func(databaseName, tableName string) bool {
		require.Equal(t, "testdb", databaseName)
		require.Equal(t, "ignored_table", tableName)
		return false
	})
	require.NotNil(t, decodeFunc)

	rowsEvent := newTestRowsEvent("testdb", "ignored_table")
	err := decodeFunc(rowsEvent, newRowsEventDataWithInvalidRowPayload())
	require.NoError(t, err)
	require.Equal(t, uint64(7), rowsEvent.TableID)
	require.Equal(t, "testdb", string(rowsEvent.Table.Schema))
	require.Equal(t, "ignored_table", string(rowsEvent.Table.Table))
	require.Empty(t, rowsEvent.Rows)
}

func TestRowsEventDecodeFuncDecodesRowDataForMatchingTable(t *testing.T) {
	decodeFunc := newRowsEventDecodeFunc(func(databaseName, tableName string) bool {
		require.Equal(t, "testdb", databaseName)
		require.Equal(t, "wanted_table", tableName)
		return true
	})
	require.NotNil(t, decodeFunc)

	rowsEvent := newTestRowsEvent("testdb", "wanted_table")
	err := decodeFunc(rowsEvent, newRowsEventDataWithInvalidRowPayload())
	require.Error(t, err)
}

func TestRowsEventDecodeFuncIsNilWithoutFilter(t *testing.T) {
	require.Nil(t, newRowsEventDecodeFunc(nil))
}

func TestWriterLagProgressRequiresSourcePosition(t *testing.T) {
	for _, flavor := range []string{"file", mysql.MySQLFlavor, mysql.MariaDBFlavor} {
		t.Run(flavor, func(t *testing.T) {
			var processed, source mysql.BinlogCoordinates
			switch flavor {
			case "file":
				processed = &mysql.FileBinlogCoordinates{LogFile: "mysql-bin.000001", LogPos: 100}
				source = &mysql.FileBinlogCoordinates{LogFile: "mysql-bin.000001", LogPos: 200}
			case mysql.MySQLFlavor:
				processed, _ = mysql.NewGTIDBinlogCoordinates(flavor, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1")
				source, _ = mysql.NewGTIDBinlogCoordinates(flavor, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-2")
			case mysql.MariaDBFlavor:
				processed, _ = mysql.NewGTIDBinlogCoordinates(flavor, "0-1-1")
				source, _ = mysql.NewGTIDBinlogCoordinates(flavor, "0-1-2")
			}
			mc := base.NewMigrationContext()
			defer mc.CancelContext()
			reader := &GoMySQLReader{
				migrationContext: mc,
				ReadSourceCoordinates: func() (mysql.BinlogCoordinates, error) {
					return source, nil
				},
			}
			entries := make(chan *BinlogEntry, 1)
			require.NoError(t, reader.sendStreamProgress(processed, entries))
			require.Empty(t, entries, "streamer silence must not conceal unread source events")

			source = processed.Clone()
			var queryStarted time.Time
			reader.ReadSourceCoordinates = func() (mysql.BinlogCoordinates, error) {
				queryStarted = time.Now()
				return source, nil
			}
			require.NoError(t, reader.sendStreamProgress(processed, entries))
			progress := <-entries
			require.Nil(t, progress.DmlEvent)
			require.False(t, progress.Timestamp.After(queryStarted), "query/delivery delay must count as lag")
		})
	}
}

func TestWriterLagProgressErrors(t *testing.T) {
	mc := base.NewMigrationContext()
	defer mc.CancelContext()
	coords := &mysql.FileBinlogCoordinates{LogFile: "mysql-bin.000001", LogPos: 100}
	boom := errors.New("source probe failed")
	reader := &GoMySQLReader{
		migrationContext: mc,
		ReadSourceCoordinates: func() (mysql.BinlogCoordinates, error) {
			return nil, boom
		},
	}
	entries := make(chan *BinlogEntry)
	require.ErrorIs(t, reader.sendStreamProgress(coords, entries), boom)

	reader.ReadSourceCoordinates = func() (mysql.BinlogCoordinates, error) { return coords, nil }
	mc.CancelContext()
	require.ErrorIs(t, reader.sendStreamProgress(coords, entries), context.Canceled)
}

func TestWriterLagProgressOnIdleStream(t *testing.T) {
	mc := base.NewMigrationContext()
	defer mc.CancelContext()
	mc.HeartbeatIntervalMilliseconds = 1
	coords := &mysql.FileBinlogCoordinates{LogFile: "mysql-bin.000001", LogPos: 100}
	reader := &GoMySQLReader{
		migrationContext:        mc,
		currentCoordinates:      coords.Clone(),
		currentCoordinatesMutex: &sync.Mutex{},
		binlogStreamer:          replication.NewBinlogStreamer(),
		ReadSourceCoordinates: func() (mysql.BinlogCoordinates, error) {
			return coords.Clone(), nil
		},
	}
	// Startup's synthetic events must not move the processed position backwards.
	require.NoError(t, reader.binlogStreamer.AddEventToStreamer(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.ROTATE_EVENT},
		Event:  &replication.RotateEvent{NextLogName: []byte(coords.LogFile), Position: uint64(coords.LogPos)},
	}))
	require.NoError(t, reader.binlogStreamer.AddEventToStreamer(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.FORMAT_DESCRIPTION_EVENT},
		Event:  &replication.FormatDescriptionEvent{},
	}))
	entries := make(chan *BinlogEntry)
	done := make(chan error, 1)
	go func() { done <- reader.StreamEvents(func() bool { return false }, entries) }()
	select {
	case progress := <-entries:
		require.Nil(t, progress.DmlEvent)
		require.True(t, coords.Equals(progress.Coordinates))
	case <-time.After(5 * time.Second):
		t.Fatal("idle source did not produce a catch-up marker")
	}
	mc.CancelContext()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestWriterLagProgressWaitsForCompleteGTIDTransaction(t *testing.T) {
	mc := base.NewMigrationContext()
	defer mc.CancelContext()
	mc.UseGTIDs = true
	mc.HeartbeatIntervalMilliseconds = 1
	initial, err := mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1")
	require.NoError(t, err)
	source, err := mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-2")
	require.NoError(t, err)
	probed := make(chan struct{}, 1)
	reader := &GoMySQLReader{
		migrationContext:        mc,
		currentCoordinates:      initial,
		currentCoordinatesMutex: &sync.Mutex{},
		binlogStreamer:          replication.NewBinlogStreamer(),
		ReadSourceCoordinates: func() (mysql.BinlogCoordinates, error) {
			select {
			case probed <- struct{}{}:
			default:
			}
			return source, nil
		},
	}
	sid := make([]byte, 16)
	for i := range sid {
		sid[i] = 0xaa
	}
	require.NoError(t, reader.binlogStreamer.AddEventToStreamer(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
		Event:  &replication.GTIDEvent{SID: sid, GNO: 2},
	}))
	require.NoError(t, reader.binlogStreamer.AddEventToStreamer(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.QUERY_EVENT},
		Event:  &replication.QueryEvent{Query: []byte("BEGIN"), GSet: source.GTIDSet},
	}))
	entries := make(chan *BinlogEntry, 10)
	done := make(chan error, 1)
	go func() { done <- reader.StreamEvents(func() bool { return false }, entries) }()
	for i := 0; i < 3; i++ {
		select {
		case <-probed:
		case <-time.After(5 * time.Second):
			t.Fatal("source position was not probed")
		}
	}
	require.Empty(t, entries, "reading GTID_NEXT is not the same as processing its rows")

	require.NoError(t, reader.binlogStreamer.AddEventToStreamer(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XID_EVENT},
		Event:  &replication.XIDEvent{GSet: source.GTIDSet},
	}))
	select {
	case progress := <-entries:
		require.True(t, source.Equals(progress.Coordinates))
	case <-time.After(5 * time.Second):
		t.Fatal("completed transaction did not produce a catch-up marker")
	}
	mc.CancelContext()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestWriterLagProgressAfterStandaloneDDL(t *testing.T) {
	mc := base.NewMigrationContext()
	defer mc.CancelContext()
	mc.UseGTIDs = true
	mc.HeartbeatIntervalMilliseconds = 1
	initial, err := mysql.NewGTIDBinlogCoordinates(mysql.MariaDBFlavor, "0-1-1")
	require.NoError(t, err)
	source, err := mysql.NewGTIDBinlogCoordinates(mysql.MariaDBFlavor, "0-1-2")
	require.NoError(t, err)
	reader := &GoMySQLReader{
		migrationContext:        mc,
		currentCoordinates:      initial,
		currentCoordinatesMutex: &sync.Mutex{},
		binlogStreamer:          replication.NewBinlogStreamer(),
		ReadSourceCoordinates: func() (mysql.BinlogCoordinates, error) {
			return source, nil
		},
	}
	require.NoError(t, reader.binlogStreamer.AddEventToStreamer(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.QUERY_EVENT},
		Event:  &replication.QueryEvent{Query: []byte("CREATE TABLE unrelated (id INT)"), GSet: source.GTIDSet},
	}))
	entries := make(chan *BinlogEntry, 1)
	done := make(chan error, 1)
	go func() { done <- reader.StreamEvents(func() bool { return false }, entries) }()
	select {
	case progress := <-entries:
		require.True(t, source.Equals(progress.Coordinates))
	case <-time.After(5 * time.Second):
		t.Fatal("standalone DDL must not prevent idle catch-up detection")
	}
	mc.CancelContext()
	require.ErrorIs(t, <-done, context.Canceled)
}
