package binlog

import (
	"reflect"
	"testing"
	"unsafe"

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
