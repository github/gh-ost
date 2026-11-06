package binlog

import (
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/metrics"
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

type gtidMetricsSpy struct {
	metrics.Emitter
	histograms map[string][]float64
	gauges     map[string][]float64
}

func (s *gtidMetricsSpy) Histogram(name string, value float64, tags ...string) {
	s.histograms[name] = append(s.histograms[name], value)
}

func (s *gtidMetricsSpy) Gauge(name string, value float64, tags ...string) {
	s.gauges[name] = append(s.gauges[name], value)
}

func TestStreamEventsGTIDMetrics(t *testing.T) {
	for _, tt := range []struct {
		name                  string
		event                 replication.Event
		wantGTID              string
		wantTransactionLength []float64
		wantCommitGroupSize   []float64
		wantLastCommitted     int64
	}{
		{
			name: "MySQL",
			event: &replication.GTIDEvent{
				SID:               make([]byte, 16),
				GNO:               1,
				TransactionLength: 128,
				LastCommitted:     5,
				SequenceNumber:    8,
			},
			wantGTID:              "00000000-0000-0000-0000-000000000000:1",
			wantTransactionLength: []float64{128, 128},
			wantCommitGroupSize:   []float64{3},
			wantLastCommitted:     5,
		},
		{
			name: "MariaDB",
			event: &replication.MariadbGTIDEvent{
				GTID: gomysql.MariadbGTID{DomainID: 0, ServerID: 1, SequenceNumber: 1},
			},
			wantGTID:          "0-1-1",
			wantLastCommitted: 4,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			emit := &gtidMetricsSpy{
				Emitter:    metrics.Noop,
				histograms: make(map[string][]float64),
				gauges:     make(map[string][]float64),
			}
			migrationContext := base.NewMigrationContext()
			migrationContext.UseGTIDs = true
			migrationContext.Metrics = emit
			streamer := replication.NewBinlogStreamer()
			reader := &GoMySQLReader{
				migrationContext:         migrationContext,
				binlogStreamer:           streamer,
				currentCoordinates:       &mysql.GTIDBinlogCoordinates{},
				currentCoordinatesMutex:  &sync.Mutex{},
				transactionRowEventTotal: 2,
				transactionRowTotal:      3,
				previousLastCommitted:    4,
			}
			// Repeat the event to check that flushed counters and the same
			// MySQL commit group are not emitted again.
			for i := 0; i < 2; i++ {
				require.NoError(t, streamer.AddEventToStreamer(&replication.BinlogEvent{
					Event: tt.event,
				}))
			}
			iterations := 0
			err := reader.StreamEvents(func() bool {
				iterations++
				return iterations > 2
			}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.wantGTID, reader.GetCurrentBinlogCoordinates().(*mysql.GTIDBinlogCoordinates).GTIDSet.String())
			require.Equal(t, []float64{2}, emit.histograms["transaction_num_row_events"])
			require.Equal(t, []float64{3}, emit.histograms["transaction_num_rows"])
			require.Equal(t, tt.wantTransactionLength, emit.histograms["gtid_event_transaction_length_bytes"])
			require.Equal(t, tt.wantCommitGroupSize, emit.gauges["unfiltered_commit_group_size"])
			require.Zero(t, reader.transactionRowEventTotal)
			require.Zero(t, reader.transactionRowTotal)
			require.Equal(t, tt.wantLastCommitted, reader.previousLastCommitted)
		})
	}
}
